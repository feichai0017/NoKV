use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};

use crate::store::ObjectError;

#[derive(Clone, Debug, Default)]
pub struct ObjectReadCoordinator {
    inner: Arc<ObjectReadCoordinatorInner>,
}

#[derive(Debug, Default)]
struct ObjectReadCoordinatorInner {
    reads: Mutex<BTreeMap<ObjectReadCoordinatorKey, Arc<InflightObjectRead>>>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ObjectReadCoordinatorKey {
    object_key: String,
    digest_uri: String,
    object_offset: u64,
    len: usize,
}

#[derive(Debug)]
struct InflightObjectRead {
    state: Mutex<Option<InflightObjectReadResult>>,
    ready: Condvar,
}

type InflightObjectReadResult = Result<(u64, Vec<u8>), ObjectError>;

#[derive(Clone, Debug)]
pub(super) struct CoordinatedObjectRead {
    pub object_offset: u64,
    pub bytes: Vec<u8>,
    pub fetched: bool,
}

impl ObjectReadCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    pub(super) fn read_or_wait(
        &self,
        key: ObjectReadCoordinatorKey,
        fetch: impl FnOnce() -> Result<(u64, Vec<u8>), ObjectError>,
    ) -> Result<CoordinatedObjectRead, ObjectError> {
        let (inflight, owner) = {
            let mut reads = self
                .inner
                .reads
                .lock()
                .map_err(ObjectError::from_poisoned_lock)?;
            if let Some(inflight) = reads.get(&key) {
                (Arc::clone(inflight), false)
            } else {
                let inflight = Arc::new(InflightObjectRead {
                    state: Mutex::new(None),
                    ready: Condvar::new(),
                });
                reads.insert(key.clone(), Arc::clone(&inflight));
                (inflight, true)
            }
        };

        if !owner {
            let (object_offset, bytes) = inflight.wait()?;
            return Ok(CoordinatedObjectRead {
                object_offset,
                bytes,
                fetched: false,
            });
        }

        let result = fetch();
        inflight.complete(result.clone())?;
        let mut reads = self
            .inner
            .reads
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?;
        reads.remove(&key);
        let (object_offset, bytes) = result?;
        Ok(CoordinatedObjectRead {
            object_offset,
            bytes,
            fetched: true,
        })
    }
}

impl ObjectReadCoordinatorKey {
    pub(super) fn new(
        object_key: String,
        digest_uri: String,
        object_offset: u64,
        len: usize,
    ) -> Self {
        Self {
            object_key,
            digest_uri,
            object_offset,
            len,
        }
    }
}

impl InflightObjectRead {
    fn wait(&self) -> Result<(u64, Vec<u8>), ObjectError> {
        let mut state = self.state.lock().map_err(ObjectError::from_poisoned_lock)?;
        loop {
            if let Some(result) = state.as_ref() {
                return result.clone();
            }
            state = self
                .ready
                .wait(state)
                .map_err(ObjectError::from_poisoned_lock)?;
        }
    }

    fn complete(&self, result: Result<(u64, Vec<u8>), ObjectError>) -> Result<(), ObjectError> {
        let mut state = self.state.lock().map_err(ObjectError::from_poisoned_lock)?;
        *state = Some(result);
        self.ready.notify_all();
        Ok(())
    }
}
