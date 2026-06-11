use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::sync::mpsc::{self, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::cache::{BlockCache, ObjectBlockCache};
use crate::chunk::{
    BlockReadOptions, ChunkStore, ObjectReadBlock, ObjectReadCoordinator, ReadCacheFillMode,
    DEFAULT_BLOCK_SIZE,
};
use crate::store::ObjectError;

#[derive(Clone)]
pub struct ObjectPrefetcher<O, C = ObjectBlockCache> {
    sender: mpsc::SyncSender<QueuedPrefetchRequest>,
    pending: Arc<Mutex<BTreeSet<ObjectPrefetchKey>>>,
    stats: Arc<Mutex<ObjectPrefetchStats>>,
    read_coordinator: ObjectReadCoordinator,
    _state: PhantomData<(O, C)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectPrefetchOptions {
    pub queue_capacity: usize,
    pub workers: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectPrefetchRequest {
    pub output_len: usize,
    pub blocks: Vec<ObjectReadBlock>,
    pub cache_fill: ReadCacheFillMode,
}

#[derive(Clone, Debug)]
struct QueuedPrefetchRequest {
    key: ObjectPrefetchKey,
    request: ObjectPrefetchRequest,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ObjectPrefetchKey {
    cache_fill: ReadCacheFillMode,
    blocks: Vec<ObjectPrefetchBlockKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ObjectPrefetchBlockKey {
    object_key: String,
    digest_uri: String,
    object_offset: u64,
    len: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ObjectPrefetchStats {
    pub enqueued: u64,
    pub dropped: u64,
    pub completed: u64,
    pub failed: u64,
    pub object_gets: u64,
    pub object_get_bytes: u64,
    pub cache_hits: u64,
    pub cache_hit_bytes: u64,
}

impl Default for ObjectPrefetchOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 64,
            workers: 1,
        }
    }
}

impl ObjectPrefetchRequest {
    pub fn new(output_len: usize, blocks: Vec<ObjectReadBlock>) -> Self {
        Self {
            output_len,
            blocks,
            cache_fill: ReadCacheFillMode::ForwardAligned {
                block_size: DEFAULT_BLOCK_SIZE,
            },
        }
    }

    pub fn exact(output_len: usize, blocks: Vec<ObjectReadBlock>) -> Self {
        Self {
            output_len,
            blocks,
            cache_fill: ReadCacheFillMode::Exact,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.output_len == 0 || self.blocks.is_empty()
    }
}

impl ObjectPrefetchKey {
    fn new(request: &ObjectPrefetchRequest) -> Self {
        let mut blocks = request
            .blocks
            .iter()
            .map(|block| ObjectPrefetchBlockKey {
                object_key: block.object_key.clone(),
                digest_uri: block.digest_uri.clone(),
                object_offset: block.object_offset,
                len: block.len,
            })
            .collect::<Vec<_>>();
        blocks.sort();
        Self {
            cache_fill: request.cache_fill,
            blocks,
        }
    }
}

impl<O, C> ObjectPrefetcher<O, C>
where
    O: ChunkStore + Clone + Send + 'static,
    C: BlockCache + Clone + Send + 'static,
{
    pub fn new(store: O, cache: C, options: ObjectPrefetchOptions) -> Self {
        let capacity = options.queue_capacity.max(1);
        let workers = options.workers.max(1);
        let (sender, receiver) = mpsc::sync_channel::<QueuedPrefetchRequest>(capacity);
        let receiver = Arc::new(Mutex::new(receiver));
        let pending = Arc::new(Mutex::new(BTreeSet::<ObjectPrefetchKey>::new()));
        let stats = Arc::new(Mutex::new(ObjectPrefetchStats::default()));
        let read_coordinator = ObjectReadCoordinator::new();
        for worker in 0..workers {
            let store = store.clone();
            let cache = cache.clone();
            let receiver = Arc::clone(&receiver);
            let pending = Arc::clone(&pending);
            let stats = Arc::clone(&stats);
            let read_coordinator = read_coordinator.clone();
            let name = format!("nokv-prefetch-{worker}");
            let _ = thread::Builder::new().name(name).spawn(move || loop {
                let queued = {
                    let Ok(receiver) = receiver.lock() else {
                        break;
                    };
                    match receiver.recv() {
                        Ok(request) => request,
                        Err(_) => break,
                    }
                };
                let request = queued.request;
                let read_options = BlockReadOptions::default()
                    .with_cache_fill(request.cache_fill)
                    .with_read_coordinator(read_coordinator.clone());
                let result = store.read_blocks_with_options(
                    Some(&cache),
                    request.output_len,
                    &request.blocks,
                    read_options,
                );
                if let Ok(mut pending) = pending.lock() {
                    pending.remove(&queued.key);
                }
                match result {
                    Ok(outcome) => {
                        if let Ok(mut stats) = stats.lock() {
                            stats.completed = stats.completed.saturating_add(1);
                            stats.object_gets =
                                stats.object_gets.saturating_add(outcome.object_gets as u64);
                            stats.object_get_bytes = stats
                                .object_get_bytes
                                .saturating_add(outcome.object_get_bytes);
                            stats.cache_hits =
                                stats.cache_hits.saturating_add(outcome.cache_hits as u64);
                            stats.cache_hit_bytes = stats
                                .cache_hit_bytes
                                .saturating_add(outcome.cache_hit_bytes);
                        }
                    }
                    Err(_) => {
                        if let Ok(mut stats) = stats.lock() {
                            stats.failed = stats.failed.saturating_add(1);
                        }
                    }
                }
            });
        }
        Self {
            sender,
            pending,
            stats,
            read_coordinator,
            _state: PhantomData,
        }
    }

    pub fn submit(&self, request: ObjectPrefetchRequest) -> Result<bool, ObjectError> {
        if request.is_empty() {
            return Ok(false);
        }
        let key = ObjectPrefetchKey::new(&request);
        {
            let mut pending = self
                .pending
                .lock()
                .map_err(ObjectError::from_poisoned_lock)?;
            if !pending.insert(key.clone()) {
                self.with_stats(|stats| {
                    stats.dropped = stats.dropped.saturating_add(1);
                })?;
                return Ok(false);
            }
        }
        let queued = QueuedPrefetchRequest {
            key: key.clone(),
            request,
        };
        match self.sender.try_send(queued) {
            Ok(()) => {
                self.with_stats(|stats| {
                    stats.enqueued = stats.enqueued.saturating_add(1);
                })?;
                Ok(true)
            }
            Err(TrySendError::Full(_)) => {
                self.remove_pending(&key)?;
                self.with_stats(|stats| {
                    stats.dropped = stats.dropped.saturating_add(1);
                })?;
                Ok(false)
            }
            Err(TrySendError::Disconnected(_)) => {
                self.remove_pending(&key)?;
                Err(ObjectError::Backend(
                    "object prefetch worker stopped".to_owned(),
                ))
            }
        }
    }

    pub fn stats(&self) -> Result<ObjectPrefetchStats, ObjectError> {
        self.stats
            .lock()
            .map_err(ObjectError::from_poisoned_lock)
            .map(|stats| *stats)
    }

    pub fn read_coordinator(&self) -> ObjectReadCoordinator {
        self.read_coordinator.clone()
    }

    fn with_stats(&self, update: impl FnOnce(&mut ObjectPrefetchStats)) -> Result<(), ObjectError> {
        let mut stats = self.stats.lock().map_err(ObjectError::from_poisoned_lock)?;
        update(&mut stats);
        Ok(())
    }

    fn remove_pending(&self, key: &ObjectPrefetchKey) -> Result<(), ObjectError> {
        let mut pending = self
            .pending
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?;
        pending.remove(key);
        Ok(())
    }
}
