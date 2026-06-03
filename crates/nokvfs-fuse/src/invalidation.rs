use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use fuser::{INodeNo, Notifier};
use nokvfs_meta::command::MetadataStore;
use nokvfs_meta::NoKvFs;
use nokvfs_object::ObjectStore;
use nokvfs_types::{DentryName, InodeId, WatchCursor, WatchEvent, WatchRecord};

const DEFAULT_INTERVAL: Duration = Duration::from_millis(250);
const DEFAULT_LIMIT: usize = 1024;
const FULL_INODE_LEN: i64 = i64::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FuseInvalidationOptions {
    pub interval: Duration,
    pub limit: usize,
    pub run_immediately: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FuseInvalidationWorkerState {
    pub iterations: u64,
    pub events: u64,
    pub entry_invalidations: u64,
    pub inode_invalidations: u64,
    pub last_error: Option<String>,
}

pub(crate) struct FuseInvalidationWorker {
    stop: Arc<(Mutex<bool>, Condvar)>,
    _state: Arc<Mutex<FuseInvalidationWorkerState>>,
    handle: Option<JoinHandle<()>>,
}

#[derive(Default)]
pub(crate) struct InvalidationRegistry {
    cursors: RwLock<HashMap<u64, WatchCursor>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InvalidationTarget {
    pub parent: Option<InodeId>,
    pub name: Option<DentryName>,
    pub inode: InodeId,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct InvalidationCounts {
    events: u64,
    entry_invalidations: u64,
    inode_invalidations: u64,
}

impl Default for FuseInvalidationOptions {
    fn default() -> Self {
        Self {
            interval: DEFAULT_INTERVAL,
            limit: DEFAULT_LIMIT,
            run_immediately: true,
        }
    }
}

impl FuseInvalidationWorker {
    pub(crate) fn spawn<M, O>(
        service: Arc<NoKvFs<M, O>>,
        notifier: Notifier,
        registry: Arc<InvalidationRegistry>,
        options: FuseInvalidationOptions,
    ) -> Self
    where
        M: MetadataStore + Send + Sync + 'static,
        O: ObjectStore + Send + Sync + 'static,
    {
        let options = options.normalized();
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let state = Arc::new(Mutex::new(FuseInvalidationWorkerState::default()));
        let worker_stop = Arc::clone(&stop);
        let worker_state = Arc::clone(&state);
        let handle = thread::spawn(move || {
            if options.run_immediately {
                run_once(&service, &notifier, &registry, options.limit, &worker_state);
            }
            loop {
                let (lock, cvar) = &*worker_stop;
                let stopped = match lock.lock() {
                    Ok(stopped) => stopped,
                    Err(_) => break,
                };
                let (stopped, _) = match cvar.wait_timeout(stopped, options.interval) {
                    Ok(waited) => waited,
                    Err(_) => break,
                };
                if *stopped {
                    break;
                }
                drop(stopped);
                run_once(&service, &notifier, &registry, options.limit, &worker_state);
            }
        });
        Self {
            stop,
            _state: state,
            handle: Some(handle),
        }
    }

    fn stop(&mut self) {
        let (lock, cvar) = &*self.stop;
        if let Ok(mut stopped) = lock.lock() {
            *stopped = true;
            cvar.notify_all();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for FuseInvalidationWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

impl InvalidationRegistry {
    pub(crate) fn register_scope(&self, scope: InodeId, cursor: WatchCursor) {
        if let Ok(mut cursors) = self.cursors.write() {
            cursors.entry(scope.get()).or_insert(cursor);
        }
    }

    fn scopes(&self) -> Vec<(InodeId, WatchCursor)> {
        self.cursors
            .read()
            .map(|cursors| {
                cursors
                    .iter()
                    .filter_map(|(scope, cursor)| {
                        InodeId::new(*scope).ok().map(|scope| (scope, *cursor))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn advance(&self, scope: InodeId, cursor: WatchCursor) {
        if let Ok(mut cursors) = self.cursors.write() {
            cursors.insert(scope.get(), cursor);
        }
    }
}

impl FuseInvalidationOptions {
    fn normalized(self) -> Self {
        Self {
            interval: if self.interval.is_zero() {
                DEFAULT_INTERVAL
            } else {
                self.interval
            },
            ..self
        }
    }
}

fn run_once<M, O>(
    service: &NoKvFs<M, O>,
    notifier: &Notifier,
    registry: &InvalidationRegistry,
    limit: usize,
    state: &Mutex<FuseInvalidationWorkerState>,
) where
    M: MetadataStore,
    O: ObjectStore,
{
    let result = replay_registered_scopes(service, notifier, registry, limit);
    if let Ok(mut state) = state.lock() {
        state.iterations += 1;
        match result {
            Ok(counts) => {
                state.events += counts.events;
                state.entry_invalidations += counts.entry_invalidations;
                state.inode_invalidations += counts.inode_invalidations;
                state.last_error = None;
            }
            Err(err) => {
                state.last_error = Some(err.to_string());
            }
        }
    }
}

fn replay_registered_scopes<M, O>(
    service: &NoKvFs<M, O>,
    notifier: &Notifier,
    registry: &InvalidationRegistry,
    limit: usize,
) -> io::Result<InvalidationCounts>
where
    M: MetadataStore,
    O: ObjectStore,
{
    let mut totals = InvalidationCounts::default();
    for (scope, cursor) in registry.scopes() {
        let records = service
            .replay_watch(scope, cursor, limit)
            .map_err(|err| io::Error::other(err.to_string()))?;
        let Some(last) = records.last().map(|record| record.cursor) else {
            continue;
        };
        for record in &records {
            let counts = invalidate_record(notifier, record)?;
            totals.events += counts.events;
            totals.entry_invalidations += counts.entry_invalidations;
            totals.inode_invalidations += counts.inode_invalidations;
        }
        registry.advance(scope, last);
    }
    Ok(totals)
}

fn invalidate_record(notifier: &Notifier, record: &WatchRecord) -> io::Result<InvalidationCounts> {
    let target = invalidation_target(&record.event);
    let mut counts = InvalidationCounts {
        events: 1,
        ..InvalidationCounts::default()
    };
    if let (Some(parent), Some(name)) = (target.parent, target.name.as_ref()) {
        notifier.inval_entry(INodeNo(parent.get()), OsStr::from_bytes(name.as_bytes()))?;
        counts.entry_invalidations += 1;
    }
    notifier.inval_inode(INodeNo(target.inode.get()), 0, FULL_INODE_LEN)?;
    counts.inode_invalidations += 1;
    Ok(counts)
}

pub(crate) fn invalidation_target(event: &WatchEvent) -> InvalidationTarget {
    InvalidationTarget {
        parent: event.parent,
        name: event.name.clone(),
        inode: event.inode,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokvfs_types::{WatchEventKind, WatchRecord};

    fn inode(raw: u64) -> InodeId {
        InodeId::new(raw).unwrap()
    }

    fn name(raw: &[u8]) -> DentryName {
        DentryName::new(raw.to_vec()).unwrap()
    }

    #[test]
    fn registry_registers_scope_once_and_advances_cursor() {
        let registry = InvalidationRegistry::default();
        registry.register_scope(
            inode(2),
            WatchCursor {
                version: 10,
                event_id: u64::MAX,
            },
        );
        registry.register_scope(
            inode(2),
            WatchCursor {
                version: 11,
                event_id: u64::MAX,
            },
        );
        assert_eq!(
            registry.scopes(),
            vec![(
                inode(2),
                WatchCursor {
                    version: 10,
                    event_id: u64::MAX,
                },
            )]
        );
        registry.advance(
            inode(2),
            WatchCursor {
                version: 12,
                event_id: 0,
            },
        );
        assert_eq!(
            registry.scopes(),
            vec![(
                inode(2),
                WatchCursor {
                    version: 12,
                    event_id: 0,
                },
            )]
        );
    }

    #[test]
    fn invalidation_target_uses_typed_watch_fields() {
        let event = WatchEvent {
            kind: WatchEventKind::PublishArtifact,
            parent: Some(inode(1)),
            name: Some(name(b"checkpoint.bin")),
            inode: inode(42),
            version: 7,
        };
        assert_eq!(
            invalidation_target(&event),
            InvalidationTarget {
                parent: Some(inode(1)),
                name: Some(name(b"checkpoint.bin")),
                inode: inode(42),
            }
        );
        let record = WatchRecord {
            cursor: WatchCursor {
                version: 7,
                event_id: 0,
            },
            event,
        };
        assert_eq!(invalidation_target(&record.event).inode, inode(42));
    }

    #[test]
    fn zero_interval_normalizes_to_default_interval() {
        let options = FuseInvalidationOptions {
            interval: Duration::ZERO,
            limit: 7,
            run_immediately: false,
        }
        .normalized();
        assert_eq!(options.interval, DEFAULT_INTERVAL);
        assert_eq!(options.limit, 7);
        assert!(!options.run_immediately);
    }
}
