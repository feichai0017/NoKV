use std::io;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use openraft::storage::LogFlushed;
use openraft::{AnyError, ErrorSubject, ErrorVerb, StorageError};

use crate::{metrics, NodeId, OpenRaftEntry, RaftEntryLog, RaftStoreConfig, SegmentedEntryLog};

const DEFAULT_GROUP_COMMIT_DELAY: Duration = Duration::from_millis(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionLogSyncPolicy {
    /// Complete OpenRaft log callbacks after appending to the local log file
    /// without issuing fsync. This is the default throughput mode for the
    /// metadata data plane and does not claim power-loss durability.
    Buffered,
    /// Wait for a short group window, fsync the raft log once, then complete
    /// all callbacks covered by that flush.
    GroupCommit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionLogFlushOptions {
    pub sync_policy: RegionLogSyncPolicy,
    pub group_commit_delay: Duration,
}

impl RegionLogFlushOptions {
    pub fn buffered() -> Self {
        Self {
            sync_policy: RegionLogSyncPolicy::Buffered,
            group_commit_delay: DEFAULT_GROUP_COMMIT_DELAY,
        }
    }

    pub fn group_commit(delay: Duration) -> Self {
        Self {
            sync_policy: RegionLogSyncPolicy::GroupCommit,
            group_commit_delay: delay,
        }
    }
}

impl Default for RegionLogFlushOptions {
    fn default() -> Self {
        Self::buffered()
    }
}

#[derive(Clone)]
pub(crate) struct RegionLogFlusher {
    log: Arc<Mutex<SegmentedEntryLog>>,
    options: RegionLogFlushOptions,
    pending: Arc<Mutex<PendingFlush>>,
}

impl RegionLogFlusher {
    pub(crate) fn new(log: Arc<Mutex<SegmentedEntryLog>>, options: RegionLogFlushOptions) -> Self {
        Self {
            log,
            options,
            pending: Arc::new(Mutex::new(PendingFlush::default())),
        }
    }

    pub(crate) fn append_entries(
        &self,
        entries: &[OpenRaftEntry],
        callback: LogFlushed<RaftStoreConfig>,
    ) -> Result<(), StorageError<NodeId>> {
        let entry_count = entries.len() as u64;
        let mut pending = self.lock_pending(ErrorVerb::Write)?;
        let append_started = Instant::now();
        let result = self.lock_log(ErrorVerb::Write).and_then(|mut log| {
            log.append_entries(entries)
                .map_err(|err| storage_error(ErrorVerb::Write, err.to_string()))
        });
        match result {
            Ok(()) => metrics::record_log_append(entry_count, append_started.elapsed()),
            Err(err) => {
                let message = err.to_string();
                callback.log_io_completed(Err(io::Error::other(message.clone())));
                return Err(storage_error(ErrorVerb::Write, message));
            }
        }
        match self.options.sync_policy {
            RegionLogSyncPolicy::Buffered => {
                metrics::record_log_flush_skipped(entry_count);
                drop(pending);
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            RegionLogSyncPolicy::GroupCommit => {
                let should_spawn = Self::enqueue_locked(&mut pending, entry_count, callback);
                drop(pending);
                if should_spawn {
                    self.spawn_delayed_flush();
                }
                Ok(())
            }
        }
    }

    pub(crate) async fn flush_now(&self, verb: ErrorVerb) -> Result<(), StorageError<NodeId>> {
        self.flush_pending(verb)
    }

    fn enqueue_locked(
        pending: &mut PendingFlush,
        entry_count: u64,
        callback: LogFlushed<RaftStoreConfig>,
    ) -> bool {
        pending.callbacks.push(PendingCallback {
            callback,
            entry_count,
        });
        if pending.scheduled {
            false
        } else {
            pending.scheduled = true;
            true
        }
    }

    fn spawn_delayed_flush(&self) {
        let flusher = self.clone();
        let delay = self.options.group_commit_delay;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if let Err(err) = flusher.flush_now(ErrorVerb::Write).await {
                tracing::error!(error = %err, "raft log group commit flush failed");
            }
        });
    }

    fn flush_pending(&self, verb: ErrorVerb) -> Result<(), StorageError<NodeId>> {
        let mut pending = self.lock_pending(verb)?;
        if pending.callbacks.is_empty() {
            pending.scheduled = false;
            return Ok(());
        }
        pending.scheduled = false;
        let callbacks = std::mem::take(&mut pending.callbacks);
        let callback_count = callbacks.len() as u64;
        let entry_count = callbacks
            .iter()
            .map(|callback| callback.entry_count)
            .sum::<u64>();
        let started = Instant::now();
        let result = self.lock_log(verb).and_then(|log| {
            log.sync()
                .map_err(|err| storage_error(verb, err.to_string()))
        });
        let sync_duration = started.elapsed();
        drop(pending);
        match result {
            Ok(()) => {
                metrics::record_log_group_flush(callback_count, entry_count, sync_duration);
                for pending in callbacks {
                    pending.callback.log_io_completed(Ok(()));
                }
                Ok(())
            }
            Err(err) => {
                let message = err.to_string();
                metrics::record_log_group_flush_error(callback_count, entry_count, sync_duration);
                for pending in callbacks {
                    pending
                        .callback
                        .log_io_completed(Err(io::Error::other(message.clone())));
                }
                Err(storage_error(verb, message))
            }
        }
    }

    fn lock_log(
        &self,
        verb: ErrorVerb,
    ) -> Result<MutexGuard<'_, SegmentedEntryLog>, StorageError<NodeId>> {
        self.log
            .lock()
            .map_err(|_| storage_error(verb, "region log mutex poisoned"))
    }

    fn lock_pending(
        &self,
        verb: ErrorVerb,
    ) -> Result<MutexGuard<'_, PendingFlush>, StorageError<NodeId>> {
        self.pending
            .lock()
            .map_err(|_| storage_error(verb, "region log flush mutex poisoned"))
    }
}

#[derive(Default)]
struct PendingFlush {
    callbacks: Vec<PendingCallback>,
    scheduled: bool,
}

struct PendingCallback {
    callback: LogFlushed<RaftStoreConfig>,
    entry_count: u64,
}

fn storage_error(verb: ErrorVerb, message: impl Into<String>) -> StorageError<NodeId> {
    StorageError::IO {
        source: openraft::StorageIOError::new(
            ErrorSubject::Store,
            verb,
            AnyError::error(message.into()),
        ),
    }
}
