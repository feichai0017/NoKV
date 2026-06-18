//! Synchronous logical metadata log archiving for commit ACK durability.

use super::log_archive::archive_metadata_log_segment_to_store;
use super::*;
use crate::{MetadataLogEntry, MetadataLogSegment};

/// A pointer to one archived logical-log segment in the live chain.
///
/// The sync state keeps the ordered chain of every segment archived above the
/// latest checkpoint so the control-plane `LogRef` can enumerate all of them.
/// A single latest pointer would lose every segment but the newest on failover.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogSegmentPointer {
    pub segment_key: String,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub last_digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogSyncConfig {
    pub archive: MetadataLogArchiveConfig,
    pub shard_id: String,
    pub epoch: u64,
    pub durable_lsn: u64,
    pub last_digest: [u8; 32],
    /// Segment chain inherited from the control record (e.g. after failover),
    /// so the new owner's future `LogRef` publishes keep the full chain.
    pub segments: Vec<MetadataLogSegmentPointer>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogSyncSnapshot {
    pub shard_id: String,
    pub epoch: u64,
    pub durable_lsn: u64,
    pub last_digest: [u8; 32],
    /// Ordered (oldest first) segment chain above the latest checkpoint.
    pub segments: Vec<MetadataLogSegmentPointer>,
}

pub(super) struct MetadataLogSyncState {
    config: MetadataLogArchiveConfig,
    shard_id: String,
    epoch: u64,
    next_lsn: u64,
    prev_digest: [u8; 32],
    segments: Vec<MetadataLogSegmentPointer>,
}

impl MetadataLogSyncState {
    fn snapshot(&self) -> Option<MetadataLogSyncSnapshot> {
        let last = self.segments.last()?;
        Some(MetadataLogSyncSnapshot {
            shard_id: self.shard_id.clone(),
            epoch: self.epoch,
            durable_lsn: last.last_lsn,
            last_digest: last.last_digest,
            segments: self.segments.clone(),
        })
    }
}

impl MetadataLogSyncConfig {
    pub fn new(
        archive_prefix: impl Into<String>,
        shard_id: impl Into<String>,
        epoch: u64,
        durable_lsn: u64,
        last_digest: [u8; 32],
    ) -> Self {
        Self {
            archive: MetadataLogArchiveConfig::new(archive_prefix),
            shard_id: shard_id.into(),
            epoch,
            durable_lsn,
            last_digest,
            segments: Vec::new(),
        }
    }

    /// Inherit a previously-archived segment chain (above the latest checkpoint)
    /// so future `LogRef` publishes keep the full chain after a failover restore.
    pub fn with_segments(mut self, segments: Vec<MetadataLogSegmentPointer>) -> Self {
        self.segments = segments;
        self
    }
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn enable_sync_metadata_log(
        &self,
        config: MetadataLogSyncConfig,
    ) -> Result<(), MetadError> {
        if config.shard_id.is_empty() {
            return Err(MetadError::Codec(
                "metadata log shard id is empty".to_owned(),
            ));
        }
        if config.epoch == 0 {
            return Err(MetadError::InvalidOwnerEpoch);
        }
        let next_lsn = config
            .durable_lsn
            .checked_add(1)
            .ok_or_else(|| MetadError::Codec("metadata log LSN is exhausted".to_owned()))?;
        let state = MetadataLogSyncState {
            config: config.archive,
            shard_id: config.shard_id,
            epoch: config.epoch,
            next_lsn,
            prev_digest: config.last_digest,
            segments: config.segments,
        };
        *self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(state);
        Ok(())
    }

    /// Drop archived segments fully covered by a new checkpoint at
    /// `checkpoint_lsn` (their effects now live in the checkpoint image), keeping
    /// the live chain bounded. Called after a checkpoint is published.
    pub fn prune_sync_metadata_log_segments(&self, checkpoint_lsn: u64) {
        let mut guard = self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if let Some(state) = guard.as_mut() {
            state
                .segments
                .retain(|segment| segment.last_lsn > checkpoint_lsn);
        }
    }

    pub fn disable_sync_metadata_log(&self) {
        *self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = None;
    }

    pub fn sync_metadata_log_snapshot(&self) -> Option<MetadataLogSyncSnapshot> {
        self.metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .and_then(|state| state.snapshot())
    }

    pub(super) fn record_committed_metadata_command(
        &self,
        command: &MetadataCommand,
        result: &CommitResult,
    ) -> Result<Option<MetadataLogSyncSnapshot>, MetadError> {
        self.record_committed_metadata_commands(&[(command, result)])
    }

    pub(super) fn record_committed_metadata_commands(
        &self,
        commands: &[(&MetadataCommand, &CommitResult)],
    ) -> Result<Option<MetadataLogSyncSnapshot>, MetadError> {
        if commands.is_empty() {
            return Ok(None);
        }
        let mut guard = self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let Some(state) = guard.as_mut() else {
            return Ok(None);
        };

        let mut entries = Vec::with_capacity(commands.len());
        let mut next_lsn = state.next_lsn;
        let mut prev_digest = state.prev_digest;
        for (command, result) in commands {
            let entry = MetadataLogEntry::seal(
                state.shard_id.clone(),
                state.epoch,
                next_lsn,
                (*command).clone(),
                (*result).clone(),
                prev_digest,
            )
            .map_err(|err| MetadError::Codec(format!("metadata log entry seal failed: {err}")))?;
            next_lsn = next_lsn.checked_add(1).ok_or_else(|| {
                MetadError::Codec("metadata log LSN is exhausted before archive".to_owned())
            })?;
            prev_digest = entry.digest;
            entries.push(entry);
        }

        let first_lsn = entries
            .first()
            .expect("non-empty command list yields non-empty log entries")
            .lsn;
        let last = entries
            .last()
            .expect("non-empty command list yields non-empty log entries")
            .clone();
        let segment = MetadataLogSegment::seal(entries)
            .map_err(|err| MetadError::Codec(format!("metadata log segment seal failed: {err}")))?;
        let archived =
            archive_metadata_log_segment_to_store(&self.objects, &state.config, &segment)?;
        self.metadata_log_segments_archived_total
            .fetch_add(1, Ordering::Relaxed);
        self.metadata_log_entries_archived_total
            .fetch_add(segment.entries.len() as u64, Ordering::Relaxed);
        self.metadata_log_archive_bytes_total
            .fetch_add(archived.encoded_bytes, Ordering::Relaxed);
        // Advance the chain only after the segment is durable in object storage;
        // on archive failure `?` returns above and next_lsn/prev_digest/segments
        // are left untouched so the next command re-chains from the same prev.
        state.next_lsn = next_lsn;
        state.prev_digest = last.digest;
        state.segments.push(MetadataLogSegmentPointer {
            segment_key: archived.segment_key,
            first_lsn,
            last_lsn: last.lsn,
            last_digest: last.digest,
        });
        Ok(state.snapshot())
    }
}
