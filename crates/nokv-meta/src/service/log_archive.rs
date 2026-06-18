//! Object-store archive helpers for logical metadata log segments.
//!
//! This module stores already sealed [`MetadataLogSegment`] values in the object
//! store. It owns object key construction and roundtrip validation, but not
//! control-plane pointer publication or restore-time command application.

use super::*;
use crate::{metadata_log_replay_entries, MetadataCheckpointStore, MetadataLogSegment};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogArchiveConfig {
    pub prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogSegmentArchiveOutcome {
    pub segment_key: String,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub first_epoch: u64,
    pub last_epoch: u64,
    pub encoded_bytes: u64,
    pub digest_hex: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogRestoreOutcome {
    pub checkpoint: MetadataRestoreOutcome,
    pub replayed_entries: usize,
    pub durable_lsn: u64,
    pub last_digest: [u8; 32],
}

impl MetadataLogArchiveConfig {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn archive_metadata_log_segment(
        &self,
        config: &MetadataLogArchiveConfig,
        segment: &MetadataLogSegment,
    ) -> Result<MetadataLogSegmentArchiveOutcome, MetadError> {
        archive_metadata_log_segment_to_store(&self.objects, config, segment)
    }

    pub fn load_metadata_log_segment(
        &self,
        segment_key: &str,
    ) -> Result<MetadataLogSegment, MetadError> {
        let object_key = ObjectKey::new(segment_key.to_owned())?;
        let bytes = self.objects.get(&object_key, None)?;
        MetadataLogSegment::decode(&bytes)
            .map_err(|err| MetadError::Codec(format!("metadata log segment decode failed: {err}")))
    }
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore + MetadataCheckpointStore,
    O: ObjectStore,
{
    pub fn restore_metadata_with_archived_log_segments(
        &self,
        checkpoint_config: &MetadataArchiveConfig,
        segment_keys: &[String],
        checkpoint_lsn: u64,
        checkpoint_digest: [u8; 32],
    ) -> Result<Option<MetadataLogRestoreOutcome>, MetadError> {
        let segments = segment_keys
            .iter()
            .map(|key| self.load_metadata_log_segment(key))
            .collect::<Result<Vec<_>, _>>()?;
        self.restore_metadata_with_log_segments(
            checkpoint_config,
            &segments,
            checkpoint_lsn,
            checkpoint_digest,
        )
    }

    pub fn restore_metadata_with_log_segments(
        &self,
        checkpoint_config: &MetadataArchiveConfig,
        segments: &[MetadataLogSegment],
        checkpoint_lsn: u64,
        checkpoint_digest: [u8; 32],
    ) -> Result<Option<MetadataLogRestoreOutcome>, MetadError> {
        let entries = metadata_log_replay_entries(segments, checkpoint_lsn, checkpoint_digest)
            .map_err(|err| MetadError::Codec(format!("metadata log replay failed: {err}")))?;
        let Some(checkpoint) = self.restore_metadata(checkpoint_config)? else {
            return Ok(None);
        };

        let mut durable_lsn = checkpoint_lsn;
        let mut last_digest = checkpoint_digest;
        let mut replay_next_inode = None;
        for entry in &entries {
            let result = self.commit_metadata_without_sync_log(entry.command.clone())?;
            if result != entry.result {
                return Err(MetadError::Codec(format!(
                    "metadata log replay result mismatch at lsn {}",
                    entry.lsn
                )));
            }
            self.clock
                .fetch_max(result.commit_version.get(), Ordering::Relaxed);
            replay_next_inode = max_optional_u64(
                replay_next_inode,
                command_replay_next_inode(&entry.command)?,
            );
            durable_lsn = entry.lsn;
            last_digest = entry.digest;
        }
        if let Some(next_inode) = replay_next_inode {
            self.next_inode.fetch_max(next_inode, Ordering::Relaxed);
            self.reserved_next_inode
                .fetch_max(next_inode, Ordering::Relaxed);
        }
        self.refresh_allocator_state()?;

        Ok(Some(MetadataLogRestoreOutcome {
            checkpoint,
            replayed_entries: entries.len(),
            durable_lsn,
            last_digest,
        }))
    }
}

pub(super) fn archive_metadata_log_segment_to_store<O: ObjectStore>(
    objects: &O,
    config: &MetadataLogArchiveConfig,
    segment: &MetadataLogSegment,
) -> Result<MetadataLogSegmentArchiveOutcome, MetadError> {
    segment
        .verify()
        .map_err(|err| MetadError::Codec(format!("metadata log segment is invalid: {err}")))?;
    let encoded = segment
        .encode()
        .map_err(|err| MetadError::Codec(format!("metadata log segment encode failed: {err}")))?;
    let digest_hex = hex_digest(&segment.digest);
    let segment_key = log_segment_key(config, segment, &digest_hex);
    let object_key = ObjectKey::new(segment_key.clone())?;

    objects.put(&object_key, encoded.clone())?;

    let stored = objects.get(&object_key, None)?;
    if stored != encoded {
        return Err(MetadError::Codec(
            "metadata log segment read-after-write mismatch".to_owned(),
        ));
    }
    let decoded = MetadataLogSegment::decode(&stored)
        .map_err(|err| MetadError::Codec(format!("metadata log segment decode failed: {err}")))?;
    if &decoded != segment {
        return Err(MetadError::Codec(
            "metadata log segment decoded content mismatch".to_owned(),
        ));
    }

    Ok(MetadataLogSegmentArchiveOutcome {
        segment_key,
        first_lsn: segment.first_lsn,
        last_lsn: segment.last_lsn,
        first_epoch: segment.first_epoch,
        last_epoch: segment.last_epoch,
        encoded_bytes: encoded.len() as u64,
        digest_hex,
    })
}

fn command_replay_next_inode(command: &MetadataCommand) -> Result<Option<u64>, MetadError> {
    let mut max_inode = None;
    for mutation in &command.mutations {
        if mutation.op != MutationOp::Put {
            continue;
        }
        let Some(value) = &mutation.value else {
            continue;
        };
        match mutation.family {
            RecordFamily::Inode => {
                let attr = decode_inode_attr(&value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                max_inode = max_optional_u64(max_inode, Some(attr.inode.get()));
            }
            RecordFamily::Dentry => {
                let projection = decode_dentry_projection(&value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                max_inode = max_optional_u64(
                    max_inode,
                    Some(
                        projection
                            .attr
                            .inode
                            .get()
                            .max(projection.dentry.child.get()),
                    ),
                );
            }
            _ => {}
        }
    }
    max_inode
        .map(|inode| inode.checked_add(1).ok_or(MetadError::AllocatorExhausted))
        .transpose()
}

fn max_optional_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn log_segment_key(
    config: &MetadataLogArchiveConfig,
    segment: &MetadataLogSegment,
    digest_hex: &str,
) -> String {
    format!(
        "{}/log/{:020}-{:020}-{}.segment",
        normalize_log_prefix(&config.prefix),
        segment.first_lsn,
        segment.last_lsn,
        &digest_hex[..16],
    )
}

fn normalize_log_prefix(prefix: &str) -> &str {
    prefix.trim_end_matches('/')
}

fn hex_digest(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
