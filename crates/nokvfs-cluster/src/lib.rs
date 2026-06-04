//! Shared-log metadata replication contracts for NoKV-FS.
//!
//! This crate owns the metadata replication boundary above `nokvfs-meta`.
//! Log entries contain semantic `MetadataCommand` batches, not raw storage
//! mutations. Concrete implementations may use Raft, an external shared log, or
//! another quorum log, but those details must not leak into filesystem metadata
//! semantics.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Mutex;

use nokvfs_meta::command::{MetadataCommand, Version};
use nokvfs_types::MountId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogTerm(u64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogIndex(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogPosition {
    pub term: LogTerm,
    pub index: LogIndex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogEntry {
    pub position: LogPosition,
    pub mount: MountId,
    pub commands: Vec<MetadataCommand>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableReceipt {
    pub position: LogPosition,
    pub mount: MountId,
    pub batch_position: usize,
    pub request_id: Vec<u8>,
    pub commit_version: Version,
}

pub trait SharedMetadataLog {
    fn append_batch(
        &self,
        term: LogTerm,
        mount: MountId,
        commands: Vec<MetadataCommand>,
    ) -> Result<Vec<DurableReceipt>, SharedLogError>;

    fn read_from(
        &self,
        start: LogIndex,
        limit: usize,
    ) -> Result<Vec<MetadataLogEntry>, SharedLogError>;

    fn compact_through(&self, index: LogIndex) -> Result<(), SharedLogError>;

    fn committed_index(&self) -> LogIndex;
}

#[derive(Debug)]
pub struct InMemorySharedLog {
    inner: Mutex<InMemoryState>,
}

#[derive(Debug)]
struct InMemoryState {
    next_index: u64,
    compacted_through: LogIndex,
    entries: VecDeque<MetadataLogEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SharedLogError {
    ZeroTerm,
    ZeroIndex,
    EmptyBatch,
    Compacted {
        requested: LogIndex,
        compacted: LogIndex,
    },
    Backend(String),
}

impl Default for InMemorySharedLog {
    fn default() -> Self {
        Self {
            inner: Mutex::new(InMemoryState {
                next_index: 1,
                compacted_through: LogIndex::ZERO,
                entries: VecDeque::new(),
            }),
        }
    }
}

impl InMemorySharedLog {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SharedMetadataLog for InMemorySharedLog {
    fn append_batch(
        &self,
        term: LogTerm,
        mount: MountId,
        commands: Vec<MetadataCommand>,
    ) -> Result<Vec<DurableReceipt>, SharedLogError> {
        if commands.is_empty() {
            return Err(SharedLogError::EmptyBatch);
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| SharedLogError::Backend("shared log mutex poisoned".to_owned()))?;
        let index = LogIndex::new(inner.next_index)?;
        inner.next_index = inner.next_index.saturating_add(1);
        let position = LogPosition { term, index };
        let receipts = commands
            .iter()
            .enumerate()
            .map(|(batch_position, command)| DurableReceipt {
                position,
                mount,
                batch_position,
                request_id: command.request_id.clone(),
                commit_version: command.commit_version,
            })
            .collect::<Vec<_>>();
        inner.entries.push_back(MetadataLogEntry {
            position,
            mount,
            commands,
        });
        Ok(receipts)
    }

    fn read_from(
        &self,
        start: LogIndex,
        limit: usize,
    ) -> Result<Vec<MetadataLogEntry>, SharedLogError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| SharedLogError::Backend("shared log mutex poisoned".to_owned()))?;
        if start <= inner.compacted_through {
            return Err(SharedLogError::Compacted {
                requested: start,
                compacted: inner.compacted_through,
            });
        }
        let limit = if limit == 0 { usize::MAX } else { limit };
        Ok(inner
            .entries
            .iter()
            .filter(|entry| entry.position.index >= start)
            .take(limit)
            .cloned()
            .collect())
    }

    fn compact_through(&self, index: LogIndex) -> Result<(), SharedLogError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| SharedLogError::Backend("shared log mutex poisoned".to_owned()))?;
        inner.compacted_through = inner.compacted_through.max(index);
        while inner
            .entries
            .front()
            .is_some_and(|entry| entry.position.index <= inner.compacted_through)
        {
            inner.entries.pop_front();
        }
        Ok(())
    }

    fn committed_index(&self) -> LogIndex {
        self.inner
            .lock()
            .map(|inner| LogIndex(inner.next_index.saturating_sub(1)))
            .unwrap_or(LogIndex::ZERO)
    }
}

impl LogTerm {
    pub fn new(term: u64) -> Result<Self, SharedLogError> {
        if term == 0 {
            return Err(SharedLogError::ZeroTerm);
        }
        Ok(Self(term))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl LogIndex {
    pub const ZERO: Self = Self(0);

    pub fn new(index: u64) -> Result<Self, SharedLogError> {
        if index == 0 {
            return Err(SharedLogError::ZeroIndex);
        }
        Ok(Self(index))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for SharedLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroTerm => write!(f, "log term must be non-zero"),
            Self::ZeroIndex => write!(f, "log index must be non-zero"),
            Self::EmptyBatch => write!(f, "metadata log entry batch is empty"),
            Self::Compacted {
                requested,
                compacted,
            } => write!(
                f,
                "requested log index {} was compacted through {}",
                requested.get(),
                compacted.get()
            ),
            Self::Backend(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for SharedLogError {}

#[cfg(test)]
mod tests {
    use super::*;
    use nokvfs_meta::command::{CommandKind, Mutation, MutationOp, Value};
    use nokvfs_types::RecordFamily;

    fn version(raw: u64) -> Version {
        Version::new(raw).unwrap()
    }

    fn command(id: &[u8], commit_version: u64) -> MetadataCommand {
        MetadataCommand {
            request_id: id.to_vec(),
            kind: CommandKind::CreateFile,
            read_version: version(commit_version - 1),
            commit_version: version(commit_version),
            primary_family: RecordFamily::Dentry,
            primary_key: id.to_vec(),
            predicates: Vec::new(),
            mutations: vec![Mutation {
                family: RecordFamily::Dentry,
                key: id.to_vec(),
                op: MutationOp::Put,
                value: Some(Value(b"value".to_vec())),
            }],
            watch: Vec::new(),
        }
    }

    #[test]
    fn append_batch_returns_per_command_receipts() {
        let log = InMemorySharedLog::new();
        let term = LogTerm::new(1).unwrap();
        let mount = MountId::new(7).unwrap();
        let receipts = log
            .append_batch(term, mount, vec![command(b"a", 2), command(b"b", 3)])
            .unwrap();

        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts[0].position.index.get(), 1);
        assert_eq!(receipts[0].position.term, term);
        assert_eq!(receipts[0].batch_position, 0);
        assert_eq!(receipts[0].request_id, b"a");
        assert_eq!(receipts[0].commit_version, version(2));
        assert_eq!(receipts[1].batch_position, 1);
        assert_eq!(receipts[1].request_id, b"b");
        assert_eq!(log.committed_index().get(), 1);
    }

    #[test]
    fn read_from_replays_log_entries_in_index_order() {
        let log = InMemorySharedLog::new();
        let mount = MountId::new(1).unwrap();
        log.append_batch(LogTerm::new(1).unwrap(), mount, vec![command(b"a", 2)])
            .unwrap();
        log.append_batch(LogTerm::new(1).unwrap(), mount, vec![command(b"b", 3)])
            .unwrap();

        let entries = log.read_from(LogIndex::new(2).unwrap(), 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].position.index.get(), 2);
        assert_eq!(entries[0].commands[0].request_id, b"b");
    }

    #[test]
    fn compact_through_removes_old_entries_and_rejects_stale_reads() {
        let log = InMemorySharedLog::new();
        let mount = MountId::new(1).unwrap();
        log.append_batch(LogTerm::new(1).unwrap(), mount, vec![command(b"a", 2)])
            .unwrap();
        log.append_batch(LogTerm::new(1).unwrap(), mount, vec![command(b"b", 3)])
            .unwrap();

        log.compact_through(LogIndex::new(1).unwrap()).unwrap();
        assert!(matches!(
            log.read_from(LogIndex::new(1).unwrap(), 10),
            Err(SharedLogError::Compacted { .. })
        ));
        let entries = log.read_from(LogIndex::new(2).unwrap(), 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].commands[0].request_id, b"b");
    }

    #[test]
    fn rejects_empty_batches_and_zero_positions() {
        let log = InMemorySharedLog::new();
        assert_eq!(LogTerm::new(0), Err(SharedLogError::ZeroTerm));
        assert_eq!(LogIndex::new(0), Err(SharedLogError::ZeroIndex));
        assert_eq!(
            log.append_batch(
                LogTerm::new(1).unwrap(),
                MountId::new(1).unwrap(),
                Vec::new()
            ),
            Err(SharedLogError::EmptyBatch)
        );
    }
}
