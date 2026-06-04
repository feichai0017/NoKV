//! Metadata transaction contract for NoKV.
//!
//! This crate owns the storage-engine-neutral commit contract between metad
//! semantics and concrete metadata storage. It does not know about Holt trees,
//! object storage, FUSE, Raft, or protobuf.

use std::fmt;

use nokvfs_types::RecordFamily;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version(u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value(pub Vec<u8>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadItem {
    pub value: Value,
    pub version: Version,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadPurpose {
    UserStrong,
    WritePlanLocal,
    Snapshot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandKind {
    ReserveAllocator,
    CreateFile,
    CreateFiles,
    CreateDir,
    Rename,
    RenameReplace,
    RemoveFile,
    RemoveEmptyDir,
    PublishArtifact,
    ReplaceArtifact,
    SnapshotSubtree,
    RetireSnapshot,
    WatchSubtree,
    CleanupObjects,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Predicate {
    Exists,
    NotExists,
    PrefixEmpty,
    VersionEquals(Version),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PredicateRef {
    pub family: RecordFamily,
    pub key: Vec<u8>,
    pub predicate: Predicate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutationOp {
    Put,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mutation {
    pub family: RecordFamily,
    pub key: Vec<u8>,
    pub op: MutationOp,
    pub value: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchProjection {
    pub family: RecordFamily,
    pub key: Vec<u8>,
    pub event: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataCommand {
    pub request_id: Vec<u8>,
    pub kind: CommandKind,
    pub read_version: Version,
    pub commit_version: Version,
    pub primary_family: RecordFamily,
    pub primary_key: Vec<u8>,
    pub predicates: Vec<PredicateRef>,
    pub mutations: Vec<Mutation>,
    pub watch: Vec<WatchProjection>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitResult {
    pub commit_version: Version,
    pub applied_mutations: usize,
    pub watch_events: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetadataStoreStats {
    pub commit_total: u64,
    pub dedupe_hit_total: u64,
    pub predicate_total: u64,
    pub prefix_empty_predicate_total: u64,
    pub current_put_total: u64,
    pub current_delete_total: u64,
    pub history_write_total: u64,
    pub watch_write_total: u64,
    pub dedupe_write_total: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryPruneRequest {
    pub retain_from: Option<Version>,
    pub limit: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HistoryPruneOutcome {
    pub scanned: usize,
    pub removed: usize,
    pub retained_by_snapshots: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanRequest {
    pub family: RecordFamily,
    pub prefix: Vec<u8>,
    pub version: Version,
    pub limit: usize,
    pub purpose: ReadPurpose,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanItem {
    pub key: Vec<u8>,
    pub value: Value,
    pub version: Version,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataError {
    ZeroVersion,
    CommitBeforeRead,
    EmptyRequestId,
    EmptyPrimaryKey,
    PutWithoutValue,
    DeleteWithValue,
    PredicateFailed,
    Backend(String),
}

pub trait MetadataStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError>;

    fn get(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<Value>, MetadataError> {
        self.get_versioned(family, key, version, purpose)
            .map(|item| item.map(|item| item.value))
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError>;

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError>;

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError>;
}

pub trait MetadataStoreStatsProvider {
    fn metadata_store_stats(&self) -> MetadataStoreStats;
}

impl Version {
    pub fn new(version: u64) -> Result<Self, MetadataError> {
        if version == 0 {
            return Err(MetadataError::ZeroVersion);
        }
        Ok(Self(version))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl MetadataCommand {
    pub fn validate(&self) -> Result<(), MetadataError> {
        if self.request_id.is_empty() {
            return Err(MetadataError::EmptyRequestId);
        }
        if self.primary_key.is_empty() {
            return Err(MetadataError::EmptyPrimaryKey);
        }
        if self.commit_version < self.read_version {
            return Err(MetadataError::CommitBeforeRead);
        }
        for mutation in &self.mutations {
            match (mutation.op, mutation.value.as_ref()) {
                (MutationOp::Put, None) => return Err(MetadataError::PutWithoutValue),
                (MutationOp::Delete, Some(_)) => return Err(MetadataError::DeleteWithValue),
                _ => {}
            }
        }
        Ok(())
    }
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroVersion => write!(f, "metadata version must be non-zero"),
            Self::CommitBeforeRead => write!(f, "commit version is before read version"),
            Self::EmptyRequestId => write!(f, "metadata command request id is empty"),
            Self::EmptyPrimaryKey => write!(f, "metadata command primary key is empty"),
            Self::PutWithoutValue => write!(f, "put mutation is missing value"),
            Self::DeleteWithValue => write!(f, "delete mutation has a value"),
            Self::PredicateFailed => write!(f, "metadata command predicate failed"),
            Self::Backend(err) => write!(f, "metadata backend error: {err}"),
        }
    }
}

impl std::error::Error for MetadataError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(raw: u64) -> Version {
        Version::new(raw).unwrap()
    }

    fn command() -> MetadataCommand {
        MetadataCommand {
            request_id: b"req-1".to_vec(),
            kind: CommandKind::CreateFile,
            read_version: version(10),
            commit_version: version(11),
            primary_family: RecordFamily::Dentry,
            primary_key: b"primary".to_vec(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Dentry,
                key: b"primary".to_vec(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Dentry,
                key: b"primary".to_vec(),
                op: MutationOp::Put,
                value: Some(Value(b"value".to_vec())),
            }],
            watch: Vec::new(),
        }
    }

    #[test]
    fn version_rejects_zero() {
        assert_eq!(Version::new(0), Err(MetadataError::ZeroVersion));
    }

    #[test]
    fn command_validation_accepts_well_formed_command() {
        command().validate().unwrap();
    }

    #[test]
    fn command_validation_rejects_missing_id_or_primary_key() {
        let mut cmd = command();
        cmd.request_id.clear();
        assert_eq!(cmd.validate(), Err(MetadataError::EmptyRequestId));

        let mut cmd = command();
        cmd.primary_key.clear();
        assert_eq!(cmd.validate(), Err(MetadataError::EmptyPrimaryKey));
    }

    #[test]
    fn command_validation_rejects_invalid_mutation_payloads() {
        let mut cmd = command();
        cmd.mutations[0].value = None;
        assert_eq!(cmd.validate(), Err(MetadataError::PutWithoutValue));

        let mut cmd = command();
        cmd.mutations[0].op = MutationOp::Delete;
        assert_eq!(cmd.validate(), Err(MetadataError::DeleteWithValue));
    }
}
