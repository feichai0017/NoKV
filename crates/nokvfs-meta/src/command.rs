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
    CreateSymlink,
    UpdateAttr,
    SetXattr,
    RemoveXattr,
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
    pub get_total: u64,
    pub get_user_strong_total: u64,
    pub get_write_plan_local_total: u64,
    pub get_snapshot_total: u64,
    pub scan_total: u64,
    pub scan_user_strong_total: u64,
    pub scan_write_plan_local_total: u64,
    pub scan_snapshot_total: u64,
    pub scan_key_visited_total: u64,
    pub scan_key_returned_total: u64,
    pub history_lookup_total: u64,
    pub active_snapshot_pin_total: u64,
    pub commit_total: u64,
    pub dedupe_hit_total: u64,
    pub predicate_total: u64,
    pub prefix_empty_predicate_total: u64,
    pub current_put_total: u64,
    pub current_delete_total: u64,
    pub history_write_total: u64,
    pub watch_write_total: u64,
    pub dedupe_write_total: u64,
    pub commit_prepare_ns_total: u64,
    pub atomic_apply_total: u64,
    pub atomic_apply_command_total: u64,
    pub atomic_apply_max_batch: u64,
    pub atomic_apply_ns_total: u64,
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
    pub start_after: Option<Vec<u8>>,
    pub version: Version,
    pub limit: usize,
    pub purpose: ReadPurpose,
}

/// Current-state key listing for metadata paths that do not need value bytes.
///
/// Snapshot reads that need historical visibility should use [`ScanRequest`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyScanRequest {
    pub family: RecordFamily,
    pub prefix: Vec<u8>,
    pub start_after: Option<Vec<u8>>,
    pub limit: usize,
    pub purpose: ReadPurpose,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelimitedScanRequest {
    pub family: RecordFamily,
    pub prefix: Vec<u8>,
    pub start_after: Option<Vec<u8>>,
    pub delimiter: u8,
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
pub enum DelimitedScanItem {
    Key(ScanItem),
    CommonPrefix(Vec<u8>),
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
    ReadNotFresh {
        required_term: u64,
        required_index: u64,
        applied_term: Option<u64>,
        applied_index: Option<u64>,
    },
    ForwardToLeader {
        leader_id: Option<u64>,
        address: Option<String>,
    },
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

    fn scan_delimited(
        &self,
        request: DelimitedScanRequest,
    ) -> Result<Vec<DelimitedScanItem>, MetadataError> {
        let limit = scan_limit(request.limit);
        let mut out = Vec::new();
        let start_after = request.start_after.as_deref();
        for item in self.scan(ScanRequest {
            family: request.family,
            prefix: request.prefix.clone(),
            start_after: request.start_after.clone(),
            version: request.version,
            limit: 0,
            purpose: request.purpose,
        })? {
            let collapsed = collapse_delimited_scan_item(item, &request.prefix, request.delimiter);
            if item_after_marker(&collapsed, start_after) {
                if out.last() != Some(&collapsed) {
                    out.push(collapsed);
                }
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    fn scan_keys(&self, request: KeyScanRequest) -> Result<Vec<Vec<u8>>, MetadataError> {
        self.scan(ScanRequest {
            family: request.family,
            prefix: request.prefix,
            start_after: request.start_after,
            version: Version::new(u64::MAX)?,
            limit: request.limit,
            purpose: request.purpose,
        })
        .map(|items| items.into_iter().map(|item| item.key).collect())
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError>;

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        commands
            .iter()
            .cloned()
            .map(|command| self.commit_metadata(command))
            .collect()
    }

    fn committed_request_result(
        &self,
        _request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        Ok(None)
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError>;
}

pub trait MetadataCheckpointStore {
    fn checkpoint(&self) -> Result<(), MetadataError>;
    fn export_checkpoint_image(&self) -> Result<Vec<u8>, MetadataError>;
    fn install_checkpoint_image(&self, image: &[u8]) -> Result<(), MetadataError>;
}

pub trait MetadataStoreStatsProvider {
    fn metadata_store_stats(&self) -> MetadataStoreStats;
}

pub fn metadata_commands_conflict(left: &MetadataCommand, right: &MetadataCommand) -> bool {
    left.request_id == right.request_id
        || left.mutations.iter().any(|left_mutation| {
            right.mutations.iter().any(|right_mutation| {
                same_metadata_key(
                    left_mutation.family,
                    &left_mutation.key,
                    right_mutation.family,
                    &right_mutation.key,
                )
            })
        })
        || left.predicates.iter().any(|predicate| {
            right
                .mutations
                .iter()
                .any(|mutation| predicate_conflicts_with_mutation(predicate, mutation))
        })
        || right.predicates.iter().any(|predicate| {
            left.mutations
                .iter()
                .any(|mutation| predicate_conflicts_with_mutation(predicate, mutation))
        })
}

fn predicate_conflicts_with_mutation(predicate: &PredicateRef, mutation: &Mutation) -> bool {
    match predicate.predicate {
        Predicate::PrefixEmpty => {
            predicate.family == mutation.family && mutation.key.starts_with(&predicate.key)
        }
        Predicate::Exists | Predicate::NotExists | Predicate::VersionEquals(_) => {
            same_metadata_key(
                predicate.family,
                &predicate.key,
                mutation.family,
                &mutation.key,
            )
        }
    }
}

fn scan_limit(limit: usize) -> usize {
    if limit == 0 {
        usize::MAX
    } else {
        limit
    }
}

fn collapse_delimited_scan_item(item: ScanItem, prefix: &[u8], delimiter: u8) -> DelimitedScanItem {
    let suffix = item.key.get(prefix.len()..).unwrap_or_default();
    if let Some(offset) = suffix.iter().position(|byte| *byte == delimiter) {
        DelimitedScanItem::CommonPrefix(item.key[..prefix.len() + offset + 1].to_vec())
    } else {
        DelimitedScanItem::Key(item)
    }
}

fn item_after_marker(item: &DelimitedScanItem, start_after: Option<&[u8]>) -> bool {
    let Some(start_after) = start_after else {
        return true;
    };
    match item {
        DelimitedScanItem::Key(item) => item.key.as_slice() > start_after,
        DelimitedScanItem::CommonPrefix(prefix) => prefix.as_slice() > start_after,
    }
}

fn same_metadata_key(
    left_family: RecordFamily,
    left_key: &[u8],
    right_family: RecordFamily,
    right_key: &[u8],
) -> bool {
    left_family == right_family && left_key == right_key
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
            Self::ReadNotFresh {
                required_term,
                required_index,
                applied_term,
                applied_index,
            } => {
                write!(
                    f,
                    "metadata read requires applied frontier {required_term}:{required_index}"
                )?;
                if let (Some(applied_term), Some(applied_index)) = (applied_term, applied_index) {
                    write!(
                        f,
                        ", current applied frontier is {applied_term}:{applied_index}"
                    )?;
                }
                Ok(())
            }
            Self::ForwardToLeader { leader_id, address } => {
                write!(f, "metadata write must be forwarded to leader")?;
                if let Some(leader_id) = leader_id {
                    write!(f, " {leader_id}")?;
                }
                if let Some(address) = address {
                    write!(f, " at {address}")?;
                }
                Ok(())
            }
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
