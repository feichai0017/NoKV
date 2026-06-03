//! Holt-backed metadata store for NoKV-FS.
//!
//! This crate owns the mapping from storage-engine-neutral metadata commands to
//! Holt family trees. It does not own filesystem semantics, object storage,
//! Raft replication, FUSE, or protobuf types.

use std::path::Path;
use std::sync::{Arc, Mutex};

use holt::{RangeEntry, Tree, TreeConfig, DB};
use nokv_fs_layout::{history_key, history_prefix};
use nokv_fs_metastore::{
    CommitResult, MetadataCommand, MetadataError, MetadataStore, MutationOp, Predicate,
    ReadPurpose, ScanItem, ScanRequest, Value, Version,
};
use nokv_fs_model::RecordFamily;

const VALUE_HEADER_LEN: usize = 8;

const MOUNT_CURRENT_TREE: &str = "mount_current";
const INODE_CURRENT_TREE: &str = "inode_current";
const DENTRY_CURRENT_TREE: &str = "dentry_current";
const PARENT_CURRENT_TREE: &str = "parent_current";
const CHUNK_MANIFEST_CURRENT_TREE: &str = "chunk_manifest_current";
const SESSION_CURRENT_TREE: &str = "session_current";
const PATH_INDEX_CURRENT_TREE: &str = "path_index_current";
const WATCH_CURRENT_TREE: &str = "watch_current";
const SNAPSHOT_CURRENT_TREE: &str = "snapshot_current";
const COMMAND_DEDUPE_CURRENT_TREE: &str = "command_dedupe_current";
const HISTORY_TREE: &str = "history";

const REQUIRED_TREES: [&str; 11] = [
    MOUNT_CURRENT_TREE,
    INODE_CURRENT_TREE,
    DENTRY_CURRENT_TREE,
    PARENT_CURRENT_TREE,
    CHUNK_MANIFEST_CURRENT_TREE,
    SESSION_CURRENT_TREE,
    PATH_INDEX_CURRENT_TREE,
    WATCH_CURRENT_TREE,
    SNAPSHOT_CURRENT_TREE,
    COMMAND_DEDUPE_CURRENT_TREE,
    HISTORY_TREE,
];

#[derive(Clone)]
pub struct HoltMetadataStore {
    db: DB,
    write_gate: Arc<Mutex<()>>,
}

impl HoltMetadataStore {
    pub fn open_memory() -> Result<Self, MetadataError> {
        Self::open(TreeConfig::memory())
    }

    pub fn open_file(path: impl AsRef<Path>) -> Result<Self, MetadataError> {
        Self::open(TreeConfig::new(path.as_ref()))
    }

    pub fn open(config: TreeConfig) -> Result<Self, MetadataError> {
        let db = DB::open(config).map_err(to_backend_error)?;
        for tree in REQUIRED_TREES {
            db.open_or_create_tree(tree).map_err(to_backend_error)?;
        }
        Ok(Self {
            db,
            write_gate: Arc::new(Mutex::new(())),
        })
    }

    pub fn checkpoint(&self) -> Result<(), MetadataError> {
        self.db.checkpoint().map_err(to_backend_error)
    }

    fn current_tree(&self, family: RecordFamily) -> Result<Tree, MetadataError> {
        self.db
            .open_tree(current_tree_name(family))
            .map_err(to_backend_error)
    }

    fn history_tree(&self) -> Result<Tree, MetadataError> {
        self.db.open_tree(HISTORY_TREE).map_err(to_backend_error)
    }
}

impl MetadataStore for HoltMetadataStore {
    fn get(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        _purpose: ReadPurpose,
    ) -> Result<Option<Value>, MetadataError> {
        read_visible(
            &self.current_tree(family)?,
            family,
            key,
            version,
            &self.history_tree()?,
        )
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        let limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit
        };
        let current = self.current_tree(request.family)?;
        let history = self.history_tree()?;
        let mut out = Vec::new();

        if request.prefix.is_empty() {
            for entry in current.range() {
                if push_visible_scan_item(
                    entry,
                    request.family,
                    request.version,
                    &history,
                    &mut out,
                    limit,
                )? {
                    break;
                }
            }
        } else {
            for entry in current.range().prefix(&request.prefix) {
                if push_visible_scan_item(
                    entry,
                    request.family,
                    request.version,
                    &history,
                    &mut out,
                    limit,
                )? {
                    break;
                }
            }
        }
        Ok(out)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        command.validate()?;
        let _guard = self
            .write_gate
            .lock()
            .map_err(|_| MetadataError::Backend("holt metadata write gate poisoned".to_owned()))?;

        let dedupe = self.current_tree(RecordFamily::CommandDedupe)?;
        if let Some(encoded) = dedupe
            .get(&command.request_id)
            .map_err(to_backend_error)?
            .as_deref()
            .map(decode_dedupe_result)
            .transpose()?
        {
            return Ok(encoded);
        }

        for predicate in &command.predicates {
            let tree = self.current_tree(predicate.family)?;
            match predicate.predicate {
                Predicate::Exists => {
                    if tree
                        .get(&predicate.key)
                        .map_err(to_backend_error)?
                        .is_none()
                    {
                        return Err(MetadataError::PredicateFailed);
                    }
                }
                Predicate::NotExists => {
                    if tree
                        .get(&predicate.key)
                        .map_err(to_backend_error)?
                        .is_some()
                    {
                        return Err(MetadataError::PredicateFailed);
                    }
                }
                Predicate::PrefixEmpty => {
                    if prefix_has_key(&tree, &predicate.key)? {
                        return Err(MetadataError::PredicateFailed);
                    }
                }
                Predicate::VersionEquals(expected) => {
                    let current = tree.get(&predicate.key).map_err(to_backend_error)?;
                    let Some(current) = current else {
                        return Err(MetadataError::PredicateFailed);
                    };
                    let (actual, _) = decode_current_value(&current)?;
                    if actual != expected {
                        return Err(MetadataError::PredicateFailed);
                    }
                }
            }
        }

        let mut history_records = Vec::new();
        for mutation in &command.mutations {
            if let Some(current) = self
                .current_tree(mutation.family)?
                .get(&mutation.key)
                .map_err(to_backend_error)?
            {
                history_records.push((mutation.family, mutation.key.clone(), current));
            }
        }

        let mut applied = 0_usize;
        let mut watch_events = 0_usize;
        let result = CommitResult {
            commit_version: command.commit_version,
            applied_mutations: command.mutations.len(),
            watch_events: command.watch.len(),
        };
        let dedupe_result = encode_dedupe_result(&result);

        self.db
            .atomic(|batch| {
                for (family, key, current) in history_records {
                    if let Ok((old_version, _)) = decode_current_value(&current) {
                        batch.put(
                            HISTORY_TREE,
                            &history_key(family, &key, old_version.get()),
                            &current,
                        );
                    }
                }
                for mutation in &command.mutations {
                    match mutation.op {
                        MutationOp::Put => {
                            let value = mutation
                                .value
                                .as_ref()
                                .expect("validated put mutation has a value");
                            batch.put(
                                current_tree_name(mutation.family),
                                &mutation.key,
                                &encode_current_value(command.commit_version, &value.0),
                            );
                        }
                        MutationOp::Delete => {
                            batch.delete(current_tree_name(mutation.family), &mutation.key);
                        }
                    }
                    applied += 1;
                }
                for event in &command.watch {
                    let key = watch_event_key(&event.key, command.commit_version, watch_events);
                    batch.put(WATCH_CURRENT_TREE, &key, &event.event);
                    watch_events += 1;
                }
                batch.put(
                    current_tree_name(RecordFamily::CommandDedupe),
                    &command.request_id,
                    &dedupe_result,
                );
            })
            .map_err(to_backend_error)?;

        Ok(CommitResult {
            applied_mutations: applied,
            watch_events,
            ..result
        })
    }
}

fn current_tree_name(family: RecordFamily) -> &'static str {
    match family {
        RecordFamily::Mount => MOUNT_CURRENT_TREE,
        RecordFamily::Inode => INODE_CURRENT_TREE,
        RecordFamily::Dentry => DENTRY_CURRENT_TREE,
        RecordFamily::Parent => PARENT_CURRENT_TREE,
        RecordFamily::ChunkManifest => CHUNK_MANIFEST_CURRENT_TREE,
        RecordFamily::Session => SESSION_CURRENT_TREE,
        RecordFamily::PathIndex => PATH_INDEX_CURRENT_TREE,
        RecordFamily::Watch => WATCH_CURRENT_TREE,
        RecordFamily::Snapshot => SNAPSHOT_CURRENT_TREE,
        RecordFamily::CommandDedupe => COMMAND_DEDUPE_CURRENT_TREE,
        RecordFamily::History => HISTORY_TREE,
    }
}

fn read_visible(
    current: &Tree,
    family: RecordFamily,
    key: &[u8],
    version: Version,
    history: &Tree,
) -> Result<Option<Value>, MetadataError> {
    let Some(encoded) = current.get(key).map_err(to_backend_error)? else {
        return Ok(None);
    };
    decode_visible_value(family, key, &encoded, version, history)
        .map(|value| value.map(|(_, bytes)| Value(bytes)))
}

fn decode_visible_value(
    family: RecordFamily,
    key: &[u8],
    encoded: &[u8],
    version: Version,
    history: &Tree,
) -> Result<Option<(Version, Vec<u8>)>, MetadataError> {
    let (current_version, current_value) = decode_current_value(encoded)?;
    if current_version <= version {
        return Ok(Some((current_version, current_value)));
    }
    for entry in history.range().prefix(&history_prefix(family, key)) {
        let RangeEntry::Key { value, .. } = entry.map_err(to_backend_error)? else {
            continue;
        };
        let (history_version, history_value) = decode_current_value(&value)?;
        if history_version <= version {
            return Ok(Some((history_version, history_value)));
        }
    }
    Ok(None)
}

fn push_visible_scan_item(
    entry: Result<RangeEntry, holt::Error>,
    family: RecordFamily,
    version: Version,
    history: &Tree,
    out: &mut Vec<ScanItem>,
    limit: usize,
) -> Result<bool, MetadataError> {
    let RangeEntry::Key { key, value, .. } = entry.map_err(to_backend_error)? else {
        return Ok(false);
    };
    if let Some((commit, visible)) = decode_visible_value(family, &key, &value, version, history)? {
        out.push(ScanItem {
            key,
            value: Value(visible),
            version: commit,
        });
    }
    Ok(out.len() >= limit)
}

fn encode_current_value(version: Version, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(VALUE_HEADER_LEN + value.len());
    out.extend_from_slice(&version.get().to_be_bytes());
    out.extend_from_slice(value);
    out
}

fn decode_current_value(encoded: &[u8]) -> Result<(Version, Vec<u8>), MetadataError> {
    if encoded.len() < VALUE_HEADER_LEN {
        return Err(MetadataError::Backend(
            "encoded current metadata value is truncated".to_owned(),
        ));
    }
    let raw = u64::from_be_bytes(
        encoded[..VALUE_HEADER_LEN]
            .try_into()
            .expect("current value header has fixed width"),
    );
    let version = Version::new(raw)?;
    Ok((version, encoded[VALUE_HEADER_LEN..].to_vec()))
}

fn prefix_has_key(tree: &Tree, prefix: &[u8]) -> Result<bool, MetadataError> {
    for entry in tree.range().prefix(prefix) {
        match entry {
            Ok(RangeEntry::Key { .. }) | Ok(RangeEntry::CommonPrefix(_)) => return Ok(true),
            Ok(_) => continue,
            Err(err) => return Err(to_backend_error(err)),
        }
    }
    Ok(false)
}

fn watch_event_key(base: &[u8], version: Version, ordinal: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(base.len() + 16);
    key.extend_from_slice(base);
    key.extend_from_slice(&version.get().to_be_bytes());
    key.extend_from_slice(&(ordinal as u64).to_be_bytes());
    key
}

fn encode_dedupe_result(result: &CommitResult) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&result.commit_version.get().to_be_bytes());
    out.extend_from_slice(&(result.applied_mutations as u64).to_be_bytes());
    out.extend_from_slice(&(result.watch_events as u64).to_be_bytes());
    out
}

fn decode_dedupe_result(encoded: &[u8]) -> Result<CommitResult, MetadataError> {
    if encoded.len() != 24 {
        return Err(MetadataError::Backend(
            "encoded command dedupe result is malformed".to_owned(),
        ));
    }
    Ok(CommitResult {
        commit_version: Version::new(u64::from_be_bytes(encoded[0..8].try_into().unwrap()))?,
        applied_mutations: u64::from_be_bytes(encoded[8..16].try_into().unwrap()) as usize,
        watch_events: u64::from_be_bytes(encoded[16..24].try_into().unwrap()) as usize,
    })
}

fn to_backend_error(err: impl std::fmt::Display) -> MetadataError {
    MetadataError::Backend(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokv_fs_metastore::{
        CommandKind, MetadataCommand, Mutation, PredicateRef, ScanRequest, Value,
    };

    fn version(raw: u64) -> Version {
        Version::new(raw).unwrap()
    }

    fn put_command(key: &[u8], request_id: &[u8], value: &[u8], commit: u64) -> MetadataCommand {
        MetadataCommand {
            request_id: request_id.to_vec(),
            kind: CommandKind::CreateFile,
            read_version: version(commit - 1),
            commit_version: version(commit),
            primary_family: RecordFamily::Dentry,
            primary_key: key.to_vec(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Dentry,
                key: key.to_vec(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Dentry,
                key: key.to_vec(),
                op: MutationOp::Put,
                value: Some(Value(value.to_vec())),
            }],
            watch: Vec::new(),
        }
    }

    #[test]
    fn commit_put_then_get_and_scan() {
        let store = HoltMetadataStore::open_memory().unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
            .unwrap();

        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(2),
                    ReadPurpose::UserStrong
                )
                .unwrap(),
            Some(Value(b"value-a".to_vec()))
        );
        let scan = store
            .scan(ScanRequest {
                family: RecordFamily::Dentry,
                prefix: b"dir/".to_vec(),
                version: version(2),
                limit: 10,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap();
        assert_eq!(scan.len(), 1);
        assert_eq!(scan[0].key, b"dir/a");
    }

    #[test]
    fn predicate_failure_does_not_apply_any_mutation() {
        let store = HoltMetadataStore::open_memory().unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
            .unwrap();
        let failed = store.commit_metadata(put_command(b"dir/a", b"req-2", b"value-b", 3));
        assert_eq!(failed, Err(MetadataError::PredicateFailed));
        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(3),
                    ReadPurpose::UserStrong
                )
                .unwrap(),
            Some(Value(b"value-a".to_vec()))
        );
    }

    #[test]
    fn prefix_empty_predicate_uses_family_prefix() {
        let store = HoltMetadataStore::open_memory().unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
            .unwrap();
        let mut command = put_command(b"dir", b"req-2", b"directory", 3);
        command.predicates = vec![PredicateRef {
            family: RecordFamily::Dentry,
            key: b"dir/".to_vec(),
            predicate: Predicate::PrefixEmpty,
        }];
        assert_eq!(
            store.commit_metadata(command),
            Err(MetadataError::PredicateFailed)
        );
    }

    #[test]
    fn duplicate_request_id_returns_original_result() {
        let store = HoltMetadataStore::open_memory().unwrap();
        let first = store
            .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
            .unwrap();
        let duplicate = store
            .commit_metadata(put_command(b"dir/b", b"req-1", b"value-b", 3))
            .unwrap();
        assert_eq!(duplicate, first);
        assert!(store
            .get(
                RecordFamily::Dentry,
                b"dir/b",
                version(3),
                ReadPurpose::UserStrong
            )
            .unwrap()
            .is_none());
    }
}
