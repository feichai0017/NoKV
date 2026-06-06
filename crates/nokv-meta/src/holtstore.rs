//! Holt-backed metadata store for NoKV.
//!
//! This crate owns the mapping from storage-engine-neutral metadata commands to
//! Holt family trees. It does not own filesystem semantics, object storage,
//! Raft replication, FUSE, or protobuf types.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::command::{
    metadata_commands_conflict, CommitResult, DelimitedScanItem, DelimitedScanRequest,
    HistoryPruneOutcome, HistoryPruneRequest, KeyScanRequest, MetadataCheckpointStore,
    MetadataCommand, MetadataError, MetadataStore, MetadataStoreStats, MetadataStoreStatsProvider,
    MutationOp, Predicate, ReadItem, ReadPurpose, ScanItem, ScanRequest, Value, Version,
};
use crate::layout::{history_key, history_prefix};
use holt::{DBAtomicBatch, KeyRangeEntry, RangeEntry, RecordVersion, Tree, TreeConfig, DB};
use nokv_types::RecordFamily;

const VALUE_HEADER_LEN: usize = 9;
const VALUE_KIND_LIVE: u8 = 1;
const VALUE_KIND_TOMBSTONE: u8 = 2;
const CHECKPOINT_IMAGE_MAGIC: &[u8; 8] = b"NKFSMI01";
const CHECKPOINT_IMAGE_VERSION: u8 = 1;

const SYSTEM_CURRENT_TREE: &str = "system_current";
const MOUNT_CURRENT_TREE: &str = "mount_current";
const INODE_CURRENT_TREE: &str = "inode_current";
const DENTRY_CURRENT_TREE: &str = "dentry_current";
const PARENT_CURRENT_TREE: &str = "parent_current";
const XATTR_CURRENT_TREE: &str = "xattr_current";
const CHUNK_MANIFEST_CURRENT_TREE: &str = "chunk_manifest_current";
const SESSION_CURRENT_TREE: &str = "session_current";
const PATH_INDEX_CURRENT_TREE: &str = "path_index_current";
const WATCH_CURRENT_TREE: &str = "watch_current";
const SNAPSHOT_CURRENT_TREE: &str = "snapshot_current";
const GC_CURRENT_TREE: &str = "gc_current";
const COMMAND_DEDUPE_CURRENT_TREE: &str = "command_dedupe_current";
const HISTORY_TREE: &str = "history";

const TREE_ID_SYSTEM_CURRENT: u8 = 1;
const TREE_ID_MOUNT_CURRENT: u8 = 2;
const TREE_ID_INODE_CURRENT: u8 = 3;
const TREE_ID_DENTRY_CURRENT: u8 = 4;
const TREE_ID_PARENT_CURRENT: u8 = 5;
const TREE_ID_XATTR_CURRENT: u8 = 6;
const TREE_ID_CHUNK_MANIFEST_CURRENT: u8 = 7;
const TREE_ID_SESSION_CURRENT: u8 = 8;
const TREE_ID_PATH_INDEX_CURRENT: u8 = 9;
const TREE_ID_WATCH_CURRENT: u8 = 10;
const TREE_ID_SNAPSHOT_CURRENT: u8 = 11;
const TREE_ID_GC_CURRENT: u8 = 12;
const TREE_ID_COMMAND_DEDUPE_CURRENT: u8 = 13;
const TREE_ID_HISTORY: u8 = 14;

#[derive(Clone)]
pub struct HoltMetadataStore {
    db: DB,
    trees: Arc<FamilyTrees>,
    stats: Arc<HoltMetadataStoreCounters>,
    active_snapshot_pins: Arc<AtomicU64>,
}

struct FamilyTrees {
    system_current: Tree,
    mount_current: Tree,
    inode_current: Tree,
    dentry_current: Tree,
    parent_current: Tree,
    xattr_current: Tree,
    chunk_manifest_current: Tree,
    session_current: Tree,
    path_index_current: Tree,
    watch_current: Tree,
    snapshot_current: Tree,
    gc_current: Tree,
    command_dedupe_current: Tree,
    history: Tree,
}

#[derive(Default)]
struct HoltMetadataStoreCounters {
    get_total: AtomicU64,
    get_user_strong_total: AtomicU64,
    get_write_plan_local_total: AtomicU64,
    get_snapshot_total: AtomicU64,
    scan_total: AtomicU64,
    scan_user_strong_total: AtomicU64,
    scan_write_plan_local_total: AtomicU64,
    scan_snapshot_total: AtomicU64,
    scan_key_visited_total: AtomicU64,
    scan_key_returned_total: AtomicU64,
    history_lookup_total: AtomicU64,
    commit_total: AtomicU64,
    dedupe_hit_total: AtomicU64,
    predicate_total: AtomicU64,
    prefix_empty_predicate_total: AtomicU64,
    current_put_total: AtomicU64,
    current_delete_total: AtomicU64,
    history_write_total: AtomicU64,
    watch_write_total: AtomicU64,
    dedupe_write_total: AtomicU64,
    commit_prepare_ns_total: AtomicU64,
    atomic_apply_total: AtomicU64,
    atomic_apply_command_total: AtomicU64,
    atomic_apply_max_batch: AtomicU64,
    atomic_apply_ns_total: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MutationGuard {
    Always,
    PutIfAbsent,
    CompareAndPut(RecordVersion),
    DeleteIfVersion(RecordVersion),
}

#[derive(Clone, Debug)]
struct PlannedMutation {
    mutation: crate::command::Mutation,
    guard: MutationGuard,
}

#[derive(Clone, Debug)]
struct VersionGuard {
    family: RecordFamily,
    key: Vec<u8>,
    version: RecordVersion,
}

#[derive(Clone, Debug)]
struct PrefixEmptyGuard {
    family: RecordFamily,
    prefix: Vec<u8>,
}

struct CommandPlan {
    mutations: Vec<PlannedMutation>,
    history_records: Vec<(RecordFamily, Vec<u8>, Vec<u8>)>,
    version_guards: Vec<VersionGuard>,
    prefix_empty_guards: Vec<PrefixEmptyGuard>,
    retain_history: bool,
    snapshot_retention_delta: i64,
}

struct PendingPlannedCommand {
    index: usize,
    command: MetadataCommand,
    plan: CommandPlan,
}

#[derive(Clone, Debug)]
struct PlannedCommandStats {
    predicate_count: u64,
    prefix_empty_predicate_count: u64,
    current_put_count: u64,
    current_delete_count: u64,
    history_write_count: u64,
    watch_write_count: u64,
    snapshot_retention_delta: i64,
    result: CommitResult,
    dedupe_result: Vec<u8>,
}

enum PreparedCommand {
    DedupeHit(CommitResult),
    Planned(CommandPlan),
}

struct CurrentRecord {
    record_version: RecordVersion,
    metadata_version: Version,
    value: Option<Vec<u8>>,
}

#[derive(Debug)]
struct CheckpointImageRecord {
    tree_id: u8,
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone)]
struct CheckpointTreeSpec {
    id: u8,
    tree: Tree,
}

#[derive(Clone, Copy)]
struct CheckpointTreeNameSpec {
    id: u8,
    name: &'static str,
}

impl HoltMetadataStore {
    pub fn open_memory() -> Result<Self, MetadataError> {
        Self::open(TreeConfig::memory())
    }

    pub fn open_raft_state_machine() -> Result<Self, MetadataError> {
        let mut config = TreeConfig::memory();
        config.memory_flush_on_write = false;
        Self::open(config)
    }

    pub fn open_file(path: impl AsRef<Path>) -> Result<Self, MetadataError> {
        Self::open(TreeConfig::new(path.as_ref()))
    }

    pub fn open(config: TreeConfig) -> Result<Self, MetadataError> {
        let db = DB::open(config).map_err(to_backend_error)?;
        let trees = Arc::new(open_family_trees(&db)?);
        let active_snapshot_pins = count_active_snapshot_pins(&db)?;
        Ok(Self {
            db,
            trees,
            stats: Arc::new(HoltMetadataStoreCounters::default()),
            active_snapshot_pins: Arc::new(AtomicU64::new(active_snapshot_pins)),
        })
    }

    pub fn checkpoint(&self) -> Result<(), MetadataError> {
        self.db.checkpoint().map_err(to_backend_error)
    }

    pub fn export_checkpoint_image(&self) -> Result<Vec<u8>, MetadataError> {
        let specs = checkpoint_tree_specs();
        let scopes = specs
            .iter()
            .map(|spec| (spec.name, b"".as_slice()))
            .collect::<Vec<_>>();
        self.db
            .view(&scopes, |view| {
                let mut records = Vec::new();
                for spec in specs {
                    let tree = view.tree(spec.name).ok_or(holt::Error::Internal(
                        "metadata checkpoint view omitted tree",
                    ))?;
                    for entry in tree.range() {
                        let RangeEntry::Key { key, value, .. } = entry? else {
                            continue;
                        };
                        records.push(CheckpointImageRecord {
                            tree_id: spec.id,
                            key,
                            value,
                        });
                    }
                }
                Ok(records)
            })
            .map_err(to_backend_error)
            .and_then(|records| encode_checkpoint_image(&records))
    }

    pub fn install_checkpoint_image(&self, image: &[u8]) -> Result<(), MetadataError> {
        let records = decode_checkpoint_image(image)?;
        let existing = self.current_checkpoint_keys()?;
        let committed = self
            .db
            .atomic(|batch| {
                for (tree_id, key) in &existing {
                    batch.delete(checkpoint_tree_name(*tree_id), key);
                }
                for record in &records {
                    batch.put(
                        checkpoint_tree_name(record.tree_id),
                        &record.key,
                        &record.value,
                    );
                }
            })
            .map_err(to_backend_error)?;
        if !committed {
            return Err(MetadataError::Backend(
                "metadata checkpoint image install did not commit".to_owned(),
            ));
        }
        self.active_snapshot_pins
            .store(count_active_snapshot_pins(&self.db)?, Ordering::Relaxed);
        Ok(())
    }

    fn current_checkpoint_keys(&self) -> Result<Vec<(u8, Vec<u8>)>, MetadataError> {
        let mut keys = Vec::new();
        for spec in self.trees.checkpoint_trees() {
            for entry in spec.tree.range_keys() {
                let KeyRangeEntry::Key { key, .. } = entry.map_err(to_backend_error)? else {
                    continue;
                };
                keys.push((spec.id, key));
            }
        }
        Ok(keys)
    }

    fn current_tree(&self, family: RecordFamily) -> Result<Tree, MetadataError> {
        self.trees.current(family)
    }

    fn history_tree(&self) -> Result<Tree, MetadataError> {
        Ok(self.trees.history.clone())
    }

    fn current_live_record(
        &self,
        family: RecordFamily,
        key: &[u8],
    ) -> Result<Option<(RecordVersion, Version, Vec<u8>)>, MetadataError> {
        let Some(record) = self.current_record(family, key)? else {
            return Ok(None);
        };
        Ok(record
            .value
            .map(|value| (record.record_version, record.metadata_version, value)))
    }

    fn current_record(
        &self,
        family: RecordFamily,
        key: &[u8],
    ) -> Result<Option<CurrentRecord>, MetadataError> {
        let Some(record) = self
            .current_tree(family)?
            .get_record(key)
            .map_err(to_backend_error)?
        else {
            return Ok(None);
        };
        let (version, value) = decode_current_value(&record.value)?;
        Ok(Some(CurrentRecord {
            record_version: record.version,
            metadata_version: version,
            value,
        }))
    }
}

impl MetadataCheckpointStore for HoltMetadataStore {
    fn checkpoint(&self) -> Result<(), MetadataError> {
        HoltMetadataStore::checkpoint(self)
    }

    fn export_checkpoint_image(&self) -> Result<Vec<u8>, MetadataError> {
        HoltMetadataStore::export_checkpoint_image(self)
    }

    fn install_checkpoint_image(&self, image: &[u8]) -> Result<(), MetadataError> {
        HoltMetadataStore::install_checkpoint_image(self, image)
    }
}

impl MetadataStoreStatsProvider for HoltMetadataStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        let mut stats = self.stats.snapshot();
        stats.active_snapshot_pin_total = self.active_snapshot_pins.load(Ordering::Relaxed);
        stats
    }
}

impl MetadataStore for HoltMetadataStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.stats.get_total.fetch_add(1, Ordering::Relaxed);
        self.stats.record_get_purpose(purpose);
        read_visible(
            &self.current_tree(family)?,
            family,
            key,
            version,
            purpose,
            &self.history_tree()?,
            &self.stats,
        )
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.stats.scan_total.fetch_add(1, Ordering::Relaxed);
        self.stats.record_scan_purpose(request.purpose);
        let limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit
        };
        let current = self.current_tree(request.family)?;
        let history = self.history_tree()?;
        let start_after = request.start_after.as_deref();
        let mut out = Vec::new();
        let mut visited_total = 0_u64;
        let mut returned_total = 0_u64;
        let context = VisibleReadContext {
            family: request.family,
            version: request.version,
            purpose: request.purpose,
            history: &history,
            stats: &self.stats,
        };

        let mut range = current.range();
        if !request.prefix.is_empty() {
            range = range.prefix(&request.prefix);
        }
        if let Some(start_after) = start_after {
            range = range.start_after(start_after);
        }
        for entry in range {
            let outcome = push_visible_scan_item(entry, &context, &mut out, limit, start_after)?;
            visited_total += outcome.visited as u64;
            returned_total += outcome.returned as u64;
            if outcome.done {
                break;
            }
        }
        self.stats
            .scan_key_visited_total
            .fetch_add(visited_total, Ordering::Relaxed);
        self.stats
            .scan_key_returned_total
            .fetch_add(returned_total, Ordering::Relaxed);
        Ok(out)
    }

    fn scan_delimited(
        &self,
        request: DelimitedScanRequest,
    ) -> Result<Vec<DelimitedScanItem>, MetadataError> {
        self.stats.scan_total.fetch_add(1, Ordering::Relaxed);
        self.stats.record_scan_purpose(request.purpose);
        let limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit
        };
        let current = self.current_tree(request.family)?;
        let history = self.history_tree()?;
        let start_after = request.start_after.as_deref();
        let mut out = Vec::new();
        let mut visited_total = 0_u64;
        let mut returned_total = 0_u64;
        let context = VisibleReadContext {
            family: request.family,
            version: request.version,
            purpose: request.purpose,
            history: &history,
            stats: &self.stats,
        };

        let mut range = current.range().delimiter(request.delimiter);
        if !request.prefix.is_empty() {
            range = range.prefix(&request.prefix);
        }
        if let Some(start_after) = start_after {
            range = range.start_after(start_after);
        }
        for entry in range {
            let outcome =
                push_visible_delimited_scan_item(entry, &context, &mut out, limit, start_after)?;
            visited_total += outcome.visited as u64;
            returned_total += outcome.returned as u64;
            if outcome.done {
                break;
            }
        }
        self.stats
            .scan_key_visited_total
            .fetch_add(visited_total, Ordering::Relaxed);
        self.stats
            .scan_key_returned_total
            .fetch_add(returned_total, Ordering::Relaxed);
        Ok(out)
    }

    fn scan_keys(&self, request: KeyScanRequest) -> Result<Vec<Vec<u8>>, MetadataError> {
        self.stats.scan_total.fetch_add(1, Ordering::Relaxed);
        self.stats.record_scan_purpose(request.purpose);
        let limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit
        };
        let current = self.current_tree(request.family)?;
        let mut range = current.range_keys();
        if !request.prefix.is_empty() {
            range = range.prefix(&request.prefix);
        }
        if let Some(start_after) = request.start_after.as_deref() {
            range = range.start_after(start_after);
        }
        let mut out = Vec::new();
        let mut visited_total = 0_u64;
        for entry in range {
            let KeyRangeEntry::Key { key, .. } = entry.map_err(to_backend_error)? else {
                continue;
            };
            visited_total += 1;
            out.push(key);
            if out.len() >= limit {
                break;
            }
        }
        self.stats
            .scan_key_visited_total
            .fetch_add(visited_total, Ordering::Relaxed);
        self.stats
            .scan_key_returned_total
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        Ok(out)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        match self.prepare_command(&command)? {
            PreparedCommand::DedupeHit(result) => Ok(result),
            PreparedCommand::Planned(plan) => self.commit_planned_command(command, plan),
        }
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        let mut results = vec![None; commands.len()];
        let mut pending = Vec::new();
        for (index, command) in commands.iter().cloned().enumerate() {
            if pending.iter().any(|pending: &PendingPlannedCommand| {
                metadata_commands_conflict(&pending.command, &command)
            }) {
                self.commit_pending_batch(&mut pending, &mut results);
            }
            match self.prepare_command(&command) {
                Ok(PreparedCommand::DedupeHit(result)) => results[index] = Some(Ok(result)),
                Ok(PreparedCommand::Planned(plan)) => {
                    if plan.snapshot_retention_delta != 0 {
                        self.commit_pending_batch(&mut pending, &mut results);
                        results[index] = Some(self.commit_planned_command(command, plan));
                    } else {
                        pending.push(PendingPlannedCommand {
                            index,
                            command,
                            plan,
                        });
                    }
                }
                Err(err) => results[index] = Some(Err(err)),
            }
        }
        self.commit_pending_batch(&mut pending, &mut results);
        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(MetadataError::Backend(
                        "holt batch result was not recorded".to_owned(),
                    ))
                })
            })
            .collect()
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.current_tree(RecordFamily::CommandDedupe)?
            .get(request_id)
            .map_err(to_backend_error)?
            .as_deref()
            .map(decode_dedupe_result)
            .transpose()
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        let remove_limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit
        };
        let history = self.history_tree()?;
        let mut outcome = HistoryPruneOutcome::default();
        let mut keys_to_remove = Vec::new();
        let mut current_prefix = Vec::new();
        let mut kept_anchor_below_floor = false;

        for entry in history.range() {
            let RangeEntry::Key { key, value, .. } = entry.map_err(to_backend_error)? else {
                continue;
            };
            let prefix = history_user_prefix(&key)?;
            if prefix != current_prefix.as_slice() {
                current_prefix.clear();
                current_prefix.extend_from_slice(prefix);
                kept_anchor_below_floor = false;
            }
            let (version, _) = decode_current_value(&value)?;
            outcome.scanned += 1;
            let remove = match request.retain_from {
                None => true,
                Some(floor) if version >= floor => {
                    outcome.retained_by_snapshots += 1;
                    false
                }
                Some(_) if !kept_anchor_below_floor => {
                    kept_anchor_below_floor = true;
                    outcome.retained_by_snapshots += 1;
                    false
                }
                Some(_) => true,
            };
            if remove {
                keys_to_remove.push(key);
                if keys_to_remove.len() >= remove_limit {
                    break;
                }
            }
        }

        outcome.removed = keys_to_remove.len();
        if !keys_to_remove.is_empty() {
            history
                .atomic(|batch| {
                    for key in &keys_to_remove {
                        batch.delete(key);
                    }
                })
                .map_err(to_backend_error)?;
        }
        Ok(outcome)
    }
}

impl FamilyTrees {
    fn current(&self, family: RecordFamily) -> Result<Tree, MetadataError> {
        let tree = match family {
            RecordFamily::System => &self.system_current,
            RecordFamily::Mount => &self.mount_current,
            RecordFamily::Inode => &self.inode_current,
            RecordFamily::Dentry => &self.dentry_current,
            RecordFamily::Parent => &self.parent_current,
            RecordFamily::Xattr => &self.xattr_current,
            RecordFamily::ChunkManifest => &self.chunk_manifest_current,
            RecordFamily::Session => &self.session_current,
            RecordFamily::PathIndex => &self.path_index_current,
            RecordFamily::Watch => &self.watch_current,
            RecordFamily::Snapshot => &self.snapshot_current,
            RecordFamily::Gc => &self.gc_current,
            RecordFamily::CommandDedupe => &self.command_dedupe_current,
            RecordFamily::History => &self.history,
        };
        Ok(tree.clone())
    }

    fn checkpoint_trees(&self) -> Vec<CheckpointTreeSpec> {
        checkpoint_tree_specs()
            .iter()
            .map(|spec| CheckpointTreeSpec {
                id: spec.id,
                tree: self.checkpoint_tree(spec.id),
            })
            .collect()
    }

    fn checkpoint_tree(&self, tree_id: u8) -> Tree {
        match tree_id {
            TREE_ID_SYSTEM_CURRENT => self.system_current.clone(),
            TREE_ID_MOUNT_CURRENT => self.mount_current.clone(),
            TREE_ID_INODE_CURRENT => self.inode_current.clone(),
            TREE_ID_DENTRY_CURRENT => self.dentry_current.clone(),
            TREE_ID_PARENT_CURRENT => self.parent_current.clone(),
            TREE_ID_XATTR_CURRENT => self.xattr_current.clone(),
            TREE_ID_CHUNK_MANIFEST_CURRENT => self.chunk_manifest_current.clone(),
            TREE_ID_SESSION_CURRENT => self.session_current.clone(),
            TREE_ID_PATH_INDEX_CURRENT => self.path_index_current.clone(),
            TREE_ID_WATCH_CURRENT => self.watch_current.clone(),
            TREE_ID_SNAPSHOT_CURRENT => self.snapshot_current.clone(),
            TREE_ID_GC_CURRENT => self.gc_current.clone(),
            TREE_ID_COMMAND_DEDUPE_CURRENT => self.command_dedupe_current.clone(),
            TREE_ID_HISTORY => self.history.clone(),
            _ => unreachable!("checkpoint tree registry returned unknown tree id"),
        }
    }
}

fn checkpoint_tree_specs() -> &'static [CheckpointTreeNameSpec] {
    &[
        CheckpointTreeNameSpec {
            id: TREE_ID_SYSTEM_CURRENT,
            name: SYSTEM_CURRENT_TREE,
        },
        CheckpointTreeNameSpec {
            id: TREE_ID_MOUNT_CURRENT,
            name: MOUNT_CURRENT_TREE,
        },
        CheckpointTreeNameSpec {
            id: TREE_ID_INODE_CURRENT,
            name: INODE_CURRENT_TREE,
        },
        CheckpointTreeNameSpec {
            id: TREE_ID_DENTRY_CURRENT,
            name: DENTRY_CURRENT_TREE,
        },
        CheckpointTreeNameSpec {
            id: TREE_ID_PARENT_CURRENT,
            name: PARENT_CURRENT_TREE,
        },
        CheckpointTreeNameSpec {
            id: TREE_ID_XATTR_CURRENT,
            name: XATTR_CURRENT_TREE,
        },
        CheckpointTreeNameSpec {
            id: TREE_ID_CHUNK_MANIFEST_CURRENT,
            name: CHUNK_MANIFEST_CURRENT_TREE,
        },
        CheckpointTreeNameSpec {
            id: TREE_ID_SESSION_CURRENT,
            name: SESSION_CURRENT_TREE,
        },
        CheckpointTreeNameSpec {
            id: TREE_ID_PATH_INDEX_CURRENT,
            name: PATH_INDEX_CURRENT_TREE,
        },
        CheckpointTreeNameSpec {
            id: TREE_ID_WATCH_CURRENT,
            name: WATCH_CURRENT_TREE,
        },
        CheckpointTreeNameSpec {
            id: TREE_ID_SNAPSHOT_CURRENT,
            name: SNAPSHOT_CURRENT_TREE,
        },
        CheckpointTreeNameSpec {
            id: TREE_ID_GC_CURRENT,
            name: GC_CURRENT_TREE,
        },
        CheckpointTreeNameSpec {
            id: TREE_ID_COMMAND_DEDUPE_CURRENT,
            name: COMMAND_DEDUPE_CURRENT_TREE,
        },
        CheckpointTreeNameSpec {
            id: TREE_ID_HISTORY,
            name: HISTORY_TREE,
        },
    ]
}

fn checkpoint_tree_name(tree_id: u8) -> &'static str {
    checkpoint_tree_specs()
        .iter()
        .find(|spec| spec.id == tree_id)
        .map(|spec| spec.name)
        .expect("validated checkpoint tree id")
}

fn encode_checkpoint_image(records: &[CheckpointImageRecord]) -> Result<Vec<u8>, MetadataError> {
    let mut out = Vec::new();
    out.extend_from_slice(CHECKPOINT_IMAGE_MAGIC);
    out.push(CHECKPOINT_IMAGE_VERSION);
    out.extend_from_slice(&(records.len() as u64).to_be_bytes());
    for record in records {
        validate_checkpoint_tree_id(record.tree_id)?;
        let key_len = u32::try_from(record.key.len()).map_err(|_| {
            MetadataError::Backend("metadata checkpoint key exceeds u32 length".to_owned())
        })?;
        let value_len = u32::try_from(record.value.len()).map_err(|_| {
            MetadataError::Backend("metadata checkpoint value exceeds u32 length".to_owned())
        })?;
        out.push(record.tree_id);
        out.extend_from_slice(&key_len.to_be_bytes());
        out.extend_from_slice(&value_len.to_be_bytes());
        out.extend_from_slice(&record.key);
        out.extend_from_slice(&record.value);
    }
    Ok(out)
}

fn decode_checkpoint_image(image: &[u8]) -> Result<Vec<CheckpointImageRecord>, MetadataError> {
    let mut cursor = CheckpointImageCursor::new(image);
    let magic = cursor.take(CHECKPOINT_IMAGE_MAGIC.len())?;
    if magic != CHECKPOINT_IMAGE_MAGIC {
        return Err(checkpoint_image_error("bad checkpoint image magic"));
    }
    let version = cursor.read_u8()?;
    if version != CHECKPOINT_IMAGE_VERSION {
        return Err(checkpoint_image_error(
            "unsupported checkpoint image version",
        ));
    }
    let count = cursor.read_u64()?;
    let count = usize::try_from(count)
        .map_err(|_| checkpoint_image_error("checkpoint image record count overflows usize"))?;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let tree_id = cursor.read_u8()?;
        validate_checkpoint_tree_id(tree_id)?;
        let key_len = cursor.read_u32()? as usize;
        let value_len = cursor.read_u32()? as usize;
        let key = cursor.take(key_len)?.to_vec();
        let value = cursor.take(value_len)?.to_vec();
        records.push(CheckpointImageRecord {
            tree_id,
            key,
            value,
        });
    }
    if !cursor.is_empty() {
        return Err(checkpoint_image_error(
            "checkpoint image has trailing bytes",
        ));
    }
    Ok(records)
}

fn validate_checkpoint_tree_id(tree_id: u8) -> Result<(), MetadataError> {
    if checkpoint_tree_specs()
        .iter()
        .any(|spec| spec.id == tree_id)
    {
        Ok(())
    } else {
        Err(checkpoint_image_error(
            "checkpoint image references unknown tree",
        ))
    }
}

struct CheckpointImageCursor<'a> {
    remaining: &'a [u8],
}

impl<'a> CheckpointImageCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], MetadataError> {
        if self.remaining.len() < len {
            return Err(checkpoint_image_error("truncated checkpoint image"));
        }
        let (head, tail) = self.remaining.split_at(len);
        self.remaining = tail;
        Ok(head)
    }

    fn read_u8(&mut self) -> Result<u8, MetadataError> {
        Ok(self.take(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, MetadataError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes(
            bytes.try_into().expect("slice length checked by take"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, MetadataError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes(
            bytes.try_into().expect("slice length checked by take"),
        ))
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
}

fn checkpoint_image_error(message: &str) -> MetadataError {
    MetadataError::Backend(format!("metadata checkpoint image: {message}"))
}

fn open_family_trees(db: &DB) -> Result<FamilyTrees, MetadataError> {
    Ok(FamilyTrees {
        system_current: open_or_create_tree(db, SYSTEM_CURRENT_TREE)?,
        mount_current: open_or_create_tree(db, MOUNT_CURRENT_TREE)?,
        inode_current: open_or_create_tree(db, INODE_CURRENT_TREE)?,
        dentry_current: open_or_create_tree(db, DENTRY_CURRENT_TREE)?,
        parent_current: open_or_create_tree(db, PARENT_CURRENT_TREE)?,
        xattr_current: open_or_create_tree(db, XATTR_CURRENT_TREE)?,
        chunk_manifest_current: open_or_create_tree(db, CHUNK_MANIFEST_CURRENT_TREE)?,
        session_current: open_or_create_tree(db, SESSION_CURRENT_TREE)?,
        path_index_current: open_or_create_tree(db, PATH_INDEX_CURRENT_TREE)?,
        watch_current: open_or_create_tree(db, WATCH_CURRENT_TREE)?,
        snapshot_current: open_or_create_tree(db, SNAPSHOT_CURRENT_TREE)?,
        gc_current: open_or_create_tree(db, GC_CURRENT_TREE)?,
        command_dedupe_current: open_or_create_tree(db, COMMAND_DEDUPE_CURRENT_TREE)?,
        history: open_or_create_tree(db, HISTORY_TREE)?,
    })
}

fn open_or_create_tree(db: &DB, name: &str) -> Result<Tree, MetadataError> {
    db.open_or_create_tree(name).map_err(to_backend_error)
}

impl HoltMetadataStore {
    fn prepare_command(&self, command: &MetadataCommand) -> Result<PreparedCommand, MetadataError> {
        command.validate()?;
        if let Some(result) = self.dedupe_result(&command.request_id)? {
            self.stats.dedupe_hit_total.fetch_add(1, Ordering::Relaxed);
            return Ok(PreparedCommand::DedupeHit(result));
        }

        let prepare_start = Instant::now();
        let plan = self.plan_command(command)?;
        self.stats.commit_prepare_ns_total.fetch_add(
            prepare_start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        Ok(PreparedCommand::Planned(plan))
    }

    fn dedupe_result(&self, request_id: &[u8]) -> Result<Option<CommitResult>, MetadataError> {
        self.current_tree(RecordFamily::CommandDedupe)?
            .get(request_id)
            .map_err(to_backend_error)?
            .as_deref()
            .map(decode_dedupe_result)
            .transpose()
    }

    fn commit_pending_batch(
        &self,
        pending: &mut Vec<PendingPlannedCommand>,
        results: &mut [Option<Result<CommitResult, MetadataError>>],
    ) {
        if pending.is_empty() {
            return;
        }
        let batch = std::mem::take(pending);
        match self.commit_planned_batch(&batch) {
            Ok(Some(committed)) => {
                for (item, result) in batch.into_iter().zip(committed) {
                    results[item.index] = Some(Ok(result));
                }
            }
            Ok(None) => {
                for item in batch {
                    results[item.index] = Some(self.commit_metadata(item.command));
                }
            }
            Err(err) => {
                for item in batch {
                    results[item.index] = Some(Err(err.clone()));
                }
            }
        }
    }

    fn commit_planned_batch(
        &self,
        batch_items: &[PendingPlannedCommand],
    ) -> Result<Option<Vec<CommitResult>>, MetadataError> {
        let stats = batch_items
            .iter()
            .map(|item| planned_command_stats(&item.command, &item.plan))
            .collect::<Vec<_>>();
        let atomic_start = Instant::now();
        let committed = self
            .db
            .atomic(|batch| {
                for (item, stats) in batch_items.iter().zip(&stats) {
                    enqueue_planned_command(batch, &item.command, &item.plan, &stats.dedupe_result);
                }
            })
            .map_err(to_backend_error)?;
        self.stats
            .record_atomic_apply(batch_items.len(), atomic_start.elapsed());
        if !committed {
            return Ok(None);
        }

        for stats in &stats {
            self.record_committed_stats(stats);
        }
        Ok(Some(stats.into_iter().map(|stats| stats.result).collect()))
    }

    fn plan_command(&self, command: &MetadataCommand) -> Result<CommandPlan, MetadataError> {
        let mut mutations = command
            .mutations
            .iter()
            .cloned()
            .map(|mutation| PlannedMutation {
                mutation,
                guard: MutationGuard::Always,
            })
            .collect::<Vec<_>>();
        let mut version_guards = Vec::new();
        let mut prefix_empty_guards = Vec::new();

        for predicate in &command.predicates {
            match predicate.predicate {
                Predicate::Exists => {
                    let (record_version, _, _) = self
                        .current_live_record(predicate.family, &predicate.key)?
                        .ok_or(MetadataError::PredicateFailed)?;
                    apply_record_version_guard(
                        &mut mutations,
                        &mut version_guards,
                        predicate.family,
                        &predicate.key,
                        record_version,
                    )?;
                }
                Predicate::NotExists => {
                    let index = mutation_index(&mutations, predicate.family, &predicate.key)
                        .ok_or(MetadataError::PredicateFailed)?;
                    if mutations[index].mutation.op != MutationOp::Put {
                        return Err(MetadataError::PredicateFailed);
                    }
                    match self.current_record(predicate.family, &predicate.key)? {
                        None => {
                            set_mutation_guard(&mut mutations[index], MutationGuard::PutIfAbsent)?;
                        }
                        Some(record) if record.value.is_none() => {
                            set_mutation_guard(
                                &mut mutations[index],
                                MutationGuard::CompareAndPut(record.record_version),
                            )?;
                        }
                        Some(_) => return Err(MetadataError::PredicateFailed),
                    }
                }
                Predicate::PrefixEmpty => {
                    prefix_empty_guards.push(PrefixEmptyGuard {
                        family: predicate.family,
                        prefix: predicate.key.clone(),
                    });
                }
                Predicate::VersionEquals(expected) => {
                    let (record_version, actual, _) = self
                        .current_live_record(predicate.family, &predicate.key)?
                        .ok_or(MetadataError::PredicateFailed)?;
                    if actual != expected {
                        return Err(MetadataError::PredicateFailed);
                    }
                    apply_record_version_guard(
                        &mut mutations,
                        &mut version_guards,
                        predicate.family,
                        &predicate.key,
                        record_version,
                    )?;
                }
            }
        }

        let retain_history = self.has_active_history_retention();
        let snapshot_retention_delta = self.snapshot_retention_delta(&mutations)?;
        let mut history_records = Vec::new();
        if retain_history {
            for planned in &mutations {
                if !family_requires_history(planned.mutation.family) {
                    continue;
                }
                if let Some(current) = self
                    .current_tree(planned.mutation.family)?
                    .get(&planned.mutation.key)
                    .map_err(to_backend_error)?
                {
                    history_records.push((
                        planned.mutation.family,
                        planned.mutation.key.clone(),
                        current,
                    ));
                }
            }
        }

        Ok(CommandPlan {
            mutations,
            history_records,
            version_guards,
            prefix_empty_guards,
            retain_history,
            snapshot_retention_delta,
        })
    }

    fn commit_planned_command(
        &self,
        command: MetadataCommand,
        plan: CommandPlan,
    ) -> Result<CommitResult, MetadataError> {
        let stats = planned_command_stats(&command, &plan);
        let atomic_start = Instant::now();
        let committed = self
            .db
            .atomic(|batch| {
                enqueue_planned_command(batch, &command, &plan, &stats.dedupe_result);
            })
            .map_err(to_backend_error)?;
        self.stats.record_atomic_apply(1, atomic_start.elapsed());
        if !committed {
            if let Some(encoded) = self.dedupe_result(&command.request_id)? {
                self.stats.dedupe_hit_total.fetch_add(1, Ordering::Relaxed);
                return Ok(encoded);
            }
            return Err(MetadataError::PredicateFailed);
        }

        self.record_committed_stats(&stats);
        Ok(stats.result)
    }

    fn record_committed_stats(&self, stats: &PlannedCommandStats) {
        self.apply_snapshot_retention_delta(stats.snapshot_retention_delta);
        self.stats.commit_total.fetch_add(1, Ordering::Relaxed);
        self.stats
            .predicate_total
            .fetch_add(stats.predicate_count, Ordering::Relaxed);
        self.stats
            .prefix_empty_predicate_total
            .fetch_add(stats.prefix_empty_predicate_count, Ordering::Relaxed);
        self.stats
            .current_put_total
            .fetch_add(stats.current_put_count, Ordering::Relaxed);
        self.stats
            .current_delete_total
            .fetch_add(stats.current_delete_count, Ordering::Relaxed);
        self.stats
            .history_write_total
            .fetch_add(stats.history_write_count, Ordering::Relaxed);
        self.stats
            .watch_write_total
            .fetch_add(stats.watch_write_count, Ordering::Relaxed);
        self.stats
            .dedupe_write_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot_retention_delta(
        &self,
        mutations: &[PlannedMutation],
    ) -> Result<i64, MetadataError> {
        let mut delta = 0_i64;
        for planned in mutations
            .iter()
            .filter(|planned| planned.mutation.family == RecordFamily::Snapshot)
        {
            let old_active = self
                .current_record(RecordFamily::Snapshot, &planned.mutation.key)?
                .is_some_and(|record| record.value.is_some());
            let new_active = planned.mutation.op == MutationOp::Put;
            delta += i64::from(new_active) - i64::from(old_active);
        }
        Ok(delta)
    }

    fn has_active_history_retention(&self) -> bool {
        self.active_snapshot_pins.load(Ordering::Relaxed) > 0
    }

    fn apply_snapshot_retention_delta(&self, delta: i64) {
        if delta > 0 {
            self.active_snapshot_pins
                .fetch_add(delta as u64, Ordering::Relaxed);
            return;
        }
        if delta < 0 {
            let decrement = delta.unsigned_abs();
            let _ = self.active_snapshot_pins.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| Some(current.saturating_sub(decrement)),
            );
        }
    }
}

impl HoltMetadataStoreCounters {
    fn record_get_purpose(&self, purpose: ReadPurpose) {
        match purpose {
            ReadPurpose::UserStrong => &self.get_user_strong_total,
            ReadPurpose::WritePlanLocal => &self.get_write_plan_local_total,
            ReadPurpose::Snapshot => &self.get_snapshot_total,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn record_scan_purpose(&self, purpose: ReadPurpose) {
        match purpose {
            ReadPurpose::UserStrong => &self.scan_user_strong_total,
            ReadPurpose::WritePlanLocal => &self.scan_write_plan_local_total,
            ReadPurpose::Snapshot => &self.scan_snapshot_total,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn record_atomic_apply(&self, command_count: usize, elapsed: std::time::Duration) {
        self.atomic_apply_total.fetch_add(1, Ordering::Relaxed);
        self.atomic_apply_command_total
            .fetch_add(command_count as u64, Ordering::Relaxed);
        self.atomic_apply_max_batch
            .fetch_max(command_count as u64, Ordering::Relaxed);
        self.atomic_apply_ns_total.fetch_add(
            elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn snapshot(&self) -> MetadataStoreStats {
        MetadataStoreStats {
            get_total: self.get_total.load(Ordering::Relaxed),
            get_user_strong_total: self.get_user_strong_total.load(Ordering::Relaxed),
            get_write_plan_local_total: self.get_write_plan_local_total.load(Ordering::Relaxed),
            get_snapshot_total: self.get_snapshot_total.load(Ordering::Relaxed),
            scan_total: self.scan_total.load(Ordering::Relaxed),
            scan_user_strong_total: self.scan_user_strong_total.load(Ordering::Relaxed),
            scan_write_plan_local_total: self.scan_write_plan_local_total.load(Ordering::Relaxed),
            scan_snapshot_total: self.scan_snapshot_total.load(Ordering::Relaxed),
            scan_key_visited_total: self.scan_key_visited_total.load(Ordering::Relaxed),
            scan_key_returned_total: self.scan_key_returned_total.load(Ordering::Relaxed),
            history_lookup_total: self.history_lookup_total.load(Ordering::Relaxed),
            active_snapshot_pin_total: 0,
            commit_total: self.commit_total.load(Ordering::Relaxed),
            dedupe_hit_total: self.dedupe_hit_total.load(Ordering::Relaxed),
            predicate_total: self.predicate_total.load(Ordering::Relaxed),
            prefix_empty_predicate_total: self.prefix_empty_predicate_total.load(Ordering::Relaxed),
            current_put_total: self.current_put_total.load(Ordering::Relaxed),
            current_delete_total: self.current_delete_total.load(Ordering::Relaxed),
            history_write_total: self.history_write_total.load(Ordering::Relaxed),
            watch_write_total: self.watch_write_total.load(Ordering::Relaxed),
            dedupe_write_total: self.dedupe_write_total.load(Ordering::Relaxed),
            commit_prepare_ns_total: self.commit_prepare_ns_total.load(Ordering::Relaxed),
            atomic_apply_total: self.atomic_apply_total.load(Ordering::Relaxed),
            atomic_apply_command_total: self.atomic_apply_command_total.load(Ordering::Relaxed),
            atomic_apply_max_batch: self.atomic_apply_max_batch.load(Ordering::Relaxed),
            atomic_apply_ns_total: self.atomic_apply_ns_total.load(Ordering::Relaxed),
        }
    }
}

fn current_tree_name(family: RecordFamily) -> &'static str {
    match family {
        RecordFamily::System => SYSTEM_CURRENT_TREE,
        RecordFamily::Mount => MOUNT_CURRENT_TREE,
        RecordFamily::Inode => INODE_CURRENT_TREE,
        RecordFamily::Dentry => DENTRY_CURRENT_TREE,
        RecordFamily::Parent => PARENT_CURRENT_TREE,
        RecordFamily::Xattr => XATTR_CURRENT_TREE,
        RecordFamily::ChunkManifest => CHUNK_MANIFEST_CURRENT_TREE,
        RecordFamily::Session => SESSION_CURRENT_TREE,
        RecordFamily::PathIndex => PATH_INDEX_CURRENT_TREE,
        RecordFamily::Watch => WATCH_CURRENT_TREE,
        RecordFamily::Snapshot => SNAPSHOT_CURRENT_TREE,
        RecordFamily::Gc => GC_CURRENT_TREE,
        RecordFamily::CommandDedupe => COMMAND_DEDUPE_CURRENT_TREE,
        RecordFamily::History => HISTORY_TREE,
    }
}

fn count_active_snapshot_pins(db: &DB) -> Result<u64, MetadataError> {
    let snapshot = db
        .open_tree(SNAPSHOT_CURRENT_TREE)
        .map_err(to_backend_error)?;
    let mut total = 0_u64;
    for entry in snapshot.range() {
        let RangeEntry::Key { value, .. } = entry.map_err(to_backend_error)? else {
            continue;
        };
        if decode_current_value(&value)?.1.is_some() {
            total += 1;
        }
    }
    Ok(total)
}

fn family_requires_history(family: RecordFamily) -> bool {
    !matches!(
        family,
        RecordFamily::System | RecordFamily::CommandDedupe | RecordFamily::Watch | RecordFamily::Gc
    )
}

fn read_visible(
    current: &Tree,
    family: RecordFamily,
    key: &[u8],
    version: Version,
    purpose: ReadPurpose,
    history: &Tree,
    stats: &HoltMetadataStoreCounters,
) -> Result<Option<ReadItem>, MetadataError> {
    let encoded = current.get(key).map_err(to_backend_error)?;
    let context = VisibleReadContext {
        family,
        version,
        purpose,
        history,
        stats,
    };
    decode_visible_value(key, encoded.as_deref(), &context).map(|value| {
        value.map(|(version, bytes)| ReadItem {
            value: Value(bytes),
            version,
        })
    })
}

fn planned_command_stats(command: &MetadataCommand, plan: &CommandPlan) -> PlannedCommandStats {
    let history_tombstone_count = plan
        .mutations
        .iter()
        .filter(|planned| {
            planned.mutation.op == MutationOp::Delete
                && plan.retain_history
                && family_requires_history(planned.mutation.family)
        })
        .count() as u64;
    let result = CommitResult {
        commit_version: command.commit_version,
        applied_mutations: plan.mutations.len(),
        watch_events: command.watch.len(),
    };
    let dedupe_result = encode_dedupe_result(&result);
    PlannedCommandStats {
        predicate_count: command.predicates.len() as u64,
        prefix_empty_predicate_count: command
            .predicates
            .iter()
            .filter(|predicate| matches!(predicate.predicate, Predicate::PrefixEmpty))
            .count() as u64,
        current_put_count: plan
            .mutations
            .iter()
            .filter(|planned| planned.mutation.op == MutationOp::Put)
            .count() as u64,
        current_delete_count: plan
            .mutations
            .iter()
            .filter(|planned| planned.mutation.op == MutationOp::Delete)
            .count() as u64,
        history_write_count: plan.history_records.len() as u64 + history_tombstone_count,
        watch_write_count: command.watch.len() as u64,
        snapshot_retention_delta: plan.snapshot_retention_delta,
        result,
        dedupe_result,
    }
}

fn enqueue_planned_command(
    batch: &mut DBAtomicBatch,
    command: &MetadataCommand,
    plan: &CommandPlan,
    dedupe_result: &[u8],
) {
    for (family, key, current) in &plan.history_records {
        if let Ok((old_version, _)) = decode_current_value(current) {
            batch.put(
                HISTORY_TREE,
                &history_key(*family, key, old_version.get()),
                current,
            );
        }
    }
    for planned in &plan.mutations {
        if planned.mutation.op == MutationOp::Delete
            && plan.retain_history
            && family_requires_history(planned.mutation.family)
        {
            batch.put(
                HISTORY_TREE,
                &history_key(
                    planned.mutation.family,
                    &planned.mutation.key,
                    command.commit_version.get(),
                ),
                &encode_tombstone_value(command.commit_version),
            );
        }
    }
    for guard in &plan.version_guards {
        batch.assert_version(current_tree_name(guard.family), &guard.key, guard.version);
    }
    for guard in &plan.prefix_empty_guards {
        batch.assert_prefix_empty(current_tree_name(guard.family), &guard.prefix);
    }
    for planned in &plan.mutations {
        match (planned.mutation.op, planned.guard) {
            (MutationOp::Put, MutationGuard::Always) => {
                let value = planned
                    .mutation
                    .value
                    .as_ref()
                    .expect("validated put mutation has a value");
                batch.put(
                    current_tree_name(planned.mutation.family),
                    &planned.mutation.key,
                    &encode_current_value(command.commit_version, &value.0),
                );
            }
            (MutationOp::Put, MutationGuard::PutIfAbsent) => {
                let value = planned
                    .mutation
                    .value
                    .as_ref()
                    .expect("validated put mutation has a value");
                batch.put_if_absent(
                    current_tree_name(planned.mutation.family),
                    &planned.mutation.key,
                    &encode_current_value(command.commit_version, &value.0),
                );
            }
            (MutationOp::Put, MutationGuard::CompareAndPut(version)) => {
                let value = planned
                    .mutation
                    .value
                    .as_ref()
                    .expect("validated put mutation has a value");
                batch.compare_and_put(
                    current_tree_name(planned.mutation.family),
                    &planned.mutation.key,
                    version,
                    &encode_current_value(command.commit_version, &value.0),
                );
            }
            (MutationOp::Put, MutationGuard::DeleteIfVersion(_)) => {
                unreachable!("put mutation cannot use delete guard")
            }
            (MutationOp::Delete, MutationGuard::Always) => {
                batch.delete(
                    current_tree_name(planned.mutation.family),
                    &planned.mutation.key,
                );
            }
            (MutationOp::Delete, MutationGuard::DeleteIfVersion(version)) => {
                batch.delete_if_version(
                    current_tree_name(planned.mutation.family),
                    &planned.mutation.key,
                    version,
                );
            }
            (MutationOp::Delete, MutationGuard::PutIfAbsent)
            | (MutationOp::Delete, MutationGuard::CompareAndPut(_)) => {
                unreachable!("delete mutation cannot use put guard")
            }
        }
    }
    for (ordinal, event) in command.watch.iter().enumerate() {
        let key = watch_event_key(&event.key, command.commit_version, ordinal);
        batch.put(
            WATCH_CURRENT_TREE,
            &key,
            &encode_current_value(command.commit_version, &event.event),
        );
    }
    batch.put_if_absent(
        current_tree_name(RecordFamily::CommandDedupe),
        &command.request_id,
        dedupe_result,
    );
}

fn decode_visible_value(
    key: &[u8],
    encoded: Option<&[u8]>,
    context: &VisibleReadContext<'_>,
) -> Result<Option<(Version, Vec<u8>)>, MetadataError> {
    if let Some(encoded) = encoded {
        let (current_version, current_value) = decode_current_value(encoded)?;
        if current_version <= context.version {
            return Ok(current_value.map(|value| (current_version, value)));
        }
    } else if context.purpose != ReadPurpose::Snapshot {
        return Ok(None);
    }
    context
        .stats
        .history_lookup_total
        .fetch_add(1, Ordering::Relaxed);
    for entry in context
        .history
        .range()
        .prefix(&history_prefix(context.family, key))
    {
        let RangeEntry::Key { value, .. } = entry.map_err(to_backend_error)? else {
            continue;
        };
        let (history_version, history_value) = decode_current_value(&value)?;
        if history_version <= context.version {
            return Ok(history_value.map(|value| (history_version, value)));
        }
    }
    Ok(None)
}

struct VisibleReadContext<'a> {
    family: RecordFamily,
    version: Version,
    purpose: ReadPurpose,
    history: &'a Tree,
    stats: &'a HoltMetadataStoreCounters,
}

struct ScanPushOutcome {
    done: bool,
    visited: usize,
    returned: usize,
}

fn push_visible_scan_item(
    entry: Result<RangeEntry, holt::Error>,
    context: &VisibleReadContext<'_>,
    out: &mut Vec<ScanItem>,
    limit: usize,
    start_after: Option<&[u8]>,
) -> Result<ScanPushOutcome, MetadataError> {
    let RangeEntry::Key { key, value, .. } = entry.map_err(to_backend_error)? else {
        return Ok(ScanPushOutcome {
            done: false,
            visited: 0,
            returned: 0,
        });
    };
    if start_after.is_some_and(|start_after| key.as_slice() <= start_after) {
        return Ok(ScanPushOutcome {
            done: false,
            visited: 1,
            returned: 0,
        });
    }
    let mut returned = 0_usize;
    if let Some((commit, visible)) = decode_visible_value(&key, Some(&value), context)? {
        out.push(ScanItem {
            key,
            value: Value(visible),
            version: commit,
        });
        returned = 1;
    }
    Ok(ScanPushOutcome {
        done: out.len() >= limit,
        visited: 1,
        returned,
    })
}

fn push_visible_delimited_scan_item(
    entry: Result<RangeEntry, holt::Error>,
    context: &VisibleReadContext<'_>,
    out: &mut Vec<DelimitedScanItem>,
    limit: usize,
    start_after: Option<&[u8]>,
) -> Result<ScanPushOutcome, MetadataError> {
    match entry.map_err(to_backend_error)? {
        RangeEntry::Key { key, value, .. } => {
            if start_after.is_some_and(|start_after| key.as_slice() <= start_after) {
                return Ok(ScanPushOutcome {
                    done: false,
                    visited: 1,
                    returned: 0,
                });
            }
            let mut returned = 0_usize;
            if let Some((commit, visible)) = decode_visible_value(&key, Some(&value), context)? {
                out.push(DelimitedScanItem::Key(ScanItem {
                    key,
                    value: Value(visible),
                    version: commit,
                }));
                returned = 1;
            }
            Ok(ScanPushOutcome {
                done: out.len() >= limit,
                visited: 1,
                returned,
            })
        }
        RangeEntry::CommonPrefix(prefix) => {
            if start_after.is_some_and(|start_after| prefix.as_slice() <= start_after) {
                return Ok(ScanPushOutcome {
                    done: false,
                    visited: 1,
                    returned: 0,
                });
            }
            out.push(DelimitedScanItem::CommonPrefix(prefix));
            Ok(ScanPushOutcome {
                done: out.len() >= limit,
                visited: 1,
                returned: 1,
            })
        }
        _ => Ok(ScanPushOutcome {
            done: false,
            visited: 0,
            returned: 0,
        }),
    }
}

fn encode_current_value(version: Version, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(VALUE_HEADER_LEN + value.len());
    out.extend_from_slice(&version.get().to_be_bytes());
    out.push(VALUE_KIND_LIVE);
    out.extend_from_slice(value);
    out
}

fn encode_tombstone_value(version: Version) -> Vec<u8> {
    let mut out = Vec::with_capacity(VALUE_HEADER_LEN);
    out.extend_from_slice(&version.get().to_be_bytes());
    out.push(VALUE_KIND_TOMBSTONE);
    out
}

fn decode_current_value(encoded: &[u8]) -> Result<(Version, Option<Vec<u8>>), MetadataError> {
    if encoded.len() < VALUE_HEADER_LEN {
        return Err(MetadataError::Backend(
            "encoded current metadata value is truncated".to_owned(),
        ));
    }
    let raw = u64::from_be_bytes(
        encoded[..8]
            .try_into()
            .expect("current value header has fixed width"),
    );
    let version = Version::new(raw)?;
    match encoded[8] {
        VALUE_KIND_LIVE => Ok((version, Some(encoded[VALUE_HEADER_LEN..].to_vec()))),
        VALUE_KIND_TOMBSTONE => {
            if encoded.len() != VALUE_HEADER_LEN {
                return Err(MetadataError::Backend(
                    "encoded tombstone metadata value has trailing bytes".to_owned(),
                ));
            }
            Ok((version, None))
        }
        tag => Err(MetadataError::Backend(format!(
            "encoded metadata value has unknown kind {tag}"
        ))),
    }
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

fn history_user_prefix(key: &[u8]) -> Result<&[u8], MetadataError> {
    if key.len() <= std::mem::size_of::<u64>() {
        return Err(MetadataError::Backend(
            "history key is missing version suffix".to_owned(),
        ));
    }
    Ok(&key[..key.len() - std::mem::size_of::<u64>()])
}

fn to_backend_error(err: impl std::fmt::Display) -> MetadataError {
    MetadataError::Backend(err.to_string())
}

fn mutation_index(
    mutations: &[PlannedMutation],
    family: RecordFamily,
    key: &[u8],
) -> Option<usize> {
    mutations
        .iter()
        .position(|planned| planned.mutation.family == family && planned.mutation.key == key)
}

fn apply_record_version_guard(
    mutations: &mut [PlannedMutation],
    version_guards: &mut Vec<VersionGuard>,
    family: RecordFamily,
    key: &[u8],
    record_version: RecordVersion,
) -> Result<(), MetadataError> {
    if let Some(index) = mutation_index(mutations, family, key) {
        let guard = match mutations[index].mutation.op {
            MutationOp::Put => MutationGuard::CompareAndPut(record_version),
            MutationOp::Delete => MutationGuard::DeleteIfVersion(record_version),
        };
        set_mutation_guard(&mut mutations[index], guard)?;
    } else {
        version_guards.push(VersionGuard {
            family,
            key: key.to_vec(),
            version: record_version,
        });
    }
    Ok(())
}

fn set_mutation_guard(
    planned: &mut PlannedMutation,
    guard: MutationGuard,
) -> Result<(), MetadataError> {
    match (planned.guard, guard) {
        (MutationGuard::Always, guard) => {
            planned.guard = guard;
            Ok(())
        }
        (current, requested) if current == requested => Ok(()),
        _ => Err(MetadataError::Backend(
            "metadata command has conflicting mutation guards".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{
        CommandKind, DelimitedScanItem, DelimitedScanRequest, HistoryPruneRequest, MetadataCommand,
        Mutation, PredicateRef, ScanRequest, Value,
    };
    use std::sync::{Arc, Barrier};
    use std::thread;

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

    fn replace_command(
        key: &[u8],
        request_id: &[u8],
        value: &[u8],
        read: u64,
        commit: u64,
    ) -> MetadataCommand {
        MetadataCommand {
            request_id: request_id.to_vec(),
            kind: CommandKind::RenameReplace,
            read_version: version(read),
            commit_version: version(commit),
            primary_family: RecordFamily::Dentry,
            primary_key: key.to_vec(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Dentry,
                key: key.to_vec(),
                predicate: Predicate::Exists,
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

    fn snapshot_pin_command(request_id: &[u8], commit: u64) -> MetadataCommand {
        MetadataCommand {
            request_id: request_id.to_vec(),
            kind: CommandKind::SnapshotSubtree,
            read_version: version(commit - 1),
            commit_version: version(commit),
            primary_family: RecordFamily::Snapshot,
            primary_key: b"snapshot/1".to_vec(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Snapshot,
                key: b"snapshot/1".to_vec(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Snapshot,
                key: b"snapshot/1".to_vec(),
                op: MutationOp::Put,
                value: Some(Value(b"pin".to_vec())),
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
                start_after: None,
                version: version(2),
                limit: 10,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap();
        assert_eq!(scan.len(), 1);
        assert_eq!(scan[0].key, b"dir/a");
    }

    #[test]
    fn scan_start_after_skips_prior_prefix_keys() {
        let store = HoltMetadataStore::open_memory().unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
            .unwrap();
        store
            .commit_metadata(put_command(b"dir/b", b"req-2", b"value-b", 3))
            .unwrap();
        store
            .commit_metadata(put_command(b"dir/c", b"req-3", b"value-c", 4))
            .unwrap();

        let before = store.metadata_store_stats();
        let scan = store
            .scan(ScanRequest {
                family: RecordFamily::Dentry,
                prefix: b"dir/".to_vec(),
                start_after: Some(b"dir/a".to_vec()),
                version: version(4),
                limit: 1,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap();

        assert_eq!(scan.len(), 1);
        assert_eq!(scan[0].key, b"dir/b");
        let after = store.metadata_store_stats();
        assert_eq!(
            after.scan_key_visited_total - before.scan_key_visited_total,
            1
        );
        assert_eq!(
            after.scan_key_returned_total - before.scan_key_returned_total,
            1
        );
    }

    #[test]
    fn scan_delimited_uses_engine_common_prefix_rollup() {
        let store = HoltMetadataStore::open_memory().unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
            .unwrap();
        store
            .commit_metadata(put_command(b"dir/sub/b", b"req-2", b"value-b", 3))
            .unwrap();
        store
            .commit_metadata(put_command(b"dir/sub/c", b"req-3", b"value-c", 4))
            .unwrap();

        let before = store.metadata_store_stats();
        let scan = store
            .scan_delimited(DelimitedScanRequest {
                family: RecordFamily::Dentry,
                prefix: b"dir/".to_vec(),
                start_after: None,
                delimiter: b'/',
                version: version(4),
                limit: 10,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap();

        assert_eq!(
            scan,
            vec![
                DelimitedScanItem::Key(ScanItem {
                    key: b"dir/a".to_vec(),
                    value: Value(b"value-a".to_vec()),
                    version: version(2),
                }),
                DelimitedScanItem::CommonPrefix(b"dir/sub/".to_vec()),
            ]
        );
        let after = store.metadata_store_stats();
        assert_eq!(
            after.scan_key_returned_total - before.scan_key_returned_total,
            2
        );
    }

    #[test]
    fn scan_keys_uses_key_only_range_with_start_after() {
        let store = HoltMetadataStore::open_memory().unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
            .unwrap();
        store
            .commit_metadata(put_command(b"dir/b", b"req-2", b"value-b", 3))
            .unwrap();
        store
            .commit_metadata(put_command(b"dir/c", b"req-3", b"value-c", 4))
            .unwrap();

        let before = store.metadata_store_stats();
        let keys = store
            .scan_keys(KeyScanRequest {
                family: RecordFamily::Dentry,
                prefix: b"dir/".to_vec(),
                start_after: Some(b"dir/a".to_vec()),
                limit: 1,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap();

        assert_eq!(keys, vec![b"dir/b".to_vec()]);
        let after = store.metadata_store_stats();
        assert_eq!(
            after.scan_key_visited_total - before.scan_key_visited_total,
            1
        );
        assert_eq!(
            after.scan_key_returned_total - before.scan_key_returned_total,
            1
        );
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
    fn independent_batch_commits_disjoint_commands() {
        let store = HoltMetadataStore::open_memory().unwrap();
        let results = store.commit_independent_batch(&[
            put_command(b"dir/a", b"req-1", b"value-a", 2),
            put_command(b"dir/b", b"req-2", b"value-b", 3),
        ]);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap().commit_version, version(2));
        assert_eq!(results[1].as_ref().unwrap().commit_version, version(3));
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
        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/b",
                    version(3),
                    ReadPurpose::UserStrong
                )
                .unwrap(),
            Some(Value(b"value-b".to_vec()))
        );
        let stats = store.metadata_store_stats();
        assert_eq!(stats.commit_total, 2);
        assert_eq!(stats.atomic_apply_total, 1);
        assert_eq!(stats.atomic_apply_command_total, 2);
        assert_eq!(stats.atomic_apply_max_batch, 2);
    }

    #[test]
    fn independent_batch_preserves_conflict_result_boundary() {
        let store = HoltMetadataStore::open_memory().unwrap();
        let results = store.commit_independent_batch(&[
            put_command(b"dir/a", b"req-1", b"value-a", 2),
            put_command(b"dir/a", b"req-2", b"value-b", 3),
            put_command(b"dir/b", b"req-3", b"value-c", 4),
        ]);

        assert_eq!(results.len(), 3);
        assert!(results[0].is_ok());
        assert_eq!(results[1], Err(MetadataError::PredicateFailed));
        assert!(results[2].is_ok());
        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(4),
                    ReadPurpose::UserStrong
                )
                .unwrap(),
            Some(Value(b"value-a".to_vec()))
        );
        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/b",
                    version(4),
                    ReadPurpose::UserStrong
                )
                .unwrap(),
            Some(Value(b"value-c".to_vec()))
        );
        let stats = store.metadata_store_stats();
        assert_eq!(stats.commit_total, 2);
        assert_eq!(stats.atomic_apply_total, 2);
        assert_eq!(stats.atomic_apply_command_total, 2);
        assert_eq!(stats.atomic_apply_max_batch, 1);
    }

    #[test]
    fn independent_batch_isolates_snapshot_retention_changes() {
        let store = HoltMetadataStore::open_memory().unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
            .unwrap();

        let results = store.commit_independent_batch(&[
            snapshot_pin_command(b"snapshot-1", 3),
            replace_command(b"dir/a", b"req-2", b"value-b", 2, 4),
        ]);

        assert!(results.iter().all(Result::is_ok));
        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(2),
                    ReadPurpose::Snapshot
                )
                .unwrap(),
            Some(Value(b"value-a".to_vec()))
        );
        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(4),
                    ReadPurpose::UserStrong
                )
                .unwrap(),
            Some(Value(b"value-b".to_vec()))
        );
        assert!(store.metadata_store_stats().history_write_total > 0);
    }

    #[test]
    fn checkpoint_image_round_trips_current_history_and_dedupe() {
        let store = HoltMetadataStore::open_memory().unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
            .unwrap();
        store
            .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
            .unwrap();
        let replace = store
            .commit_metadata(replace_command(b"dir/a", b"req-2", b"value-b", 2, 4))
            .unwrap();

        let image = store.export_checkpoint_image().unwrap();
        let restored = HoltMetadataStore::open_memory().unwrap();
        restored
            .commit_metadata(put_command(b"stale/key", b"stale", b"stale-value", 2))
            .unwrap();
        restored.install_checkpoint_image(&image).unwrap();

        assert_eq!(
            restored
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(4),
                    ReadPurpose::UserStrong
                )
                .unwrap(),
            Some(Value(b"value-b".to_vec()))
        );
        assert_eq!(
            restored
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(2),
                    ReadPurpose::Snapshot
                )
                .unwrap(),
            Some(Value(b"value-a".to_vec()))
        );
        assert_eq!(
            restored
                .get(
                    RecordFamily::Dentry,
                    b"stale/key",
                    version(4),
                    ReadPurpose::UserStrong
                )
                .unwrap(),
            None
        );
        assert_eq!(
            restored.committed_request_result(b"req-2").unwrap(),
            Some(replace)
        );
        assert_eq!(restored.metadata_store_stats().active_snapshot_pin_total, 1);
    }

    #[test]
    fn checkpoint_image_rejects_malformed_bytes() {
        let store = HoltMetadataStore::open_memory().unwrap();
        assert!(store.install_checkpoint_image(b"not-a-checkpoint").is_err());

        let mut image = store.export_checkpoint_image().unwrap();
        image.push(1);
        assert!(store.install_checkpoint_image(&image).is_err());
    }

    #[test]
    fn deleted_key_is_hidden_latest_but_visible_to_old_version() {
        let store = HoltMetadataStore::open_memory().unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
            .unwrap();
        store
            .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
            .unwrap();
        store
            .commit_metadata(MetadataCommand {
                request_id: b"req-delete".to_vec(),
                kind: CommandKind::RemoveFile,
                read_version: version(3),
                commit_version: version(4),
                primary_family: RecordFamily::Dentry,
                primary_key: b"dir/a".to_vec(),
                predicates: vec![PredicateRef {
                    family: RecordFamily::Dentry,
                    key: b"dir/a".to_vec(),
                    predicate: Predicate::Exists,
                }],
                mutations: vec![Mutation {
                    family: RecordFamily::Dentry,
                    key: b"dir/a".to_vec(),
                    op: MutationOp::Delete,
                    value: None,
                }],
                watch: Vec::new(),
            })
            .unwrap();

        let before_latest = store.metadata_store_stats();
        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(4),
                    ReadPurpose::UserStrong
                )
                .unwrap(),
            None
        );
        let after_latest = store.metadata_store_stats();
        assert_eq!(
            after_latest.history_lookup_total - before_latest.history_lookup_total,
            0,
            "live current-missing reads should not scan history"
        );
        let before_snapshot = store.metadata_store_stats();
        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(2),
                    ReadPurpose::Snapshot
                )
                .unwrap(),
            Some(Value(b"value-a".to_vec()))
        );
        let after_snapshot = store.metadata_store_stats();
        assert_eq!(
            after_snapshot.history_lookup_total - before_snapshot.history_lookup_total,
            1,
            "snapshot reads must retain historical visibility"
        );
    }

    #[test]
    fn not_exists_allows_recreate_after_tombstone() {
        let store = HoltMetadataStore::open_memory().unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
            .unwrap();
        store
            .commit_metadata(MetadataCommand {
                request_id: b"req-delete".to_vec(),
                kind: CommandKind::RemoveFile,
                read_version: version(2),
                commit_version: version(3),
                primary_family: RecordFamily::Dentry,
                primary_key: b"dir/a".to_vec(),
                predicates: vec![PredicateRef {
                    family: RecordFamily::Dentry,
                    key: b"dir/a".to_vec(),
                    predicate: Predicate::Exists,
                }],
                mutations: vec![Mutation {
                    family: RecordFamily::Dentry,
                    key: b"dir/a".to_vec(),
                    op: MutationOp::Delete,
                    value: None,
                }],
                watch: Vec::new(),
            })
            .unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-2", b"value-b", 4))
            .unwrap();

        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(4),
                    ReadPurpose::UserStrong
                )
                .unwrap(),
            Some(Value(b"value-b".to_vec()))
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

    #[test]
    fn concurrent_duplicate_request_id_commits_once() {
        let store = HoltMetadataStore::open_memory().unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let left_store = store.clone();
        let left_barrier = Arc::clone(&barrier);
        let left = thread::spawn(move || {
            left_barrier.wait();
            left_store.commit_metadata(put_command(b"dir/a", b"req-shared", b"value-a", 2))
        });
        let right_store = store.clone();
        let right = thread::spawn(move || {
            barrier.wait();
            right_store.commit_metadata(put_command(b"dir/b", b"req-shared", b"value-b", 3))
        });

        let left = left.join().unwrap().unwrap();
        let right = right.join().unwrap().unwrap();
        assert_eq!(left, right);

        let a = store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(3),
                ReadPurpose::UserStrong,
            )
            .unwrap();
        let b = store
            .get(
                RecordFamily::Dentry,
                b"dir/b",
                version(3),
                ReadPurpose::UserStrong,
            )
            .unwrap();
        assert_ne!(a.is_some(), b.is_some());
    }

    #[test]
    fn concurrent_not_exists_commits_one_writer() {
        let store = HoltMetadataStore::open_memory().unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let left_store = store.clone();
        let left_barrier = Arc::clone(&barrier);
        let left = thread::spawn(move || {
            left_barrier.wait();
            left_store.commit_metadata(put_command(b"dir/a", b"req-left", b"value-a", 2))
        });
        let right_store = store.clone();
        let right = thread::spawn(move || {
            barrier.wait();
            right_store.commit_metadata(put_command(b"dir/a", b"req-right", b"value-b", 3))
        });

        let outcomes = [left.join().unwrap(), right.join().unwrap()];
        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Err(MetadataError::PredicateFailed)))
                .count(),
            1
        );
        assert!(store
            .get(
                RecordFamily::Dentry,
                b"dir/a",
                version(3),
                ReadPurpose::UserStrong,
            )
            .unwrap()
            .is_some());
    }

    #[test]
    fn hot_path_skips_history_without_snapshot_retention() {
        let store = HoltMetadataStore::open_memory().unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
            .unwrap();
        store
            .commit_metadata(replace_command(b"dir/a", b"req-2", b"value-b", 2, 3))
            .unwrap();

        assert_eq!(store.metadata_store_stats().history_write_total, 0);
        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(2),
                    ReadPurpose::Snapshot
                )
                .unwrap(),
            None
        );
        let outcome = store
            .prune_history(HistoryPruneRequest {
                retain_from: None,
                limit: 100,
            })
            .unwrap();
        assert_eq!(outcome.removed, 0);
        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(2),
                    ReadPurpose::Snapshot
                )
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(3),
                    ReadPurpose::UserStrong
                )
                .unwrap(),
            Some(Value(b"value-b".to_vec()))
        );
    }

    #[test]
    fn prune_history_keeps_snapshot_floor_anchor_per_key() {
        let store = HoltMetadataStore::open_memory().unwrap();
        store
            .commit_metadata(put_command(b"dir/a", b"req-1", b"value-a", 2))
            .unwrap();
        store
            .commit_metadata(snapshot_pin_command(b"snapshot-1", 3))
            .unwrap();
        store
            .commit_metadata(replace_command(b"dir/a", b"req-2", b"value-b", 2, 4))
            .unwrap();
        store
            .commit_metadata(replace_command(b"dir/a", b"req-3", b"value-c", 4, 5))
            .unwrap();

        assert_eq!(store.metadata_store_stats().history_write_total, 2);

        let outcome = store
            .prune_history(HistoryPruneRequest {
                retain_from: Some(version(5)),
                limit: 100,
            })
            .unwrap();
        assert_eq!(outcome.scanned, 2);
        assert_eq!(outcome.removed, 1);
        assert_eq!(outcome.retained_by_snapshots, 1);
        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(4),
                    ReadPurpose::Snapshot
                )
                .unwrap(),
            Some(Value(b"value-b".to_vec()))
        );
        assert_eq!(
            store
                .get(
                    RecordFamily::Dentry,
                    b"dir/a",
                    version(2),
                    ReadPurpose::Snapshot
                )
                .unwrap(),
            None
        );
    }
}
