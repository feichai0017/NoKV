//! Holt-backed metadata store for NoKV.
//!
//! This crate owns the mapping from storage-engine-neutral metadata commands to
//! Holt family trees. It does not own filesystem semantics, object storage,
//! Raft replication, FUSE, or protobuf types.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::command::{
    CommitResult, HistoryPruneOutcome, HistoryPruneRequest, MetadataCommand, MetadataError,
    MetadataStore, MetadataStoreStats, MetadataStoreStatsProvider, MutationOp, Predicate, ReadItem,
    ReadPurpose, ScanItem, ScanRequest, Value, Version,
};
use crate::layout::{history_key, history_prefix};
use holt::{RangeEntry, RecordVersion, Tree, TreeConfig, DB};
use nokvfs_types::RecordFamily;

const VALUE_HEADER_LEN: usize = 9;
const VALUE_KIND_LIVE: u8 = 1;
const VALUE_KIND_TOMBSTONE: u8 = 2;

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

const REQUIRED_TREES: [&str; 14] = [
    SYSTEM_CURRENT_TREE,
    MOUNT_CURRENT_TREE,
    INODE_CURRENT_TREE,
    DENTRY_CURRENT_TREE,
    PARENT_CURRENT_TREE,
    XATTR_CURRENT_TREE,
    CHUNK_MANIFEST_CURRENT_TREE,
    SESSION_CURRENT_TREE,
    PATH_INDEX_CURRENT_TREE,
    WATCH_CURRENT_TREE,
    SNAPSHOT_CURRENT_TREE,
    GC_CURRENT_TREE,
    COMMAND_DEDUPE_CURRENT_TREE,
    HISTORY_TREE,
];

#[derive(Clone)]
pub struct HoltMetadataStore {
    db: DB,
    write_gate: Arc<Mutex<()>>,
    stats: Arc<HoltMetadataStoreCounters>,
    active_snapshot_pins: Arc<AtomicU64>,
}

#[derive(Default)]
struct HoltMetadataStoreCounters {
    get_total: AtomicU64,
    scan_total: AtomicU64,
    scan_key_visited_total: AtomicU64,
    scan_key_returned_total: AtomicU64,
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

struct CurrentRecord {
    record_version: RecordVersion,
    metadata_version: Version,
    value: Option<Vec<u8>>,
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
        let active_snapshot_pins = count_active_snapshot_pins(&db)?;
        Ok(Self {
            db,
            write_gate: Arc::new(Mutex::new(())),
            stats: Arc::new(HoltMetadataStoreCounters::default()),
            active_snapshot_pins: Arc::new(AtomicU64::new(active_snapshot_pins)),
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
        _purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.stats.get_total.fetch_add(1, Ordering::Relaxed);
        read_visible(
            &self.current_tree(family)?,
            family,
            key,
            version,
            &self.history_tree()?,
        )
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.stats.scan_total.fetch_add(1, Ordering::Relaxed);
        let limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit
        };
        let current = self.current_tree(request.family)?;
        let history = self.history_tree()?;
        let start_after = request.start_after.as_deref();
        let mut out = Vec::new();

        if request.prefix.is_empty() {
            for entry in current.range() {
                let outcome = push_visible_scan_item(
                    entry,
                    request.family,
                    request.version,
                    &history,
                    &mut out,
                    limit,
                    start_after,
                )?;
                self.stats
                    .scan_key_visited_total
                    .fetch_add(outcome.visited as u64, Ordering::Relaxed);
                self.stats
                    .scan_key_returned_total
                    .fetch_add(outcome.returned as u64, Ordering::Relaxed);
                if outcome.done {
                    break;
                }
            }
        } else {
            for entry in current.range().prefix(&request.prefix) {
                let outcome = push_visible_scan_item(
                    entry,
                    request.family,
                    request.version,
                    &history,
                    &mut out,
                    limit,
                    start_after,
                )?;
                self.stats
                    .scan_key_visited_total
                    .fetch_add(outcome.visited as u64, Ordering::Relaxed);
                self.stats
                    .scan_key_returned_total
                    .fetch_add(outcome.returned as u64, Ordering::Relaxed);
                if outcome.done {
                    break;
                }
            }
        }
        Ok(out)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        command.validate()?;
        let dedupe = self.current_tree(RecordFamily::CommandDedupe)?;
        if let Some(encoded) = dedupe
            .get(&command.request_id)
            .map_err(to_backend_error)?
            .as_deref()
            .map(decode_dedupe_result)
            .transpose()?
        {
            self.stats.dedupe_hit_total.fetch_add(1, Ordering::Relaxed);
            return Ok(encoded);
        }

        let prepare_start = Instant::now();
        let plan = self.plan_command(&command)?;
        self.stats.commit_prepare_ns_total.fetch_add(
            prepare_start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        self.commit_planned_command(command, plan)
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        let _guard = self
            .write_gate
            .lock()
            .map_err(|_| MetadataError::Backend("holt metadata write gate poisoned".to_owned()))?;
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
            self.db
                .atomic(|batch| {
                    for key in &keys_to_remove {
                        batch.delete(HISTORY_TREE, key);
                    }
                })
                .map_err(to_backend_error)?;
        }
        Ok(outcome)
    }
}

impl HoltMetadataStore {
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
        let predicate_count = command.predicates.len() as u64;
        let prefix_empty_predicate_count = command
            .predicates
            .iter()
            .filter(|predicate| matches!(predicate.predicate, Predicate::PrefixEmpty))
            .count() as u64;
        let current_put_count = plan
            .mutations
            .iter()
            .filter(|planned| planned.mutation.op == MutationOp::Put)
            .count() as u64;
        let current_delete_count = plan
            .mutations
            .iter()
            .filter(|planned| planned.mutation.op == MutationOp::Delete)
            .count() as u64;
        let history_tombstone_count = plan
            .mutations
            .iter()
            .filter(|planned| {
                planned.mutation.op == MutationOp::Delete
                    && plan.retain_history
                    && family_requires_history(planned.mutation.family)
            })
            .count() as u64;
        let history_write_count = plan.history_records.len() as u64 + history_tombstone_count;
        let watch_write_count = command.watch.len() as u64;
        let snapshot_retention_delta = plan.snapshot_retention_delta;

        let mut applied = 0_usize;
        let mut watch_events = 0_usize;
        let result = CommitResult {
            commit_version: command.commit_version,
            applied_mutations: plan.mutations.len(),
            watch_events: command.watch.len(),
        };
        let dedupe_result = encode_dedupe_result(&result);

        let atomic_start = Instant::now();
        let committed = self
            .db
            .atomic(|batch| {
                for (family, key, current) in plan.history_records {
                    if let Ok((old_version, _)) = decode_current_value(&current) {
                        batch.put(
                            HISTORY_TREE,
                            &history_key(family, &key, old_version.get()),
                            &current,
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
                for guard in plan.version_guards {
                    batch.assert_version(
                        current_tree_name(guard.family),
                        &guard.key,
                        guard.version,
                    );
                }
                for guard in plan.prefix_empty_guards {
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
                    applied += 1;
                }
                for event in &command.watch {
                    let key = watch_event_key(&event.key, command.commit_version, watch_events);
                    batch.put(
                        WATCH_CURRENT_TREE,
                        &key,
                        &encode_current_value(command.commit_version, &event.event),
                    );
                    watch_events += 1;
                }
                batch.put_if_absent(
                    current_tree_name(RecordFamily::CommandDedupe),
                    &command.request_id,
                    &dedupe_result,
                );
            })
            .map_err(to_backend_error)?;
        self.stats.atomic_apply_ns_total.fetch_add(
            atomic_start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        if !committed {
            if let Some(encoded) = self
                .current_tree(RecordFamily::CommandDedupe)?
                .get(&command.request_id)
                .map_err(to_backend_error)?
                .as_deref()
                .map(decode_dedupe_result)
                .transpose()?
            {
                self.stats.dedupe_hit_total.fetch_add(1, Ordering::Relaxed);
                return Ok(encoded);
            }
            return Err(MetadataError::PredicateFailed);
        }

        self.apply_snapshot_retention_delta(snapshot_retention_delta);
        self.stats.commit_total.fetch_add(1, Ordering::Relaxed);
        self.stats
            .predicate_total
            .fetch_add(predicate_count, Ordering::Relaxed);
        self.stats
            .prefix_empty_predicate_total
            .fetch_add(prefix_empty_predicate_count, Ordering::Relaxed);
        self.stats
            .current_put_total
            .fetch_add(current_put_count, Ordering::Relaxed);
        self.stats
            .current_delete_total
            .fetch_add(current_delete_count, Ordering::Relaxed);
        self.stats
            .history_write_total
            .fetch_add(history_write_count, Ordering::Relaxed);
        self.stats
            .watch_write_total
            .fetch_add(watch_write_count, Ordering::Relaxed);
        self.stats
            .dedupe_write_total
            .fetch_add(1, Ordering::Relaxed);

        Ok(CommitResult {
            applied_mutations: applied,
            watch_events,
            ..result
        })
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
    fn snapshot(&self) -> MetadataStoreStats {
        MetadataStoreStats {
            get_total: self.get_total.load(Ordering::Relaxed),
            scan_total: self.scan_total.load(Ordering::Relaxed),
            scan_key_visited_total: self.scan_key_visited_total.load(Ordering::Relaxed),
            scan_key_returned_total: self.scan_key_returned_total.load(Ordering::Relaxed),
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
    history: &Tree,
) -> Result<Option<ReadItem>, MetadataError> {
    let encoded = current.get(key).map_err(to_backend_error)?;
    decode_visible_value(family, key, encoded.as_deref(), version, history).map(|value| {
        value.map(|(version, bytes)| ReadItem {
            value: Value(bytes),
            version,
        })
    })
}

fn decode_visible_value(
    family: RecordFamily,
    key: &[u8],
    encoded: Option<&[u8]>,
    version: Version,
    history: &Tree,
) -> Result<Option<(Version, Vec<u8>)>, MetadataError> {
    if let Some(encoded) = encoded {
        let (current_version, current_value) = decode_current_value(encoded)?;
        if current_version <= version {
            return Ok(current_value.map(|value| (current_version, value)));
        }
    }
    for entry in history.range().prefix(&history_prefix(family, key)) {
        let RangeEntry::Key { value, .. } = entry.map_err(to_backend_error)? else {
            continue;
        };
        let (history_version, history_value) = decode_current_value(&value)?;
        if history_version <= version {
            return Ok(history_value.map(|value| (history_version, value)));
        }
    }
    Ok(None)
}

struct ScanPushOutcome {
    done: bool,
    visited: usize,
    returned: usize,
}

fn push_visible_scan_item(
    entry: Result<RangeEntry, holt::Error>,
    family: RecordFamily,
    version: Version,
    history: &Tree,
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
    if let Some((commit, visible)) =
        decode_visible_value(family, &key, Some(&value), version, history)?
    {
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
        CommandKind, HistoryPruneRequest, MetadataCommand, Mutation, PredicateRef, ScanRequest,
        Value,
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
