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
use holt::{
    CheckpointImage, DBAtomicBatch, Error as HoltError, KeyRangeEntryRef, KeyScanOutcome,
    RangeEntry, RecordVersion,
};
use holt::{Tree, TreeConfig, DB};
use nokv_types::RecordFamily;

mod codec;
mod families;
use codec::*;
use families::*;

#[derive(Clone)]
pub struct HoltMetadataStore {
    db: DB,
    stats: Arc<HoltMetadataStoreCounters>,
    active_snapshot_pins: Arc<AtomicU64>,
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
    scan_cache_hit_total: AtomicU64,
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

impl HoltMetadataStore {
    pub fn open_memory() -> Result<Self, MetadataError> {
        Self::open(TreeConfig::memory())
    }

    pub fn open_file(path: impl AsRef<Path>) -> Result<Self, MetadataError> {
        Self::open(TreeConfig::new(path.as_ref()))
    }

    pub fn open(config: TreeConfig) -> Result<Self, MetadataError> {
        let db = DB::open(config).map_err(to_backend_error)?;
        let active_snapshot_pins = count_active_snapshot_pins(&db)?;
        Ok(Self {
            db,
            stats: Arc::new(HoltMetadataStoreCounters::default()),
            active_snapshot_pins: Arc::new(AtomicU64::new(active_snapshot_pins)),
        })
    }

    pub fn checkpoint(&self) -> Result<(), MetadataError> {
        self.db.checkpoint().map_err(to_backend_error)?;
        self.reclaim_unreachable_storage()?;
        Ok(())
    }

    pub fn export_checkpoint_image(&self) -> Result<Vec<u8>, MetadataError> {
        self.db
            .export_checkpoint()
            .map(CheckpointImage::into_bytes)
            .map_err(to_backend_error)
    }

    pub fn install_checkpoint_image(&self, image: &[u8]) -> Result<(), MetadataError> {
        let checkpoint = CheckpointImage::from_bytes(image.to_vec());
        checkpoint.validate().map_err(to_backend_error)?;
        self.db
            .install_checkpoint(&checkpoint)
            .map_err(to_backend_error)?;
        self.active_snapshot_pins
            .store(count_active_snapshot_pins(&self.db)?, Ordering::Relaxed);
        Ok(())
    }

    pub fn reclaim_unreachable_storage(&self) -> Result<usize, MetadataError> {
        self.db.gc().map_err(to_backend_error)
    }

    fn current_tree(&self, family: RecordFamily) -> Result<Tree, MetadataError> {
        self.db
            .open_or_create_tree(current_tree_name(family))
            .map_err(to_backend_error)
    }

    fn history_tree(&self) -> Result<Tree, MetadataError> {
        self.db
            .open_or_create_tree(HISTORY_TREE)
            .map_err(to_backend_error)
    }

    fn ensure_metadata_trees(&self) -> Result<(), MetadataError> {
        for name in METADATA_TREE_NAMES {
            self.db
                .open_or_create_tree(name)
                .map_err(to_backend_error)?;
        }
        Ok(())
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

    fn reclaim_unreachable_storage(&self) -> Result<usize, MetadataError> {
        HoltMetadataStore::reclaim_unreachable_storage(self)
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

        let snapshot = current
            .snapshot(&request.prefix)
            .map_err(to_backend_error)?;
        let mut range = snapshot.view().range();
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

        let snapshot = current
            .snapshot(&request.prefix)
            .map_err(to_backend_error)?;
        let mut range = snapshot.view().range().delimiter(request.delimiter);
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
        let mut range = current.range_keys().prefix(&request.prefix);
        if let Some(start_after) = request.start_after.as_deref() {
            range = range.start_after(start_after);
        }
        let mut out = Vec::new();
        let outcome = range
            .visit_with_outcome(limit, |entry| {
                if let KeyRangeEntryRef::Key { key, .. } = entry {
                    out.push(key.to_vec());
                }
                Ok(())
            })
            .map_err(to_backend_error)?;
        self.stats.record_key_scan_outcome(outcome);
        Ok(out)
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

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        match self.prepare_command(&command)? {
            PreparedCommand::DedupeHit(result) => Ok(result),
            PreparedCommand::Planned(plan) => self.commit_planned_command(command, plan),
        }
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

        outcome.removed = self.delete_history_keys(&history, &keys_to_remove)?;
        Ok(outcome)
    }
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
        self.ensure_metadata_trees()?;
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
                    let count = self
                        .current_tree(predicate.family)?
                        .prefix_count(&predicate.key, 1)
                        .map_err(to_backend_error)?;
                    self.stats.record_key_scan_outcome(KeyScanOutcome {
                        stats: count.stats,
                        cache_hit: count.cache_hit,
                    });
                    if count.count > 0 {
                        return Err(MetadataError::PredicateFailed);
                    }
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
        self.ensure_metadata_trees()?;
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

    fn delete_history_keys(
        &self,
        history: &Tree,
        keys: &[Vec<u8>],
    ) -> Result<usize, MetadataError> {
        if keys.is_empty() {
            return Ok(0);
        }
        history
            .atomic(|batch| {
                for key in keys {
                    batch.delete(key);
                }
            })
            .map_err(to_backend_error)?;
        Ok(keys.len())
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

    fn record_key_scan_outcome(&self, outcome: KeyScanOutcome) {
        if outcome.cache_hit {
            self.scan_cache_hit_total.fetch_add(1, Ordering::Relaxed);
        }
        self.scan_key_visited_total
            .fetch_add(outcome.stats.visited, Ordering::Relaxed);
        self.scan_key_returned_total.fetch_add(
            outcome.stats.returned + outcome.stats.rollup,
            Ordering::Relaxed,
        );
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
            scan_cache_hit_total: self.scan_cache_hit_total.load(Ordering::Relaxed),
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

fn count_active_snapshot_pins(db: &DB) -> Result<u64, MetadataError> {
    let snapshot = match db.open_tree(SNAPSHOT_CURRENT_TREE) {
        Ok(snapshot) => snapshot,
        Err(HoltError::TreeNotFound { .. }) => return Ok(0),
        Err(err) => return Err(to_backend_error(err)),
    };
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
mod tests;
