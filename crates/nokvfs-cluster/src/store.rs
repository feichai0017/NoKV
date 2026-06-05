use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use std::time::Instant;

use nokvfs_meta::command::{
    metadata_commands_conflict, CommitResult, DelimitedScanItem, DelimitedScanRequest,
    HistoryPruneOutcome, HistoryPruneRequest, KeyScanRequest, MetadataCommand, MetadataError,
    MetadataStore, MetadataStoreStats, MetadataStoreStatsProvider, MutationOp, Predicate, ReadItem,
    ReadPurpose, ScanItem, ScanRequest, Version,
};
use nokvfs_types::{MountId, RecordFamily};

use crate::{
    AppliedFrontierStore, AppliedMetadataCommand, ApplyFrontier, CheckpointFrontier,
    DurableReceipt, LogIndex, LogPosition, LogTerm, MemoryAppliedFrontierStore, MetadataGroup,
    MetadataLogSink, ReadFreshness, ReplayDriver, ReplayError, ReplayOutcome, SharedLogError,
    SharedMetadataLog,
};

#[derive(Debug)]
pub struct SharedLogMetadataStore<M, L, F = MemoryAppliedFrontierStore> {
    store: M,
    log: L,
    term: LogTerm,
    mount: MountId,
    frontier_store: F,
    apply_gate: Mutex<()>,
    applied_frontier: Mutex<Option<ApplyFrontier>>,
    runtime_stats: SharedLogRuntimeCounters,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SharedLogRuntimeStats {
    pub commit_entry_total: u64,
    pub commit_command_total: u64,
    pub max_commands_per_entry: u64,
    pub precheck_command_total: u64,
    pub precheck_predicate_total: u64,
    pub precheck_ns_total: u64,
    pub stale_read_total: u64,
}

#[derive(Debug, Default)]
struct SharedLogRuntimeCounters {
    commit_entry_total: AtomicU64,
    commit_command_total: AtomicU64,
    max_commands_per_entry: AtomicU64,
    precheck_command_total: AtomicU64,
    precheck_predicate_total: AtomicU64,
    precheck_ns_total: AtomicU64,
    stale_read_total: AtomicU64,
}

impl<M, L> SharedLogMetadataStore<M, L, MemoryAppliedFrontierStore>
where
    M: MetadataStore,
    L: SharedMetadataLog,
{
    pub fn new(store: M, log: L, term: LogTerm, mount: MountId) -> Self {
        Self {
            store,
            log,
            term,
            mount,
            frontier_store: MemoryAppliedFrontierStore::new(),
            apply_gate: Mutex::new(()),
            applied_frontier: Mutex::new(None),
            runtime_stats: SharedLogRuntimeCounters::default(),
        }
    }

    pub fn recover(
        store: M,
        log: L,
        term: LogTerm,
        mount: MountId,
    ) -> Result<(Self, ReplayOutcome), ReplayError> {
        let shared = Self::new(store, log, term, mount);
        let start = first_available_replay_index(&shared.log)?;
        let outcome = ReplayDriver::new(&shared.log, &shared).replay_from(start, 0)?;
        Ok((shared, outcome))
    }
}

impl<M, L, F> SharedLogMetadataStore<M, L, F>
where
    M: MetadataStore,
    L: SharedMetadataLog,
    F: AppliedFrontierStore,
{
    pub fn with_frontier_store(
        store: M,
        log: L,
        term: LogTerm,
        mount: MountId,
        frontier_store: F,
    ) -> Result<Self, SharedLogError> {
        let applied_frontier = frontier_store.load()?;
        Ok(Self {
            store,
            log,
            term,
            mount,
            frontier_store,
            apply_gate: Mutex::new(()),
            applied_frontier: Mutex::new(applied_frontier),
            runtime_stats: SharedLogRuntimeCounters::default(),
        })
    }

    pub fn recover_with_frontier_store(
        store: M,
        log: L,
        term: LogTerm,
        mount: MountId,
        frontier_store: F,
    ) -> Result<(Self, ReplayOutcome), ReplayError> {
        let shared = Self::with_frontier_store(store, log, term, mount, frontier_store)
            .map_err(ReplayError::from)?;
        let start = match shared.applied_frontier() {
            Some(frontier) => next_log_index(frontier.position.index)?,
            None => first_available_replay_index(&shared.log)?,
        };
        let outcome = ReplayDriver::new(&shared.log, &shared).replay_from(start, 0)?;
        Ok((shared, outcome))
    }

    pub fn commit_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Result<Vec<CommitResult>, MetadataError> {
        let _guard = self
            .apply_gate
            .lock()
            .map_err(|_| MetadataError::Backend("shared-log apply gate poisoned".to_owned()))?;
        self.commit_batch_locked(commands)
    }

    pub fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        let mut results = vec![None; commands.len()];
        let _guard = match self.apply_gate.lock() {
            Ok(guard) => guard,
            Err(_) => {
                let error = MetadataError::Backend("shared-log apply gate poisoned".to_owned());
                return commands.iter().map(|_| Err(error.clone())).collect();
            }
        };
        let mut pending = Vec::new();
        for (index, command) in commands.iter().cloned().enumerate() {
            match self.store.committed_request_result(&command.request_id) {
                Ok(Some(result)) => {
                    results[index] = Some(Ok(result));
                    continue;
                }
                Ok(None) => {}
                Err(err) => {
                    results[index] = Some(Err(err));
                    continue;
                }
            }
            if command_conflicts_with_pending_batch(&pending, &command) {
                self.commit_prevalidated_pending_batch(&mut pending, &mut results);
            }
            match self.validate_command_for_log_append(&command) {
                Ok(()) => pending.push((index, command)),
                Err(err) => results[index] = Some(Err(err)),
            }
        }
        self.commit_prevalidated_pending_batch(&mut pending, &mut results);
        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(MetadataError::Backend(
                        "shared-log batch result was not recorded".to_owned(),
                    ))
                })
            })
            .collect()
    }

    fn commit_batch_locked(
        &self,
        commands: &[MetadataCommand],
    ) -> Result<Vec<CommitResult>, MetadataError> {
        if let Some(results) = self.committed_batch_results(commands)? {
            return Ok(results);
        }
        validate_batch_independence(commands)?;
        self.validate_commands_for_log_append(commands)?;
        self.commit_prevalidated_batch_locked(commands)
    }

    fn commit_prevalidated_batch_locked(
        &self,
        commands: &[MetadataCommand],
    ) -> Result<Vec<CommitResult>, MetadataError> {
        let commit = MetadataGroup::new(&self.log, self, self.term, self.mount)
            .commit_batch(commands)
            .map_err(|err| MetadataError::Backend(err.to_string()))?;
        self.record_runtime_commit(commands.len());
        Ok(commit
            .applied
            .into_iter()
            .map(|applied| CommitResult {
                commit_version: applied.receipt.commit_version,
                applied_mutations: applied.applied_mutations,
                watch_events: applied.watch_events,
            })
            .collect())
    }

    fn commit_prevalidated_pending_batch(
        &self,
        pending: &mut Vec<(usize, MetadataCommand)>,
        results: &mut [Option<Result<CommitResult, MetadataError>>],
    ) {
        if pending.is_empty() {
            return;
        }
        let indexes = pending.iter().map(|(index, _)| *index).collect::<Vec<_>>();
        let commands = pending
            .iter()
            .map(|(_, command)| command.clone())
            .collect::<Vec<_>>();
        match self.commit_prevalidated_batch_locked(&commands) {
            Ok(committed) => {
                for (index, result) in indexes.into_iter().zip(committed) {
                    results[index] = Some(Ok(result));
                }
            }
            Err(err) => {
                for index in indexes {
                    results[index] = Some(Err(err.clone()));
                }
            }
        }
        pending.clear();
    }

    pub fn inner(&self) -> &M {
        &self.store
    }

    pub fn log(&self) -> &L {
        &self.log
    }

    pub fn applied_frontier(&self) -> Option<ApplyFrontier> {
        self.applied_frontier
            .lock()
            .map(|frontier| *frontier)
            .unwrap_or(None)
    }

    pub fn runtime_stats(&self) -> SharedLogRuntimeStats {
        SharedLogRuntimeStats {
            commit_entry_total: self
                .runtime_stats
                .commit_entry_total
                .load(Ordering::Relaxed),
            commit_command_total: self
                .runtime_stats
                .commit_command_total
                .load(Ordering::Relaxed),
            max_commands_per_entry: self
                .runtime_stats
                .max_commands_per_entry
                .load(Ordering::Relaxed),
            precheck_command_total: self
                .runtime_stats
                .precheck_command_total
                .load(Ordering::Relaxed),
            precheck_predicate_total: self
                .runtime_stats
                .precheck_predicate_total
                .load(Ordering::Relaxed),
            precheck_ns_total: self.runtime_stats.precheck_ns_total.load(Ordering::Relaxed),
            stale_read_total: self.runtime_stats.stale_read_total.load(Ordering::Relaxed),
        }
    }

    pub fn replay_committed_tail(&self, limit: usize) -> Result<ReplayOutcome, ReplayError> {
        let start = match self.applied_frontier() {
            Some(frontier) => next_log_index(frontier.position.index)?,
            None => first_available_replay_index(&self.log)?,
        };
        ReplayDriver::new(&self.log, self).replay_from(start, limit)
    }

    pub fn install_checkpoint_state<I>(
        &self,
        frontier: ApplyFrontier,
        install: I,
    ) -> Result<(), SharedLogError>
    where
        I: FnOnce(&M) -> Result<(), MetadataError>,
    {
        let _guard = self
            .apply_gate
            .lock()
            .map_err(|_| SharedLogError::Backend("shared-log apply gate poisoned".to_owned()))?;
        install(&self.store).map_err(|err| SharedLogError::Backend(err.to_string()))?;
        self.frontier_store.save(frontier)?;
        *self.applied_frontier.lock().map_err(|_| {
            SharedLogError::Backend("shared-log applied frontier mutex poisoned".to_owned())
        })? = Some(frontier);
        Ok(())
    }

    pub fn ensure_read_freshness(&self, freshness: ReadFreshness) -> Result<(), SharedLogError> {
        let Some(required) = self.required_read_position(freshness) else {
            return Ok(());
        };
        let applied = self.applied_frontier().map(|frontier| frontier.position);
        if applied
            .map(|applied| position_covers(applied, required))
            .unwrap_or(false)
        {
            return Ok(());
        }
        self.runtime_stats
            .stale_read_total
            .fetch_add(1, Ordering::Relaxed);
        Err(SharedLogError::ReadNotFresh { required, applied })
    }

    pub fn get_versioned_with_freshness(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
        freshness: ReadFreshness,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.ensure_read_freshness(freshness)
            .map_err(metadata_error_from_shared_log)?;
        self.store.get_versioned(family, key, version, purpose)
    }

    pub fn scan_with_freshness(
        &self,
        request: ScanRequest,
        freshness: ReadFreshness,
    ) -> Result<Vec<ScanItem>, MetadataError> {
        self.ensure_read_freshness(freshness)
            .map_err(metadata_error_from_shared_log)?;
        self.store.scan(request)
    }

    pub fn scan_keys_with_freshness(
        &self,
        request: KeyScanRequest,
        freshness: ReadFreshness,
    ) -> Result<Vec<Vec<u8>>, MetadataError> {
        self.ensure_read_freshness(freshness)
            .map_err(metadata_error_from_shared_log)?;
        self.store.scan_keys(request)
    }

    pub fn scan_delimited_with_freshness(
        &self,
        request: DelimitedScanRequest,
        freshness: ReadFreshness,
    ) -> Result<Vec<DelimitedScanItem>, MetadataError> {
        self.ensure_read_freshness(freshness)
            .map_err(metadata_error_from_shared_log)?;
        self.store.scan_delimited(request)
    }

    pub fn checkpoint_frontier(
        &self,
        target_min_retained_index: LogIndex,
    ) -> Result<Option<CheckpointFrontier>, SharedLogError> {
        let Some(applied) = self.applied_frontier() else {
            return Ok(None);
        };
        let Some(committed) = self.log.committed_position() else {
            return Err(SharedLogError::Backend(
                "applied frontier exists but shared log has no committed entries".to_owned(),
            ));
        };
        let compact_index = previous_log_index(target_min_retained_index)
            .map(|requested| requested.min(applied.position.index));
        let min_retained_index = match compact_index {
            Some(index) => next_retained_index(index)?,
            None => target_min_retained_index,
        };
        Ok(Some(CheckpointFrontier {
            durable_position: committed,
            applied_position: applied.position,
            min_retained_index,
            max_commit_version: applied.commit_version,
        }))
    }

    pub fn compact_applied_log(
        &self,
        target_min_retained_index: LogIndex,
    ) -> Result<Option<CheckpointFrontier>, SharedLogError> {
        let Some(frontier) = self.checkpoint_frontier(target_min_retained_index)? else {
            return Ok(None);
        };
        if let Some(compact_through) = frontier.compact_through() {
            self.log.compact_through(compact_through)?;
        }
        Ok(Some(frontier))
    }

    fn record_runtime_commit(&self, command_count: usize) {
        self.runtime_stats
            .commit_entry_total
            .fetch_add(1, Ordering::Relaxed);
        self.runtime_stats
            .commit_command_total
            .fetch_add(command_count as u64, Ordering::Relaxed);
        record_max(
            &self.runtime_stats.max_commands_per_entry,
            command_count as u64,
        );
    }

    fn committed_batch_results(
        &self,
        commands: &[MetadataCommand],
    ) -> Result<Option<Vec<CommitResult>>, MetadataError> {
        let mut results = Vec::with_capacity(commands.len());
        for command in commands {
            let Some(result) = self.store.committed_request_result(&command.request_id)? else {
                return Ok(None);
            };
            results.push(result);
        }
        Ok(Some(results))
    }

    fn validate_command_for_log_append(
        &self,
        command: &MetadataCommand,
    ) -> Result<(), MetadataError> {
        let started = Instant::now();
        let result = validate_against_current_store(&self.store, command);
        self.record_runtime_precheck(std::slice::from_ref(command), started.elapsed());
        result
    }

    fn validate_commands_for_log_append(
        &self,
        commands: &[MetadataCommand],
    ) -> Result<(), MetadataError> {
        let started = Instant::now();
        let result = commands
            .iter()
            .try_for_each(|command| validate_against_current_store(&self.store, command));
        self.record_runtime_precheck(commands, started.elapsed());
        result
    }

    fn record_runtime_precheck(&self, commands: &[MetadataCommand], elapsed: std::time::Duration) {
        self.runtime_stats
            .precheck_command_total
            .fetch_add(commands.len() as u64, Ordering::Relaxed);
        self.runtime_stats.precheck_predicate_total.fetch_add(
            commands
                .iter()
                .map(|command| command.predicates.len() as u64)
                .sum::<u64>(),
            Ordering::Relaxed,
        );
        self.runtime_stats.precheck_ns_total.fetch_add(
            elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn required_read_position(&self, freshness: ReadFreshness) -> Option<LogPosition> {
        match freshness {
            ReadFreshness::AnyApplied => None,
            ReadFreshness::AppliedThrough(position) => {
                (position.index != LogIndex::ZERO).then_some(position)
            }
            ReadFreshness::CurrentCommitted => self.log.committed_position(),
        }
    }
}

fn record_max(value: &AtomicU64, candidate: u64) {
    let mut current = value.load(Ordering::Relaxed);
    while candidate > current {
        match value.compare_exchange_weak(current, candidate, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn freshness_for_read_purpose(purpose: ReadPurpose) -> ReadFreshness {
    match purpose {
        ReadPurpose::UserStrong => ReadFreshness::CurrentCommitted,
        ReadPurpose::WritePlanLocal | ReadPurpose::Snapshot => ReadFreshness::AnyApplied,
    }
}

fn position_covers(applied: LogPosition, required: LogPosition) -> bool {
    applied.index >= required.index
}

impl<M, L, F> MetadataLogSink for SharedLogMetadataStore<M, L, F>
where
    M: MetadataStore,
    F: AppliedFrontierStore,
{
    fn apply_command(
        &self,
        receipt: DurableReceipt,
        command: MetadataCommand,
    ) -> Result<AppliedMetadataCommand, ReplayError> {
        let result = self
            .store
            .commit_metadata(command)
            .map_err(|err| ReplayError::Apply {
                position: receipt.position,
                batch_position: receipt.batch_position,
                message: err.to_string(),
            })?;
        self.record_applied_frontier(ApplyFrontier {
            position: receipt.position,
            commit_version: result.commit_version,
        })?;
        Ok(AppliedMetadataCommand {
            receipt,
            applied_mutations: result.applied_mutations,
            watch_events: result.watch_events,
        })
    }

    fn apply_batch(
        &self,
        receipts: Vec<DurableReceipt>,
        commands: Vec<MetadataCommand>,
    ) -> Result<Vec<AppliedMetadataCommand>, ReplayError> {
        debug_assert_eq!(receipts.len(), commands.len());
        let results = self.store.commit_independent_batch(&commands);
        let mut applied = Vec::with_capacity(results.len());
        let mut frontier = None;
        for (receipt, result) in receipts.into_iter().zip(results) {
            let result = result.map_err(|err| ReplayError::Apply {
                position: receipt.position,
                batch_position: receipt.batch_position,
                message: err.to_string(),
            })?;
            frontier = Some(ApplyFrontier {
                position: receipt.position,
                commit_version: result.commit_version,
            });
            applied.push(AppliedMetadataCommand {
                receipt,
                applied_mutations: result.applied_mutations,
                watch_events: result.watch_events,
            });
        }
        if let Some(frontier) = frontier {
            self.record_applied_frontier(frontier)?;
        }
        Ok(applied)
    }
}

impl<M, L, F> MetadataStore for SharedLogMetadataStore<M, L, F>
where
    M: MetadataStore,
    L: SharedMetadataLog,
    F: AppliedFrontierStore,
{
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: nokvfs_meta::Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        let freshness = freshness_for_read_purpose(purpose);
        self.get_versioned_with_freshness(family, key, version, purpose, freshness)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        let freshness = freshness_for_read_purpose(request.purpose);
        self.scan_with_freshness(request, freshness)
    }

    fn scan_keys(&self, request: KeyScanRequest) -> Result<Vec<Vec<u8>>, MetadataError> {
        let freshness = freshness_for_read_purpose(request.purpose);
        self.scan_keys_with_freshness(request, freshness)
    }

    fn scan_delimited(
        &self,
        request: DelimitedScanRequest,
    ) -> Result<Vec<DelimitedScanItem>, MetadataError> {
        let freshness = freshness_for_read_purpose(request.purpose);
        self.scan_delimited_with_freshness(request, freshness)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        let results = self.commit_batch(std::slice::from_ref(&command))?;
        results.into_iter().next().ok_or_else(|| {
            MetadataError::Backend("shared-log commit returned no result".to_owned())
        })
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        SharedLogMetadataStore::commit_independent_batch(self, commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.store.committed_request_result(request_id)
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.store.prune_history(request)
    }
}

impl<M, L, F> MetadataStoreStatsProvider for SharedLogMetadataStore<M, L, F>
where
    M: MetadataStoreStatsProvider,
    F: AppliedFrontierStore,
{
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.store.metadata_store_stats()
    }
}

fn first_available_replay_index<L>(log: &L) -> Result<LogIndex, ReplayError>
where
    L: SharedMetadataLog,
{
    let first = LogIndex::new(1)?;
    match log.read_from(first, 1) {
        Ok(_) => Ok(first),
        Err(SharedLogError::Compacted { compacted, .. }) => next_log_index(compacted),
        Err(err) => Err(err.into()),
    }
}

fn next_log_index(index: LogIndex) -> Result<LogIndex, ReplayError> {
    let next = index
        .get()
        .checked_add(1)
        .ok_or(ReplayError::IndexOverflow(index))?;
    LogIndex::new(next).map_err(ReplayError::from)
}

fn validate_against_current_store<M>(
    store: &M,
    command: &MetadataCommand,
) -> Result<(), MetadataError>
where
    M: MetadataStore,
{
    command.validate()?;
    let max_version = Version::new(u64::MAX)?;
    for predicate in &command.predicates {
        match predicate.predicate {
            Predicate::Exists => {
                if store
                    .get_versioned(
                        predicate.family,
                        &predicate.key,
                        max_version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .is_none()
                {
                    return allow_if_deduped_retry(store, command);
                }
            }
            Predicate::NotExists => {
                let writes_key = command.mutations.iter().any(|mutation| {
                    mutation.family == predicate.family
                        && mutation.key == predicate.key
                        && mutation.op == MutationOp::Put
                });
                if !writes_key {
                    return allow_if_deduped_retry(store, command);
                }
                if store
                    .get_versioned(
                        predicate.family,
                        &predicate.key,
                        max_version,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .is_some()
                {
                    return allow_if_deduped_retry(store, command);
                }
            }
            Predicate::PrefixEmpty => {
                if !store
                    .scan_keys(KeyScanRequest {
                        family: predicate.family,
                        prefix: predicate.key.clone(),
                        start_after: None,
                        limit: 1,
                        purpose: ReadPurpose::WritePlanLocal,
                    })?
                    .is_empty()
                {
                    return allow_if_deduped_retry(store, command);
                }
            }
            Predicate::VersionEquals(expected) => {
                let Some(item) = store.get_versioned(
                    predicate.family,
                    &predicate.key,
                    max_version,
                    ReadPurpose::WritePlanLocal,
                )?
                else {
                    return allow_if_deduped_retry(store, command);
                };
                if item.version != expected {
                    return allow_if_deduped_retry(store, command);
                }
            }
        }
    }
    Ok(())
}

fn allow_if_deduped_retry<M>(store: &M, command: &MetadataCommand) -> Result<(), MetadataError>
where
    M: MetadataStore,
{
    match store.committed_request_result(&command.request_id)? {
        Some(_) => Ok(()),
        None => Err(MetadataError::PredicateFailed),
    }
}

fn validate_batch_independence(commands: &[MetadataCommand]) -> Result<(), MetadataError> {
    for (index, command) in commands.iter().enumerate() {
        if command_conflicts_with_prior_commands(&commands[..index], command) {
            return Err(MetadataError::PredicateFailed);
        }
    }
    Ok(())
}

fn command_conflicts_with_pending_batch(
    pending: &[(usize, MetadataCommand)],
    command: &MetadataCommand,
) -> bool {
    pending
        .iter()
        .any(|(_, previous)| metadata_commands_conflict(command, previous))
}

fn command_conflicts_with_prior_commands(
    commands: &[MetadataCommand],
    command: &MetadataCommand,
) -> bool {
    commands
        .iter()
        .any(|previous| metadata_commands_conflict(command, previous))
}

fn metadata_error_from_shared_log(err: SharedLogError) -> MetadataError {
    match err {
        SharedLogError::ReadNotFresh { required, applied } => MetadataError::ReadNotFresh {
            required_term: required.term.get(),
            required_index: required.index.get(),
            applied_term: applied.map(|position| position.term.get()),
            applied_index: applied.map(|position| position.index.get()),
        },
        other => MetadataError::Backend(other.to_string()),
    }
}

impl<M, L, F> SharedLogMetadataStore<M, L, F>
where
    F: AppliedFrontierStore,
{
    fn record_applied_frontier(&self, frontier: ApplyFrontier) -> Result<(), ReplayError> {
        self.frontier_store
            .save(frontier)
            .map_err(|err| ReplayError::Apply {
                position: frontier.position,
                batch_position: 0,
                message: err.to_string(),
            })?;
        let mut current = self
            .applied_frontier
            .lock()
            .map_err(|_| ReplayError::Apply {
                position: frontier.position,
                batch_position: 0,
                message: "shared-log applied frontier mutex poisoned".to_owned(),
            })?;
        if current
            .map(|existing| frontier_position_is_newer(frontier.position, existing.position))
            .unwrap_or(true)
        {
            *current = Some(frontier);
        }
        Ok(())
    }
}

fn frontier_position_is_newer(next: LogPosition, current: LogPosition) -> bool {
    (next.term, next.index) >= (current.term, current.index)
}

fn previous_log_index(index: LogIndex) -> Option<LogIndex> {
    let previous = index.get().checked_sub(1)?;
    if previous == 0 {
        return None;
    }
    LogIndex::new(previous).ok()
}

fn next_retained_index(index: LogIndex) -> Result<LogIndex, SharedLogError> {
    let next = index.get().checked_add(1).ok_or_else(|| {
        SharedLogError::Backend(format!("log index overflow after {}", index.get()))
    })?;
    LogIndex::new(next)
}
