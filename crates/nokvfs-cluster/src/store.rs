use std::sync::Mutex;

use nokvfs_meta::command::{
    CommitResult, HistoryPruneOutcome, HistoryPruneRequest, MetadataCommand, MetadataError,
    MetadataStore, MetadataStoreStats, MetadataStoreStatsProvider, MutationOp, Predicate, ReadItem,
    ReadPurpose, ScanItem, ScanRequest, Version,
};
use nokvfs_types::{MountId, RecordFamily};

use crate::{
    AppliedMetadataCommand, DurableReceipt, LogIndex, LogTerm, MetadataGroup, MetadataLogSink,
    ReplayDriver, ReplayError, ReplayOutcome, SharedLogError, SharedMetadataLog,
};

#[derive(Debug)]
pub struct SharedLogMetadataStore<M, L> {
    store: M,
    log: L,
    term: LogTerm,
    mount: MountId,
    apply_gate: Mutex<()>,
}

impl<M, L> SharedLogMetadataStore<M, L>
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
            apply_gate: Mutex::new(()),
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

    pub fn commit_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Result<Vec<CommitResult>, MetadataError> {
        let _guard = self
            .apply_gate
            .lock()
            .map_err(|_| MetadataError::Backend("shared-log apply gate poisoned".to_owned()))?;
        for command in commands {
            validate_against_current_store(&self.store, command)?;
        }
        let commit = MetadataGroup::new(&self.log, self, self.term, self.mount)
            .commit_batch(commands)
            .map_err(|err| MetadataError::Backend(err.to_string()))?;
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

    pub fn inner(&self) -> &M {
        &self.store
    }

    pub fn log(&self) -> &L {
        &self.log
    }
}

impl<M, L> MetadataLogSink for SharedLogMetadataStore<M, L>
where
    M: MetadataStore,
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
        Ok(AppliedMetadataCommand {
            receipt,
            applied_mutations: result.applied_mutations,
            watch_events: result.watch_events,
        })
    }
}

impl<M, L> MetadataStore for SharedLogMetadataStore<M, L>
where
    M: MetadataStore,
    L: SharedMetadataLog,
{
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: nokvfs_meta::Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.store.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.store.scan(request)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        let results = self.commit_batch(std::slice::from_ref(&command))?;
        results.into_iter().next().ok_or_else(|| {
            MetadataError::Backend("shared-log commit returned no result".to_owned())
        })
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.store.prune_history(request)
    }
}

impl<M, L> MetadataStoreStatsProvider for SharedLogMetadataStore<M, L>
where
    M: MetadataStoreStatsProvider,
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
                    return Err(MetadataError::PredicateFailed);
                }
            }
            Predicate::NotExists => {
                let writes_key = command.mutations.iter().any(|mutation| {
                    mutation.family == predicate.family
                        && mutation.key == predicate.key
                        && mutation.op == MutationOp::Put
                });
                if !writes_key {
                    return Err(MetadataError::PredicateFailed);
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
                    return Err(MetadataError::PredicateFailed);
                }
            }
            Predicate::PrefixEmpty => {
                if !store
                    .scan(ScanRequest {
                        family: predicate.family,
                        prefix: predicate.key.clone(),
                        start_after: None,
                        version: max_version,
                        limit: 1,
                        purpose: ReadPurpose::WritePlanLocal,
                    })?
                    .is_empty()
                {
                    return Err(MetadataError::PredicateFailed);
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
                    return Err(MetadataError::PredicateFailed);
                };
                if item.version != expected {
                    return Err(MetadataError::PredicateFailed);
                }
            }
        }
    }
    Ok(())
}
