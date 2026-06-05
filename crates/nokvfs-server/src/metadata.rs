use std::sync::Arc;

use nokvfs_cluster::{
    AppliedFrontierStore, ApplyFrontier, FileAppliedFrontierStore, LogPosition,
    OpenRaftMetadataStore, ReadFreshness, SharedLogError, SharedLogMetadataStore,
    SharedLogRuntimeStats, SharedMetadataLog,
};
use nokvfs_meta::command::{
    CommitResult, DelimitedScanItem, DelimitedScanRequest, HistoryPruneOutcome,
    HistoryPruneRequest, KeyScanRequest, MetadataCommand, MetadataError, MetadataStore,
    MetadataStoreStats, MetadataStoreStatsProvider, ReadItem, ReadPurpose, ScanItem, ScanRequest,
};
use nokvfs_meta::holtstore::HoltMetadataStore;
use nokvfs_protocol::{
    WireMetadataRaftAppendEntriesRequest, WireMetadataRaftAppendEntriesResponse,
    WireMetadataRaftInstallSnapshotRequest, WireMetadataRaftInstallSnapshotResponse,
    WireMetadataRaftVoteRequest, WireMetadataRaftVoteResponse,
};
use nokvfs_types::RecordFamily;

use crate::replication::MajorityMetadataLog;

pub(crate) type FileLoggedMetadataStore =
    SharedLogMetadataStore<HoltMetadataStore, MajorityMetadataLog, FileAppliedFrontierStore>;
pub(crate) type OpenRaftLoggedMetadataStore = OpenRaftMetadataStore<HoltMetadataStore>;

pub(crate) trait ServerMetadataBackend:
    MetadataStore + MetadataStoreStatsProvider + Send + Sync
{
}

impl<T> ServerMetadataBackend for T where T: MetadataStore + MetadataStoreStatsProvider + Send + Sync
{}

pub(crate) trait ServerMetadataLogStatus: Send + Sync {
    fn applied_frontier(&self) -> Option<ApplyFrontier>;
    fn ensure_applied(&self, position: LogPosition) -> Result<(), SharedLogError>;
    fn runtime_stats(&self) -> SharedLogRuntimeStats;
}

impl<M, L, F> ServerMetadataLogStatus for SharedLogMetadataStore<M, L, F>
where
    M: MetadataStore + Send + Sync,
    L: SharedMetadataLog + Send + Sync,
    F: AppliedFrontierStore + Send + Sync,
{
    fn applied_frontier(&self) -> Option<ApplyFrontier> {
        SharedLogMetadataStore::applied_frontier(self)
    }

    fn ensure_applied(&self, position: LogPosition) -> Result<(), SharedLogError> {
        self.ensure_read_freshness(ReadFreshness::AppliedThrough(position))
    }

    fn runtime_stats(&self) -> SharedLogRuntimeStats {
        SharedLogMetadataStore::runtime_stats(self)
    }
}

pub(crate) enum ServerMetadataStore {
    OpenRaft(Box<OpenRaftLoggedMetadataStore>),
    SharedLogged(Arc<dyn ServerMetadataBackend>),
}

impl ServerMetadataStore {
    pub(crate) fn openraft(store: OpenRaftLoggedMetadataStore) -> Self {
        Self::OpenRaft(Box::new(store))
    }

    pub(crate) fn shared_logged(store: Arc<dyn ServerMetadataBackend>) -> Self {
        Self::SharedLogged(store)
    }

    pub(crate) fn handle_metadata_raft_vote(
        &self,
        request: WireMetadataRaftVoteRequest,
    ) -> Result<WireMetadataRaftVoteResponse, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.handle_vote_rpc(request),
            Self::SharedLogged(_) => Err(metadata_raft_disabled()),
        }
    }

    pub(crate) fn handle_metadata_raft_append_entries(
        &self,
        request: WireMetadataRaftAppendEntriesRequest,
    ) -> Result<WireMetadataRaftAppendEntriesResponse, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.handle_append_entries_rpc(request),
            Self::SharedLogged(_) => Err(metadata_raft_disabled()),
        }
    }

    pub(crate) fn handle_metadata_raft_install_snapshot(
        &self,
        request: WireMetadataRaftInstallSnapshotRequest,
    ) -> Result<WireMetadataRaftInstallSnapshotResponse, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.handle_install_snapshot_rpc(request),
            Self::SharedLogged(_) => Err(metadata_raft_disabled()),
        }
    }

    #[cfg(test)]
    pub(crate) fn shutdown_openraft(&self) -> Result<(), MetadataError> {
        match self {
            Self::OpenRaft(store) => store.shutdown(),
            Self::SharedLogged(_) => Err(metadata_raft_disabled()),
        }
    }
}

fn metadata_raft_disabled() -> MetadataError {
    MetadataError::Backend("metadata OpenRaft group is not enabled".to_owned())
}

impl MetadataStore for ServerMetadataStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: nokvfs_meta::Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.get_versioned(family, key, version, purpose),
            Self::SharedLogged(store) => store.get_versioned(family, key, version, purpose),
        }
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.scan(request),
            Self::SharedLogged(store) => store.scan(request),
        }
    }

    fn scan_delimited(
        &self,
        request: DelimitedScanRequest,
    ) -> Result<Vec<DelimitedScanItem>, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.scan_delimited(request),
            Self::SharedLogged(store) => store.scan_delimited(request),
        }
    }

    fn scan_keys(&self, request: KeyScanRequest) -> Result<Vec<Vec<u8>>, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.scan_keys(request),
            Self::SharedLogged(store) => store.scan_keys(request),
        }
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.commit_metadata(command),
            Self::SharedLogged(store) => store.commit_metadata(command),
        }
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        match self {
            Self::OpenRaft(store) => store.commit_independent_batch(commands),
            Self::SharedLogged(store) => store.commit_independent_batch(commands),
        }
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.committed_request_result(request_id),
            Self::SharedLogged(store) => store.committed_request_result(request_id),
        }
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.prune_history(request),
            Self::SharedLogged(store) => store.prune_history(request),
        }
    }
}

impl MetadataStoreStatsProvider for ServerMetadataStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        match self {
            Self::OpenRaft(store) => store.metadata_store_stats(),
            Self::SharedLogged(store) => store.metadata_store_stats(),
        }
    }
}
