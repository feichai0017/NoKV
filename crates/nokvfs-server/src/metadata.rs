use nokvfs_cluster::{LogPosition, NodeId, OpenRaftMetadataStore};
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

pub(crate) type OpenRaftLoggedMetadataStore = OpenRaftMetadataStore<HoltMetadataStore>;

pub(crate) enum ServerMetadataStore {
    OpenRaft(Box<OpenRaftLoggedMetadataStore>),
}

impl ServerMetadataStore {
    pub(crate) fn openraft(store: OpenRaftLoggedMetadataStore) -> Self {
        Self::OpenRaft(Box::new(store))
    }

    pub(crate) fn handle_metadata_raft_vote(
        &self,
        request: WireMetadataRaftVoteRequest,
    ) -> Result<WireMetadataRaftVoteResponse, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.handle_vote_rpc(request),
        }
    }

    pub(crate) fn handle_metadata_raft_append_entries(
        &self,
        request: WireMetadataRaftAppendEntriesRequest,
    ) -> Result<WireMetadataRaftAppendEntriesResponse, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.handle_append_entries_rpc(request),
        }
    }

    pub(crate) fn handle_metadata_raft_install_snapshot(
        &self,
        request: WireMetadataRaftInstallSnapshotRequest,
    ) -> Result<WireMetadataRaftInstallSnapshotResponse, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.handle_install_snapshot_rpc(request),
        }
    }

    pub(crate) fn add_metadata_raft_learner(
        &self,
        node: NodeId,
        address: String,
        blocking: bool,
    ) -> Result<LogPosition, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.add_learner(node, address, blocking),
        }
    }

    #[cfg(test)]
    pub(crate) fn shutdown_openraft(&self) -> Result<(), MetadataError> {
        match self {
            Self::OpenRaft(store) => store.shutdown(),
        }
    }

    pub(crate) fn trigger_openraft_snapshot(&self) -> Result<(), MetadataError> {
        match self {
            Self::OpenRaft(store) => store.trigger_snapshot(),
        }
    }

    pub(crate) fn export_openraft_checkpoint_image(&self) -> Result<Vec<u8>, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.export_checkpoint_image(),
        }
    }
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
        }
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.scan(request),
        }
    }

    fn scan_delimited(
        &self,
        request: DelimitedScanRequest,
    ) -> Result<Vec<DelimitedScanItem>, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.scan_delimited(request),
        }
    }

    fn scan_keys(&self, request: KeyScanRequest) -> Result<Vec<Vec<u8>>, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.scan_keys(request),
        }
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.commit_metadata(command),
        }
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        match self {
            Self::OpenRaft(store) => store.commit_independent_batch(commands),
        }
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.committed_request_result(request_id),
        }
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        match self {
            Self::OpenRaft(store) => store.prune_history(request),
        }
    }
}

impl MetadataStoreStatsProvider for ServerMetadataStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        match self {
            Self::OpenRaft(store) => store.metadata_store_stats(),
        }
    }
}
