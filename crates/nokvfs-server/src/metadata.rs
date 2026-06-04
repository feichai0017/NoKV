use std::sync::Arc;

use nokvfs_cluster::{FileAppliedFrontierStore, FileSharedLog, SharedLogMetadataStore};
use nokvfs_meta::command::{
    CommitResult, HistoryPruneOutcome, HistoryPruneRequest, MetadataCommand, MetadataError,
    MetadataStore, MetadataStoreStats, MetadataStoreStatsProvider, ReadItem, ReadPurpose, ScanItem,
    ScanRequest,
};
use nokvfs_meta::holtstore::HoltMetadataStore;
use nokvfs_types::RecordFamily;

pub(crate) type FileLoggedMetadataStore =
    SharedLogMetadataStore<HoltMetadataStore, FileSharedLog, FileAppliedFrontierStore>;

pub(crate) enum ServerMetadataStore {
    Direct(Box<HoltMetadataStore>),
    FileLogged(Arc<FileLoggedMetadataStore>),
}

impl ServerMetadataStore {
    pub(crate) fn direct(store: HoltMetadataStore) -> Self {
        Self::Direct(Box::new(store))
    }

    pub(crate) fn file_logged(store: Arc<FileLoggedMetadataStore>) -> Self {
        Self::FileLogged(store)
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
            Self::Direct(store) => store.get_versioned(family, key, version, purpose),
            Self::FileLogged(store) => store.get_versioned(family, key, version, purpose),
        }
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        match self {
            Self::Direct(store) => store.scan(request),
            Self::FileLogged(store) => store.scan(request),
        }
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        match self {
            Self::Direct(store) => store.commit_metadata(command),
            Self::FileLogged(store) => store.commit_metadata(command),
        }
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        match self {
            Self::Direct(store) => store.prune_history(request),
            Self::FileLogged(store) => store.prune_history(request),
        }
    }
}

impl MetadataStoreStatsProvider for ServerMetadataStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        match self {
            Self::Direct(store) => store.metadata_store_stats(),
            Self::FileLogged(store) => store.metadata_store_stats(),
        }
    }
}
