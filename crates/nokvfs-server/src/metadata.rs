use nokvfs_cluster::{FileSharedLog, LogTerm, SharedLogMetadataStore};
use nokvfs_meta::command::{
    CommitResult, HistoryPruneOutcome, HistoryPruneRequest, MetadataCommand, MetadataError,
    MetadataStore, MetadataStoreStats, MetadataStoreStatsProvider, ReadItem, ReadPurpose, ScanItem,
    ScanRequest,
};
use nokvfs_meta::holtstore::HoltMetadataStore;
use nokvfs_types::{MountId, RecordFamily};

pub(crate) type FileLoggedMetadataStore = SharedLogMetadataStore<HoltMetadataStore, FileSharedLog>;

pub(crate) enum ServerMetadataStore {
    Direct(HoltMetadataStore),
    FileLogged(FileLoggedMetadataStore),
}

impl ServerMetadataStore {
    pub(crate) fn direct(store: HoltMetadataStore) -> Self {
        Self::Direct(store)
    }

    pub(crate) fn file_logged(
        store: HoltMetadataStore,
        log: FileSharedLog,
        mount: MountId,
    ) -> Result<Self, nokvfs_cluster::SharedLogError> {
        Ok(Self::FileLogged(SharedLogMetadataStore::new(
            store,
            log,
            LogTerm::new(1)?,
            mount,
        )))
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
