use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::watch::{ApplyWatchProvider, ApplyWatchReplay, ApplyWatchReplayRequest};
use crate::{OpenRaftEntry, RegionId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedProposal {
    pub region_id: RegionId,
    pub index: u64,
    pub term: u64,
    pub payload: Vec<u8>,
    pub descriptor_changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyStatus {
    pub region_id: RegionId,
    pub term: u64,
    pub applied_index: u64,
}

pub trait ApplyStatusProvider: Clone + Send + Sync + 'static {
    fn apply_status(&self) -> ApplyStatus;
}

pub trait MetadataCommandExecutor: Clone + Send + Sync + 'static {
    fn execute_metadata_command<'a>(
        &'a self,
        req: &'a metadatapb::MetadataCommitRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataCommitResponse>,
    > + Send
           + 'a;
}

pub trait MetadataReadExecutor: Clone + Send + Sync + 'static {
    fn execute_metadata_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataGetRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataGetResponse>,
    > + Send
           + 'a;

    fn execute_metadata_batch_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataBatchGetRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataBatchGetResponse>,
    > + Send
           + 'a;

    fn execute_metadata_scan<'a>(
        &'a self,
        req: &'a metadatapb::MetadataScanRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataScanResponse>,
    > + Send
           + 'a;
}

pub trait MetadataRetentionExecutor: Clone + Send + Sync + 'static {
    fn prune_metadata_versions<'a>(
        &'a self,
        retention_floor: u64,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<nokv_metadata_state::MetadataRetentionResult>,
    > + Send
           + 'a;
}

pub trait RegionApplyEngine: ApplyStatusProvider + ApplyWatchProvider {
    fn apply_openraft_entries<I>(
        &self,
        entries: I,
    ) -> nokv_metadata_state::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>;
}

pub trait RegionSnapshotEngine: RegionApplyEngine {
    fn region_descriptor(&self) -> nokv_metadata_state::Result<Option<metapb::RegionDescriptor>>;

    fn export_region_snapshot(&self) -> nokv_metadata_state::Result<Vec<u8>>;
    fn install_region_snapshot(&self, snapshot: &[u8]) -> nokv_metadata_state::Result<ApplyStatus>;
}

pub trait RegionMetadataSink: Clone + Send + Sync + 'static {
    fn save_apply_status(&self, status: &ApplyStatus) -> nokv_metadata_state::Result<()>;

    fn save_apply_watch_event(
        &self,
        _event: &metadatapb::MetadataApplyWatchEvent,
    ) -> nokv_metadata_state::Result<()> {
        Ok(())
    }

    fn replay_apply_watch(
        &self,
        _request: &ApplyWatchReplayRequest,
    ) -> nokv_metadata_state::Result<Option<ApplyWatchReplay>> {
        Ok(None)
    }

    fn save_region_descriptor(
        &self,
        _descriptor: &metapb::RegionDescriptor,
    ) -> nokv_metadata_state::Result<()> {
        Ok(())
    }
}

pub trait RegionDescriptorCatalog: std::fmt::Debug + Send + Sync + 'static {
    fn region_descriptor(
        &self,
        region_id: RegionId,
    ) -> nokv_metadata_state::Result<Option<metapb::RegionDescriptor>>;
}
