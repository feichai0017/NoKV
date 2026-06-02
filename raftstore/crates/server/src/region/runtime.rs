//! Region runtime traits used by the admin and MetadataPlane service layers.

use nokv_proto::nokv::meta::v1 as metapb;
use nokv_raftnode::{
    AppliedMetadataEngine, ApplyStatusProvider, BasicNode, MetadataRetentionExecutor,
    PersistentAppliedMetadataEngine, RegionMetadataSink, RegionSnapshotEngine,
};
use tonic::Status;

use crate::internal_error;

fn membership_unimplemented() -> Status {
    Status::unimplemented("rust raft membership requires an OpenRaftRegion")
}

#[tonic::async_trait]
pub trait RaftMembershipAdmin: Clone + Send + Sync + 'static {
    async fn add_voter(&self, peer_id: u64, node: BasicNode) -> Result<(), Status>;
    async fn remove_voter(&self, peer_id: u64) -> Result<(), Status>;
    async fn transfer_leader(&self, peer_id: u64) -> Result<(), Status>;
    async fn propose_region_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> Result<(), Status>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RaftRuntimeStatus {
    pub local_peer_id: u64,
    pub leader_peer_id: u64,
    pub leader: bool,
    pub hosted: bool,
}

pub trait RaftRuntimeStatusProvider: ApplyStatusProvider {
    fn raft_runtime_status(&self) -> RaftRuntimeStatus;
}

pub trait AppliedRegionDescriptorProvider: ApplyStatusProvider {
    fn applied_region_descriptor(&self) -> Result<Option<metapb::RegionDescriptor>, Status> {
        Ok(None)
    }
}

#[tonic::async_trait]
impl<E> RaftMembershipAdmin for nokv_raftnode::OpenRaftRegion<E>
where
    E: RegionSnapshotEngine,
{
    async fn add_voter(&self, peer_id: u64, node: BasicNode) -> Result<(), Status> {
        self.add_voter(peer_id, node)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))
    }

    async fn remove_voter(&self, peer_id: u64) -> Result<(), Status> {
        // Retain the removed peer as a learner long enough for the descriptor
        // update to demote its local serving state before physical cleanup.
        nokv_raftnode::OpenRaftRegion::remove_voter(self, peer_id, true)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))
    }

    async fn transfer_leader(&self, peer_id: u64) -> Result<(), Status> {
        nokv_raftnode::OpenRaftRegion::transfer_leader(self, peer_id)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))
    }

    async fn propose_region_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> Result<(), Status> {
        self.propose_region_descriptor(descriptor)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))
    }
}

impl<E> RaftRuntimeStatusProvider for nokv_raftnode::OpenRaftRegion<E>
where
    E: RegionSnapshotEngine,
{
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        let local_peer_id = self.node_id();
        let metrics = self.raft_handle().metrics();
        let metrics = metrics.borrow();
        let leader_peer_id = metrics.current_leader.unwrap_or_default();
        let hosted = metrics
            .membership_config
            .voter_ids()
            .any(|peer_id| peer_id == local_peer_id);
        RaftRuntimeStatus {
            local_peer_id,
            leader_peer_id,
            leader: leader_peer_id == local_peer_id && leader_peer_id != 0,
            hosted,
        }
    }
}

impl<E> AppliedRegionDescriptorProvider for nokv_raftnode::OpenRaftRegion<E>
where
    E: RegionSnapshotEngine,
{
    fn applied_region_descriptor(&self) -> Result<Option<metapb::RegionDescriptor>, Status> {
        nokv_raftnode::OpenRaftRegion::region_descriptor(self).map_err(internal_error)
    }
}

#[tonic::async_trait]
impl<E> RaftMembershipAdmin for AppliedMetadataEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    async fn add_voter(&self, _peer_id: u64, _node: BasicNode) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn remove_voter(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn transfer_leader(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn propose_region_descriptor(
        &self,
        _descriptor: &metapb::RegionDescriptor,
    ) -> Result<(), Status> {
        Err(membership_unimplemented())
    }
}

impl<E> RaftRuntimeStatusProvider for AppliedMetadataEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        let known = self.apply_status().region_id != 0;
        RaftRuntimeStatus {
            local_peer_id: u64::from(known),
            leader_peer_id: u64::from(known),
            leader: known,
            hosted: known,
        }
    }
}

impl<E> AppliedRegionDescriptorProvider for AppliedMetadataEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn applied_region_descriptor(&self) -> Result<Option<metapb::RegionDescriptor>, Status> {
        AppliedMetadataEngine::region_descriptor(self).map_err(internal_error)
    }
}

#[tonic::async_trait]
impl<E, S> RaftMembershipAdmin for PersistentAppliedMetadataEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: RegionMetadataSink,
{
    async fn add_voter(&self, _peer_id: u64, _node: BasicNode) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn remove_voter(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn transfer_leader(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn propose_region_descriptor(
        &self,
        _descriptor: &metapb::RegionDescriptor,
    ) -> Result<(), Status> {
        Err(membership_unimplemented())
    }
}

impl<E, S> RaftRuntimeStatusProvider for PersistentAppliedMetadataEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: RegionMetadataSink,
{
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        let known = self.apply_status().region_id != 0;
        RaftRuntimeStatus {
            local_peer_id: u64::from(known),
            leader_peer_id: u64::from(known),
            leader: known,
            hosted: known,
        }
    }
}

impl<E, S> AppliedRegionDescriptorProvider for PersistentAppliedMetadataEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: RegionMetadataSink,
{
    fn applied_region_descriptor(&self) -> Result<Option<metapb::RegionDescriptor>, Status> {
        self.inner().region_descriptor().map_err(internal_error)
    }
}

#[derive(Debug, Clone, Default)]
pub struct EmptyApplyStatus;

#[tonic::async_trait]
impl RaftMembershipAdmin for EmptyApplyStatus {
    async fn add_voter(&self, _peer_id: u64, _node: BasicNode) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn remove_voter(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn transfer_leader(&self, _peer_id: u64) -> Result<(), Status> {
        Err(membership_unimplemented())
    }

    async fn propose_region_descriptor(
        &self,
        _descriptor: &metapb::RegionDescriptor,
    ) -> Result<(), Status> {
        Err(membership_unimplemented())
    }
}

impl RaftRuntimeStatusProvider for EmptyApplyStatus {
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        RaftRuntimeStatus::default()
    }
}

impl AppliedRegionDescriptorProvider for EmptyApplyStatus {}

impl MetadataRetentionExecutor for EmptyApplyStatus {
    fn prune_metadata_versions<'a>(
        &'a self,
        _retention_floor: u64,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<nokv_metadata_state::MetadataRetentionResult>,
    > + Send
           + 'a {
        async move {
            Err(nokv_metadata_state::Error::Backend(
                "metadata retention requires a hosted region".to_owned(),
            ))
        }
    }
}

impl ApplyStatusProvider for EmptyApplyStatus {
    fn apply_status(&self) -> nokv_raftnode::ApplyStatus {
        nokv_raftnode::ApplyStatus {
            region_id: 0,
            term: 0,
            applied_index: 0,
        }
    }
}
