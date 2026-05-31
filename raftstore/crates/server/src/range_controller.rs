use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use nokv_holtstore::HoltMetadataStore;
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_raftnode::{
    AdminCommand, AdminCommandType, AppliedMetadataEngine, BasicNode, MergeCommand,
    PersistentAppliedMetadataEngine, SplitCommand,
};
use nokv_raftstore_server::{
    apply_status_from_holt, openraft_metadata_service_pair, HoltRegionMetadataSink,
    MultiRegionMetadataPlaneService, MultiRegionRaftAdminService, PeerEndpointCatalog,
    RaftRuntimeStatusProvider, RegionAdmission, TopologyPublisher,
};

use crate::coordinator::CoordinatorHeartbeatConfig;
use crate::hosted_region::{
    HoltApplyEngine, HoltRegion, HoltRegionDescriptorCatalog, HostedRegionRegistry,
};
use crate::range_topology::{
    build_merge_descriptor, build_split_descriptors, descriptor_membership_nodes,
    ensure_merge_store_coverage, local_peer_for_store, local_peer_is_first, merge_region_ids,
    merge_root_event, merge_source_already_absorbed, merged_source_region_ids_for_store,
    split_root_event,
};
use crate::region_open::{
    open_openraft_region, region_log_dir, spawn_recovered_region_leadership_retries,
};
use crate::root_publication::publish_root_event_with_pending;
use crate::scheduler_operations::SchedulerOperationOutcome;
use crate::startup::ServerIdentity;

#[derive(Clone)]
pub(crate) struct HoltRangeController {
    pub(crate) store_id: u64,
    pub(crate) advertised_addr: String,
    pub(crate) persistent_root: PathBuf,
    pub(crate) coordinator: Option<CoordinatorHeartbeatConfig>,
    pub(crate) metadata_store: HoltMetadataStore,
    pub(crate) transport: nokv_raftnode::TonicRaftTransportRegistry,
    pub(crate) metadata_services: MultiRegionMetadataPlaneService<HoltRegion>,
    pub(crate) admin_services: MultiRegionRaftAdminService<HoltRegion, HoltRegionMetadataSink>,
    pub(crate) hosted_regions: HostedRegionRegistry<HoltApplyEngine>,
    pub(crate) peer_endpoints: PeerEndpointCatalog,
    pub(crate) topology_publisher: Arc<dyn TopologyPublisher>,
}

impl HoltRangeController {
    pub(crate) async fn execute_split(
        &self,
        operation: &coordpb::SchedulerOperation,
    ) -> Result<SchedulerOperationOutcome, tonic::Status> {
        let child = operation
            .split_child
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("split child descriptor is required"))?;
        if child.region_id == 0 || operation.region_id == 0 || operation.split_key.is_empty() {
            return Ok(SchedulerOperationOutcome::Invalid {
                reason: "split requires region, split key, and child descriptor",
            });
        }
        if self
            .hosted_regions
            .get(child.region_id)
            .map_err(internal_status)?
            .is_some()
        {
            return Ok(SchedulerOperationOutcome::Applied);
        }
        let (_identity, parent) = self
            .hosted_regions
            .get(operation.region_id)
            .map_err(internal_status)?
            .ok_or_else(|| {
                tonic::Status::failed_precondition(format!(
                    "split parent region {} is not hosted",
                    operation.region_id
                ))
            })?;
        let runtime = parent.raft_runtime_status();
        if !runtime.leader {
            return Err(tonic::Status::failed_precondition(format!(
                "split parent region {} is not leader",
                operation.region_id
            )));
        }
        let parent_descriptor = parent
            .region_descriptor()
            .map_err(|err| tonic::Status::internal(err.to_string()))?
            .ok_or_else(|| tonic::Status::failed_precondition("split parent descriptor missing"))?;
        let (left, right) =
            build_split_descriptors(&parent_descriptor, &operation.split_key, child)?;
        local_peer_for_store(&right, self.store_id)?;
        self.validate_child_bootstrap_members(&right)?;

        self.publish_split_event(
            metapb::RootEventKind::RegionSplitPlanned,
            &left,
            &right,
            true,
        )
        .await?;
        let split_command = AdminCommand {
            r#type: AdminCommandType::Split as i32,
            split: Some(SplitCommand {
                parent_region_id: operation.region_id,
                split_key: operation.split_key.clone(),
                child: Some(right.clone()),
            }),
            merge: None,
        };
        parent
            .propose_admin_command(operation.region_id, &split_command)
            .await
            .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?;
        self.open_missing_local_region_descriptor(right.clone(), true)
            .await?;
        self.reconcile_local_region_descriptors().await?;
        self.publish_split_event(
            metapb::RootEventKind::RegionSplitCommitted,
            &left,
            &right,
            false,
        )
        .await?;
        Ok(SchedulerOperationOutcome::Applied)
    }

    pub(crate) async fn reconcile_local_region_descriptors(&self) -> Result<(), tonic::Status> {
        let descriptors = self
            .metadata_store
            .region_descriptors()
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        let merged_sources = merged_source_region_ids_for_store(&descriptors, self.store_id);
        for region_id in &merged_sources {
            self.remove_merged_source_region(*region_id).await?;
        }
        for descriptor in descriptors {
            if merged_sources.contains(&descriptor.region_id) {
                continue;
            }
            self.open_missing_local_region_descriptor(descriptor, false)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn open_missing_local_region_descriptor(
        &self,
        descriptor: metapb::RegionDescriptor,
        force_membership_init: bool,
    ) -> Result<(), tonic::Status> {
        if descriptor.region_id == 0
            || self
                .hosted_regions
                .get(descriptor.region_id)
                .map_err(internal_status)?
                .is_some()
        {
            return Ok(());
        }
        let peer = match local_peer_for_store(&descriptor, self.store_id) {
            Ok(peer) => peer,
            Err(err) if err.code() == tonic::Code::FailedPrecondition => return Ok(()),
            Err(err) => return Err(err),
        };
        let membership_init =
            self.child_membership_init(&descriptor, &peer, force_membership_init)?;
        self.open_split_child_region(
            ServerIdentity {
                region_id: descriptor.region_id,
                store_id: peer.store_id,
                peer_id: peer.peer_id,
                bootstrap: descriptor.peers.len() == 1,
            },
            descriptor,
            membership_init,
        )
        .await
    }

    pub(crate) fn validate_child_bootstrap_members(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> Result<(), tonic::Status> {
        let local_peer = local_peer_for_store(descriptor, self.store_id)?;
        self.child_membership_init(descriptor, &local_peer, true)
            .map(|_| ())
    }

    pub(crate) fn child_membership_init(
        &self,
        descriptor: &metapb::RegionDescriptor,
        local_peer: &metapb::RegionPeer,
        force_membership_init: bool,
    ) -> Result<Option<BTreeMap<u64, BasicNode>>, tonic::Status> {
        if descriptor.peers.len() <= 1 {
            return Ok(None);
        }
        if !force_membership_init && !local_peer_is_first(descriptor, local_peer) {
            return Ok(None);
        }
        descriptor_membership_nodes(
            descriptor,
            local_peer,
            &self.advertised_addr,
            &self.peer_endpoints,
        )
        .map(Some)
    }

    pub(crate) async fn execute_merge(
        &self,
        operation: &coordpb::SchedulerOperation,
    ) -> Result<SchedulerOperationOutcome, tonic::Status> {
        if operation.region_id == 0 || operation.source_region_id == 0 {
            return Ok(SchedulerOperationOutcome::Invalid {
                reason: "merge requires target region and source region",
            });
        }
        let (_identity, target) = self
            .hosted_regions
            .get(operation.region_id)
            .map_err(internal_status)?
            .ok_or_else(|| {
                tonic::Status::failed_precondition(format!(
                    "merge target region {} is not hosted",
                    operation.region_id
                ))
            })?;
        let target_descriptor = target
            .region_descriptor()
            .map_err(|err| tonic::Status::internal(err.to_string()))?
            .ok_or_else(|| tonic::Status::failed_precondition("merge target descriptor missing"))?;
        let Some((_source_identity, source)) = self
            .hosted_regions
            .get(operation.source_region_id)
            .map_err(internal_status)?
        else {
            if merge_source_already_absorbed(&target_descriptor, operation.source_region_id) {
                return Ok(SchedulerOperationOutcome::Applied);
            }
            return Err(tonic::Status::failed_precondition(format!(
                "merge source region {} is not hosted",
                operation.source_region_id
            )));
        };
        let runtime = target.raft_runtime_status();
        if !runtime.leader {
            return Err(tonic::Status::failed_precondition(format!(
                "merge target region {} is not leader",
                operation.region_id
            )));
        }
        let source_descriptor = source
            .region_descriptor()
            .map_err(|err| tonic::Status::internal(err.to_string()))?
            .ok_or_else(|| tonic::Status::failed_precondition("merge source descriptor missing"))?;
        ensure_merge_store_coverage(&target_descriptor, &source_descriptor)?;
        let merged = build_merge_descriptor(&target_descriptor, &source_descriptor)?;
        let (left_id, right_id) = merge_region_ids(&target_descriptor, &source_descriptor);

        self.publish_merge_event(
            metapb::RootEventKind::RegionMergePlanned,
            left_id,
            right_id,
            &merged,
            true,
        )
        .await?;
        let merge_command = AdminCommand {
            r#type: AdminCommandType::Merge as i32,
            split: None,
            merge: Some(MergeCommand {
                target_region_id: operation.region_id,
                source_region_id: operation.source_region_id,
            }),
        };
        target
            .propose_admin_command(operation.region_id, &merge_command)
            .await
            .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?;
        self.reconcile_local_region_descriptors().await?;
        self.publish_merge_event(
            metapb::RootEventKind::RegionMerged,
            left_id,
            right_id,
            &merged,
            false,
        )
        .await?;
        Ok(SchedulerOperationOutcome::Applied)
    }

    pub(crate) async fn publish_split_event(
        &self,
        kind: metapb::RootEventKind,
        left: &metapb::RegionDescriptor,
        right: &metapb::RegionDescriptor,
        require_published: bool,
    ) -> Result<(), tonic::Status> {
        let Some(config) = &self.coordinator else {
            return Ok(());
        };
        let event = split_root_event(kind, left, right);
        let outcome =
            publish_root_event_with_pending(&config.endpoints, Some(&self.metadata_store), event)
                .await;
        match outcome.publish_state() {
            adminpb::ExecutionPublishState::TerminalPublished => Ok(()),
            adminpb::ExecutionPublishState::TerminalPending
            | adminpb::ExecutionPublishState::TerminalBlocked
                if !require_published =>
            {
                Ok(())
            }
            adminpb::ExecutionPublishState::TerminalPending
            | adminpb::ExecutionPublishState::TerminalBlocked => Err(tonic::Status::unavailable(
                format!("split root event not published: {}", outcome.last_error()),
            )),
            adminpb::ExecutionPublishState::TerminalFailed => {
                Err(tonic::Status::internal(outcome.last_error().to_owned()))
            }
            adminpb::ExecutionPublishState::Unspecified
            | adminpb::ExecutionPublishState::NotRequired
            | adminpb::ExecutionPublishState::PlannedPublished
                if !require_published =>
            {
                Ok(())
            }
            adminpb::ExecutionPublishState::Unspecified
            | adminpb::ExecutionPublishState::NotRequired
            | adminpb::ExecutionPublishState::PlannedPublished => {
                Err(tonic::Status::internal(format!(
                    "split root event reached invalid publish state {:?}",
                    outcome.publish_state()
                )))
            }
        }
    }

    pub(crate) async fn publish_merge_event(
        &self,
        kind: metapb::RootEventKind,
        left_region_id: u64,
        right_region_id: u64,
        merged: &metapb::RegionDescriptor,
        require_published: bool,
    ) -> Result<(), tonic::Status> {
        let Some(config) = &self.coordinator else {
            return Ok(());
        };
        let event = merge_root_event(kind, left_region_id, right_region_id, merged);
        let outcome =
            publish_root_event_with_pending(&config.endpoints, Some(&self.metadata_store), event)
                .await;
        match outcome.publish_state() {
            adminpb::ExecutionPublishState::TerminalPublished => Ok(()),
            adminpb::ExecutionPublishState::TerminalPending
            | adminpb::ExecutionPublishState::TerminalBlocked
                if !require_published =>
            {
                Ok(())
            }
            adminpb::ExecutionPublishState::TerminalPending
            | adminpb::ExecutionPublishState::TerminalBlocked => Err(tonic::Status::unavailable(
                format!("merge root event not published: {}", outcome.last_error()),
            )),
            adminpb::ExecutionPublishState::TerminalFailed => {
                Err(tonic::Status::internal(outcome.last_error().to_owned()))
            }
            adminpb::ExecutionPublishState::Unspecified
            | adminpb::ExecutionPublishState::NotRequired
            | adminpb::ExecutionPublishState::PlannedPublished
                if !require_published =>
            {
                Ok(())
            }
            adminpb::ExecutionPublishState::Unspecified
            | adminpb::ExecutionPublishState::NotRequired
            | adminpb::ExecutionPublishState::PlannedPublished => {
                Err(tonic::Status::internal(format!(
                    "merge root event reached invalid publish state {:?}",
                    outcome.publish_state()
                )))
            }
        }
    }

    pub(crate) async fn open_split_child_region(
        &self,
        identity: ServerIdentity,
        descriptor: metapb::RegionDescriptor,
        membership_init: Option<BTreeMap<u64, BasicNode>>,
    ) -> Result<(), tonic::Status> {
        self.metadata_store
            .put_region_descriptor(&descriptor)
            .and_then(|_| self.metadata_store.checkpoint())
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        let apply_status = self
            .metadata_store
            .get_region_apply_state(descriptor.region_id)
            .map_err(|err| tonic::Status::internal(err.to_string()))?
            .map(apply_status_from_holt)
            .unwrap_or(nokv_raftnode::ApplyStatus {
                region_id: descriptor.region_id,
                term: 1,
                applied_index: 0,
            });
        let engine = AppliedMetadataEngine::with_status(apply_status, self.metadata_store.clone());
        engine
            .set_region_descriptor_catalog(Arc::new(HoltRegionDescriptorCatalog::new(
                self.metadata_store.clone(),
            )))
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        engine
            .set_region_descriptor(descriptor.clone())
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        let engine = PersistentAppliedMetadataEngine::new(
            engine,
            HoltRegionMetadataSink::new(self.metadata_store.clone()),
        );
        let log_dir = region_log_dir(
            self.persistent_root.join("raftlog"),
            identity.region_id,
            true,
        );
        let open_identity = ServerIdentity {
            bootstrap: identity.bootstrap && membership_init.is_none(),
            ..identity
        };
        let region = open_openraft_region(open_identity, &self.advertised_addr, log_dir, engine)
            .await
            .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?;
        self.transport
            .register(identity.region_id, region.raft_handle());
        if let Some(members) = membership_init {
            let single_member = members.len() == 1;
            let initialized = region
                .initialize_members(members)
                .await
                .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?;
            if initialized && single_member {
                region
                    .elect_and_wait(identity.peer_id)
                    .await
                    .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?;
                region
                    .ensure_linearizable()
                    .await
                    .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?;
            } else if initialized {
                // Remote child peers may open after observing the same split event.
                // Register locally now and let retries elect once quorum is reachable.
                spawn_recovered_region_leadership_retries(vec![(identity, region.clone())]);
            }
        }
        let admission = RegionAdmission::from_descriptor(&descriptor, identity.bootstrap)
            .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
        let (metadata, admin) = openraft_metadata_service_pair(
            region.clone(),
            admission,
            self.peer_endpoints.clone(),
            HoltRegionMetadataSink::new(self.metadata_store.clone()),
            self.topology_publisher.clone(),
            Arc::new(self.metadata_store.clone()),
        );
        self.metadata_services
            .insert_region(identity.region_id, metadata)?;
        self.admin_services
            .insert_region(identity.region_id, admin)?;
        self.hosted_regions
            .insert(identity, region)
            .map_err(internal_status)?;
        Ok(())
    }

    pub(crate) async fn remove_merged_source_region(
        &self,
        region_id: u64,
    ) -> Result<(), tonic::Status> {
        let removed = self
            .hosted_regions
            .remove(region_id)
            .map_err(internal_status)?;
        self.metadata_services.remove_region(region_id)?;
        self.admin_services.remove_region(region_id)?;
        self.transport.unregister(region_id);
        self.metadata_store
            .delete_region_descriptor(region_id)
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        if let Some((_identity, region)) = removed {
            region
                .shutdown()
                .await
                .map_err(|err| tonic::Status::internal(err.to_string()))?;
        }
        Ok(())
    }
}

pub(crate) fn internal_status(message: impl ToString) -> tonic::Status {
    tonic::Status::internal(message.to_string())
}
