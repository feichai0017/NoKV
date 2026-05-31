use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nokv_holtstore::HoltMetadataStore;
use nokv_metastore::MemoryMetadataStore;
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::raft::v1 as raftpb;
use nokv_raftnode::{
    AppliedMetadataEngine, BasicNode, OpenRaftRegion, PersistentAppliedMetadataEngine,
    RegionDescriptorCatalog, RegionLogStorage, RegionSnapshotEngine, RegionStateMachine,
    SegmentedEntryLog, TonicRaftNetworkFactory,
};
use nokv_raftstore_server::{
    apply_status_from_holt, openraft_metadata_service_pair, serve_with_metadata_region_services,
    EmptyRegionDescriptorSink, EmptyRestartDiagnostics, EmptyTopologyPublisher,
    HoltRegionMetadataSink, MultiRegionMetadataPlaneService, MultiRegionRaftAdminService,
    PeerEndpointCatalog, RaftRuntimeStatusProvider, RegionAdmission, TopologyPublisher,
};

use crate::coordinator::{
    spawn_multi_region_coordinator_heartbeat, spawn_pending_topology_retries,
    CoordinatorHeartbeatConfig, SchedulerOperationOutcome,
};
use crate::metrics::spawn_metrics_server;
use crate::root_publication::{
    publish_root_event_with_pending, spawn_startup_root_publication_for_regions,
    CoordinatorTopologyPublisher,
};
use crate::startup::{RegionKeyRange, RegionRangeCatalog, ServerIdentity};

#[derive(Clone)]
pub(crate) struct HostedRegionRegistry<E> {
    regions: Arc<RwLock<BTreeMap<u64, (ServerIdentity, OpenRaftRegion<E>)>>>,
}

impl<E> HostedRegionRegistry<E> {
    pub(crate) fn new(
        regions: impl IntoIterator<Item = (ServerIdentity, OpenRaftRegion<E>)>,
    ) -> Result<Self, String> {
        let registry = Self {
            regions: Arc::new(RwLock::new(BTreeMap::new())),
        };
        for (identity, region) in regions {
            registry.insert(identity, region)?;
        }
        Ok(registry)
    }

    pub(crate) fn insert(
        &self,
        identity: ServerIdentity,
        region: OpenRaftRegion<E>,
    ) -> Result<(), String> {
        if identity.region_id == 0 {
            return Err("hosted region id is required".to_owned());
        }
        let mut regions = self
            .regions
            .write()
            .map_err(|_| "hosted region registry lock poisoned".to_owned())?;
        if regions
            .insert(identity.region_id, (identity, region))
            .is_some()
        {
            return Err(format!("duplicate hosted region {}", identity.region_id));
        }
        Ok(())
    }

    pub(crate) fn get(
        &self,
        region_id: u64,
    ) -> Result<Option<(ServerIdentity, OpenRaftRegion<E>)>, String>
    where
        E: Clone,
    {
        self.regions
            .read()
            .map_err(|_| "hosted region registry lock poisoned".to_owned())
            .map(|regions| regions.get(&region_id).cloned())
    }

    pub(crate) fn remove(
        &self,
        region_id: u64,
    ) -> Result<Option<(ServerIdentity, OpenRaftRegion<E>)>, String>
    where
        E: Clone,
    {
        self.regions
            .write()
            .map_err(|_| "hosted region registry lock poisoned".to_owned())
            .map(|mut regions| regions.remove(&region_id))
    }

    pub(crate) fn snapshot(&self) -> Result<Vec<(ServerIdentity, OpenRaftRegion<E>)>, String>
    where
        E: Clone,
    {
        self.regions
            .read()
            .map_err(|_| "hosted region registry lock poisoned".to_owned())
            .map(|regions| regions.values().cloned().collect())
    }
}

pub(crate) type HoltApplyEngine =
    PersistentAppliedMetadataEngine<HoltMetadataStore, HoltRegionMetadataSink>;
pub(crate) type HoltRegion = OpenRaftRegion<HoltApplyEngine>;

#[derive(Clone)]
pub(crate) struct HoltRegionDescriptorCatalog {
    store: HoltMetadataStore,
}

impl HoltRegionDescriptorCatalog {
    pub(crate) fn new(store: HoltMetadataStore) -> Self {
        Self { store }
    }
}

impl std::fmt::Debug for HoltRegionDescriptorCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HoltRegionDescriptorCatalog")
            .finish_non_exhaustive()
    }
}

impl RegionDescriptorCatalog for HoltRegionDescriptorCatalog {
    fn region_descriptor(
        &self,
        region_id: u64,
    ) -> nokv_metastore::Result<Option<metapb::RegionDescriptor>> {
        self.store
            .get_region_descriptor(region_id)
            .map_err(|err| nokv_metastore::Error::Backend(err.to_string()))
    }
}

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
        let split_command = raftpb::AdminCommand {
            r#type: raftpb::admin_command::Type::Split as i32,
            split: Some(raftpb::SplitCommand {
                parent_region_id: operation.region_id,
                split_key: operation.split_key.clone(),
                child: Some(right.clone()),
            }),
            ..Default::default()
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
        let merge_command = raftpb::AdminCommand {
            r#type: raftpb::admin_command::Type::Merge as i32,
            merge: Some(raftpb::MergeCommand {
                target_region_id: operation.region_id,
                source_region_id: operation.source_region_id,
            }),
            ..Default::default()
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

pub(crate) fn ensure_merge_store_coverage(
    target: &metapb::RegionDescriptor,
    source: &metapb::RegionDescriptor,
) -> Result<(), tonic::Status> {
    let target_stores = region_peer_store_ids(target)?;
    let source_stores = region_peer_store_ids(source)?;
    if target_stores == source_stores {
        return Ok(());
    }
    Err(tonic::Status::unimplemented(format!(
        "merge target region {} and source region {} must cover the same store set before raftstore can safely retire source peers",
        target.region_id, source.region_id
    )))
}

pub(crate) fn region_peer_store_ids(
    descriptor: &metapb::RegionDescriptor,
) -> Result<BTreeSet<u64>, tonic::Status> {
    let mut stores = BTreeSet::new();
    for peer in &descriptor.peers {
        if peer.store_id == 0 || peer.peer_id == 0 {
            return Err(tonic::Status::invalid_argument(format!(
                "region {} has an invalid peer entry",
                descriptor.region_id
            )));
        }
        if !stores.insert(peer.store_id) {
            return Err(tonic::Status::invalid_argument(format!(
                "region {} has duplicate peer store {}",
                descriptor.region_id, peer.store_id
            )));
        }
    }
    if stores.is_empty() {
        return Err(tonic::Status::invalid_argument(format!(
            "region {} has no peers",
            descriptor.region_id
        )));
    }
    Ok(stores)
}

pub(crate) fn merged_source_region_ids_for_store(
    descriptors: &[metapb::RegionDescriptor],
    store_id: u64,
) -> HashSet<u64> {
    descriptors
        .iter()
        .filter(|descriptor| {
            descriptor
                .peers
                .iter()
                .any(|peer| peer.store_id == store_id && peer.peer_id != 0)
        })
        .flat_map(|descriptor| {
            descriptor
                .lineage
                .iter()
                .filter(|lineage| {
                    lineage.region_id != 0
                        && lineage.kind == metapb::DescriptorLineageKind::MergeSource as i32
                })
                .map(|lineage| lineage.region_id)
        })
        .collect()
}

pub(crate) fn local_peer_for_store(
    descriptor: &metapb::RegionDescriptor,
    store_id: u64,
) -> Result<metapb::RegionPeer, tonic::Status> {
    descriptor
        .peers
        .iter()
        .find(|peer| peer.store_id == store_id)
        .cloned()
        .ok_or_else(|| {
            tonic::Status::failed_precondition(format!(
                "region {} has no peer on store {}",
                descriptor.region_id, store_id
            ))
        })
}

pub(crate) fn local_peer_is_first(
    descriptor: &metapb::RegionDescriptor,
    local_peer: &metapb::RegionPeer,
) -> bool {
    descriptor.peers.first().is_some_and(|peer| {
        peer.store_id == local_peer.store_id && peer.peer_id == local_peer.peer_id
    })
}

pub(crate) fn descriptor_membership_nodes(
    descriptor: &metapb::RegionDescriptor,
    local_peer: &metapb::RegionPeer,
    local_addr: &str,
    peer_endpoints: &PeerEndpointCatalog,
) -> Result<BTreeMap<u64, BasicNode>, tonic::Status> {
    let mut members = BTreeMap::new();
    for peer in &descriptor.peers {
        if peer.store_id == 0 || peer.peer_id == 0 {
            return Err(tonic::Status::invalid_argument(format!(
                "region {} has an invalid peer entry",
                descriptor.region_id
            )));
        }
        let node = if peer.store_id == local_peer.store_id && peer.peer_id == local_peer.peer_id {
            BasicNode::new(local_addr.to_owned())
        } else {
            peer_endpoints.node_for_peer(peer.store_id, peer.peer_id)?
        };
        if members.insert(peer.peer_id, node).is_some() {
            return Err(tonic::Status::invalid_argument(format!(
                "region {} has duplicate peer id {}",
                descriptor.region_id, peer.peer_id
            )));
        }
    }
    Ok(members)
}

pub(crate) fn internal_status(message: impl ToString) -> tonic::Status {
    tonic::Status::internal(message.to_string())
}

pub(crate) fn build_split_descriptors(
    parent: &metapb::RegionDescriptor,
    split_key: &[u8],
    child: &metapb::RegionDescriptor,
) -> Result<(metapb::RegionDescriptor, metapb::RegionDescriptor), tonic::Status> {
    if parent.region_id == 0 || child.region_id == 0 {
        return Err(tonic::Status::invalid_argument(
            "split parent and child region ids are required",
        ));
    }
    if split_key.is_empty()
        || split_key <= parent.start_key.as_slice()
        || (!parent.end_key.is_empty() && split_key >= parent.end_key.as_slice())
    {
        return Err(tonic::Status::invalid_argument(
            "split key must be inside parent range",
        ));
    }
    let Some(parent_epoch) = parent.epoch.clone() else {
        return Err(tonic::Status::invalid_argument(
            "split parent epoch is required",
        ));
    };
    let mut left = parent.clone();
    left.end_key = split_key.to_vec();
    let epoch = left.epoch.get_or_insert_with(Default::default);
    epoch.version = epoch.version.saturating_add(1);
    left.hash.clear();
    append_split_lineage(&mut left, parent, &parent_epoch);

    let mut right = child.clone();
    if right.start_key.is_empty() {
        right.start_key = split_key.to_vec();
    }
    if right.start_key != split_key {
        return Err(tonic::Status::invalid_argument(
            "split child start key must equal split key",
        ));
    }
    if right.end_key != parent.end_key {
        return Err(tonic::Status::invalid_argument(
            "split child end key must equal original parent end key",
        ));
    }
    if right.epoch.is_none() {
        right.epoch = Some(parent_epoch.clone());
    }
    if right.peers.is_empty() {
        return Err(tonic::Status::invalid_argument(
            "split child peers are required",
        ));
    }
    right.hash.clear();
    append_split_lineage(&mut right, parent, &parent_epoch);
    Ok((left, right))
}

pub(crate) fn append_split_lineage(
    descriptor: &mut metapb::RegionDescriptor,
    parent: &metapb::RegionDescriptor,
    parent_epoch: &metapb::RegionEpoch,
) {
    descriptor.lineage.push(metapb::DescriptorLineageRef {
        region_id: parent.region_id,
        epoch: Some(parent_epoch.clone()),
        hash: parent.hash.clone(),
        kind: metapb::DescriptorLineageKind::SplitParent as i32,
    });
}

pub(crate) fn build_merge_descriptor(
    target: &metapb::RegionDescriptor,
    source: &metapb::RegionDescriptor,
) -> Result<metapb::RegionDescriptor, tonic::Status> {
    if target.region_id == 0 || source.region_id == 0 {
        return Err(tonic::Status::invalid_argument(
            "merge target and source region ids are required",
        ));
    }
    if target.end_key != source.start_key {
        return Err(tonic::Status::unimplemented(
            "raftstore merge currently requires the source region to be the target's right sibling",
        ));
    }
    let Some(source_epoch) = source.epoch.clone() else {
        return Err(tonic::Status::invalid_argument(
            "merge source epoch is required",
        ));
    };
    let Some(target_epoch) = target.epoch.clone() else {
        return Err(tonic::Status::invalid_argument(
            "merge target epoch is required",
        ));
    };
    let mut merged = target.clone();
    merged.end_key = source.end_key.clone();
    let epoch = merged.epoch.get_or_insert(target_epoch);
    epoch.version = epoch.version.saturating_add(1);
    merged.hash.clear();
    merged.lineage.push(metapb::DescriptorLineageRef {
        region_id: source.region_id,
        epoch: Some(source_epoch),
        hash: source.hash.clone(),
        kind: metapb::DescriptorLineageKind::MergeSource as i32,
    });
    Ok(merged)
}

pub(crate) fn merge_region_ids(
    target: &metapb::RegionDescriptor,
    source: &metapb::RegionDescriptor,
) -> (u64, u64) {
    if source.start_key < target.start_key {
        (source.region_id, target.region_id)
    } else {
        (target.region_id, source.region_id)
    }
}

pub(crate) fn merge_source_already_absorbed(
    target: &metapb::RegionDescriptor,
    source_region_id: u64,
) -> bool {
    target.lineage.iter().any(|lineage| {
        lineage.region_id == source_region_id
            && lineage.kind == metapb::DescriptorLineageKind::MergeSource as i32
    })
}

pub(crate) fn split_root_event(
    kind: metapb::RootEventKind,
    left: &metapb::RegionDescriptor,
    right: &metapb::RegionDescriptor,
) -> metapb::RootEvent {
    metapb::RootEvent {
        kind: kind as i32,
        payload: Some(metapb::root_event::Payload::RangeSplit(
            metapb::RootRangeSplit {
                parent_region_id: left.region_id,
                split_key: right.start_key.clone(),
                left: Some(left.clone()),
                right: Some(right.clone()),
                ..Default::default()
            },
        )),
    }
}

pub(crate) fn merge_root_event(
    kind: metapb::RootEventKind,
    left_region_id: u64,
    right_region_id: u64,
    merged: &metapb::RegionDescriptor,
) -> metapb::RootEvent {
    metapb::RootEvent {
        kind: kind as i32,
        payload: Some(metapb::root_event::Payload::RangeMerge(
            metapb::RootRangeMerge {
                left_region_id,
                right_region_id,
                merged: Some(merged.clone()),
                ..Default::default()
            },
        )),
    }
}

fn coordinator_topology_publisher(
    config: Option<CoordinatorHeartbeatConfig>,
    pending_store: Option<HoltMetadataStore>,
) -> Arc<dyn TopologyPublisher> {
    config
        .map(|config| {
            Arc::new(CoordinatorTopologyPublisher {
                endpoints: config.endpoints,
                pending_store,
            }) as Arc<dyn TopologyPublisher>
        })
        .unwrap_or_else(|| Arc::new(EmptyTopologyPublisher))
}

pub(crate) async fn serve_holt_regions(
    addr: SocketAddr,
    advertised_addr: String,
    identities: Vec<ServerIdentity>,
    coordinator: Option<CoordinatorHeartbeatConfig>,
    peer_endpoints: PeerEndpointCatalog,
    region_ranges: RegionRangeCatalog,
    persistent_root: PathBuf,
    metrics_addr: Option<SocketAddr>,
    temp_log_dir: &mut Option<tempfile::TempDir>,
) -> Result<(), Box<dyn std::error::Error>> {
    let metadata_store = HoltMetadataStore::open_file(&persistent_root)?;
    let configured_region_ids = identities
        .iter()
        .map(|identity| identity.region_id)
        .collect::<HashSet<_>>();
    let identities = recover_holt_hosted_identities(&metadata_store, identities)?;
    tracing::info!(
        %addr,
        %advertised_addr,
        path = %persistent_root.display(),
        region_count = identities.len(),
        "starting raftstore server with multi-region Holt metadata store"
    );
    let topology_publisher =
        coordinator_topology_publisher(coordinator.clone(), Some(metadata_store.clone()));
    let mut metadata_services = Vec::with_capacity(identities.len());
    let mut admin_services = Vec::with_capacity(identities.len());
    let mut hosted_regions = Vec::with_capacity(identities.len());
    let mut startup_descriptors = Vec::with_capacity(identities.len());
    let mut recovered_leadership = Vec::new();
    let transport = nokv_raftnode::TonicRaftTransportRegistry::default();
    let multi_region = true;

    for identity in identities.iter().copied() {
        let descriptor = startup_region_descriptor(
            &metadata_store,
            identity,
            region_ranges.get(identity.region_id),
        )?;
        let admission = RegionAdmission::from_descriptor(&descriptor, identity.bootstrap)?;
        let apply_status = metadata_store
            .get_region_apply_state(descriptor.region_id)?
            .map(apply_status_from_holt)
            .unwrap_or(nokv_raftnode::ApplyStatus {
                region_id: descriptor.region_id,
                term: 1,
                applied_index: 0,
            });
        let engine = AppliedMetadataEngine::with_status(apply_status, metadata_store.clone());
        engine.set_region_descriptor_catalog(Arc::new(HoltRegionDescriptorCatalog::new(
            metadata_store.clone(),
        )))?;
        engine.set_region_descriptor(descriptor.clone())?;
        let engine = PersistentAppliedMetadataEngine::new(
            engine,
            HoltRegionMetadataSink::new(metadata_store.clone()),
        );
        let log_dir =
            raft_log_dir_for_region(Some(&persistent_root), identity, multi_region, temp_log_dir)?;
        let region = open_openraft_region(identity, &advertised_addr, log_dir, engine).await?;
        transport.register(identity.region_id, region.raft_handle());
        if let Some(members) = recovered_descriptor_membership_init(
            &descriptor,
            identity,
            &advertised_addr,
            &peer_endpoints,
        )? {
            region.initialize_members(members).await?;
            recovered_leadership.push((identity, region.clone()));
        }
        let (metadata_service, admin_service) = openraft_metadata_service_pair(
            region.clone(),
            admission,
            peer_endpoints.clone(),
            HoltRegionMetadataSink::new(metadata_store.clone()),
            topology_publisher.clone(),
            Arc::new(EmptyRestartDiagnostics),
        );
        metadata_services.push((identity.region_id, metadata_service));
        admin_services.push((identity.region_id, admin_service));
        hosted_regions.push((identity, region));
        if configured_region_ids.contains(&identity.region_id) {
            startup_descriptors.push(descriptor);
        }
    }

    spawn_startup_root_publication_for_regions(
        coordinator.clone(),
        identities.clone(),
        startup_descriptors,
        Some(metadata_store.clone()),
    );
    let hosted_region_registry = HostedRegionRegistry::new(hosted_regions)?;
    spawn_recovered_region_leadership_retries(recovered_leadership);
    let metadata_router = MultiRegionMetadataPlaneService::new(metadata_services)?;
    let admin_router = MultiRegionRaftAdminService::new(admin_services)?
        .with_restart_diagnostics(Arc::new(metadata_store.clone()));
    spawn_metrics_server(
        metrics_addr,
        identities[0].store_id,
        advertised_addr.clone(),
        hosted_region_registry.clone(),
        Some(metadata_store.clone()),
    );
    let range_controller = HoltRangeController {
        store_id: identities[0].store_id,
        advertised_addr: advertised_addr.clone(),
        persistent_root: persistent_root.clone(),
        coordinator: coordinator.clone(),
        metadata_store: metadata_store.clone(),
        transport: transport.clone(),
        metadata_services: metadata_router.clone(),
        admin_services: admin_router.clone(),
        hosted_regions: hosted_region_registry.clone(),
        peer_endpoints: peer_endpoints.clone(),
        topology_publisher: topology_publisher.clone(),
    };
    spawn_multi_region_coordinator_heartbeat(
        coordinator.clone(),
        identities[0].store_id,
        addr,
        advertised_addr,
        hosted_region_registry,
        Some(metadata_store.clone()),
        Some(range_controller.clone()),
    );
    spawn_pending_topology_retries(
        coordinator,
        metadata_store.clone(),
        addr,
        Some(range_controller),
    );
    serve_with_metadata_region_services(addr, metadata_router, admin_router, transport).await?;
    Ok(())
}

pub(crate) fn recover_holt_hosted_identities(
    store: &HoltMetadataStore,
    configured: Vec<ServerIdentity>,
) -> Result<Vec<ServerIdentity>, Box<dyn std::error::Error>> {
    let Some(first) = configured.first().copied() else {
        return Ok(configured);
    };
    let local_store_id = first.store_id;
    let mut seen = configured
        .iter()
        .map(|identity| identity.region_id)
        .collect::<HashSet<_>>();
    let mut identities = configured;
    let descriptors = store.region_descriptors()?;
    let merged_sources = merged_source_region_ids_for_store(&descriptors, local_store_id);
    for descriptor in descriptors {
        if descriptor.region_id == 0 || seen.contains(&descriptor.region_id) {
            continue;
        }
        if merged_sources.contains(&descriptor.region_id) {
            continue;
        }
        let Some(peer) = descriptor
            .peers
            .iter()
            .find(|peer| peer.store_id == local_store_id)
            .cloned()
        else {
            continue;
        };
        identities.push(ServerIdentity {
            region_id: descriptor.region_id,
            store_id: peer.store_id,
            peer_id: peer.peer_id,
            bootstrap: descriptor.peers.len() == 1,
        });
        seen.insert(descriptor.region_id);
    }
    identities.sort_by_key(|identity| identity.region_id);
    Ok(identities)
}

pub(crate) fn recovered_descriptor_membership_init(
    descriptor: &metapb::RegionDescriptor,
    identity: ServerIdentity,
    addr: &str,
    peer_endpoints: &PeerEndpointCatalog,
) -> Result<Option<BTreeMap<u64, BasicNode>>, tonic::Status> {
    if descriptor.peers.len() <= 1 {
        return Ok(None);
    }
    let local_peer = local_peer_for_store(descriptor, identity.store_id)?;
    if local_peer.peer_id != identity.peer_id || !local_peer_is_first(descriptor, &local_peer) {
        return Ok(None);
    }
    descriptor_membership_nodes(descriptor, &local_peer, addr, peer_endpoints).map(Some)
}

pub(crate) fn spawn_recovered_region_leadership_retries<E>(
    regions: Vec<(ServerIdentity, OpenRaftRegion<E>)>,
) where
    E: RegionSnapshotEngine + Send + Sync + 'static,
{
    for (identity, region) in regions {
        tokio::spawn(async move {
            for attempt in 1..=50 {
                let voter_count = region
                    .raft_handle()
                    .metrics()
                    .borrow()
                    .membership_config
                    .voter_ids()
                    .count();
                let election = if voter_count <= 1 {
                    region.elect_and_wait(identity.peer_id).await
                } else {
                    region.wait_for_leader(identity.peer_id).await
                };
                match election {
                    Ok(()) => match region.ensure_linearizable().await {
                        Ok(()) => return,
                        Err(err) => {
                            tracing::debug!(
                                region_id = identity.region_id,
                                peer_id = identity.peer_id,
                                attempt,
                                error = %err,
                                "raftstore recovered region linearizable wait failed"
                            );
                        }
                    },
                    Err(err) => {
                        tracing::debug!(
                            region_id = identity.region_id,
                            peer_id = identity.peer_id,
                            voter_count,
                            attempt,
                            error = %err,
                            "raftstore recovered region leadership wait failed"
                        );
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            tracing::warn!(
                region_id = identity.region_id,
                peer_id = identity.peer_id,
                "raftstore recovered region did not elect a startup leader"
            );
        });
    }
}

pub(crate) async fn serve_memory_regions(
    addr: SocketAddr,
    advertised_addr: String,
    identities: Vec<ServerIdentity>,
    coordinator: Option<CoordinatorHeartbeatConfig>,
    peer_endpoints: PeerEndpointCatalog,
    region_ranges: RegionRangeCatalog,
    metrics_addr: Option<SocketAddr>,
    temp_log_dir: &mut Option<tempfile::TempDir>,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!(
        %addr,
        %advertised_addr,
        region_count = identities.len(),
        "starting raftstore server with multi-region in-memory metadata store"
    );
    let topology_publisher = coordinator_topology_publisher(coordinator.clone(), None);
    let mut metadata_services = Vec::with_capacity(identities.len());
    let mut admin_services = Vec::with_capacity(identities.len());
    let mut hosted_regions = Vec::with_capacity(identities.len());
    let mut startup_descriptors = Vec::with_capacity(identities.len());
    let transport = nokv_raftnode::TonicRaftTransportRegistry::default();
    let multi_region = identities.len() > 1;

    for identity in identities.iter().copied() {
        let engine = AppliedMetadataEngine::new(identity.region_id, MemoryMetadataStore::new());
        let descriptor =
            default_region_descriptor_with_range(identity, region_ranges.get(identity.region_id));
        engine.set_region_descriptor(descriptor.clone())?;
        let admission = RegionAdmission::from_descriptor(&descriptor, identity.bootstrap)?;
        let log_dir = raft_log_dir_for_region(None, identity, multi_region, temp_log_dir)?;
        let region = open_openraft_region(identity, &advertised_addr, log_dir, engine).await?;
        transport.register(identity.region_id, region.raft_handle());
        let (metadata_service, admin_service) = openraft_metadata_service_pair(
            region.clone(),
            admission,
            peer_endpoints.clone(),
            EmptyRegionDescriptorSink,
            topology_publisher.clone(),
            Arc::new(EmptyRestartDiagnostics),
        );
        metadata_services.push((identity.region_id, metadata_service));
        admin_services.push((identity.region_id, admin_service));
        hosted_regions.push((identity, region));
        startup_descriptors.push(descriptor);
    }

    spawn_startup_root_publication_for_regions(
        coordinator.clone(),
        identities.clone(),
        startup_descriptors,
        None,
    );
    let hosted_region_registry = HostedRegionRegistry::new(hosted_regions)?;
    spawn_metrics_server(
        metrics_addr,
        identities[0].store_id,
        advertised_addr.clone(),
        hosted_region_registry.clone(),
        None,
    );
    spawn_multi_region_coordinator_heartbeat(
        coordinator,
        identities[0].store_id,
        addr,
        advertised_addr,
        hosted_region_registry,
        None,
        None,
    );
    serve_with_metadata_region_services(
        addr,
        MultiRegionMetadataPlaneService::new(metadata_services)?,
        MultiRegionRaftAdminService::new(admin_services)?,
        transport,
    )
    .await?;
    Ok(())
}

pub(crate) async fn open_openraft_region<E>(
    identity: ServerIdentity,
    addr: &str,
    log_dir: PathBuf,
    engine: E,
) -> Result<OpenRaftRegion<E>, Box<dyn std::error::Error>>
where
    E: RegionSnapshotEngine,
{
    let log = SegmentedEntryLog::open(identity.region_id, log_dir)?;
    let state_machine = RegionStateMachine::new(engine);
    if identity.bootstrap {
        return Ok(OpenRaftRegion::bootstrap_single_node_with_network(
            identity.peer_id,
            identity.region_id,
            RegionLogStorage::new(log),
            state_machine,
            TonicRaftNetworkFactory::new(identity.region_id),
            addr.to_owned(),
        )
        .await?);
    }
    Ok(OpenRaftRegion::open_with_network(
        identity.peer_id,
        identity.region_id,
        RegionLogStorage::new(log),
        state_machine,
        TonicRaftNetworkFactory::new(identity.region_id),
    )
    .await?)
}

pub(crate) fn raft_log_dir_for_region(
    persistent_root: Option<&Path>,
    identity: ServerIdentity,
    multi_region: bool,
    temp_log_dir: &mut Option<tempfile::TempDir>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Ok(path) = std::env::var("NOKV_RAFTSTORE_LOG_DIR") {
        let root = PathBuf::from(path);
        return Ok(region_log_dir(root, identity.region_id, multi_region));
    }
    if let Some(root) = persistent_root {
        return Ok(region_log_dir(
            root.join("raftlog"),
            identity.region_id,
            multi_region,
        ));
    }
    if temp_log_dir.is_none() {
        *temp_log_dir = Some(tempfile::tempdir()?);
    }
    let root = temp_log_dir
        .as_ref()
        .expect("temp log dir is initialized")
        .path()
        .to_path_buf();
    Ok(region_log_dir(root, identity.region_id, multi_region))
}

pub(crate) fn region_log_dir(root: PathBuf, region_id: u64, multi_region: bool) -> PathBuf {
    if multi_region {
        root.join(format!("region-{region_id}"))
    } else {
        root
    }
}

#[cfg(test)]
pub(crate) fn default_region_descriptor(identity: ServerIdentity) -> metapb::RegionDescriptor {
    default_region_descriptor_with_range(identity, None)
}

pub(crate) fn default_region_descriptor_with_range(
    identity: ServerIdentity,
    range: Option<&RegionKeyRange>,
) -> metapb::RegionDescriptor {
    metapb::RegionDescriptor {
        region_id: identity.region_id,
        start_key: range
            .map(|range| range.start_key.clone())
            .unwrap_or_default(),
        end_key: range.map(|range| range.end_key.clone()).unwrap_or_default(),
        epoch: Some(metapb::RegionEpoch {
            version: 1,
            conf_version: 1,
        }),
        peers: vec![metapb::RegionPeer {
            store_id: identity.store_id,
            peer_id: identity.peer_id,
        }],
        ..Default::default()
    }
}

pub(crate) fn startup_region_descriptor(
    store: &HoltMetadataStore,
    identity: ServerIdentity,
    range: Option<&RegionKeyRange>,
) -> nokv_holtstore::Result<metapb::RegionDescriptor> {
    let default = default_region_descriptor_with_range(identity, range);
    if identity.bootstrap {
        return store.load_or_bootstrap_region_descriptor(&default);
    }
    Ok(store
        .get_region_descriptor(identity.region_id)?
        .unwrap_or(default))
}
