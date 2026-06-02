use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use nokv_holtstore::HoltMetadataStore;
use nokv_metadata_state::MemoryMetadataStore;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_raftnode::{AppliedMetadataEngine, BasicNode, PersistentAppliedMetadataEngine};
use nokv_raftstore_server::{
    apply_status_from_holt, openraft_metadata_service_pair, serve_with_metadata_region_services,
    EmptyRegionDescriptorSink, EmptyRestartDiagnostics, EmptyTopologyPublisher,
    HoltRegionMetadataSink, MultiRegionMetadataPlaneService, MultiRegionRaftAdminService,
    PeerEndpointCatalog, RegionAdmission, TopologyPublisher,
};

use super::coordinator::{
    spawn_multi_region_coordinator_heartbeat, spawn_pending_topology_retries,
    CoordinatorHeartbeatConfig,
};
use super::hosted_region::HostedRegionRegistry;
use super::metrics::spawn_metrics_server;
use super::region_open::{
    open_openraft_region, raft_log_dir_for_region, spawn_recovered_region_leadership_retries,
};
use super::root_publication::{
    spawn_startup_root_publication_for_regions, CoordinatorTopologyPublisher,
};
use super::startup::{RegionKeyRange, RegionRangeCatalog, ServerIdentity};

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
    spawn_multi_region_coordinator_heartbeat(
        coordinator.clone(),
        identities[0].store_id,
        addr,
        advertised_addr,
        hosted_region_registry,
        Some(metadata_store.clone()),
    );
    spawn_pending_topology_retries(coordinator, metadata_store.clone(), addr);
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
    for descriptor in descriptors {
        if descriptor.region_id == 0 || seen.contains(&descriptor.region_id) {
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
