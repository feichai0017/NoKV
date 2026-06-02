use std::sync::Arc;

use nokv_metadata_state::MemoryMetadataStore;
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::admin::v1::raft_admin_server::RaftAdmin;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_proto::nokv::metadata::v1::metadata_plane_server::MetadataPlane;
use nokv_raftnode::{
    ApplyStatusProvider, ApplyWatchProvider, BasicNode, MetadataCommandExecutor,
    MetadataReadExecutor, MetadataRetentionExecutor,
};
use tonic::{Request, Status};

use crate::admission::RegionAdmissionState;
use crate::execution::ExecutionRuntime;
use crate::{
    AppliedRegionDescriptorProvider, MetadataPlaneService, MultiRegionMetadataPlaneService,
    MultiRegionRaftAdminService, RaftAdminService, RaftMembershipAdmin, RaftRuntimeStatus,
    RaftRuntimeStatusProvider, RegionAdmission,
};

#[derive(Debug, Clone)]
struct FixedRuntimeEngine {
    inner: nokv_raftnode::AppliedMetadataEngine<MemoryMetadataStore>,
    runtime: RaftRuntimeStatus,
}

impl FixedRuntimeEngine {
    fn leader(region_id: u64, local_peer_id: u64) -> Self {
        Self {
            inner: nokv_raftnode::AppliedMetadataEngine::new(region_id, MemoryMetadataStore::new()),
            runtime: RaftRuntimeStatus {
                local_peer_id,
                leader_peer_id: local_peer_id,
                leader: true,
                hosted: true,
            },
        }
    }

    fn set_region_descriptor(&self, descriptor: metapb::RegionDescriptor) {
        self.inner.set_region_descriptor(descriptor).unwrap();
    }
}

impl MetadataReadExecutor for FixedRuntimeEngine {
    fn execute_metadata_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataGetRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataGetResponse>,
    > + Send
           + 'a {
        self.inner.execute_metadata_get(req)
    }

    fn execute_metadata_batch_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataBatchGetRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataBatchGetResponse>,
    > + Send
           + 'a {
        self.inner.execute_metadata_batch_get(req)
    }

    fn execute_metadata_scan<'a>(
        &'a self,
        req: &'a metadatapb::MetadataScanRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataScanResponse>,
    > + Send
           + 'a {
        self.inner.execute_metadata_scan(req)
    }
}

impl MetadataCommandExecutor for FixedRuntimeEngine {
    fn execute_metadata_command<'a>(
        &'a self,
        req: &'a metadatapb::MetadataCommitRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataCommitResponse>,
    > + Send
           + 'a {
        self.inner.execute_metadata_command(req)
    }
}

impl MetadataRetentionExecutor for FixedRuntimeEngine {
    fn prune_metadata_versions<'a>(
        &'a self,
        retention_floor: u64,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<nokv_metadata_state::MetadataRetentionResult>,
    > + Send
           + 'a {
        self.inner.prune_metadata_versions(retention_floor)
    }
}

impl ApplyStatusProvider for FixedRuntimeEngine {
    fn apply_status(&self) -> nokv_raftnode::ApplyStatus {
        self.inner.apply_status()
    }
}

impl ApplyWatchProvider for FixedRuntimeEngine {
    fn subscribe_apply(
        &self,
    ) -> tokio::sync::broadcast::Receiver<metadatapb::MetadataApplyWatchEvent> {
        self.inner.subscribe_apply()
    }

    fn replay_apply(
        &self,
        request: nokv_raftnode::ApplyWatchReplayRequest,
    ) -> nokv_metadata_state::Result<nokv_raftnode::ApplyWatchReplay> {
        self.inner.replay_apply(request)
    }
}

impl RaftRuntimeStatusProvider for FixedRuntimeEngine {
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        self.runtime
    }
}

impl AppliedRegionDescriptorProvider for FixedRuntimeEngine {
    fn applied_region_descriptor(&self) -> Result<Option<metapb::RegionDescriptor>, Status> {
        self.inner
            .region_descriptor()
            .map_err(|err| Status::internal(err.to_string()))
    }
}

#[derive(Clone, Default)]
struct NoopAdminStatus;

impl ApplyStatusProvider for NoopAdminStatus {
    fn apply_status(&self) -> nokv_raftnode::ApplyStatus {
        nokv_raftnode::ApplyStatus {
            region_id: 1,
            term: 1,
            applied_index: 1,
        }
    }
}

impl RaftRuntimeStatusProvider for NoopAdminStatus {
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        RaftRuntimeStatus {
            local_peer_id: 1,
            leader_peer_id: 1,
            leader: true,
            hosted: true,
        }
    }
}

impl AppliedRegionDescriptorProvider for NoopAdminStatus {}

impl MetadataRetentionExecutor for NoopAdminStatus {
    fn prune_metadata_versions<'a>(
        &'a self,
        retention_floor: u64,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<nokv_metadata_state::MetadataRetentionResult>,
    > + Send
           + 'a {
        async move {
            Ok(nokv_metadata_state::MetadataRetentionResult {
                retention_floor,
                ..Default::default()
            })
        }
    }
}

#[tonic::async_trait]
impl RaftMembershipAdmin for NoopAdminStatus {
    async fn add_voter(&self, _peer_id: u64, _node: BasicNode) -> Result<(), Status> {
        Ok(())
    }

    async fn remove_voter(&self, _peer_id: u64) -> Result<(), Status> {
        Ok(())
    }

    async fn transfer_leader(&self, _peer_id: u64) -> Result<(), Status> {
        Ok(())
    }

    async fn propose_region_descriptor(
        &self,
        _descriptor: &metapb::RegionDescriptor,
    ) -> Result<(), Status> {
        Ok(())
    }
}

fn metadata_context(admission: &RegionAdmission) -> metadatapb::MetadataContext {
    metadatapb::MetadataContext {
        region_id: admission.region_id,
        region_epoch: Some(metapb::RegionEpoch {
            version: admission.epoch_version,
            conf_version: admission.epoch_conf_version,
        }),
        peer: Some(metapb::RegionPeer {
            store_id: admission.store_id,
            peer_id: admission.peer_id,
        }),
        ..Default::default()
    }
}

fn metadata_service(
    engine: FixedRuntimeEngine,
    admission: RegionAdmission,
) -> MetadataPlaneService<FixedRuntimeEngine> {
    MetadataPlaneService::with_admission_state_and_execution(
        engine,
        RegionAdmissionState::new(admission),
        ExecutionRuntime::default(),
    )
}

fn test_region_descriptor(
    region_id: u64,
    store_id: u64,
    peer_id: u64,
    start_key: &[u8],
    end_key: &[u8],
) -> metapb::RegionDescriptor {
    metapb::RegionDescriptor {
        region_id,
        start_key: start_key.to_vec(),
        end_key: end_key.to_vec(),
        epoch: Some(metapb::RegionEpoch {
            version: 1,
            conf_version: 1,
        }),
        peers: vec![metapb::RegionPeer { store_id, peer_id }],
        ..Default::default()
    }
}

#[tokio::test]
async fn metadata_plane_routes_by_context_region() {
    let descriptor1 = test_region_descriptor(1, 1, 10, b"", b"m");
    let descriptor2 = test_region_descriptor(2, 1, 20, b"m", b"");
    let admission1 = RegionAdmission::from_descriptor(&descriptor1, true).unwrap();
    let admission2 = RegionAdmission::from_descriptor(&descriptor2, true).unwrap();
    let engine1 = FixedRuntimeEngine::leader(1, 10);
    let engine2 = FixedRuntimeEngine::leader(2, 20);
    engine1.set_region_descriptor(descriptor1);
    engine2.set_region_descriptor(descriptor2);
    let service = MultiRegionMetadataPlaneService::new([
        (1, metadata_service(engine1, admission1.clone())),
        (2, metadata_service(engine2, admission2.clone())),
    ])
    .unwrap();

    let key = b"m-artifact".to_vec();
    let get = service
        .get(Request::new(metadatapb::MetadataGetRequest {
            context: Some(metadata_context(&admission2)),
            key: key.clone(),
            version: 11,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(get.region_error.is_none());
    assert!(get.not_found);

    let wrong_region = service
        .get(Request::new(metadatapb::MetadataGetRequest {
            context: Some(metadata_context(&RegionAdmission {
                region_id: 99,
                store_id: 1,
                peer_id: 99,
                ..admission1
            })),
            key,
            version: 11,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        wrong_region
            .region_error
            .unwrap()
            .region_not_found
            .unwrap()
            .region_id,
        99
    );
}

#[tokio::test]
async fn metadata_plane_routes_region_inserted_after_construction() {
    let descriptor1 = test_region_descriptor(1, 1, 10, b"", b"m");
    let descriptor2 = test_region_descriptor(2, 1, 20, b"m", b"");
    let admission1 = RegionAdmission::from_descriptor(&descriptor1, true).unwrap();
    let admission2 = RegionAdmission::from_descriptor(&descriptor2, true).unwrap();
    let engine1 = FixedRuntimeEngine::leader(1, 10);
    let engine2 = FixedRuntimeEngine::leader(2, 20);
    engine1.set_region_descriptor(descriptor1);
    engine2.set_region_descriptor(descriptor2);
    let service =
        MultiRegionMetadataPlaneService::new([(1, metadata_service(engine1, admission1))]).unwrap();

    service
        .insert_region(2, metadata_service(engine2, admission2.clone()))
        .unwrap();

    let response = service
        .get(Request::new(metadatapb::MetadataGetRequest {
            context: Some(metadata_context(&admission2)),
            key: b"m-dynamic".to_vec(),
            version: 21,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.region_error.is_none());
    assert!(response.not_found);

    let duplicate = service.insert_region(
        2,
        metadata_service(FixedRuntimeEngine::leader(2, 20), admission2.clone()),
    );
    assert_eq!(duplicate.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn removed_metadata_region_is_not_routable() {
    let descriptor = test_region_descriptor(2, 1, 20, b"m", b"");
    let admission = RegionAdmission::from_descriptor(&descriptor, true).unwrap();
    let engine = FixedRuntimeEngine::leader(2, 20);
    engine.set_region_descriptor(descriptor);
    let service =
        MultiRegionMetadataPlaneService::new([(2, metadata_service(engine, admission.clone()))])
            .unwrap();

    let removed = service.remove_region(2).unwrap();
    assert!(removed.is_some());

    let response = service
        .get(Request::new(metadatapb::MetadataGetRequest {
            context: Some(metadata_context(&admission)),
            key: b"m-removed".to_vec(),
            version: 21,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response
            .region_error
            .unwrap()
            .region_not_found
            .unwrap()
            .region_id,
        2
    );
}

#[tokio::test]
async fn admin_routes_region_scoped_membership() {
    let admission1 =
        RegionAdmission::from_descriptor(&test_region_descriptor(1, 1, 10, b"", b"m"), true)
            .unwrap();
    let admission2 =
        RegionAdmission::from_descriptor(&test_region_descriptor(2, 1, 20, b"m", b""), true)
            .unwrap();
    let service = MultiRegionRaftAdminService::new([
        (
            1,
            RaftAdminService::with_admission(NoopAdminStatus, admission1),
        ),
        (
            2,
            RaftAdminService::with_admission(NoopAdminStatus, admission2),
        ),
    ])
    .unwrap();

    let added = service
        .add_peer(Request::new(adminpb::AddPeerRequest {
            region_id: 2,
            store_id: 3,
            peer_id: 30,
        }))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    assert_eq!(added.region_id, 2);
    assert_eq!(added.epoch.unwrap().conf_version, 2);
    assert!(added
        .peers
        .iter()
        .any(|peer| peer.store_id == 3 && peer.peer_id == 30));

    let missing = service
        .remove_peer(Request::new(adminpb::RemovePeerRequest {
            region_id: 3,
            peer_id: 30,
        }))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::FailedPrecondition);
    assert!(missing.message().contains("region 3"));
}

#[tokio::test]
async fn removed_admin_region_rejects_membership_changes() {
    let admission =
        RegionAdmission::from_descriptor(&test_region_descriptor(2, 1, 20, b"m", b""), true)
            .unwrap();
    let service = MultiRegionRaftAdminService::new([(
        2,
        RaftAdminService::with_admission(NoopAdminStatus, admission),
    )])
    .unwrap();

    let removed = service.remove_region(2).unwrap();
    assert!(removed.is_some());

    let missing = service
        .add_peer(Request::new(adminpb::AddPeerRequest {
            region_id: 2,
            store_id: 3,
            peer_id: 30,
        }))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::FailedPrecondition);
    assert!(missing.message().contains("region 2"));
}

#[tokio::test]
async fn admin_counts_region_inserted_after_construction() {
    let admission1 =
        RegionAdmission::from_descriptor(&test_region_descriptor(1, 1, 10, b"", b"m"), true)
            .unwrap();
    let admission2 =
        RegionAdmission::from_descriptor(&test_region_descriptor(2, 1, 20, b"m", b""), true)
            .unwrap();
    let service = MultiRegionRaftAdminService::new([(
        1,
        RaftAdminService::with_admission(NoopAdminStatus, admission1),
    )])
    .unwrap();

    service
        .insert_region(
            2,
            RaftAdminService::with_admission(NoopAdminStatus, admission2),
        )
        .unwrap();

    let status = service
        .execution_status(Request::new(adminpb::ExecutionStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.restart.unwrap().region_count, 2);
}

#[tokio::test]
async fn admin_execution_status_counts_process_diagnostics_once() {
    let admission1 =
        RegionAdmission::from_descriptor(&test_region_descriptor(1, 1, 10, b"", b"m"), true)
            .unwrap();
    let admission2 =
        RegionAdmission::from_descriptor(&test_region_descriptor(2, 1, 20, b"m", b""), true)
            .unwrap();
    let diagnostics = nokv_holtstore::HoltMetadataStore::open_memory().unwrap();
    diagnostics
        .enqueue_pending_root_event(&metapb::RootEvent {
            kind: metapb::RootEventKind::PeerAdded as i32,
            payload: Some(metapb::root_event::Payload::PeerChange(
                metapb::RootPeerChange {
                    region_id: 2,
                    store_id: 3,
                    peer_id: 30,
                    ..Default::default()
                },
            )),
        })
        .unwrap();
    let service = MultiRegionRaftAdminService::new([
        (
            1,
            RaftAdminService::with_admission(NoopAdminStatus, admission1),
        ),
        (
            2,
            RaftAdminService::with_admission(NoopAdminStatus, admission2),
        ),
    ])
    .unwrap()
    .with_restart_diagnostics(Arc::new(diagnostics));

    let status = service
        .execution_status(Request::new(adminpb::ExecutionStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    let restart = status.restart.unwrap();
    assert_eq!(restart.region_count, 2);
    assert_eq!(restart.raft_group_count, 2);
    assert_eq!(restart.pending_root_event_count, 1);
    assert_eq!(status.topology.len(), 1);
    assert_eq!(status.topology[0].transition_id, "peer:2:add:3:30");
}
