use super::*;
use crate::admission_state::RegionAdmissionState;
use crate::execution::ExecutionRuntime;
use crate::serve::serve_with_openraft_metadata_region_admission_and_peer_endpoints;
use crate::wire_helpers::chunk_apply_watch_keys;
use adminpb::raft_admin_server::RaftAdmin;
use metadatapb::metadata_plane_server::MetadataPlane;
use nokv_holtstore::HoltMvccStore;
use nokv_mvcc::{KvEngine, MvccStore};
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_proto::nokv::raft::v1 as raftpb;
use nokv_raftnode::{
    ApplyStatusProvider, ApplyWatchProvider, ApplyWatchReplayRequest, BasicNode,
    MetadataReadExecutor, RegionMetadataSink,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_stream::StreamExt;
use tonic::{Request, Status};

#[derive(Clone, Default)]
struct CaptureTopologyPublisher {
    events: Arc<Mutex<Vec<(String, u64, u64, u64, u64)>>>,
}

#[tonic::async_trait]
impl TopologyPublisher for CaptureTopologyPublisher {
    async fn publish_peer_added(
        &self,
        region_id: u64,
        store_id: u64,
        peer_id: u64,
        region: &metapb::RegionDescriptor,
    ) -> TopologyPublishOutcome {
        self.events.lock().unwrap().push((
            "added".to_owned(),
            region_id,
            store_id,
            peer_id,
            region
                .epoch
                .as_ref()
                .map(|epoch| epoch.conf_version)
                .unwrap_or_default(),
        ));
        TopologyPublishOutcome::terminal_published()
    }

    async fn publish_peer_removed(
        &self,
        region_id: u64,
        store_id: u64,
        peer_id: u64,
        region: &metapb::RegionDescriptor,
    ) -> TopologyPublishOutcome {
        self.events.lock().unwrap().push((
            "removed".to_owned(),
            region_id,
            store_id,
            peer_id,
            region
                .epoch
                .as_ref()
                .map(|epoch| epoch.conf_version)
                .unwrap_or_default(),
        ));
        TopologyPublishOutcome::terminal_published()
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

#[derive(Clone, Default)]
struct FailedTopologyPublisher;

#[tonic::async_trait]
impl TopologyPublisher for FailedTopologyPublisher {
    async fn publish_peer_added(
        &self,
        _region_id: u64,
        _store_id: u64,
        _peer_id: u64,
        _region: &metapb::RegionDescriptor,
    ) -> TopologyPublishOutcome {
        TopologyPublishOutcome::terminal_failed("coordinator unavailable")
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

fn metadata_put_request(
    admission: &RegionAdmission,
    key: impl Into<Vec<u8>>,
    value: impl Into<Vec<u8>>,
    read_version: u64,
    commit_version: u64,
) -> metadatapb::MetadataCommitRequest {
    let key = key.into();
    metadatapb::MetadataCommitRequest {
        context: Some(metadata_context(admission)),
        command: Some(metadatapb::MetadataCommand {
            request_id: key.clone(),
            read_version,
            commit_version,
            mutations: vec![metadatapb::MetadataMutation {
                key: key.clone(),
                value: value.into(),
                op: metadatapb::metadata_mutation::Op::Put as i32,
                ..Default::default()
            }],
            watch_keys: vec![key],
            ..Default::default()
        }),
    }
}

fn open_persistent_holt_engine(
    path: &Path,
    region_id: u64,
) -> (
    nokv_holtstore::HoltMvccStore,
    nokv_raftnode::PersistentAppliedKvEngine<nokv_holtstore::HoltMvccStore, HoltRegionMetadataSink>,
) {
    let store = nokv_holtstore::HoltMvccStore::open_file(path).unwrap();
    let status = store
        .get_region_apply_state(region_id)
        .unwrap()
        .map(apply_status_from_holt)
        .unwrap_or(nokv_raftnode::ApplyStatus {
            region_id,
            term: 1,
            applied_index: 0,
        });
    let engine = nokv_raftnode::AppliedKvEngine::with_status(status, store.clone());
    (
        store.clone(),
        nokv_raftnode::PersistentAppliedKvEngine::new(engine, HoltRegionMetadataSink::new(store)),
    )
}

#[derive(Debug, Clone)]
struct FixedRuntimeEngine {
    inner: nokv_raftnode::AppliedKvEngine<MvccStore>,
    runtime: RaftRuntimeStatus,
}

impl FixedRuntimeEngine {
    fn leader(region_id: u64, local_peer_id: u64) -> Self {
        Self {
            inner: nokv_raftnode::AppliedKvEngine::new(region_id, MvccStore::new()),
            runtime: RaftRuntimeStatus {
                local_peer_id,
                leader_peer_id: local_peer_id,
                leader: true,
                hosted: true,
            },
        }
    }

    fn follower(region_id: u64, local_peer_id: u64, leader_peer_id: u64) -> Self {
        Self {
            inner: nokv_raftnode::AppliedKvEngine::new(region_id, MvccStore::new()),
            runtime: RaftRuntimeStatus {
                local_peer_id,
                leader_peer_id,
                leader: false,
                hosted: true,
            },
        }
    }

    fn unhosted(region_id: u64, local_peer_id: u64) -> Self {
        Self {
            inner: nokv_raftnode::AppliedKvEngine::new(region_id, MvccStore::new()),
            runtime: RaftRuntimeStatus {
                local_peer_id,
                leader_peer_id: 0,
                leader: false,
                hosted: false,
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
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<metadatapb::MetadataGetResponse>> + Send + 'a
    {
        self.inner.execute_metadata_get(req)
    }

    fn execute_metadata_batch_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataBatchGetRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<metadatapb::MetadataBatchGetResponse>>
           + Send
           + 'a {
        self.inner.execute_metadata_batch_get(req)
    }

    fn execute_metadata_scan<'a>(
        &'a self,
        req: &'a metadatapb::MetadataScanRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<metadatapb::MetadataScanResponse>> + Send + 'a
    {
        self.inner.execute_metadata_scan(req)
    }
}

impl nokv_raftnode::MetadataCommandExecutor for FixedRuntimeEngine {
    fn execute_metadata_command<'a>(
        &'a self,
        req: &'a metadatapb::MetadataCommitRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<metadatapb::MetadataCommitResponse>>
           + Send
           + 'a {
        self.inner.execute_metadata_command(req)
    }
}

impl ApplyStatusProvider for FixedRuntimeEngine {
    fn apply_status(&self) -> nokv_raftnode::ApplyStatus {
        self.inner.apply_status()
    }
}

impl ApplyWatchProvider for FixedRuntimeEngine {
    fn subscribe_apply(&self) -> tokio::sync::broadcast::Receiver<kvpb::ApplyWatchEvent> {
        self.inner.subscribe_apply()
    }

    fn replay_apply(
        &self,
        request: nokv_raftnode::ApplyWatchReplayRequest,
    ) -> nokv_mvcc::Result<nokv_raftnode::ApplyWatchReplay> {
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
        self.inner.region_descriptor().map_err(internal_error)
    }
}

#[derive(Debug, Clone)]
struct MetadataOnlyEngine {
    inner: nokv_raftnode::AppliedKvEngine<MvccStore>,
    runtime: RaftRuntimeStatus,
}

impl MetadataOnlyEngine {
    fn leader(region_id: u64, local_peer_id: u64) -> Self {
        let inner = nokv_raftnode::AppliedKvEngine::new(region_id, MvccStore::new());
        inner
            .set_region_descriptor(metapb::RegionDescriptor {
                region_id,
                epoch: Some(metapb::RegionEpoch {
                    version: 1,
                    conf_version: 1,
                }),
                peers: vec![metapb::RegionPeer {
                    store_id: local_peer_id,
                    peer_id: local_peer_id,
                }],
                ..Default::default()
            })
            .unwrap();
        Self {
            inner,
            runtime: RaftRuntimeStatus {
                local_peer_id,
                leader_peer_id: local_peer_id,
                leader: true,
                hosted: true,
            },
        }
    }
}

impl MetadataReadExecutor for MetadataOnlyEngine {
    fn execute_metadata_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataGetRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<metadatapb::MetadataGetResponse>> + Send + 'a
    {
        self.inner.execute_metadata_get(req)
    }

    fn execute_metadata_batch_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataBatchGetRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<metadatapb::MetadataBatchGetResponse>>
           + Send
           + 'a {
        self.inner.execute_metadata_batch_get(req)
    }

    fn execute_metadata_scan<'a>(
        &'a self,
        req: &'a metadatapb::MetadataScanRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<metadatapb::MetadataScanResponse>> + Send + 'a
    {
        self.inner.execute_metadata_scan(req)
    }
}

impl nokv_raftnode::MetadataCommandExecutor for MetadataOnlyEngine {
    fn execute_metadata_command<'a>(
        &'a self,
        req: &'a metadatapb::MetadataCommitRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<metadatapb::MetadataCommitResponse>>
           + Send
           + 'a {
        self.inner.execute_metadata_command(req)
    }
}

impl ApplyStatusProvider for MetadataOnlyEngine {
    fn apply_status(&self) -> nokv_raftnode::ApplyStatus {
        self.inner.apply_status()
    }
}

impl ApplyWatchProvider for MetadataOnlyEngine {
    fn subscribe_apply(&self) -> tokio::sync::broadcast::Receiver<kvpb::ApplyWatchEvent> {
        self.inner.subscribe_apply()
    }

    fn replay_apply(
        &self,
        request: nokv_raftnode::ApplyWatchReplayRequest,
    ) -> nokv_mvcc::Result<nokv_raftnode::ApplyWatchReplay> {
        self.inner.replay_apply(request)
    }
}

impl RaftRuntimeStatusProvider for MetadataOnlyEngine {
    fn raft_runtime_status(&self) -> RaftRuntimeStatus {
        self.runtime
    }
}

impl AppliedRegionDescriptorProvider for MetadataOnlyEngine {
    fn applied_region_descriptor(&self) -> Result<Option<metapb::RegionDescriptor>, Status> {
        self.inner.region_descriptor().map_err(internal_error)
    }
}

#[tokio::test]
async fn metadata_plane_read_does_not_require_legacy_raft_command_executor() {
    let admission = RegionAdmission {
        region_id: 7,
        store_id: 2,
        peer_id: 2,
        epoch_version: 1,
        epoch_conf_version: 1,
        leader: true,
        leader_peer_id: 2,
        peers: BTreeMap::from([(2, 2)]),
        ..Default::default()
    };
    let service = MetadataPlaneService::with_admission_state_and_execution(
        MetadataOnlyEngine::leader(admission.region_id, admission.peer_id),
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
    );

    let response = service
        .get(Request::new(metadatapb::MetadataGetRequest {
            context: Some(metadatapb::MetadataContext {
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
            }),
            key: b"missing".to_vec(),
            version: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.region_error.is_none());
    assert!(response.not_found);
}

#[tokio::test]
async fn metadata_plane_can_run_against_holt_engine() {
    let engine = nokv_raftnode::AppliedKvEngine::new(
        1,
        nokv_holtstore::HoltMvccStore::open_memory().unwrap(),
    );
    let admission = RegionAdmission::default();
    let service = MetadataPlaneService::with_admission_state_and_execution(
        engine.clone(),
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
    );
    let response = service
        .commit_metadata(Request::new(metadata_put_request(
            &admission,
            b"k".to_vec(),
            b"v".to_vec(),
            1,
            2,
        )))
        .await
        .unwrap()
        .into_inner();
    assert!(
        response.region_error.is_none(),
        "unexpected region error: {:?}",
        response.region_error
    );
    assert_eq!(response.result.unwrap().applied_mutations, 1);
    assert_eq!(engine.status().applied_index, 1);
}

#[tokio::test]
async fn metadata_admission_refreshes_from_applied_region_descriptor() {
    let engine = FixedRuntimeEngine::leader(7, 2);
    engine.set_region_descriptor(metapb::RegionDescriptor {
        region_id: 7,
        epoch: Some(metapb::RegionEpoch {
            version: 1,
            conf_version: 2,
        }),
        peers: vec![
            metapb::RegionPeer {
                store_id: 1,
                peer_id: 1,
            },
            metapb::RegionPeer {
                store_id: 2,
                peer_id: 2,
            },
        ],
        ..Default::default()
    });
    let service = MetadataPlaneService::with_admission_state_and_execution(
        engine,
        RegionAdmissionState::new(RegionAdmission {
            region_id: 7,
            store_id: 1,
            peer_id: 1,
            epoch_conf_version: 1,
            peers: BTreeMap::from([(1, 1)]),
            ..Default::default()
        }),
        ExecutionRuntime::default(),
    );
    let updated_admission = RegionAdmission {
        region_id: 7,
        store_id: 2,
        peer_id: 2,
        epoch_conf_version: 2,
        peers: BTreeMap::from([(1, 1), (2, 2)]),
        ..Default::default()
    };

    let response = service
        .commit_metadata(Request::new(metadata_put_request(
            &updated_admission,
            b"applied-descriptor".to_vec(),
            b"accepted".to_vec(),
            10,
            11,
        )))
        .await
        .unwrap()
        .into_inner();

    assert!(
        response.region_error.is_none(),
        "unexpected region error: {:?}",
        response.region_error
    );
    assert_eq!(response.result.unwrap().applied_mutations, 1);
}

#[tokio::test]
async fn metadata_plane_can_run_against_openraft_region() {
    let dir = tempfile::tempdir().unwrap();
    let log = nokv_raftnode::SegmentedEntryLog::open(1, dir.path()).unwrap();
    let state_machine = nokv_raftnode::RegionStateMachine::new(
        nokv_raftnode::AppliedKvEngine::new(1, MvccStore::new()),
    );
    let region = nokv_raftnode::OpenRaftRegion::bootstrap_single_node(
        1,
        1,
        nokv_raftnode::RegionLogStorage::new(log),
        state_machine,
    )
    .await
    .unwrap();
    let admission = RegionAdmission::default();
    let service = MetadataPlaneService::with_admission_state_and_execution(
        region.clone(),
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
    );

    let response = service
        .commit_metadata(Request::new(metadata_put_request(
            &admission,
            b"k".to_vec(),
            b"v".to_vec(),
            1,
            2,
        )))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.result.unwrap().applied_mutations, 1);
    assert_eq!(region.apply_status().applied_index, 2);
}

#[tokio::test]
async fn server_mounts_openraft_transport_for_metadata_replication() {
    let mut handles = Vec::new();
    let mut dirs = Vec::new();
    let mut engines = BTreeMap::new();
    let mut regions = BTreeMap::new();
    let addrs = (1..=3)
        .map(|node_id| (node_id, reserve_loopback_addr()))
        .collect::<BTreeMap<_, _>>();
    let peer_endpoints = PeerEndpointCatalog::new();
    for (peer_id, addr) in &addrs {
        peer_endpoints
            .insert_peer(*peer_id, addr.to_string())
            .unwrap();
    }
    let peers = BTreeMap::from([(1, 1), (2, 2), (3, 3)]);

    for node_id in 1..=3 {
        let addr = *addrs.get(&node_id).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let log = nokv_raftnode::SegmentedEntryLog::open(7, dir.path()).unwrap();
        let engine = nokv_raftnode::AppliedKvEngine::new(7, MvccStore::new());
        let region = nokv_raftnode::OpenRaftRegion::open_with_network(
            node_id,
            7,
            nokv_raftnode::RegionLogStorage::new(log),
            nokv_raftnode::RegionStateMachine::new(engine.clone()),
            nokv_raftnode::TonicRaftNetworkFactory::new(7),
        )
        .await
        .unwrap();
        let admission = RegionAdmission {
            region_id: 7,
            store_id: node_id,
            peer_id: node_id,
            peers: BTreeMap::from([(node_id, node_id)]),
            leader_peer_id: 1,
            epoch_conf_version: 1,
            leader: node_id == 1,
            ..Default::default()
        };
        let handle = tokio::spawn(
            serve_with_openraft_metadata_region_admission_and_peer_endpoints(
                addr,
                region.clone(),
                admission,
                peer_endpoints.clone(),
            ),
        );
        wait_for_server(addr).await;
        dirs.push(dir);
        engines.insert(node_id, engine);
        regions.insert(node_id, region);
        handles.push(handle);
    }

    let leader = regions.get(&1).unwrap();
    leader
        .initialize_members(BTreeMap::from([(
            1,
            BasicNode::new(addrs.get(&1).unwrap().to_string()),
        )]))
        .await
        .unwrap();
    leader.wait_for_leader(1).await.unwrap();
    leader.ensure_linearizable().await.unwrap();
    let leader_addr = addrs.get(&1).unwrap();
    let mut admin_client =
        adminpb::raft_admin_client::RaftAdminClient::connect(format!("http://{leader_addr}"))
            .await
            .unwrap();
    admin_client
        .add_peer(adminpb::AddPeerRequest {
            region_id: 7,
            store_id: 2,
            peer_id: 2,
        })
        .await
        .unwrap();
    admin_client
        .add_peer(adminpb::AddPeerRequest {
            region_id: 7,
            store_id: 3,
            peer_id: 3,
        })
        .await
        .unwrap();

    let mut client = metadatapb::metadata_plane_client::MetadataPlaneClient::connect(format!(
        "http://{leader_addr}"
    ))
    .await
    .unwrap();
    let admission = RegionAdmission {
        region_id: 7,
        store_id: 1,
        peer_id: 1,
        peers,
        leader_peer_id: 1,
        epoch_conf_version: 3,
        leader: true,
        ..Default::default()
    };
    let response = client
        .commit_metadata(metadatapb::MetadataCommitRequest {
            context: Some(metadata_context(&admission)),
            command: Some(metadatapb::MetadataCommand {
                request_id: b"server-transport".to_vec(),
                read_version: 41,
                commit_version: 42,
                mutations: vec![metadatapb::MetadataMutation {
                    key: b"server-transport".to_vec(),
                    value: b"replicated".to_vec(),
                    op: metadatapb::metadata_mutation::Op::Put as i32,
                    ..Default::default()
                }],
                watch_keys: vec![b"server-transport".to_vec()],
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        response.region_error.is_none(),
        "unexpected region error: {:?}",
        response.region_error
    );
    assert_eq!(response.result.unwrap().applied_mutations, 1);
    let target_index = leader.apply_status().applied_index;

    for region in regions.values() {
        region
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(target_index), "server transport replication")
            .await
            .unwrap();
    }
    for engine in engines.values() {
        assert_eq!(
            engine
                .get(&kvpb::GetRequest {
                    key: b"server-transport".to_vec(),
                    version: 42,
                })
                .unwrap()
                .value,
            b"replicated".to_vec()
        );
    }

    admin_client
        .transfer_leader(adminpb::TransferLeaderRequest {
            region_id: 7,
            peer_id: 2,
        })
        .await
        .unwrap();
    regions.get(&2).unwrap().wait_for_leader(2).await.unwrap();
    let peer2_addr = addrs.get(&2).unwrap();
    let mut peer2_admin =
        adminpb::raft_admin_client::RaftAdminClient::connect(format!("http://{peer2_addr}"))
            .await
            .unwrap();
    let peer2_status = peer2_admin
        .region_runtime_status(adminpb::RegionRuntimeStatusRequest { region_id: 7 })
        .await
        .unwrap()
        .into_inner();
    assert!(peer2_status.leader);
    assert_eq!(peer2_status.leader_peer_id, 2);

    for region in regions.values() {
        region.shutdown().await.unwrap();
    }
    for handle in handles {
        handle.abort();
    }
}

#[tokio::test]
async fn bounded_stale_follower_prefer_read_serves_local_openraft_state() {
    let mut handles = Vec::new();
    let mut dirs = Vec::new();
    let mut regions = BTreeMap::new();
    let addrs = (1..=2)
        .map(|node_id| (node_id, reserve_loopback_addr()))
        .collect::<BTreeMap<_, _>>();
    let peer_endpoints = PeerEndpointCatalog::new();
    for (peer_id, addr) in &addrs {
        peer_endpoints
            .insert_peer(*peer_id, addr.to_string())
            .unwrap();
    }

    for node_id in 1..=2 {
        let addr = *addrs.get(&node_id).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let log = nokv_raftnode::SegmentedEntryLog::open(7, dir.path()).unwrap();
        let engine = nokv_raftnode::AppliedKvEngine::new(7, MvccStore::new());
        let region = nokv_raftnode::OpenRaftRegion::open_with_network(
            node_id,
            7,
            nokv_raftnode::RegionLogStorage::new(log),
            nokv_raftnode::RegionStateMachine::new(engine),
            nokv_raftnode::TonicRaftNetworkFactory::new(7),
        )
        .await
        .unwrap();
        let admission = RegionAdmission {
            region_id: 7,
            store_id: node_id,
            peer_id: node_id,
            peers: BTreeMap::from([(node_id, node_id)]),
            leader_peer_id: 1,
            epoch_conf_version: 1,
            leader: node_id == 1,
            ..Default::default()
        };
        let handle = tokio::spawn(
            serve_with_openraft_metadata_region_admission_and_peer_endpoints(
                addr,
                region.clone(),
                admission,
                peer_endpoints.clone(),
            ),
        );
        wait_for_server(addr).await;
        dirs.push(dir);
        regions.insert(node_id, region);
        handles.push(handle);
    }

    let leader = regions.get(&1).unwrap();
    leader
        .initialize_members(BTreeMap::from([(
            1,
            BasicNode::new(addrs.get(&1).unwrap().to_string()),
        )]))
        .await
        .unwrap();
    leader.wait_for_leader(1).await.unwrap();
    leader.ensure_linearizable().await.unwrap();

    let leader_addr = addrs.get(&1).unwrap();
    let mut admin_client =
        adminpb::raft_admin_client::RaftAdminClient::connect(format!("http://{leader_addr}"))
            .await
            .unwrap();
    admin_client
        .add_peer(adminpb::AddPeerRequest {
            region_id: 7,
            store_id: 2,
            peer_id: 2,
        })
        .await
        .unwrap();

    let peers = BTreeMap::from([(1, 1), (2, 2)]);
    let leader_admission = RegionAdmission {
        region_id: 7,
        store_id: 1,
        peer_id: 1,
        peers: peers.clone(),
        leader_peer_id: 1,
        epoch_conf_version: 2,
        leader: true,
        ..Default::default()
    };
    let mut leader_client = metadatapb::metadata_plane_client::MetadataPlaneClient::connect(
        format!("http://{leader_addr}"),
    )
    .await
    .unwrap();
    let write = leader_client
        .commit_metadata(metadatapb::MetadataCommitRequest {
            context: Some(metadata_context(&leader_admission)),
            command: Some(metadatapb::MetadataCommand {
                request_id: b"bounded-stale".to_vec(),
                read_version: 41,
                commit_version: 42,
                mutations: vec![metadatapb::MetadataMutation {
                    key: b"bounded-stale".to_vec(),
                    value: b"local-follower".to_vec(),
                    op: metadatapb::metadata_mutation::Op::Put as i32,
                    ..Default::default()
                }],
                watch_keys: vec![b"bounded-stale".to_vec()],
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        write.region_error.is_none(),
        "unexpected region error: {:?}",
        write.region_error
    );
    let write_index = leader.apply_status().applied_index;
    for region in regions.values() {
        region
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(write_index), "bounded-stale write replication")
            .await
            .unwrap();
    }

    let follower_addr = addrs.get(&2).unwrap();
    let mut follower_client = metadatapb::metadata_plane_client::MetadataPlaneClient::connect(
        format!("http://{follower_addr}"),
    )
    .await
    .unwrap();
    let mut follower_context = metadata_context(&RegionAdmission {
        region_id: 7,
        store_id: 2,
        peer_id: 2,
        peers,
        leader_peer_id: 1,
        epoch_conf_version: 2,
        leader: false,
        ..Default::default()
    });
    follower_context.read_consistency = metadatapb::ReadConsistency::BoundedStale as i32;
    follower_context.read_preference = metadatapb::ReadPreference::FollowerPrefer as i32;

    let response = follower_client
        .get(metadatapb::MetadataGetRequest {
            context: Some(follower_context.clone()),
            key: b"bounded-stale".to_vec(),
            version: 42,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        response.region_error.is_none(),
        "unexpected region error: {:?}",
        response.region_error
    );
    assert_eq!(response.kv.unwrap().value, b"local-follower".to_vec());

    for region in regions.values() {
        region.shutdown().await.unwrap();
    }
    for handle in handles {
        handle.abort();
    }
}

#[tokio::test]
async fn holt_snapshot_installed_peer_survives_openraft_restart() {
    let registry = nokv_raftnode::MemoryRaftNetworkRegistry::default();
    let leader_log_dir = tempfile::tempdir().unwrap();
    let leader_log = nokv_raftnode::SegmentedEntryLog::open(7, leader_log_dir.path()).unwrap();
    let leader_engine = nokv_raftnode::AppliedKvEngine::new(7, MvccStore::new());
    let leader = nokv_raftnode::OpenRaftRegion::open_with_network(
        1,
        7,
        nokv_raftnode::RegionLogStorage::new(leader_log),
        nokv_raftnode::RegionStateMachine::new(leader_engine),
        registry.factory(),
    )
    .await
    .unwrap();
    registry.register(1, leader.raft_handle());
    leader
        .initialize_members(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    leader.wait_for_leader(1).await.unwrap();

    let put_command =
        |request_id: u64, key: &[u8], value: &[u8], commit_version: u64| raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id: 7,
                request_id,
                ..Default::default()
            }),
            requests: vec![raftpb::Request {
                cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                cmd: Some(raftpb::request::Cmd::TryAtomicMutate(
                    kvpb::TryAtomicMutateRequest {
                        mutations: vec![kvpb::Mutation {
                            key: key.to_vec(),
                            value: value.to_vec(),
                            op: kvpb::mutation::Op::Put as i32,
                            ..Default::default()
                        }],
                        commit_version,
                        ..Default::default()
                    },
                )),
            }],
        };

    let mut last_applied = None;
    for version in 1..=8 {
        let command = put_command(
            version,
            format!("k{version}").as_bytes(),
            format!("v{version}").as_bytes(),
            version,
        );
        last_applied = Some(
            leader
                .propose(nokv_raftnode::Proposal::from_raft_command(&command).unwrap())
                .await
                .unwrap(),
        );
    }
    let last_applied = last_applied.unwrap();
    leader.trigger_snapshot().await.unwrap();
    leader
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .snapshot
                    .map(|snapshot| snapshot.index >= last_applied.index)
                    .unwrap_or(false)
            },
            "leader snapshot before Holt peer catch-up",
        )
        .await
        .unwrap();
    leader.trigger_log_purge(last_applied.index).await.unwrap();
    leader
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .purged
                    .map(|purged| purged.index >= last_applied.index)
                    .unwrap_or(false)
            },
            "leader purges snapshot-covered logs before Holt peer join",
        )
        .await
        .unwrap();

    let joining_log_dir = tempfile::tempdir().unwrap();
    let joining_holt_dir = tempfile::tempdir().unwrap();
    let (joining_store, joining_engine) = open_persistent_holt_engine(joining_holt_dir.path(), 7);
    let joining = nokv_raftnode::OpenRaftRegion::open_with_network(
        2,
        7,
        nokv_raftnode::RegionLogStorage::new(
            nokv_raftnode::SegmentedEntryLog::open(7, joining_log_dir.path()).unwrap(),
        ),
        nokv_raftnode::RegionStateMachine::new(joining_engine.clone()),
        registry.factory(),
    )
    .await
    .unwrap();
    registry.register(2, joining.raft_handle());

    leader.add_voter(2, BasicNode::new("node-2")).await.unwrap();
    joining
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(last_applied.index), "Holt peer installs snapshot")
        .await
        .unwrap();
    assert!(
        joining.raft_handle().metrics().borrow().snapshot.is_some(),
        "joining Holt peer should install a snapshot before replaying new logs"
    );
    assert_eq!(
        joining_store
            .get(&kvpb::GetRequest {
                key: b"k8".to_vec(),
                version: 8,
            })
            .unwrap()
            .value,
        b"v8".to_vec()
    );

    joining.shutdown().await.unwrap();
    drop(joining);
    drop(joining_engine);
    drop(joining_store);

    let (restarted_store, restarted_engine) =
        open_persistent_holt_engine(joining_holt_dir.path(), 7);
    let persisted_apply = restarted_store
        .get_region_apply_state(7)
        .unwrap()
        .expect("snapshot install should persist Holt apply state");
    assert!(persisted_apply.applied_index >= last_applied.index);

    let restarted_joining = nokv_raftnode::OpenRaftRegion::open_with_network(
        2,
        7,
        nokv_raftnode::RegionLogStorage::new(
            nokv_raftnode::SegmentedEntryLog::open(7, joining_log_dir.path()).unwrap(),
        ),
        nokv_raftnode::RegionStateMachine::new(restarted_engine.clone()),
        registry.factory(),
    )
    .await
    .unwrap();
    registry.register(2, restarted_joining.raft_handle());
    restarted_joining.wait_for_voter(2, true).await.unwrap();

    let after_restart = put_command(9, b"after-snapshot-restart", b"ok", 9);
    let applied_after_restart = leader
        .propose(nokv_raftnode::Proposal::from_raft_command(&after_restart).unwrap())
        .await
        .unwrap();
    restarted_joining
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(
            Some(applied_after_restart.index),
            "Holt peer applies after restart",
        )
        .await
        .unwrap();
    assert_eq!(
        restarted_store
            .get(&kvpb::GetRequest {
                key: b"after-snapshot-restart".to_vec(),
                version: 9,
            })
            .unwrap()
            .value,
        b"ok".to_vec()
    );

    leader.shutdown().await.unwrap();
    restarted_joining.shutdown().await.unwrap();
}

#[tokio::test]
async fn batch_get_empty_does_not_require_region_admission() {
    let admission = RegionAdmission {
        leader: false,
        ..Default::default()
    };
    let service = MetadataPlaneService::with_admission_state_and_execution(
        FixedRuntimeEngine::follower(admission.region_id, admission.peer_id, 99),
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
    );

    let response = service
        .batch_get(Request::new(metadatapb::MetadataBatchGetRequest {
            context: Some(metadatapb::MetadataContext {
                region_id: admission.region_id,
                ..Default::default()
            }),
            requests: Vec::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.region_error.is_none());
    assert!(response.responses.is_empty());

    let missing_context = service
        .batch_get(Request::new(metadatapb::MetadataBatchGetRequest {
            requests: Vec::new(),
            ..Default::default()
        }))
        .await;
    assert_eq!(
        missing_context.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
}

#[tokio::test]
async fn watch_apply_streams_matching_apply_events() {
    let engine = nokv_raftnode::AppliedKvEngine::new(1, MvccStore::new());
    let admission = RegionAdmission::default();
    let service = MetadataPlaneService::with_admission_state_and_execution(
        engine,
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
    );
    let mut stream = service
        .watch_apply(Request::new(metadatapb::MetadataWatchApplyRequest {
            key_prefix: b"prefix/".to_vec(),
            buffer: 4,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    service
        .commit_metadata(Request::new(metadatapb::MetadataCommitRequest {
            context: Some(metadata_context(&admission)),
            command: Some(metadatapb::MetadataCommand {
                request_id: b"watch-commit".to_vec(),
                read_version: 8,
                commit_version: 9,
                mutations: vec![
                    metadatapb::MetadataMutation {
                        key: b"prefix/k".to_vec(),
                        value: b"v".to_vec(),
                        op: metadatapb::metadata_mutation::Op::Put as i32,
                        ..Default::default()
                    },
                    metadatapb::MetadataMutation {
                        key: b"other/k".to_vec(),
                        value: b"ignored".to_vec(),
                        op: metadatapb::metadata_mutation::Op::Put as i32,
                        ..Default::default()
                    },
                ],
                watch_keys: vec![b"prefix/k".to_vec(), b"other/k".to_vec()],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

    let response = stream.next().await.unwrap().unwrap();
    let event = response.event.unwrap();
    assert_eq!(event.commit_version, 9);
    assert_eq!(event.keys, vec![b"prefix/k".to_vec()]);
}

#[test]
fn apply_watch_chunks_large_key_sets() {
    let keys = (0..(DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE + 7))
        .map(|idx| format!("prefix/{idx:04}").into_bytes())
        .collect::<Vec<_>>();
    let chunks = chunk_apply_watch_keys(keys);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].len(), DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE);
    assert_eq!(chunks[1].len(), 7);
}

#[test]
fn holt_region_metadata_sink_replays_watch_history_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let event = kvpb::ApplyWatchEvent {
        region_id: 7,
        term: 2,
        index: 10,
        commit_version: 99,
        keys: vec![b"artifact/a".to_vec()],
        ..Default::default()
    };
    {
        let store = HoltMvccStore::open_file(dir.path()).unwrap();
        let sink = HoltRegionMetadataSink::new(store);
        sink.save_apply_watch_event(&event).unwrap();
    }

    let reopened = HoltRegionMetadataSink::new(HoltMvccStore::open_file(dir.path()).unwrap());
    let replay = reopened
        .replay_apply_watch(&ApplyWatchReplayRequest {
            region_id: 7,
            term: 2,
            index: 9,
            key_prefix: b"artifact/".to_vec(),
        })
        .unwrap()
        .unwrap();

    assert!(!replay.expired);
    assert_eq!(replay.events, vec![event]);
}

#[tokio::test]
async fn get_requires_context() {
    let service = MetadataPlaneService::with_admission_state_and_execution(
        FixedRuntimeEngine::leader(1, 1),
        RegionAdmissionState::new(RegionAdmission::default()),
        ExecutionRuntime::default(),
    );
    let err = service
        .get(Request::new(metadatapb::MetadataGetRequest {
            key: b"k".to_vec(),
            version: 1,
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn get_rejects_region_not_found() {
    let service = MetadataPlaneService::with_admission_state_and_execution(
        FixedRuntimeEngine::leader(1, 1),
        RegionAdmissionState::new(RegionAdmission::default()),
        ExecutionRuntime::default(),
    );
    let mut ctx = metadata_context(&RegionAdmission::default());
    ctx.region_id = 99;
    let response = service
        .get(Request::new(metadatapb::MetadataGetRequest {
            context: Some(ctx),
            key: b"k".to_vec(),
            version: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    let region_error = response.region_error.unwrap();
    assert!(region_error.region_not_found.is_some());
    assert!(response.kv.is_none());
}

#[tokio::test]
async fn get_rejects_store_not_match() {
    let service = MetadataPlaneService::with_admission_state_and_execution(
        FixedRuntimeEngine::leader(1, 1),
        RegionAdmissionState::new(RegionAdmission::default()),
        ExecutionRuntime::default(),
    );
    let mut ctx = metadata_context(&RegionAdmission::default());
    ctx.peer.as_mut().unwrap().store_id = 999;
    let response = service
        .get(Request::new(metadatapb::MetadataGetRequest {
            context: Some(ctx),
            key: b"k".to_vec(),
            version: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    let mismatch = response.region_error.unwrap().store_not_match.unwrap();
    assert_eq!(mismatch.request_store_id, 999);
    assert_eq!(mismatch.actual_store_id, 1);
    assert!(response.kv.is_none());
}

#[tokio::test]
async fn get_rejects_epoch_not_match() {
    let service = MetadataPlaneService::with_admission_state_and_execution(
        FixedRuntimeEngine::leader(1, 1),
        RegionAdmissionState::new(RegionAdmission::default()),
        ExecutionRuntime::default(),
    );
    let mut ctx = metadata_context(&RegionAdmission::default());
    ctx.region_epoch.as_mut().unwrap().version = 99;
    let response = service
        .get(Request::new(metadatapb::MetadataGetRequest {
            context: Some(ctx),
            key: b"k".to_vec(),
            version: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    let mismatch = response.region_error.unwrap().epoch_not_match.unwrap();
    assert_eq!(mismatch.current_epoch.unwrap().version, 1);
    assert_eq!(mismatch.regions.len(), 1);
    assert!(response.kv.is_none());
}

#[tokio::test]
async fn get_rejects_key_not_in_region() {
    let admission = RegionAdmission {
        region_id: 10,
        store_id: 7,
        peer_id: 77,
        peers: BTreeMap::from([(77, 7)]),
        leader_peer_id: 77,
        epoch_version: 3,
        epoch_conf_version: 2,
        start_key: b"a".to_vec(),
        end_key: b"m".to_vec(),
        leader: true,
        hosted: true,
    };
    let service = MetadataPlaneService::with_admission_state_and_execution(
        FixedRuntimeEngine::leader(10, 77),
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
    );
    let response = service
        .get(Request::new(metadatapb::MetadataGetRequest {
            context: Some(metadata_context(&admission)),
            key: b"z".to_vec(),
            version: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    let out = response.region_error.unwrap().key_not_in_region.unwrap();
    assert_eq!(out.key, b"z".to_vec());
    assert_eq!(out.region_id, 10);
    assert_eq!(out.start_key, b"a".to_vec());
    assert_eq!(out.end_key, b"m".to_vec());
    assert!(response.kv.is_none());
}

#[tokio::test]
async fn scan_rejects_not_leader() {
    let admission = RegionAdmission {
        leader: false,
        ..Default::default()
    };
    let service = MetadataPlaneService::with_admission_state_and_execution(
        FixedRuntimeEngine::follower(1, 1, 0),
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
    );
    let response = service
        .scan(Request::new(metadatapb::MetadataScanRequest {
            context: Some(metadata_context(&admission)),
            start_key: b"k".to_vec(),
            limit: 1,
            version: 1,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.region_error.unwrap().not_leader.is_some());
    assert!(response.kvs.is_empty());
}

#[tokio::test]
async fn scan_trims_keys_outside_region_end() {
    let store = MvccStore::new();
    for (key, value) in [
        (b"ak".as_slice(), b"value-a".as_slice()),
        (b"bk".as_slice(), b"value-b".as_slice()),
        (b"mz".as_slice(), b"value-z".as_slice()),
    ] {
        store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: key.to_vec(),
                    value: value.to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                commit_version: 20,
                ..Default::default()
            })
            .unwrap();
    }

    let admission = RegionAdmission {
        start_key: b"a".to_vec(),
        end_key: b"m".to_vec(),
        ..Default::default()
    };
    let service = MetadataPlaneService::with_admission_state_and_execution(
        nokv_raftnode::AppliedKvEngine::new(1, store),
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
    );

    let scan = service
        .scan(Request::new(metadatapb::MetadataScanRequest {
            context: Some(metadata_context(&admission)),
            start_key: b"a".to_vec(),
            limit: 10,
            version: 20,
            include_start: true,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    let keys = scan
        .kvs
        .iter()
        .map(|kv| kv.key.as_slice())
        .collect::<Vec<_>>();
    assert_eq!(keys, vec![b"ak".as_slice(), b"bk".as_slice()]);
}

#[tokio::test]
async fn scan_zero_limit_matches_go_default_limit() {
    let store = MvccStore::new();
    for (key, value) in [
        (b"scan-limit/a".as_slice(), b"value-a".as_slice()),
        (b"scan-limit/b".as_slice(), b"value-b".as_slice()),
    ] {
        store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: key.to_vec(),
                    value: value.to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                commit_version: 20,
                ..Default::default()
            })
            .unwrap();
    }
    let admission = RegionAdmission::default();
    let service = MetadataPlaneService::with_admission_state_and_execution(
        nokv_raftnode::AppliedKvEngine::new(1, store),
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
    );

    let scan = service
        .scan(Request::new(metadatapb::MetadataScanRequest {
            context: Some(metadata_context(&admission)),
            start_key: b"scan-limit/".to_vec(),
            version: 20,
            include_start: true,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(scan.kvs.len(), 1);
    assert_eq!(scan.kvs[0].key, b"scan-limit/a".to_vec());
}

#[tokio::test]
async fn follower_prefer_read_returns_stale_for_leader_fallback() {
    let admission = RegionAdmission {
        leader: false,
        ..Default::default()
    };
    let service = MetadataPlaneService::with_admission_state_and_execution(
        FixedRuntimeEngine::follower(1, 1, 0),
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
    );
    let mut ctx = metadata_context(&admission);
    ctx.read_preference = metadatapb::ReadPreference::FollowerPrefer as i32;

    let response = service
        .get(Request::new(metadatapb::MetadataGetRequest {
            context: Some(ctx),
            key: b"k".to_vec(),
            version: 1,
        }))
        .await
        .unwrap()
        .into_inner();

    let region_error = response.region_error.unwrap();
    assert!(region_error.stale_command.is_some());
    assert!(region_error.not_leader.is_none());
    assert!(response.kv.is_none());
}

#[tokio::test]
async fn metadata_admission_uses_live_follower_status() {
    let admission = RegionAdmission {
        store_id: 2,
        peer_id: 2,
        peers: BTreeMap::from([(1, 1), (2, 2)]),
        leader_peer_id: 2,
        leader: true,
        ..Default::default()
    };
    let service = MetadataPlaneService::with_admission_state_and_execution(
        FixedRuntimeEngine::follower(1, 2, 1),
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
    );

    let response = service
        .commit_metadata(Request::new(metadata_put_request(
            &admission,
            b"k".to_vec(),
            b"v".to_vec(),
            10,
            11,
        )))
        .await
        .unwrap()
        .into_inner();

    let not_leader = response.region_error.unwrap().not_leader.unwrap();
    assert_eq!(not_leader.region_id, 1);
    assert_eq!(
        not_leader.leader,
        Some(metapb::RegionPeer {
            store_id: 1,
            peer_id: 1
        })
    );
    assert!(response.result.is_none());
}

#[tokio::test]
async fn writes_remain_leader_only_when_follower_prefer_is_set() {
    let admission = RegionAdmission {
        leader: false,
        ..Default::default()
    };
    let service = MetadataPlaneService::with_admission_state_and_execution(
        FixedRuntimeEngine::follower(1, 1, 0),
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
    );
    let mut ctx = metadata_context(&admission);
    ctx.read_preference = metadatapb::ReadPreference::FollowerPrefer as i32;
    let mut request = metadata_put_request(&admission, b"k".to_vec(), b"v".to_vec(), 10, 11);
    request.context = Some(ctx);

    let response = service
        .commit_metadata(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    let region_error = response.region_error.unwrap();
    assert!(region_error.not_leader.is_some());
    assert!(region_error.stale_command.is_none());
    assert!(response.result.is_none());
}

#[tokio::test]
async fn scan_rejects_reverse_scan() {
    let admission = RegionAdmission::default();
    let service = MetadataPlaneService::with_admission_state_and_execution(
        FixedRuntimeEngine::leader(1, 1),
        RegionAdmissionState::new(admission.clone()),
        ExecutionRuntime::default(),
    );
    let err = service
        .scan(Request::new(metadatapb::MetadataScanRequest {
            context: Some(metadata_context(&admission)),
            start_key: b"k".to_vec(),
            limit: 1,
            reverse: true,
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
}

#[tokio::test]
async fn admin_membership_is_explicitly_not_wired() {
    let service = RaftAdminService::new(EmptyApplyStatus);
    let err = service
        .add_peer(Request::new(adminpb::AddPeerRequest {
            region_id: 1,
            store_id: 1,
            peer_id: 1,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
}

#[tokio::test]
async fn admin_add_peer_requires_region_store_and_peer() {
    let service = RaftAdminService::new(EmptyApplyStatus);

    for request in [
        adminpb::AddPeerRequest {
            region_id: 0,
            store_id: 1,
            peer_id: 1,
        },
        adminpb::AddPeerRequest {
            region_id: 1,
            store_id: 0,
            peer_id: 1,
        },
        adminpb::AddPeerRequest {
            region_id: 1,
            store_id: 1,
            peer_id: 0,
        },
    ] {
        let err = service.add_peer(Request::new(request)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "region_id, store_id, and peer_id are required"
        );
    }
}

#[tokio::test]
async fn admin_remove_peer_requires_region_and_peer() {
    let service = RaftAdminService::new(EmptyApplyStatus);

    for request in [
        adminpb::RemovePeerRequest {
            region_id: 0,
            peer_id: 1,
        },
        adminpb::RemovePeerRequest {
            region_id: 1,
            peer_id: 0,
        },
    ] {
        let err = service
            .remove_peer(Request::new(request))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "region_id and peer_id are required");
    }
}

#[test]
fn strict_peer_endpoint_catalog_rejects_missing_peer() {
    let catalog = PeerEndpointCatalog::require_configured();
    let err = catalog.node_for_peer(2, 202).unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("peer 202"));
}

#[tokio::test]
async fn admin_add_peer_records_failed_topology_publish_without_failing_change() {
    let service = RaftAdminService::with_admission(NoopAdminStatus, RegionAdmission::default())
        .with_topology_publisher(Arc::new(FailedTopologyPublisher));

    let add = service
        .add_peer(Request::new(adminpb::AddPeerRequest {
            region_id: 1,
            store_id: 2,
            peer_id: 2,
        }))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    assert_eq!(add.epoch.unwrap().conf_version, 2);

    let execution = service
        .execution_status(Request::new(adminpb::ExecutionStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(execution.topology.len(), 1);
    assert_eq!(
        execution.topology[0].publish,
        adminpb::ExecutionPublishState::TerminalFailed as i32
    );
    assert!(execution.topology[0]
        .last_error
        .contains("coordinator unavailable"));
}

#[tokio::test]
async fn execution_status_reports_holt_root_event_catalog_counts() {
    let store = HoltMvccStore::open_memory().unwrap();
    let event = metapb::RootEvent {
        kind: metapb::RootEventKind::PeerAdded as i32,
        payload: Some(metapb::root_event::Payload::PeerChange(
            metapb::RootPeerChange {
                region_id: 1,
                store_id: 2,
                peer_id: 2,
                target: Some(metapb::RegionDescriptor {
                    region_id: 1,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    };
    let pending_event = metapb::RootEvent {
        kind: metapb::RootEventKind::PeerRemoved as i32,
        payload: Some(metapb::root_event::Payload::PeerChange(
            metapb::RootPeerChange {
                region_id: 1,
                store_id: 2,
                peer_id: 3,
                ..Default::default()
            },
        )),
    };
    let blocked_sequence = store.enqueue_pending_root_event(&event).unwrap();
    store
        .block_pending_root_event(
            blocked_sequence,
            &event,
            "peer:1:add:2:2",
            "catalog precondition",
        )
        .unwrap();
    store.enqueue_pending_root_event(&pending_event).unwrap();
    let pending_scheduler = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
        region_id: 1,
        split_key: b"m".to_vec(),
        split_child: Some(metapb::RegionDescriptor {
            region_id: 2,
            ..Default::default()
        }),
        ..Default::default()
    };
    store
        .record_pending_scheduler_operation(&pending_scheduler)
        .unwrap();
    store
        .increment_pending_scheduler_operation_attempts(&pending_scheduler)
        .unwrap();
    let blocked_scheduler = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::MergeRegion as i32,
        region_id: 1,
        source_region_id: 2,
        ..Default::default()
    };
    store
        .block_pending_scheduler_operation(&blocked_scheduler, 8, "attempt limit reached")
        .unwrap();

    let service = RaftAdminService::with_admission(NoopAdminStatus, RegionAdmission::default())
        .with_restart_diagnostics(Arc::new(store));
    let execution = service
        .execution_status(Request::new(adminpb::ExecutionStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    let restart = execution.restart.unwrap();
    assert_eq!(restart.pending_root_event_count, 1);
    assert_eq!(restart.blocked_root_event_count, 1);
    assert_eq!(restart.pending_scheduler_operation_count, 1);
    assert_eq!(execution.topology.len(), 4);
    let blocked = execution
        .topology
        .iter()
        .find(|status| status.transition_id == "peer:1:add:2:2")
        .unwrap();
    assert_eq!(blocked.region_id, 1);
    assert_eq!(blocked.action, "peer change");
    assert_eq!(
        blocked.outcome,
        adminpb::ExecutionTopologyOutcome::Failed as i32
    );
    assert_eq!(
        blocked.publish,
        adminpb::ExecutionPublishState::TerminalBlocked as i32
    );
    assert_eq!(blocked.last_error, "catalog precondition");

    let root_pending = execution
        .topology
        .iter()
        .find(|status| status.transition_id == "peer:1:remove:2:3")
        .unwrap();
    assert_eq!(root_pending.region_id, 1);
    assert_eq!(root_pending.action, "peer change");
    assert_eq!(
        root_pending.outcome,
        adminpb::ExecutionTopologyOutcome::Applied as i32
    );
    assert_eq!(
        root_pending.publish,
        adminpb::ExecutionPublishState::TerminalPending as i32
    );
    assert_eq!(root_pending.last_error, "root event publish pending");

    let pending = execution
        .topology
        .iter()
        .find(|status| status.transition_id == "split:1:6d")
        .unwrap();
    assert_eq!(pending.region_id, 1);
    assert_eq!(pending.action, "range split");
    assert_eq!(
        pending.outcome,
        adminpb::ExecutionTopologyOutcome::Queued as i32
    );
    assert_eq!(
        pending.publish,
        adminpb::ExecutionPublishState::NotRequired as i32
    );
    assert_eq!(
        pending.last_error,
        "scheduler operation pending after 1 attempt(s)"
    );

    let blocked_scheduler_status = execution
        .topology
        .iter()
        .find(|status| status.transition_id == "merge:1:2")
        .unwrap();
    assert_eq!(blocked_scheduler_status.region_id, 1);
    assert_eq!(blocked_scheduler_status.action, "range merge");
    assert_eq!(
        blocked_scheduler_status.outcome,
        adminpb::ExecutionTopologyOutcome::Failed as i32
    );
    assert_eq!(
        blocked_scheduler_status.publish,
        adminpb::ExecutionPublishState::NotRequired as i32
    );
    assert_eq!(
        blocked_scheduler_status.last_error,
        "scheduler operation blocked after 8 attempt(s): attempt limit reached"
    );
}

#[tokio::test]
async fn admin_adds_and_removes_openraft_voter() {
    let registry = nokv_raftnode::MemoryRaftNetworkRegistry::default();
    let mut dirs = Vec::new();
    let mut regions = BTreeMap::new();

    for node_id in 1..=2 {
        let dir = tempfile::tempdir().unwrap();
        let log = nokv_raftnode::SegmentedEntryLog::open(7, dir.path()).unwrap();
        let log_store = nokv_raftnode::RegionLogStorage::new(log);
        let state_machine = nokv_raftnode::RegionStateMachine::new(
            nokv_raftnode::AppliedKvEngine::new(7, MvccStore::new()),
        );
        let region = nokv_raftnode::OpenRaftRegion::open_with_network(
            node_id,
            7,
            log_store,
            state_machine,
            registry.factory(),
        )
        .await
        .unwrap();
        registry.register(node_id, region.raft_handle());
        dirs.push(dir);
        regions.insert(node_id, region);
    }

    let leader = regions.get(&1).unwrap();
    leader
        .initialize_members(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    leader.wait_for_leader(1).await.unwrap();

    let admission = RegionAdmission {
        region_id: 7,
        store_id: 1,
        peer_id: 1,
        epoch_conf_version: 1,
        ..Default::default()
    };
    let peer_endpoints = PeerEndpointCatalog::new();
    let descriptor_dir = tempfile::tempdir().unwrap();
    let descriptor_store = HoltMvccStore::open_file(descriptor_dir.path()).unwrap();
    let topology_publisher = CaptureTopologyPublisher::default();
    let published_topology = topology_publisher.events.clone();
    let service =
        RaftAdminService::with_admission_state_execution_peer_endpoints_and_descriptor_sink(
            leader.clone(),
            RegionAdmissionState::new(admission),
            ExecutionRuntime::default(),
            peer_endpoints,
            HoltRegionMetadataSink::new(descriptor_store.clone()),
        )
        .with_topology_publisher(Arc::new(topology_publisher));
    let metadata_service = MetadataPlaneService::with_admission_state_and_execution(
        leader.clone(),
        service.admission.clone(),
        ExecutionRuntime::default(),
    );

    let add = service
        .add_peer(Request::new(adminpb::AddPeerRequest {
            region_id: 7,
            store_id: 2,
            peer_id: 2,
        }))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    assert_eq!(add.region_id, 7);
    assert_eq!(add.epoch.unwrap().conf_version, 2);
    assert_eq!(
        add.peers,
        vec![
            metapb::RegionPeer {
                store_id: 1,
                peer_id: 1
            },
            metapb::RegionPeer {
                store_id: 2,
                peer_id: 2
            },
        ]
    );
    assert_eq!(
        descriptor_store
            .get_region_descriptor(7)
            .unwrap()
            .unwrap()
            .epoch
            .unwrap()
            .conf_version,
        2
    );
    assert_eq!(
        published_topology.lock().unwrap().as_slice(),
        &[("added".to_owned(), 7, 2, 2, 2)]
    );
    let execution_after_add = service
        .execution_status(Request::new(adminpb::ExecutionStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        execution_after_add.last_admission.unwrap().class,
        adminpb::ExecutionAdmissionClass::Topology as i32
    );
    assert_eq!(execution_after_add.topology.len(), 1);
    assert_eq!(execution_after_add.topology[0].action, "peer change");
    assert_eq!(
        execution_after_add.topology[0].outcome,
        adminpb::ExecutionTopologyOutcome::Applied as i32
    );
    assert_eq!(
        execution_after_add.topology[0].publish,
        adminpb::ExecutionPublishState::TerminalPublished as i32
    );

    let leader_status = service
        .region_runtime_status(Request::new(adminpb::RegionRuntimeStatusRequest {
            region_id: 7,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(leader_status.local_peer_id, 1);
    assert_eq!(leader_status.leader_peer_id, 1);
    assert!(leader_status.leader);

    let stale_epoch = metadata_service
        .commit_metadata(Request::new(metadata_put_request(
            &RegionAdmission {
                region_id: 7,
                store_id: 1,
                peer_id: 1,
                epoch_conf_version: 1,
                ..Default::default()
            },
            b"admin-updated-admission-stale".to_vec(),
            b"rejected".to_vec(),
            9,
            10,
        )))
        .await
        .unwrap()
        .into_inner();
    assert!(stale_epoch.region_error.unwrap().epoch_not_match.is_some());

    let accepted = metadata_service
        .commit_metadata(Request::new(metadata_put_request(
            &RegionAdmission {
                region_id: 7,
                store_id: 1,
                peer_id: 1,
                epoch_conf_version: 2,
                ..Default::default()
            },
            b"admin-updated-admission".to_vec(),
            b"accepted".to_vec(),
            10,
            11,
        )))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(accepted.result.unwrap().applied_mutations, 1);

    let transfer = service
        .transfer_leader(Request::new(adminpb::TransferLeaderRequest {
            region_id: 7,
            peer_id: 1,
        }))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    assert_eq!(transfer.region_id, 7);

    let err = service
        .transfer_leader(Request::new(adminpb::TransferLeaderRequest {
            region_id: 7,
            peer_id: 2,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err
        .message()
        .contains("routing the request to the target peer"));

    let follower_service = RaftAdminService::with_admission(
        regions.get(&2).unwrap().clone(),
        RegionAdmission {
            region_id: 7,
            store_id: 2,
            peer_id: 2,
            epoch_conf_version: 2,
            ..Default::default()
        },
    );
    let follower_status = follower_service
        .region_runtime_status(Request::new(adminpb::RegionRuntimeStatusRequest {
            region_id: 7,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(follower_status.local_peer_id, 2);
    assert_eq!(follower_status.leader_peer_id, 1);
    assert!(!follower_status.leader);

    let remove = service
        .remove_peer(Request::new(adminpb::RemovePeerRequest {
            region_id: 7,
            peer_id: 2,
        }))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    assert_eq!(remove.epoch.unwrap().conf_version, 3);
    assert_eq!(
        remove.peers,
        vec![metapb::RegionPeer {
            store_id: 1,
            peer_id: 1
        }]
    );
    let persisted_after_remove = descriptor_store.get_region_descriptor(7).unwrap().unwrap();
    assert_eq!(persisted_after_remove.epoch.unwrap().conf_version, 3);
    assert_eq!(
        persisted_after_remove.peers,
        vec![metapb::RegionPeer {
            store_id: 1,
            peer_id: 1
        }]
    );
    assert_eq!(
        published_topology.lock().unwrap().as_slice(),
        &[
            ("added".to_owned(), 7, 2, 2, 2),
            ("removed".to_owned(), 7, 2, 2, 3),
        ]
    );
    let execution_after_remove = service
        .execution_status(Request::new(adminpb::ExecutionStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        execution_after_remove.last_admission.unwrap().class,
        adminpb::ExecutionAdmissionClass::Topology as i32
    );
    assert_eq!(execution_after_remove.topology.len(), 2);
    assert_eq!(execution_after_remove.topology[1].action, "peer change");
    assert_eq!(
        execution_after_remove.topology[1].outcome,
        adminpb::ExecutionTopologyOutcome::Applied as i32
    );
    assert_eq!(
        execution_after_remove.topology[1].publish,
        adminpb::ExecutionPublishState::TerminalPublished as i32
    );
}

#[tokio::test]
async fn admin_runtime_status_reports_apply_index() {
    let engine = nokv_raftnode::AppliedKvEngine::new(11, MvccStore::new());
    engine
        .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
            mutations: vec![kvpb::Mutation {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            commit_version: 2,
            ..Default::default()
        })
        .unwrap();
    let service = RaftAdminService::new(engine);
    let response = service
        .region_runtime_status(Request::new(adminpb::RegionRuntimeStatusRequest {
            region_id: 11,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.known);
    assert_eq!(response.applied_index, 1);
    assert_eq!(response.applied_term, 1);
}

#[tokio::test]
async fn admin_runtime_status_reports_unhosted_joining_peer() {
    let dir = tempfile::tempdir().unwrap();
    let log = nokv_raftnode::SegmentedEntryLog::open(7, dir.path()).unwrap();
    let state_machine = nokv_raftnode::RegionStateMachine::new(
        nokv_raftnode::AppliedKvEngine::new(7, MvccStore::new()),
    );
    let region = nokv_raftnode::OpenRaftRegion::open_with_network(
        2,
        7,
        nokv_raftnode::RegionLogStorage::new(log),
        state_machine,
        nokv_raftnode::MemoryRaftNetworkRegistry::default().factory(),
    )
    .await
    .unwrap();
    let service = RaftAdminService::with_admission(
        region,
        RegionAdmission {
            region_id: 7,
            store_id: 2,
            peer_id: 2,
            leader: false,
            ..Default::default()
        },
    );

    let response = service
        .region_runtime_status(Request::new(adminpb::RegionRuntimeStatusRequest {
            region_id: 7,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!response.known);
    assert!(!response.hosted);
    assert_eq!(response.local_peer_id, 2);
    assert_eq!(response.leader_peer_id, 0);
    assert!(!response.leader);
    assert!(response.region.is_none());
}

#[tokio::test]
async fn admin_execution_status_reports_unhosted_joining_peer_degraded() {
    let dir = tempfile::tempdir().unwrap();
    let log = nokv_raftnode::SegmentedEntryLog::open(7, dir.path()).unwrap();
    let state_machine = nokv_raftnode::RegionStateMachine::new(
        nokv_raftnode::AppliedKvEngine::new(7, MvccStore::new()),
    );
    let region = nokv_raftnode::OpenRaftRegion::open_with_network(
        2,
        7,
        nokv_raftnode::RegionLogStorage::new(log),
        state_machine,
        nokv_raftnode::MemoryRaftNetworkRegistry::default().factory(),
    )
    .await
    .unwrap();
    let service = RaftAdminService::with_admission(
        region,
        RegionAdmission {
            region_id: 7,
            store_id: 2,
            peer_id: 2,
            leader: false,
            ..Default::default()
        },
    );

    let response = service
        .execution_status(Request::new(adminpb::ExecutionStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    let restart = response.restart.unwrap();

    assert_eq!(
        restart.state,
        adminpb::ExecutionRestartState::Degraded as i32
    );
    assert_eq!(restart.region_count, 0);
    assert_eq!(restart.raft_group_count, 0);
}

#[tokio::test]
async fn admin_runtime_status_requires_region_id() {
    let service = RaftAdminService::new(nokv_raftnode::AppliedKvEngine::new(11, MvccStore::new()));
    let err = service
        .region_runtime_status(Request::new(adminpb::RegionRuntimeStatusRequest {
            region_id: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn admin_execution_status_reports_default_admission() {
    let service = RaftAdminService::new(nokv_raftnode::AppliedKvEngine::new(11, MvccStore::new()));
    let response = service
        .execution_status(Request::new(adminpb::ExecutionStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    let admission = response.last_admission.unwrap();
    assert!(!admission.observed);
    let restart = response.restart.unwrap();
    assert_eq!(restart.state, adminpb::ExecutionRestartState::Ready as i32);
    assert_eq!(restart.region_count, 1);
    assert_eq!(restart.raft_group_count, 1);
}

#[tokio::test]
async fn admin_execution_status_reports_metadata_admission() {
    let execution = ExecutionRuntime::default();
    let admission = RegionAdmission {
        store_id: 2,
        peer_id: 2,
        peers: BTreeMap::from([(1, 1), (2, 2)]),
        leader_peer_id: 2,
        leader: true,
        ..Default::default()
    };
    let metadata_service = MetadataPlaneService::with_admission_state_and_execution(
        FixedRuntimeEngine::follower(1, 2, 1),
        RegionAdmissionState::new(admission.clone()),
        execution.clone(),
    );
    let admin_service = RaftAdminService::with_admission_and_execution(
        nokv_raftnode::AppliedKvEngine::new(1, MvccStore::new()),
        admission.clone(),
        execution,
    );

    let response = metadata_service
        .commit_metadata(Request::new(metadata_put_request(
            &admission,
            b"k".to_vec(),
            b"v".to_vec(),
            10,
            11,
        )))
        .await
        .unwrap()
        .into_inner();
    assert!(response.region_error.unwrap().not_leader.is_some());

    let execution_status = admin_service
        .execution_status(Request::new(adminpb::ExecutionStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    let admission = execution_status.last_admission.unwrap();
    assert!(admission.observed);
    assert_eq!(
        admission.class,
        adminpb::ExecutionAdmissionClass::Write as i32
    );
    assert_eq!(
        admission.reason,
        adminpb::ExecutionAdmissionReason::NotLeader as i32
    );
    assert!(!admission.accepted);
    assert_eq!(admission.region_id, 1);
    assert_eq!(admission.peer_id, 2);
    assert_eq!(admission.detail, "not leader");
    assert!(admission.at_unix_nano > 0);
}

#[tokio::test]
async fn metadata_admission_rejects_unhosted_runtime() {
    let execution = ExecutionRuntime::default();
    let admission = RegionAdmission {
        store_id: 2,
        peer_id: 2,
        peers: BTreeMap::from([(2, 2)]),
        leader_peer_id: 0,
        leader: false,
        ..Default::default()
    };
    let metadata_service = MetadataPlaneService::with_admission_state_and_execution(
        FixedRuntimeEngine::unhosted(1, 2),
        RegionAdmissionState::new(admission.clone()),
        execution.clone(),
    );
    let admin_service = RaftAdminService::with_admission_and_execution(
        nokv_raftnode::AppliedKvEngine::new(1, MvccStore::new()),
        admission.clone(),
        execution,
    );

    let response = metadata_service
        .commit_metadata(Request::new(metadata_put_request(
            &admission,
            b"k".to_vec(),
            b"v".to_vec(),
            10,
            11,
        )))
        .await
        .unwrap()
        .into_inner();
    assert!(response.region_error.unwrap().region_not_found.is_some());

    let execution_status = admin_service
        .execution_status(Request::new(adminpb::ExecutionStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    let admission = execution_status.last_admission.unwrap();
    assert!(admission.observed);
    assert_eq!(
        admission.reason,
        adminpb::ExecutionAdmissionReason::NotHosted as i32
    );
    assert!(!admission.accepted);
    assert_eq!(admission.detail, "region not hosted");
}

fn reserve_loopback_addr() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn wait_for_server(addr: std::net::SocketAddr) {
    let endpoint = format!("http://{addr}");
    for _ in 0..50 {
        if tonic::transport::Endpoint::from_shared(endpoint.clone())
            .unwrap()
            .connect()
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("raftstore server at {addr} did not become ready");
}
