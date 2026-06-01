use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use nokv_metadata_state::MemoryMetadataStore;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use tokio::task::JoinHandle;

use super::*;
use crate::{
    AppliedMetadataEngine, ApplyStatusProvider, MetadataCommandExecutor, MetadataReadExecutor,
    OpenRaftRegion, Proposal, RegionLogStorage, RegionStateMachine, SegmentedEntryLog,
};

#[tokio::test]
async fn tonic_raft_network_replicates_proposal_between_servers() {
    let mut handles = Vec::new();
    let mut transports = BTreeMap::new();
    let mut node_addrs = BTreeMap::new();
    for node_id in 1..=3 {
        let transport = TonicRaftTransportRegistry::default();
        let (addr, handle) = spawn_transport_server(transport.service()).await;
        transports.insert(node_id, transport);
        node_addrs.insert(node_id, addr.to_string());
        handles.push(handle);
    }

    let mut dirs = Vec::new();
    let mut regions = Vec::new();
    let mut engines = BTreeMap::new();
    for node_id in 1..=3 {
        let dir = tempfile::tempdir().unwrap();
        let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        let log_store = RegionLogStorage::new(log);
        let engine = AppliedMetadataEngine::new(7, MemoryMetadataStore::new());
        let region = OpenRaftRegion::open_with_network(
            node_id,
            7,
            log_store,
            RegionStateMachine::new(engine.clone()),
            TonicRaftNetworkFactory::new(7),
        )
        .await
        .unwrap();
        transports
            .get(&node_id)
            .unwrap()
            .register(7, region.raft_handle());
        dirs.push(dir);
        engines.insert(node_id, engine);
        regions.push(region);
    }

    regions[0]
        .initialize_members(
            node_addrs
                .iter()
                .map(|(node_id, addr)| (*node_id, BasicNode::new(addr.clone())))
                .collect(),
        )
        .await
        .unwrap();
    regions[0].wait_for_leader(1).await.unwrap();

    let command = metadatapb::MetadataCommitRequest {
        context: Some(metadatapb::MetadataContext {
            region_id: 7,
            ..Default::default()
        }),
        command: Some(metadatapb::MetadataCommand {
            request_id: b"tonic-network".to_vec(),
            read_version: 9,
            commit_version: 10,
            mutations: vec![metadatapb::MetadataMutation {
                key: b"tonic-network".to_vec(),
                value: b"replicated".to_vec(),
                op: metadatapb::metadata_mutation::Op::Put as i32,
                ..Default::default()
            }],
            watch_keys: vec![b"tonic-network".to_vec()],
            ..Default::default()
        }),
    };
    regions[0].execute_metadata_command(&command).await.unwrap();
    let target_index = regions[0].apply_status().applied_index;

    for region in &regions {
        region
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(target_index), "tonic raft network replication")
            .await
            .unwrap();
    }
    for (_node_id, engine) in engines {
        let response = engine
            .execute_metadata_get(&metadatapb::MetadataGetRequest {
                context: Some(metadatapb::MetadataContext {
                    region_id: 7,
                    ..Default::default()
                }),
                key: b"tonic-network".to_vec(),
                version: 10,
            })
            .await
            .unwrap();
        assert_eq!(response.kv.unwrap().value, b"replicated".to_vec());
    }

    for handle in handles {
        handle.abort();
    }
}

#[tokio::test]
async fn tonic_raft_network_catches_up_joining_peer_from_snapshot() {
    let mut handles = Vec::new();
    let mut transports = BTreeMap::new();
    let mut node_addrs = BTreeMap::new();
    for node_id in 1..=2 {
        let transport = TonicRaftTransportRegistry::default();
        let (addr, handle) = spawn_transport_server(transport.service()).await;
        transports.insert(node_id, transport);
        node_addrs.insert(node_id, addr.to_string());
        handles.push(handle);
    }

    let leader_dir = tempfile::tempdir().unwrap();
    let leader_log = SegmentedEntryLog::open(7, leader_dir.path()).unwrap();
    let leader_log_store = RegionLogStorage::new(leader_log);
    let leader_engine = AppliedMetadataEngine::new(7, MemoryMetadataStore::new());
    let leader = OpenRaftRegion::open_with_network_for_test(
        1,
        7,
        leader_log_store,
        RegionStateMachine::new(leader_engine),
        TonicRaftNetworkFactory::new(7),
        |config| {
            config.snapshot_policy = openraft::SnapshotPolicy::Never;
            config.replication_lag_threshold = 1;
            config.max_in_snapshot_log_to_keep = 0;
        },
    )
    .await
    .unwrap();
    transports
        .get(&1)
        .unwrap()
        .register(7, leader.raft_handle());
    leader
        .initialize_members(BTreeMap::from([(
            1,
            BasicNode::new(node_addrs.get(&1).unwrap().clone()),
        )]))
        .await
        .unwrap();
    leader.wait_for_leader(1).await.unwrap();

    let mut last_applied = None;
    for version in 1u64..=8 {
        let command = metadatapb::MetadataCommitRequest {
            context: Some(metadatapb::MetadataContext {
                region_id: 7,
                ..Default::default()
            }),
            command: Some(metadatapb::MetadataCommand {
                request_id: version.to_be_bytes().to_vec(),
                read_version: version,
                commit_version: version.saturating_add(1),
                mutations: vec![metadatapb::MetadataMutation {
                    key: format!("k{version}").into_bytes(),
                    value: format!("v{version}").into_bytes(),
                    op: metadatapb::metadata_mutation::Op::Put as i32,
                    ..Default::default()
                }],
                watch_keys: vec![format!("k{version}").into_bytes()],
                ..Default::default()
            }),
        };
        last_applied = Some(
            leader
                .propose(Proposal::from_metadata_command(&command).unwrap())
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
            "tonic leader snapshot before joining peer",
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
            "tonic leader purges snapshot-covered log",
        )
        .await
        .unwrap();

    let joining_dir = tempfile::tempdir().unwrap();
    let joining_log = SegmentedEntryLog::open(7, joining_dir.path()).unwrap();
    let joining_engine = AppliedMetadataEngine::new(7, MemoryMetadataStore::new());
    let joining = OpenRaftRegion::open_with_network_for_test(
        2,
        7,
        RegionLogStorage::new(joining_log),
        RegionStateMachine::new(joining_engine.clone()),
        TonicRaftNetworkFactory::new(7),
        |config| {
            config.snapshot_policy = openraft::SnapshotPolicy::Never;
            config.replication_lag_threshold = 1;
            config.max_in_snapshot_log_to_keep = 0;
        },
    )
    .await
    .unwrap();
    transports
        .get(&2)
        .unwrap()
        .register(7, joining.raft_handle());

    leader
        .add_voter(2, BasicNode::new(node_addrs.get(&2).unwrap().clone()))
        .await
        .unwrap();
    joining
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(
            Some(last_applied.index),
            "tonic joining peer snapshot catch-up",
        )
        .await
        .unwrap();
    assert!(
        joining.raft_handle().metrics().borrow().snapshot.is_some(),
        "joining peer should install a snapshot over tonic transport"
    );

    let response = joining_engine
        .execute_metadata_get(&metadatapb::MetadataGetRequest {
            context: Some(metadatapb::MetadataContext {
                region_id: 7,
                ..Default::default()
            }),
            key: b"k8".to_vec(),
            version: 9,
        })
        .await
        .unwrap();
    assert_eq!(response.kv.unwrap().value, b"v8".to_vec());

    for handle in handles {
        handle.abort();
    }
}

async fn spawn_transport_server(
    service: TonicRaftTransportService,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RaftTransportServer::new(service))
            .serve(addr)
            .await
            .unwrap();
    });
    wait_for_transport(addr).await;
    (addr, handle)
}

async fn wait_for_transport(addr: SocketAddr) {
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
    panic!("tonic raft transport at {addr} did not become ready");
}
