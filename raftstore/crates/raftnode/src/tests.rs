use super::*;
use std::collections::BTreeMap;
use std::time::Duration;

use nokv_mvcc::MvccStore;
use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_proto::nokv::raft::v1 as raftpb;
use prost::Message;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct RecordingRegionMetadataSink {
    statuses: Arc<Mutex<Vec<ApplyStatus>>>,
    descriptors: Arc<Mutex<Vec<metapb::RegionDescriptor>>>,
    events: Arc<Mutex<Vec<kvpb::ApplyWatchEvent>>>,
}

impl RegionMetadataSink for RecordingRegionMetadataSink {
    fn save_apply_status(&self, status: &ApplyStatus) -> nokv_mvcc::Result<()> {
        self.statuses.lock().unwrap().push(status.clone());
        Ok(())
    }

    fn save_region_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> nokv_mvcc::Result<()> {
        self.descriptors.lock().unwrap().push(descriptor.clone());
        Ok(())
    }

    fn save_apply_watch_event(&self, event: &kvpb::ApplyWatchEvent) -> nokv_mvcc::Result<()> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

#[derive(Debug, Default)]
struct StaticRegionDescriptorCatalog {
    descriptors: BTreeMap<RegionId, metapb::RegionDescriptor>,
}

impl RegionDescriptorCatalog for StaticRegionDescriptorCatalog {
    fn region_descriptor(
        &self,
        region_id: RegionId,
    ) -> nokv_mvcc::Result<Option<metapb::RegionDescriptor>> {
        Ok(self.descriptors.get(&region_id).cloned())
    }
}

fn metadata_put_request(
    region_id: RegionId,
    request_id: u64,
    key: impl Into<Vec<u8>>,
    value: impl Into<Vec<u8>>,
    read_version: u64,
    commit_version: u64,
) -> metadatapb::MetadataCommitRequest {
    let key = key.into();
    metadatapb::MetadataCommitRequest {
        context: Some(metadatapb::MetadataContext {
            region_id,
            ..Default::default()
        }),
        command: Some(metadatapb::MetadataCommand {
            request_id: request_id.to_be_bytes().to_vec(),
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

fn metadata_get_request(
    region_id: RegionId,
    key: impl Into<Vec<u8>>,
    version: u64,
) -> metadatapb::MetadataGetRequest {
    metadatapb::MetadataGetRequest {
        context: Some(metadatapb::MetadataContext {
            region_id,
            ..Default::default()
        }),
        key: key.into(),
        version,
    }
}

async fn execute_metadata_put<E>(
    engine: &E,
    region_id: RegionId,
    request_id: u64,
    key: impl Into<Vec<u8>>,
    value: impl Into<Vec<u8>>,
    read_version: u64,
    commit_version: u64,
) -> nokv_mvcc::Result<metadatapb::MetadataCommitResponse>
where
    E: MetadataCommandExecutor,
{
    engine
        .execute_metadata_command(&metadata_put_request(
            region_id,
            request_id,
            key,
            value,
            read_version,
            commit_version,
        ))
        .await
}

async fn read_metadata_value<E>(
    engine: &E,
    region_id: RegionId,
    key: impl Into<Vec<u8>>,
    version: u64,
) -> Vec<u8>
where
    E: MetadataReadExecutor,
{
    engine
        .execute_metadata_get(&metadata_get_request(region_id, key, version))
        .await
        .unwrap()
        .kv
        .unwrap()
        .value
}

#[tokio::test]
async fn openraft_region_bootstraps_single_node_and_applies_proposal() {
    let dir = tempfile::tempdir().unwrap();
    let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
    let log_store = RegionLogStorage::new(log);
    let state_machine = RegionStateMachine::new(AppliedKvEngine::new(7, MvccStore::new()));
    let raft = OpenRaftRegion::bootstrap_single_node(1, 7, log_store, state_machine)
        .await
        .unwrap();

    let applied = raft
        .propose(
            Proposal::from_metadata_command(&metadata_put_request(7, 1, b"k", b"v", 1, 2)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(applied.region_id, 7);
    assert_eq!(applied.index, 2);
}

#[tokio::test]
async fn openraft_region_serves_read_without_advancing_apply_index() {
    let dir = tempfile::tempdir().unwrap();
    let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
    let log_store = RegionLogStorage::new(log);
    let state_machine = RegionStateMachine::new(AppliedKvEngine::new(7, MvccStore::new()));
    let raft = OpenRaftRegion::bootstrap_single_node(1, 7, log_store, state_machine)
        .await
        .unwrap();

    execute_metadata_put(&raft, 7, 1, b"k", b"v", 1, 2)
        .await
        .unwrap();
    let applied_after_write = raft.apply_status().applied_index;
    assert!(applied_after_write > 0);

    let response = raft
        .execute_metadata_get(&metadata_get_request(7, b"k", 2))
        .await
        .unwrap();

    assert_eq!(raft.apply_status().applied_index, applied_after_write);
    assert_eq!(response.kv.unwrap().value, b"v".to_vec());
}

#[tokio::test]
async fn applied_kv_engine_executes_metadata_command_payload() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    let response = engine
        .execute_metadata_command(&metadatapb::MetadataCommitRequest {
            context: Some(metadatapb::MetadataContext {
                region_id: 7,
                ..Default::default()
            }),
            command: Some(metadatapb::MetadataCommand {
                request_id: b"metadata-1".to_vec(),
                mount: "vol".to_owned(),
                mount_key_id: 1,
                primary_key: b"k".to_vec(),
                read_version: 1,
                commit_version: 0,
                predicates: vec![metadatapb::MetadataPredicate {
                    key: b"k".to_vec(),
                    kind: metadatapb::MetadataPredicateKind::NotExists as i32,
                    ..Default::default()
                }],
                mutations: vec![metadatapb::MetadataMutation {
                    op: metadatapb::metadata_mutation::Op::Put as i32,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    ..Default::default()
                }],
                watch_keys: vec![b"k".to_vec()],
            }),
        })
        .await
        .unwrap();
    let result = response.result.unwrap();
    assert_eq!(result.region_id, 7);
    assert_eq!(result.commit_version, 2);
    assert_eq!(result.applied_mutations, 1);

    assert_eq!(
        read_metadata_value(&engine, 7, b"k", result.commit_version).await,
        b"v".to_vec()
    );
}

#[tokio::test]
async fn openraft_region_restart_write_returns_client_response() {
    let dir = tempfile::tempdir().unwrap();
    let status = {
        let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        let log_store = RegionLogStorage::new(log);
        let state_machine = RegionStateMachine::new(AppliedKvEngine::new(7, MvccStore::new()));
        let raft = OpenRaftRegion::bootstrap_single_node(1, 7, log_store, state_machine)
            .await
            .unwrap();
        execute_metadata_put(&raft, 7, 1, b"k1", b"v1", 1, 2)
            .await
            .unwrap();
        raft.apply_status()
    };

    let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
    let log_store = RegionLogStorage::new(log);
    let state_machine =
        RegionStateMachine::new(AppliedKvEngine::with_status(status, MvccStore::new()));
    let raft = OpenRaftRegion::bootstrap_single_node(1, 7, log_store, state_machine)
        .await
        .unwrap();

    let response = execute_metadata_put(&raft, 7, 2, b"k2", b"v2", 3, 4)
        .await
        .unwrap();

    assert_eq!(response.result.unwrap().applied_mutations, 1);
}

#[tokio::test]
async fn openraft_region_replicates_proposal_to_memory_peers() {
    let registry = MemoryRaftNetworkRegistry::default();
    let mut dirs = Vec::new();
    let mut regions = Vec::new();
    let mut engines = BTreeMap::new();

    for node_id in 1..=3 {
        let dir = tempfile::tempdir().unwrap();
        let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        let log_store = RegionLogStorage::new(log);
        let engine = AppliedKvEngine::new(7, MvccStore::new());
        let state_machine = RegionStateMachine::new(engine.clone());
        let region = OpenRaftRegion::open_with_network(
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
        engines.insert(node_id, engine);
        regions.push(region);
    }

    let mut members = BTreeMap::new();
    members.insert(1, BasicNode::new("node-1"));
    members.insert(2, BasicNode::new("node-2"));
    members.insert(3, BasicNode::new("node-3"));
    regions[0].initialize_members(members).await.unwrap();
    regions[0].wait_for_leader(1).await.unwrap();

    let applied = regions[0]
        .propose(
            Proposal::from_metadata_command(&metadata_put_request(7, 1, b"k", b"v", 9, 10))
                .unwrap(),
        )
        .await
        .unwrap();
    for region in &regions {
        region
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(applied.index), "memory peer proposal")
            .await
            .unwrap();
    }

    for node_id in 1..=3 {
        assert_eq!(
            read_metadata_value(engines.get(&node_id).unwrap(), 7, b"k", 10).await,
            b"v".to_vec(),
            "node {node_id} did not apply"
        );
    }
}

#[tokio::test]
async fn openraft_region_adds_voter_and_replicates_to_new_peer() {
    let registry = MemoryRaftNetworkRegistry::default();
    let mut dirs = Vec::new();
    let mut regions = BTreeMap::new();
    let mut engines = BTreeMap::new();

    for node_id in 1..=2 {
        let dir = tempfile::tempdir().unwrap();
        let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        let log_store = RegionLogStorage::new(log);
        let engine = AppliedKvEngine::new(7, MvccStore::new());
        let state_machine = RegionStateMachine::new(engine.clone());
        let region = OpenRaftRegion::open_with_network(
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
        engines.insert(node_id, engine);
        regions.insert(node_id, region);
    }

    let leader = regions.get(&1).unwrap();
    leader
        .initialize_members(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    leader.wait_for_leader(1).await.unwrap();
    leader.add_voter(2, BasicNode::new("node-2")).await.unwrap();

    let applied = leader
        .propose(
            Proposal::from_metadata_command(&metadata_put_request(7, 2, b"joined", b"yes", 19, 20))
                .unwrap(),
        )
        .await
        .unwrap();
    for region in regions.values() {
        region
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(applied.index), "added voter proposal")
            .await
            .unwrap();
    }

    for node_id in 1..=2 {
        assert_eq!(
            read_metadata_value(engines.get(&node_id).unwrap(), 7, b"joined", 20).await,
            b"yes".to_vec(),
            "node {node_id} did not apply after membership change"
        );
    }

    leader.remove_voter(2, false).await.unwrap();
    let voters = leader
        .raft_handle()
        .metrics()
        .borrow()
        .membership_config
        .voter_ids()
        .collect::<Vec<_>>();
    assert_eq!(voters, vec![1]);
}

#[tokio::test]
async fn openraft_region_target_peer_can_take_leadership() {
    let registry = MemoryRaftNetworkRegistry::default();
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();

    let leader_engine = AppliedKvEngine::new(7, MvccStore::new());
    let leader = OpenRaftRegion::open_with_network(
        1,
        7,
        RegionLogStorage::new(SegmentedEntryLog::open(7, leader_dir.path()).unwrap()),
        RegionStateMachine::new(leader_engine.clone()),
        registry.factory(),
    )
    .await
    .unwrap();
    registry.register(1, leader.raft_handle());

    let follower_engine = AppliedKvEngine::new(7, MvccStore::new());
    let follower = OpenRaftRegion::open_with_network(
        2,
        7,
        RegionLogStorage::new(SegmentedEntryLog::open(7, follower_dir.path()).unwrap()),
        RegionStateMachine::new(follower_engine.clone()),
        registry.factory(),
    )
    .await
    .unwrap();
    registry.register(2, follower.raft_handle());

    leader
        .initialize_members(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    leader.wait_for_leader(1).await.unwrap();
    leader.add_voter(2, BasicNode::new("node-2")).await.unwrap();
    follower.wait_for_voter(2, true).await.unwrap();

    follower.transfer_leader(2).await.unwrap();
    leader.wait_for_leader(2).await.unwrap();
    follower.wait_for_leader(2).await.unwrap();

    let applied = follower
        .propose(
            Proposal::from_metadata_command(&metadata_put_request(
                7,
                20,
                b"new-leader",
                b"peer-2",
                21,
                22,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    leader
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(applied.index), "transferred leader proposal")
        .await
        .unwrap();
    assert_eq!(
        read_metadata_value(&leader_engine, 7, b"new-leader", 22).await,
        b"peer-2".to_vec()
    );
}

#[tokio::test]
async fn openraft_region_restarts_after_membership_change_without_single_node_vote() {
    let registry = MemoryRaftNetworkRegistry::default();
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();

    let leader_engine = AppliedKvEngine::new(7, MvccStore::new());
    let leader = OpenRaftRegion::open_with_network(
        1,
        7,
        RegionLogStorage::new(SegmentedEntryLog::open(7, leader_dir.path()).unwrap()),
        RegionStateMachine::new(leader_engine.clone()),
        registry.factory(),
    )
    .await
    .unwrap();
    registry.register(1, leader.raft_handle());

    let follower_engine = AppliedKvEngine::new(7, MvccStore::new());
    let follower = OpenRaftRegion::open_with_network(
        2,
        7,
        RegionLogStorage::new(SegmentedEntryLog::open(7, follower_dir.path()).unwrap()),
        RegionStateMachine::new(follower_engine.clone()),
        registry.factory(),
    )
    .await
    .unwrap();
    registry.register(2, follower.raft_handle());

    leader
        .initialize_members(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    leader.wait_for_leader(1).await.unwrap();
    leader.add_voter(2, BasicNode::new("node-2")).await.unwrap();
    follower.wait_for_voter(2, true).await.unwrap();

    let applied_before_restart = leader
        .propose(
            Proposal::from_metadata_command(&metadata_put_request(
                7,
                10,
                b"before-restart",
                b"ok",
                29,
                30,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    for region in [&leader, &follower] {
        region
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(
                Some(applied_before_restart.index),
                "membership restart baseline proposal",
            )
            .await
            .unwrap();
    }

    let leader_status = leader.apply_status();
    let follower_status = follower.apply_status();
    leader.shutdown().await.unwrap();
    follower.shutdown().await.unwrap();
    drop(leader);
    drop(follower);
    drop(leader_engine);
    drop(follower_engine);

    let restarted_registry = MemoryRaftNetworkRegistry::default();
    let restarted_leader_engine = AppliedKvEngine::with_status(leader_status, MvccStore::new());
    let restarted_leader = OpenRaftRegion::bootstrap_single_node_with_network(
        1,
        7,
        RegionLogStorage::new(SegmentedEntryLog::open(7, leader_dir.path()).unwrap()),
        RegionStateMachine::new(restarted_leader_engine.clone()),
        restarted_registry.factory(),
        "node-1",
    )
    .await
    .unwrap();
    restarted_registry.register(1, restarted_leader.raft_handle());

    let restarted_follower_engine = AppliedKvEngine::with_status(follower_status, MvccStore::new());
    let restarted_follower = OpenRaftRegion::open_with_network(
        2,
        7,
        RegionLogStorage::new(SegmentedEntryLog::open(7, follower_dir.path()).unwrap()),
        RegionStateMachine::new(restarted_follower_engine.clone()),
        restarted_registry.factory(),
    )
    .await
    .unwrap();
    restarted_registry.register(2, restarted_follower.raft_handle());

    restarted_leader.wait_for_voter(2, true).await.unwrap();
    restarted_follower.wait_for_voter(2, true).await.unwrap();
    restarted_leader.elect_and_wait(1).await.unwrap();
    restarted_follower.wait_for_leader(1).await.unwrap();

    let applied_after_restart = restarted_leader
        .propose(
            Proposal::from_metadata_command(&metadata_put_request(
                7,
                11,
                b"after-restart",
                b"still-quorum",
                39,
                40,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    for region in [&restarted_leader, &restarted_follower] {
        region
            .raft_handle()
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(
                Some(applied_after_restart.index),
                "post-membership-restart proposal",
            )
            .await
            .unwrap();
    }

    for (node_id, engine) in [(1, restarted_leader_engine), (2, restarted_follower_engine)] {
        assert_eq!(
            read_metadata_value(&engine, 7, b"after-restart", 40).await,
            b"still-quorum".to_vec(),
            "node {node_id} did not apply after multi-voter restart"
        );
    }
}

#[tokio::test]
async fn openraft_region_catches_up_joining_peer_from_snapshot() {
    let registry = MemoryRaftNetworkRegistry::default();
    let leader_dir = tempfile::tempdir().unwrap();
    let leader_log = SegmentedEntryLog::open(7, leader_dir.path()).unwrap();
    let leader_log_store = RegionLogStorage::new(leader_log);
    let leader_engine = AppliedKvEngine::new(7, MvccStore::new());
    let leader = OpenRaftRegion::open_with_network_for_test(
        1,
        7,
        leader_log_store,
        RegionStateMachine::new(leader_engine.clone()),
        registry.factory(),
        |config| {
            config.snapshot_policy = openraft::SnapshotPolicy::Never;
            config.replication_lag_threshold = 1;
            config.max_in_snapshot_log_to_keep = 0;
        },
    )
    .await
    .unwrap();
    registry.register(1, leader.raft_handle());
    leader
        .initialize_members(BTreeMap::from([(1, BasicNode::new("node-1"))]))
        .await
        .unwrap();
    leader.wait_for_leader(1).await.unwrap();

    let mut last_applied = None;
    for version in 1..=8 {
        last_applied = Some(
            leader
                .propose(
                    Proposal::from_metadata_command(&metadata_put_request(
                        7,
                        version,
                        format!("k{version}"),
                        format!("v{version}"),
                        version,
                        version.saturating_add(1),
                    ))
                    .unwrap(),
                )
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
            "leader snapshot before joining peer",
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
            "leader purges snapshot-covered log",
        )
        .await
        .unwrap();

    let joining_dir = tempfile::tempdir().unwrap();
    let joining_log = SegmentedEntryLog::open(7, joining_dir.path()).unwrap();
    let joining_log_store = RegionLogStorage::new(joining_log);
    let joining_engine = AppliedKvEngine::new(7, MvccStore::new());
    let joining = OpenRaftRegion::open_with_network_for_test(
        2,
        7,
        joining_log_store,
        RegionStateMachine::new(joining_engine.clone()),
        registry.factory(),
        |config| {
            config.snapshot_policy = openraft::SnapshotPolicy::Never;
            config.replication_lag_threshold = 1;
            config.max_in_snapshot_log_to_keep = 0;
        },
    )
    .await
    .unwrap();
    registry.register(2, joining.raft_handle());

    leader.add_voter(2, BasicNode::new("node-2")).await.unwrap();
    joining
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(last_applied.index), "joining peer snapshot catch-up")
        .await
        .unwrap();
    assert!(
        joining.raft_handle().metrics().borrow().snapshot.is_some(),
        "joining peer should install a snapshot instead of replaying purged logs"
    );

    assert_eq!(
        read_metadata_value(&joining_engine, 7, b"k8", 9).await,
        b"v8".to_vec()
    );
}

#[test]
fn proposal_round_trips_metadata_command_payload() {
    let command = metadata_put_request(11, 7, b"k", b"v", 8, 9);
    let proposal = Proposal::from_metadata_command(&command).unwrap();
    assert_eq!(proposal.region_id, 11);
    assert_eq!(proposal.decode_metadata_command().unwrap(), command);
}

#[test]
fn proposal_rejects_region_mismatch() {
    let command = metadata_put_request(11, 7, b"k", b"v", 8, 9);
    let mut proposal = Proposal::from_metadata_command(&command).unwrap();
    proposal.region_id = 12;
    let err = proposal.decode_metadata_command().unwrap_err();
    assert!(matches!(err, Error::RegionMismatch { .. }));
}

#[tokio::test]
async fn applied_kv_engine_advances_index_only_for_writes() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    assert_eq!(engine.status().applied_index, 0);

    let get = engine
        .execute_metadata_get(&metadata_get_request(7, b"k", 1))
        .await
        .unwrap();
    assert!(get.not_found);
    assert_eq!(engine.status().applied_index, 0);

    execute_metadata_put(&engine, 7, 1, b"k", b"v", 1, 2)
        .await
        .unwrap();
    assert_eq!(engine.status().applied_index, 1);
}

#[tokio::test]
async fn applied_kv_engine_can_start_from_persisted_status() {
    let engine = AppliedKvEngine::with_status(
        ApplyStatus {
            region_id: 7,
            term: 3,
            applied_index: 41,
        },
        MvccStore::new(),
    );
    assert_eq!(
        engine.status(),
        ApplyStatus {
            region_id: 7,
            term: 3,
            applied_index: 41,
        }
    );
    execute_metadata_put(&engine, 7, 1, b"k", b"v", 1, 2)
        .await
        .unwrap();
    assert_eq!(engine.status().applied_index, 42);
}

#[tokio::test]
async fn applied_kv_engine_publishes_watch_events_for_writes() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    let mut watch = engine.subscribe();
    execute_metadata_put(&engine, 7, 1, b"k", b"v", 1, 2)
        .await
        .unwrap();
    let event = watch.try_recv().unwrap();
    assert_eq!(event.region_id, 7);
    assert_eq!(event.index, 1);
    assert_eq!(event.commit_version, 2);
    assert_eq!(event.keys, vec![b"k".to_vec()]);
}

#[tokio::test]
async fn applied_kv_engine_replays_watch_events_after_cursor() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    for (key, commit_version) in [
        (b"k/a".to_vec(), 2),
        (b"k/b".to_vec(), 3),
        (b"z".to_vec(), 4),
    ] {
        execute_metadata_put(
            &engine,
            7,
            commit_version,
            key,
            b"v",
            commit_version - 1,
            commit_version,
        )
        .await
        .unwrap();
    }

    let replay = engine
        .replay_apply(ApplyWatchReplayRequest {
            region_id: 7,
            term: 1,
            index: 1,
            key_prefix: b"k/".to_vec(),
        })
        .unwrap();

    assert!(!replay.expired);
    assert_eq!(replay.events.len(), 1);
    assert_eq!(replay.events[0].index, 2);
    assert_eq!(replay.events[0].keys, vec![b"k/b".to_vec()]);
}

#[tokio::test]
async fn applied_kv_engine_suppresses_watch_events_for_failed_writes() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    let mut watch = engine.subscribe();
    engine
        .execute_metadata_command(&metadatapb::MetadataCommitRequest {
            context: Some(metadatapb::MetadataContext {
                region_id: 7,
                ..Default::default()
            }),
            command: Some(metadatapb::MetadataCommand {
                request_id: b"first".to_vec(),
                read_version: 1,
                commit_version: 2,
                predicates: vec![metadatapb::MetadataPredicate {
                    key: b"k".to_vec(),
                    kind: metadatapb::MetadataPredicateKind::NotExists as i32,
                    read_version: 1,
                    ..Default::default()
                }],
                mutations: vec![metadatapb::MetadataMutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: metadatapb::metadata_mutation::Op::Put as i32,
                    ..Default::default()
                }],
                watch_keys: vec![b"k".to_vec()],
                ..Default::default()
            }),
        })
        .await
        .unwrap();
    assert_eq!(watch.try_recv().unwrap().keys, vec![b"k".to_vec()]);

    let rejected = engine
        .execute_metadata_command(&metadatapb::MetadataCommitRequest {
            context: Some(metadatapb::MetadataContext {
                region_id: 7,
                ..Default::default()
            }),
            command: Some(metadatapb::MetadataCommand {
                request_id: b"rejected".to_vec(),
                read_version: 2,
                commit_version: 4,
                predicates: vec![metadatapb::MetadataPredicate {
                    key: b"k".to_vec(),
                    kind: metadatapb::MetadataPredicateKind::NotExists as i32,
                    read_version: 2,
                    ..Default::default()
                }],
                mutations: vec![metadatapb::MetadataMutation {
                    key: b"k".to_vec(),
                    value: b"v2".to_vec(),
                    op: metadatapb::metadata_mutation::Op::Put as i32,
                    ..Default::default()
                }],
                watch_keys: vec![b"k".to_vec()],
                ..Default::default()
            }),
        })
        .await
        .unwrap();

    assert!(rejected.error.is_some());
    assert!(watch.try_recv().is_err());
}

#[tokio::test]
async fn applied_kv_engine_traffic_snapshot_drains_counters() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    execute_metadata_put(&engine, 7, 1, b"k", b"v", 1, 2)
        .await
        .unwrap();
    engine
        .execute_metadata_get(&metadata_get_request(7, b"k", 2))
        .await
        .unwrap();

    let snapshot = engine.traffic_snapshot();
    assert_eq!(snapshot.read_ops, 1);
    assert_eq!(snapshot.write_ops, 1);
    assert_eq!(snapshot.write_bytes, 1);
    assert_eq!(snapshot.atomic_ops, 1);
    assert!(snapshot.elapsed_secs >= 1);
    assert_eq!(engine.traffic_snapshot().read_ops, 0);
}

#[tokio::test]
async fn applied_kv_engine_executes_metadata_command_payload_and_reads_back() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    let response = execute_metadata_put(&engine, 7, 1, b"k", b"v", 1, 2)
        .await
        .unwrap();

    assert_eq!(response.result.unwrap().applied_mutations, 1);
    let read = engine
        .execute_metadata_get(&metadata_get_request(7, b"k", 2))
        .await
        .unwrap();
    assert_eq!(read.kv.unwrap().value, b"v".to_vec());
    assert_eq!(engine.status().applied_index, 1);
}

#[tokio::test]
async fn metadata_command_suppresses_watch_events_for_failed_mutation() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    let mut watch = engine.subscribe();
    engine
        .execute_metadata_command(&metadatapb::MetadataCommitRequest {
            context: Some(metadatapb::MetadataContext {
                region_id: 7,
                ..Default::default()
            }),
            command: Some(metadatapb::MetadataCommand {
                request_id: b"first".to_vec(),
                read_version: 1,
                commit_version: 2,
                predicates: vec![metadatapb::MetadataPredicate {
                    key: b"k".to_vec(),
                    kind: metadatapb::MetadataPredicateKind::NotExists as i32,
                    read_version: 1,
                    ..Default::default()
                }],
                mutations: vec![metadatapb::MetadataMutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: metadatapb::metadata_mutation::Op::Put as i32,
                    ..Default::default()
                }],
                watch_keys: vec![b"k".to_vec()],
                ..Default::default()
            }),
        })
        .await
        .unwrap();
    assert_eq!(watch.try_recv().unwrap().keys, vec![b"k".to_vec()]);

    let response = engine
        .execute_metadata_command(&metadatapb::MetadataCommitRequest {
            context: Some(metadatapb::MetadataContext {
                region_id: 7,
                ..Default::default()
            }),
            command: Some(metadatapb::MetadataCommand {
                request_id: b"rejected".to_vec(),
                read_version: 2,
                commit_version: 4,
                predicates: vec![metadatapb::MetadataPredicate {
                    key: b"k".to_vec(),
                    kind: metadatapb::MetadataPredicateKind::NotExists as i32,
                    read_version: 2,
                    ..Default::default()
                }],
                mutations: vec![metadatapb::MetadataMutation {
                    key: b"k".to_vec(),
                    value: b"v2".to_vec(),
                    op: metadatapb::metadata_mutation::Op::Put as i32,
                    ..Default::default()
                }],
                watch_keys: vec![b"k".to_vec()],
                ..Default::default()
            }),
        })
        .await
        .unwrap();

    assert!(response.error.is_some());
    assert!(watch.try_recv().is_err());
}

#[tokio::test]
async fn metadata_command_with_multiple_writes_advances_index_once() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    let mut watch = engine.subscribe();

    engine
        .execute_metadata_command(&metadatapb::MetadataCommitRequest {
            context: Some(metadatapb::MetadataContext {
                region_id: 7,
                ..Default::default()
            }),
            command: Some(metadatapb::MetadataCommand {
                request_id: b"multi".to_vec(),
                read_version: 1,
                commit_version: 2,
                mutations: vec![
                    metadatapb::MetadataMutation {
                        key: b"k1".to_vec(),
                        value: b"v1".to_vec(),
                        op: metadatapb::metadata_mutation::Op::Put as i32,
                        ..Default::default()
                    },
                    metadatapb::MetadataMutation {
                        key: b"k2".to_vec(),
                        value: b"v2".to_vec(),
                        op: metadatapb::metadata_mutation::Op::Put as i32,
                        ..Default::default()
                    },
                ],
                watch_keys: vec![b"k1".to_vec(), b"k2".to_vec()],
                ..Default::default()
            }),
        })
        .await
        .unwrap();

    assert_eq!(engine.status().applied_index, 1);
    assert_eq!(watch.try_recv().unwrap().index, 1);
    assert!(watch.try_recv().is_err());
}

#[test]
fn apply_openraft_entry_uses_committed_log_status() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    let mut watch = engine.subscribe();
    let entry = OpenRaftEntry {
        log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(5, 1), 42),
        payload: openraft::EntryPayload::Normal(
            Proposal::from_metadata_command(&metadata_put_request(7, 55, b"k", b"v", 8, 9))
                .unwrap(),
        ),
    };

    let applied = engine.apply_openraft_entries([entry]).unwrap();

    assert_eq!(
        engine.status(),
        ApplyStatus {
            region_id: 7,
            term: 5,
            applied_index: 42,
        }
    );
    let event = watch.try_recv().unwrap();
    assert_eq!(event.term, 5);
    assert_eq!(event.index, 42);
    assert_eq!(event.commit_version, 9);
    let response =
        metadatapb::MetadataCommitResponse::decode(applied[0].payload.as_slice()).unwrap();
    assert_eq!(response.result.unwrap().applied_mutations, 1);
}

#[tokio::test]
async fn persistent_applied_engine_saves_status_after_metadata_command() {
    let sink = RecordingRegionMetadataSink::default();
    let statuses = sink.statuses.clone();
    let events = sink.events.clone();
    let engine = PersistentAppliedKvEngine::new(AppliedKvEngine::new(7, MvccStore::new()), sink);

    execute_metadata_put(&engine, 7, 1, b"k", b"v", 1, 2)
        .await
        .unwrap();

    assert_eq!(
        statuses.lock().unwrap().as_slice(),
        &[ApplyStatus {
            region_id: 7,
            term: 1,
            applied_index: 1,
        }]
    );
    assert_eq!(
        events.lock().unwrap().as_slice()[0].keys,
        vec![b"k".to_vec()]
    );
}

#[test]
fn persistent_applied_engine_saves_descriptor_after_descriptor_entry() {
    let sink = RecordingRegionMetadataSink::default();
    let descriptors = sink.descriptors.clone();
    let statuses = sink.statuses.clone();
    let engine = PersistentAppliedKvEngine::new(AppliedKvEngine::new(7, MvccStore::new()), sink);
    let descriptor = metapb::RegionDescriptor {
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
    };

    engine
        .apply_openraft_entries([OpenRaftEntry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(5, 1), 42),
            payload: openraft::EntryPayload::Normal(
                Proposal::from_region_descriptor(&descriptor).unwrap(),
            ),
        }])
        .unwrap();

    assert_eq!(
        engine.inner().region_descriptor().unwrap(),
        Some(descriptor.clone())
    );
    assert_eq!(descriptors.lock().unwrap().as_slice(), &[descriptor]);
    assert_eq!(
        statuses.lock().unwrap().as_slice(),
        &[ApplyStatus {
            region_id: 7,
            term: 5,
            applied_index: 42,
        }]
    );
}

#[test]
fn applied_engine_applies_split_admin_command_to_parent_descriptor() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    engine
        .set_region_descriptor(metapb::RegionDescriptor {
            region_id: 7,
            end_key: b"z".to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 3,
                conf_version: 1,
            }),
            peers: vec![metapb::RegionPeer {
                store_id: 1,
                peer_id: 1,
            }],
            ..Default::default()
        })
        .unwrap();
    let command = raftpb::AdminCommand {
        r#type: raftpb::admin_command::Type::Split as i32,
        split: Some(raftpb::SplitCommand {
            parent_region_id: 7,
            split_key: b"m".to_vec(),
            child: Some(metapb::RegionDescriptor {
                region_id: 8,
                start_key: b"m".to_vec(),
                end_key: b"z".to_vec(),
                peers: vec![metapb::RegionPeer {
                    store_id: 1,
                    peer_id: 8,
                }],
                ..Default::default()
            }),
        }),
        ..Default::default()
    };

    let applied = engine
        .apply_openraft_entries([OpenRaftEntry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(5, 1), 42),
            payload: openraft::EntryPayload::Normal(
                Proposal::from_admin_command(7, &command).unwrap(),
            ),
        }])
        .unwrap();

    let descriptor = engine.region_descriptor().unwrap().unwrap();
    assert_eq!(descriptor.region_id, 7);
    assert_eq!(descriptor.end_key, b"m");
    assert_eq!(descriptor.epoch.unwrap().version, 4);
    let topology = engine.topology_descriptors().unwrap();
    assert_eq!(topology.len(), 1);
    assert_eq!(topology[0].region_id, 8);
    assert_eq!(topology[0].start_key, b"m");
    assert_eq!(topology[0].end_key, b"z");
    assert_eq!(topology[0].lineage.len(), 1);
    assert_eq!(topology[0].lineage[0].region_id, 7);
    assert_eq!(
        engine.status(),
        ApplyStatus {
            region_id: 7,
            term: 5,
            applied_index: 42,
        }
    );
    assert_eq!(applied.len(), 1);
    assert!(applied[0].payload.is_empty());
}

#[test]
fn persistent_applied_engine_saves_split_parent_and_child_descriptors() {
    let sink = RecordingRegionMetadataSink::default();
    let descriptors = sink.descriptors.clone();
    let engine = PersistentAppliedKvEngine::new(AppliedKvEngine::new(7, MvccStore::new()), sink);
    engine
        .inner()
        .set_region_descriptor(metapb::RegionDescriptor {
            region_id: 7,
            end_key: b"z".to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 3,
                conf_version: 1,
            }),
            peers: vec![metapb::RegionPeer {
                store_id: 1,
                peer_id: 1,
            }],
            ..Default::default()
        })
        .unwrap();
    let command = raftpb::AdminCommand {
        r#type: raftpb::admin_command::Type::Split as i32,
        split: Some(raftpb::SplitCommand {
            parent_region_id: 7,
            split_key: b"m".to_vec(),
            child: Some(metapb::RegionDescriptor {
                region_id: 8,
                start_key: b"m".to_vec(),
                end_key: b"z".to_vec(),
                peers: vec![metapb::RegionPeer {
                    store_id: 2,
                    peer_id: 20,
                }],
                ..Default::default()
            }),
        }),
        ..Default::default()
    };

    engine
        .apply_openraft_entries([OpenRaftEntry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(5, 1), 42),
            payload: openraft::EntryPayload::Normal(
                Proposal::from_admin_command(7, &command).unwrap(),
            ),
        }])
        .unwrap();

    let saved = descriptors.lock().unwrap().clone();
    assert_eq!(saved.len(), 2);
    assert_eq!(saved[0].region_id, 7);
    assert_eq!(saved[0].end_key, b"m");
    assert_eq!(saved[1].region_id, 8);
    assert_eq!(saved[1].start_key, b"m");
    assert_eq!(saved[1].end_key, b"z");
}

#[test]
fn applied_engine_applies_merge_admin_command_to_target_descriptor() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    engine
        .set_region_descriptor(metapb::RegionDescriptor {
            region_id: 7,
            start_key: b"a".to_vec(),
            end_key: b"m".to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 3,
                conf_version: 2,
            }),
            peers: vec![
                metapb::RegionPeer {
                    store_id: 1,
                    peer_id: 11,
                },
                metapb::RegionPeer {
                    store_id: 2,
                    peer_id: 12,
                },
            ],
            ..Default::default()
        })
        .unwrap();
    engine
        .record_topology_descriptor(metapb::RegionDescriptor {
            region_id: 8,
            start_key: b"m".to_vec(),
            end_key: b"z".to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 4,
                conf_version: 2,
            }),
            peers: vec![
                metapb::RegionPeer {
                    store_id: 1,
                    peer_id: 21,
                },
                metapb::RegionPeer {
                    store_id: 2,
                    peer_id: 22,
                },
            ],
            hash: b"source-hash".to_vec(),
            ..Default::default()
        })
        .unwrap();
    let command = raftpb::AdminCommand {
        r#type: raftpb::admin_command::Type::Merge as i32,
        merge: Some(raftpb::MergeCommand {
            target_region_id: 7,
            source_region_id: 8,
        }),
        ..Default::default()
    };

    let applied = engine
        .apply_openraft_entries([OpenRaftEntry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(5, 1), 42),
            payload: openraft::EntryPayload::Normal(
                Proposal::from_admin_command(7, &command).unwrap(),
            ),
        }])
        .unwrap();

    let descriptor = engine.region_descriptor().unwrap().unwrap();
    assert_eq!(descriptor.region_id, 7);
    assert_eq!(descriptor.start_key, b"a");
    assert_eq!(descriptor.end_key, b"z");
    assert_eq!(descriptor.epoch.unwrap().version, 4);
    assert!(descriptor.hash.is_empty());
    assert_eq!(descriptor.lineage.len(), 1);
    assert_eq!(descriptor.lineage[0].region_id, 8);
    assert_eq!(descriptor.lineage[0].hash, b"source-hash");
    assert_eq!(
        descriptor.lineage[0].kind,
        metapb::DescriptorLineageKind::MergeSource as i32
    );
    assert!(engine.topology_descriptors().unwrap().is_empty());
    assert_eq!(
        engine.status(),
        ApplyStatus {
            region_id: 7,
            term: 5,
            applied_index: 42,
        }
    );
    assert_eq!(applied.len(), 1);
    assert!(applied[0].payload.is_empty());
}

#[test]
fn applied_engine_uses_region_descriptor_catalog_for_merge_admin_command() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    engine
        .set_region_descriptor(metapb::RegionDescriptor {
            region_id: 7,
            start_key: b"a".to_vec(),
            end_key: b"m".to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 3,
                conf_version: 2,
            }),
            peers: vec![metapb::RegionPeer {
                store_id: 1,
                peer_id: 11,
            }],
            ..Default::default()
        })
        .unwrap();
    let source = metapb::RegionDescriptor {
        region_id: 8,
        start_key: b"m".to_vec(),
        end_key: b"z".to_vec(),
        epoch: Some(metapb::RegionEpoch {
            version: 4,
            conf_version: 2,
        }),
        peers: vec![metapb::RegionPeer {
            store_id: 1,
            peer_id: 21,
        }],
        ..Default::default()
    };
    engine
        .set_region_descriptor_catalog(Arc::new(StaticRegionDescriptorCatalog {
            descriptors: BTreeMap::from([(source.region_id, source)]),
        }))
        .unwrap();
    let command = raftpb::AdminCommand {
        r#type: raftpb::admin_command::Type::Merge as i32,
        merge: Some(raftpb::MergeCommand {
            target_region_id: 7,
            source_region_id: 8,
        }),
        ..Default::default()
    };

    engine
        .apply_openraft_entries([OpenRaftEntry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(5, 1), 42),
            payload: openraft::EntryPayload::Normal(
                Proposal::from_admin_command(7, &command).unwrap(),
            ),
        }])
        .unwrap();

    let descriptor = engine.region_descriptor().unwrap().unwrap();
    assert_eq!(descriptor.region_id, 7);
    assert_eq!(descriptor.end_key, b"z");
    assert_eq!(descriptor.lineage.len(), 1);
    assert_eq!(descriptor.lineage[0].region_id, 8);
    assert!(engine.topology_descriptors().unwrap().is_empty());
}

#[test]
fn applied_engine_replays_merge_admin_command_after_source_retired() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    engine
        .set_region_descriptor(metapb::RegionDescriptor {
            region_id: 7,
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 4,
                conf_version: 2,
            }),
            peers: vec![metapb::RegionPeer {
                store_id: 1,
                peer_id: 11,
            }],
            lineage: vec![metapb::DescriptorLineageRef {
                region_id: 8,
                epoch: Some(metapb::RegionEpoch {
                    version: 4,
                    conf_version: 2,
                }),
                kind: metapb::DescriptorLineageKind::MergeSource as i32,
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();
    let command = raftpb::AdminCommand {
        r#type: raftpb::admin_command::Type::Merge as i32,
        merge: Some(raftpb::MergeCommand {
            target_region_id: 7,
            source_region_id: 8,
        }),
        ..Default::default()
    };

    engine
        .apply_openraft_entries([OpenRaftEntry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(6, 1), 43),
            payload: openraft::EntryPayload::Normal(
                Proposal::from_admin_command(7, &command).unwrap(),
            ),
        }])
        .unwrap();

    let descriptor = engine.region_descriptor().unwrap().unwrap();
    assert_eq!(descriptor.lineage.len(), 1);
    assert_eq!(
        engine.status(),
        ApplyStatus {
            region_id: 7,
            term: 6,
            applied_index: 43,
        }
    );
}

#[test]
fn applied_engine_rejects_merge_admin_command_without_source_descriptor() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    engine
        .set_region_descriptor(metapb::RegionDescriptor {
            region_id: 7,
            start_key: b"a".to_vec(),
            end_key: b"m".to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 3,
                conf_version: 1,
            }),
            peers: vec![metapb::RegionPeer {
                store_id: 1,
                peer_id: 11,
            }],
            ..Default::default()
        })
        .unwrap();
    let command = raftpb::AdminCommand {
        r#type: raftpb::admin_command::Type::Merge as i32,
        merge: Some(raftpb::MergeCommand {
            target_region_id: 7,
            source_region_id: 8,
        }),
        ..Default::default()
    };

    let err = engine
        .apply_openraft_entries([OpenRaftEntry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(5, 1), 42),
            payload: openraft::EntryPayload::Normal(
                Proposal::from_admin_command(7, &command).unwrap(),
            ),
        }])
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("merge source descriptor must be available before apply"));
    assert_eq!(
        engine.status(),
        ApplyStatus {
            region_id: 7,
            term: 1,
            applied_index: 0,
        }
    );
}

#[test]
fn applied_engine_keeps_source_descriptor_when_merge_validation_fails() {
    let engine = AppliedKvEngine::new(7, MvccStore::new());
    engine
        .set_region_descriptor(metapb::RegionDescriptor {
            region_id: 7,
            start_key: b"a".to_vec(),
            end_key: b"m".to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 3,
                conf_version: 1,
            }),
            peers: vec![metapb::RegionPeer {
                store_id: 1,
                peer_id: 11,
            }],
            ..Default::default()
        })
        .unwrap();
    engine
        .record_topology_descriptor(metapb::RegionDescriptor {
            region_id: 8,
            start_key: b"n".to_vec(),
            end_key: b"z".to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 4,
                conf_version: 1,
            }),
            peers: vec![metapb::RegionPeer {
                store_id: 1,
                peer_id: 21,
            }],
            ..Default::default()
        })
        .unwrap();
    let command = raftpb::AdminCommand {
        r#type: raftpb::admin_command::Type::Merge as i32,
        merge: Some(raftpb::MergeCommand {
            target_region_id: 7,
            source_region_id: 8,
        }),
        ..Default::default()
    };

    let err = engine
        .apply_openraft_entries([OpenRaftEntry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(5, 1), 42),
            payload: openraft::EntryPayload::Normal(
                Proposal::from_admin_command(7, &command).unwrap(),
            ),
        }])
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("merge source must be the target's right sibling"));
    let topology = engine.topology_descriptors().unwrap();
    assert_eq!(topology.len(), 1);
    assert_eq!(topology[0].region_id, 8);
    assert_eq!(
        engine.status(),
        ApplyStatus {
            region_id: 7,
            term: 1,
            applied_index: 0,
        }
    );
}

#[test]
fn persistent_applied_engine_saves_merged_target_without_retired_source() {
    let sink = RecordingRegionMetadataSink::default();
    let descriptors = sink.descriptors.clone();
    let statuses = sink.statuses.clone();
    let engine = PersistentAppliedKvEngine::new(AppliedKvEngine::new(7, MvccStore::new()), sink);
    engine
        .inner()
        .set_region_descriptor(metapb::RegionDescriptor {
            region_id: 7,
            start_key: b"a".to_vec(),
            end_key: b"m".to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 3,
                conf_version: 2,
            }),
            peers: vec![metapb::RegionPeer {
                store_id: 1,
                peer_id: 11,
            }],
            ..Default::default()
        })
        .unwrap();
    engine
        .inner()
        .record_topology_descriptor(metapb::RegionDescriptor {
            region_id: 8,
            start_key: b"m".to_vec(),
            end_key: b"z".to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 4,
                conf_version: 2,
            }),
            peers: vec![metapb::RegionPeer {
                store_id: 1,
                peer_id: 21,
            }],
            ..Default::default()
        })
        .unwrap();
    let command = raftpb::AdminCommand {
        r#type: raftpb::admin_command::Type::Merge as i32,
        merge: Some(raftpb::MergeCommand {
            target_region_id: 7,
            source_region_id: 8,
        }),
        ..Default::default()
    };

    engine
        .apply_openraft_entries([OpenRaftEntry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(5, 1), 42),
            payload: openraft::EntryPayload::Normal(
                Proposal::from_admin_command(7, &command).unwrap(),
            ),
        }])
        .unwrap();

    let saved = descriptors.lock().unwrap().clone();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].region_id, 7);
    assert_eq!(saved[0].start_key, b"a");
    assert_eq!(saved[0].end_key, b"z");
    assert_eq!(saved[0].lineage.len(), 1);
    assert_eq!(saved[0].lineage[0].region_id, 8);
    assert_eq!(
        statuses.lock().unwrap().as_slice(),
        &[ApplyStatus {
            region_id: 7,
            term: 5,
            applied_index: 42,
        }]
    );
}

#[tokio::test]
async fn persistent_applied_engine_does_not_save_status_after_read_command() {
    let sink = RecordingRegionMetadataSink::default();
    let statuses = sink.statuses.clone();
    let engine = PersistentAppliedKvEngine::new(AppliedKvEngine::new(7, MvccStore::new()), sink);

    engine
        .execute_metadata_get(&metadata_get_request(7, b"k", 1))
        .await
        .unwrap();

    assert!(statuses.lock().unwrap().is_empty());
}
