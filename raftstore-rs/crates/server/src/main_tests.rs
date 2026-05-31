use super::*;
use nokv_holtstore::HoltMvccStore;
use nokv_mvcc::MvccStore;
use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_raftnode::AppliedKvEngine;
use nokv_raftstore_server::{
    root_event_transition_id, EmptyTopologyPublisher, MultiRegionRaftAdminService,
    MultiRegionStoreKvService, PeerEndpointCatalog,
};
use prost::Message;
use prost_types::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::{Request, Response, Status};

#[test]
fn server_args_parse_metrics_addr_from_compose_extra() {
    let args = ServerArgs::parse(vec![
        "--storage-max-batch-count=1024".to_owned(),
        "--metrics-addr=0.0.0.0:9200".to_owned(),
    ])
    .unwrap();
    assert_eq!(
        args.metrics_addr,
        Some("0.0.0.0:9200".parse::<SocketAddr>().unwrap())
    );

    let args = ServerArgs::parse(vec![
        "--metrics-addr".to_owned(),
        "127.0.0.1:9201".to_owned(),
    ])
    .unwrap();
    assert_eq!(
        args.metrics_addr,
        Some("127.0.0.1:9201".parse::<SocketAddr>().unwrap())
    );
}

#[test]
fn server_args_reject_missing_metrics_addr_value() {
    let err = ServerArgs::parse(vec!["--metrics-addr".to_owned()]).unwrap_err();
    assert!(err.to_string().contains("requires a listen address"));
}

#[derive(Clone, Default)]
struct CaptureRaftAdmin {
    transfers: Arc<Mutex<Vec<adminpb::TransferLeaderRequest>>>,
}

#[tonic::async_trait]
impl adminpb::raft_admin_server::RaftAdmin for CaptureRaftAdmin {
    async fn add_peer(
        &self,
        _request: Request<adminpb::AddPeerRequest>,
    ) -> Result<Response<adminpb::AddPeerResponse>, Status> {
        Err(Status::unimplemented("add peer is not used by this test"))
    }

    async fn remove_peer(
        &self,
        _request: Request<adminpb::RemovePeerRequest>,
    ) -> Result<Response<adminpb::RemovePeerResponse>, Status> {
        Err(Status::unimplemented(
            "remove peer is not used by this test",
        ))
    }

    async fn transfer_leader(
        &self,
        request: Request<adminpb::TransferLeaderRequest>,
    ) -> Result<Response<adminpb::TransferLeaderResponse>, Status> {
        let request = request.into_inner();
        self.transfers.lock().unwrap().push(request.clone());
        Ok(Response::new(adminpb::TransferLeaderResponse {
            region: Some(metapb::RegionDescriptor {
                region_id: request.region_id,
                ..Default::default()
            }),
        }))
    }

    async fn region_runtime_status(
        &self,
        _request: Request<adminpb::RegionRuntimeStatusRequest>,
    ) -> Result<Response<adminpb::RegionRuntimeStatusResponse>, Status> {
        Err(Status::unimplemented(
            "region runtime status is not used by this test",
        ))
    }

    async fn execution_status(
        &self,
        _request: Request<adminpb::ExecutionStatusRequest>,
    ) -> Result<Response<adminpb::ExecutionStatusResponse>, Status> {
        Err(Status::unimplemented(
            "execution status is not used by this test",
        ))
    }
}

#[test]
fn server_identity_defaults_to_single_node_bootstrap() {
    assert_eq!(
        ServerIdentity::from_values(None, None, None, None).unwrap(),
        ServerIdentity::default()
    );
}

#[test]
fn server_identity_parses_joining_peer() {
    let identity = ServerIdentity::from_values(
        Some("9".to_owned()),
        Some("12".to_owned()),
        Some("34".to_owned()),
        Some("false".to_owned()),
    )
    .unwrap();
    assert_eq!(
        identity,
        ServerIdentity {
            region_id: 9,
            store_id: 12,
            peer_id: 34,
            bootstrap: false,
        }
    );
}

#[test]
fn server_identity_parses_multi_region_list() {
    let identities = ServerIdentity::from_region_list("7:11:101:true, 8:11:102:false").unwrap();

    assert_eq!(
        identities,
        vec![
            ServerIdentity {
                region_id: 7,
                store_id: 11,
                peer_id: 101,
                bootstrap: true,
            },
            ServerIdentity {
                region_id: 8,
                store_id: 11,
                peer_id: 102,
                bootstrap: false,
            },
        ]
    );
}

#[test]
fn server_identity_rejects_multi_region_mixed_store() {
    let err = ServerIdentity::from_region_list("7:11:101:true,8:12:102:true").unwrap_err();
    assert!(err.to_string().contains("one store_id per process"));
}

#[test]
fn server_identity_rejects_multi_region_duplicate_region() {
    let err = ServerIdentity::from_region_list("7:11:101:true,7:11:102:true").unwrap_err();
    assert!(err.to_string().contains("duplicate region_id 7"));
}

#[test]
fn server_identity_rejects_multi_region_duplicate_peer() {
    let err = ServerIdentity::from_region_list("7:11:101:true,8:11:101:true").unwrap_err();
    assert!(err.to_string().contains("duplicate peer_id 101"));
}

#[test]
fn region_range_catalog_parses_hex_bounds() {
    let ranges = RegionRangeCatalog::parse("7=:6d, 8=6d:").unwrap();

    assert_eq!(
        ranges.get(7).unwrap(),
        &RegionKeyRange {
            start_key: Vec::new(),
            end_key: b"m".to_vec(),
        }
    );
    assert_eq!(
        ranges.get(8).unwrap(),
        &RegionKeyRange {
            start_key: b"m".to_vec(),
            end_key: Vec::new(),
        }
    );
}

#[test]
fn region_range_catalog_rejects_invalid_hex() {
    let err = RegionRangeCatalog::parse("7=0:6d").unwrap_err();
    assert!(err.to_string().contains("even number"));
}

#[test]
fn multi_region_bootstrap_requires_explicit_range() {
    let identities = ServerIdentity::from_region_list("7:11:101:true,8:11:102:true").unwrap();
    let err =
        validate_startup_region_ranges(&identities, &RegionRangeCatalog::default()).unwrap_err();

    assert!(err
        .to_string()
        .contains("requires NOKV_RUST_RAFTSTORE_REGION_RANGES"));
}

#[test]
fn multi_region_bootstrap_rejects_overlapping_ranges() {
    let identities = ServerIdentity::from_region_list("7:11:101:true,8:11:102:true").unwrap();
    let ranges = RegionRangeCatalog::parse("7=:6d,8=61:").unwrap();

    let err = validate_startup_region_ranges(&identities, &ranges).unwrap_err();

    assert!(err.to_string().contains("region 7 overlaps region 8"));
}

#[test]
fn server_identity_rejects_zero_peer() {
    let err = ServerIdentity::from_values(None, None, Some("0".to_owned()), None).unwrap_err();
    assert!(err.to_string().contains("NOKV_RUST_RAFTSTORE_PEER_ID"));
}

#[test]
fn server_identity_rejects_invalid_bootstrap_flag() {
    let err = ServerIdentity::from_values(None, None, None, Some("maybe".to_owned())).unwrap_err();
    assert!(err.to_string().contains("NOKV_RUST_RAFTSTORE_BOOTSTRAP"));
}

#[test]
fn coordinator_endpoint_adds_http_scheme_for_host_port() {
    assert_eq!(
        coordinator_endpoint("127.0.0.1:23790"),
        "http://127.0.0.1:23790"
    );
    assert_eq!(
        coordinator_endpoint("http://127.0.0.1:23790"),
        "http://127.0.0.1:23790"
    );
}

#[test]
fn coordinator_endpoints_split_comma_separated_addresses() {
    assert_eq!(
        coordinator_endpoints("127.0.0.1:23790, http://127.0.0.1:23791 ,,127.0.0.1:23792"),
        vec![
            "http://127.0.0.1:23790".to_owned(),
            "http://127.0.0.1:23791".to_owned(),
            "http://127.0.0.1:23792".to_owned(),
        ]
    );
}

#[test]
fn root_event_publish_status_classifies_coordinator_error_info() {
    assert!(matches!(
        classify_root_event_publish_status(status_with_coordinator_reason("not_leader")),
        RootEventPublishError::Transient(_)
    ));
    assert!(matches!(
        classify_root_event_publish_status(status_with_coordinator_reason("catalog_precondition")),
        RootEventPublishError::Permanent(_)
    ));
    assert!(matches!(
        classify_root_event_publish_status(Status::invalid_argument("bad root event")),
        RootEventPublishError::Permanent(_)
    ));
}

#[test]
fn root_event_transition_id_matches_go_peer_shape() {
    let event = metapb::RootEvent {
        kind: metapb::RootEventKind::PeerAdditionPlanned as i32,
        payload: Some(metapb::root_event::Payload::PeerChange(
            metapb::RootPeerChange {
                region_id: 11,
                store_id: 2,
                peer_id: 201,
                ..Default::default()
            },
        )),
    };
    assert_eq!(root_event_transition_id(&event), "peer:11:add:2:201");

    let event = metapb::RootEvent {
        kind: metapb::RootEventKind::PeerRemovalCancelled as i32,
        payload: Some(metapb::root_event::Payload::PeerChange(
            metapb::RootPeerChange {
                region_id: 11,
                store_id: 2,
                peer_id: 201,
                ..Default::default()
            },
        )),
    };
    assert_eq!(root_event_transition_id(&event), "peer:11:remove:2:201");
}

#[test]
fn root_event_transition_id_matches_go_range_split_shape() {
    let event = metapb::RootEvent {
        kind: metapb::RootEventKind::RegionSplitPlanned as i32,
        payload: Some(metapb::root_event::Payload::RangeSplit(
            metapb::RootRangeSplit {
                parent_region_id: 7,
                split_key: vec![0x00, 0x0a, 0xff],
                ..Default::default()
            },
        )),
    };
    assert_eq!(root_event_transition_id(&event), "split:7:000aff");
}

#[test]
fn root_event_transition_id_matches_go_range_merge_shape() {
    let event = metapb::RootEvent {
        kind: metapb::RootEventKind::RegionMergePlanned as i32,
        payload: Some(metapb::root_event::Payload::RangeMerge(
            metapb::RootRangeMerge {
                left_region_id: 7,
                right_region_id: 8,
                ..Default::default()
            },
        )),
    };
    assert_eq!(root_event_transition_id(&event), "merge:7:8");
}

fn status_with_coordinator_reason(reason: &str) -> Status {
    let mut metadata = HashMap::new();
    metadata.insert(COORDINATOR_REASON_METADATA.to_owned(), reason.to_owned());
    let info = RpcErrorInfo {
        reason: NOKV_ERROR_INFO_REASON.to_owned(),
        domain: NOKV_ERROR_INFO_DOMAIN.to_owned(),
        metadata,
    };
    let details = RpcStatusDetails {
        code: tonic::Code::FailedPrecondition as i32,
        message: reason.to_owned(),
        details: vec![Any {
            type_url: GOOGLE_RPC_ERROR_INFO_TYPE.to_owned(),
            value: info.encode_to_vec(),
        }],
    };
    Status::with_details(
        tonic::Code::FailedPrecondition,
        reason.to_owned(),
        details.encode_to_vec().into(),
    )
}

#[test]
fn local_admin_endpoint_uses_loopback_for_unspecified_bind_addr() {
    let addr: SocketAddr = "0.0.0.0:23880".parse().unwrap();
    assert_eq!(local_admin_endpoint(addr), "http://127.0.0.1:23880");
}

#[tokio::test]
async fn scheduler_operation_executes_leader_transfer_via_admin_rpc() {
    let addr = reserve_loopback_addr();
    let admin = CaptureRaftAdmin::default();
    let transfers = admin.transfers.clone();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(adminpb::raft_admin_server::RaftAdminServer::new(admin))
            .serve(addr)
            .await
            .unwrap();
    });
    wait_for_server(addr).await;

    let outcome = execute_scheduler_operation(
        &local_admin_endpoint(addr),
        None,
        &coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::LeaderTransfer as i32,
            region_id: 7,
            source_peer_id: 101,
            target_peer_id: 202,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(outcome, SchedulerOperationOutcome::Applied);
    let captured = transfers.lock().unwrap();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].region_id, 7);
    assert_eq!(captured[0].peer_id, 202);
    handle.abort();
}

#[tokio::test]
async fn store_heartbeat_queries_all_endpoints_and_prefers_operations() {
    let endpoints = vec![
        "http://standby".to_owned(),
        "http://holder".to_owned(),
        "http://down".to_owned(),
    ];
    let operation = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::LeaderTransfer as i32,
        region_id: 9,
        source_peer_id: 101,
        target_peer_id: 201,
        ..Default::default()
    };
    let responses = Arc::new(Mutex::new(BTreeMap::from([
        (
            "http://standby".to_owned(),
            Ok(coordpb::StoreHeartbeatResponse {
                accepted: true,
                ..Default::default()
            }),
        ),
        (
            "http://holder".to_owned(),
            Ok(coordpb::StoreHeartbeatResponse {
                accepted: true,
                operations: vec![operation.clone()],
            }),
        ),
        ("http://down".to_owned(), Err("unavailable".to_owned())),
    ])));
    let calls = Arc::new(Mutex::new(Vec::new()));

    let operations = send_store_heartbeat_with(
        &endpoints,
        coordpb::StoreHeartbeatRequest {
            store_id: 2,
            region_num: 1,
            leader_num: 1,
            leader_region_ids: vec![9],
            ..Default::default()
        },
        |endpoint, _request| {
            let responses = responses.clone();
            let calls = calls.clone();
            async move {
                calls.lock().unwrap().push(endpoint.clone());
                responses.lock().unwrap().get(&endpoint).unwrap().clone()
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(operations, vec![operation]);
    assert_eq!(calls.lock().unwrap().as_slice(), endpoints.as_slice());
}

#[tokio::test]
async fn scheduler_operation_reports_unsupported_split_without_dialing_admin() {
    let outcome = execute_scheduler_operation(
        "http://127.0.0.1:1",
        None,
        &coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
            region_id: 7,
            split_key: b"k".to_vec(),
            split_child: Some(metapb::RegionDescriptor {
                region_id: 8,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        outcome,
        SchedulerOperationOutcome::Unsupported {
            kind: coordpb::SchedulerOperationType::SplitRegion,
            reason: "split execution is not implemented in raftstore-rs yet",
        }
    );
}

#[tokio::test]
async fn scheduler_operation_reports_invalid_split_before_admin_rpc() {
    let outcome = execute_scheduler_operation(
        "http://127.0.0.1:1",
        None,
        &coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
            region_id: 7,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        outcome,
        SchedulerOperationOutcome::Invalid {
            reason: "split requires region, split key, and child descriptor",
        }
    );
}

#[tokio::test]
async fn scheduler_operation_reports_unsupported_merge_without_dialing_admin() {
    let outcome = execute_scheduler_operation(
        "http://127.0.0.1:1",
        None,
        &coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::MergeRegion as i32,
            region_id: 7,
            source_region_id: 8,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        outcome,
        SchedulerOperationOutcome::Unsupported {
            kind: coordpb::SchedulerOperationType::MergeRegion,
            reason: "merge execution is not implemented in raftstore-rs yet",
        }
    );
}

#[test]
fn unsupported_scheduler_operation_records_pending_holt_diagnostic() {
    let store = HoltMvccStore::open_memory().unwrap();
    let operation = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
        region_id: 7,
        split_key: b"k".to_vec(),
        split_child: Some(metapb::RegionDescriptor {
            region_id: 8,
            ..Default::default()
        }),
        ..Default::default()
    };

    record_pending_scheduler_operation(Some(&store), &operation);

    let pending = store.pending_scheduler_operations().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].operation, operation);
}

#[test]
fn failed_scheduler_operation_records_pending_holt_diagnostic() {
    let store = HoltMvccStore::open_memory().unwrap();
    let operation = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::LeaderTransfer as i32,
        region_id: 7,
        source_peer_id: 101,
        target_peer_id: 202,
        ..Default::default()
    };

    record_scheduler_operation_outcome(
        Some(&store),
        &operation,
        Err(tonic::Status::unavailable("admin unavailable")),
    );

    let pending = store.pending_scheduler_operations().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].operation, operation);
}

#[tokio::test]
async fn pending_scheduler_operation_retries_and_deletes_after_apply() {
    let addr = reserve_loopback_addr();
    let admin = CaptureRaftAdmin::default();
    let transfers = admin.transfers.clone();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(adminpb::raft_admin_server::RaftAdminServer::new(admin))
            .serve(addr)
            .await
            .unwrap();
    });
    wait_for_server(addr).await;
    let store = HoltMvccStore::open_memory().unwrap();
    let operation = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::LeaderTransfer as i32,
        region_id: 7,
        source_peer_id: 101,
        target_peer_id: 202,
        ..Default::default()
    };
    store
        .record_pending_scheduler_operation(&operation)
        .unwrap();

    retry_pending_scheduler_operations(&local_admin_endpoint(addr), &store, None).await;

    assert!(store.pending_scheduler_operations().unwrap().is_empty());
    let captured = transfers.lock().unwrap();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].region_id, 7);
    assert_eq!(captured[0].peer_id, 202);
    handle.abort();
}

#[tokio::test]
async fn pending_scheduler_operation_tracks_attempts_and_expires() {
    let store = HoltMvccStore::open_memory().unwrap();
    let operation = coordpb::SchedulerOperation {
        r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
        region_id: 7,
        split_key: b"k".to_vec(),
        split_child: Some(metapb::RegionDescriptor {
            region_id: 8,
            ..Default::default()
        }),
        ..Default::default()
    };
    store
        .record_pending_scheduler_operation(&operation)
        .unwrap();

    retry_pending_scheduler_operations("http://127.0.0.1:1", &store, None).await;
    let pending = store.pending_scheduler_operations().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].attempts, 1);

    for _ in 1..MAX_PENDING_SCHEDULER_OPERATION_ATTEMPTS {
        retry_pending_scheduler_operations("http://127.0.0.1:1", &store, None).await;
    }
    assert!(store.pending_scheduler_operations().unwrap().is_empty());
    let blocked = store.blocked_scheduler_operations().unwrap();
    assert_eq!(blocked.len(), 1);
    assert_eq!(blocked[0].operation, operation);
    assert_eq!(
        blocked[0].attempts,
        MAX_PENDING_SCHEDULER_OPERATION_ATTEMPTS
    );
    assert_eq!(
        blocked[0].last_error,
        "split execution is not implemented in raftstore-rs yet"
    );
}

#[tokio::test]
async fn non_bootstrap_start_opens_joining_peer_without_initializing_membership() {
    let dir = tempfile::tempdir().unwrap();
    let identity = ServerIdentity {
        region_id: 7,
        store_id: 2,
        peer_id: 2,
        bootstrap: false,
    };
    let region = open_openraft_region(
        identity,
        "127.0.0.1:0",
        dir.path().to_path_buf(),
        AppliedKvEngine::new(identity.region_id, MvccStore::new()),
    )
    .await
    .unwrap();
    let metrics = region.raft_handle().metrics().borrow().clone();
    assert!(metrics.current_leader.is_none());
    assert!(metrics.membership_config.voter_ids().next().is_none());

    let heartbeat = coordinator_heartbeat_request(identity, "127.0.0.1:23880", &region, None);
    assert_eq!(heartbeat.region_num, 0);
    assert_eq!(heartbeat.leader_num, 0);
    assert!(heartbeat.leader_region_ids.is_empty());
    assert!(heartbeat.region_stats.is_empty());
}

#[test]
fn non_bootstrap_holt_start_does_not_persist_default_descriptor() {
    let store = HoltMvccStore::open_memory().unwrap();
    let identity = ServerIdentity {
        region_id: 7,
        store_id: 2,
        peer_id: 2,
        bootstrap: false,
    };

    let descriptor = startup_region_descriptor(&store, identity, None).unwrap();

    assert_eq!(descriptor, default_region_descriptor(identity));
    assert!(store.get_region_descriptor(7).unwrap().is_none());
}

#[test]
fn bootstrap_holt_start_persists_default_descriptor() {
    let store = HoltMvccStore::open_memory().unwrap();
    let identity = ServerIdentity {
        region_id: 7,
        store_id: 1,
        peer_id: 1,
        bootstrap: true,
    };

    let descriptor = startup_region_descriptor(&store, identity, None).unwrap();

    assert_eq!(descriptor, default_region_descriptor(identity));
    assert_eq!(store.get_region_descriptor(7).unwrap().unwrap(), descriptor);
}

#[test]
fn recover_holt_hosted_identities_adds_persisted_local_regions() {
    let store = HoltMvccStore::open_memory().unwrap();
    store
        .put_region_descriptor(&metapb::RegionDescriptor {
            region_id: 2,
            start_key: b"m".to_vec(),
            peers: vec![metapb::RegionPeer {
                store_id: 7,
                peer_id: 20,
            }],
            ..Default::default()
        })
        .unwrap();
    store
        .put_region_descriptor(&metapb::RegionDescriptor {
            region_id: 3,
            start_key: b"z".to_vec(),
            peers: vec![metapb::RegionPeer {
                store_id: 8,
                peer_id: 30,
            }],
            ..Default::default()
        })
        .unwrap();

    let identities = recover_holt_hosted_identities(
        &store,
        vec![ServerIdentity {
            region_id: 1,
            store_id: 7,
            peer_id: 10,
            bootstrap: true,
        }],
    )
    .unwrap();

    assert_eq!(
        identities
            .iter()
            .map(|identity| (identity.region_id, identity.store_id, identity.peer_id))
            .collect::<Vec<_>>(),
        vec![(1, 7, 10), (2, 7, 20)]
    );
    assert!(identities[1].bootstrap);
}

#[tokio::test]
async fn coordinator_heartbeat_reports_local_leader_region() {
    let dir = tempfile::tempdir().unwrap();
    let identity = ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    };
    let addr: SocketAddr = "127.0.0.1:23880".parse().unwrap();
    let region = open_openraft_region(
        identity,
        &addr.to_string(),
        dir.path().to_path_buf(),
        AppliedKvEngine::new(identity.region_id, MvccStore::new()),
    )
    .await
    .unwrap();

    let req = coordinator_heartbeat_request(identity, &addr.to_string(), &region, None);

    assert_eq!(req.store_id, 11);
    assert_eq!(req.region_num, 1);
    assert_eq!(req.leader_num, 1);
    assert_eq!(req.leader_region_ids, vec![7]);
    assert_eq!(req.client_addr, "127.0.0.1:23880");
    assert_eq!(req.raft_addr, "127.0.0.1:23880");
    assert_eq!(req.region_stats.len(), 1);
    assert_eq!(req.region_stats[0].region_id, 7);
    assert_eq!(req.region_stats[0].leader_store_id, 11);
    assert!(!req.region_stats[0].pending_admin);
}

#[tokio::test]
async fn coordinator_heartbeat_uses_advertised_dns_addr() {
    let dir = tempfile::tempdir().unwrap();
    let identity = ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    };
    let bind_addr: SocketAddr = "0.0.0.0:20160".parse().unwrap();
    let advertised_addr = "nokv-store-1:20160";
    let region = open_openraft_region(
        identity,
        advertised_addr,
        dir.path().to_path_buf(),
        AppliedKvEngine::new(identity.region_id, MvccStore::new()),
    )
    .await
    .unwrap();

    let req = coordinator_heartbeat_request(identity, advertised_addr, &region, None);

    assert_eq!(local_admin_endpoint(bind_addr), "http://127.0.0.1:20160");
    assert_eq!(req.client_addr, advertised_addr);
    assert_eq!(req.raft_addr, advertised_addr);
}

#[tokio::test]
async fn coordinator_heartbeat_reports_multiple_local_regions_once() {
    let dir = tempfile::tempdir().unwrap();
    let identity1 = ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    };
    let identity2 = ServerIdentity {
        region_id: 8,
        store_id: 11,
        peer_id: 102,
        bootstrap: true,
    };
    let addr: SocketAddr = "127.0.0.1:23880".parse().unwrap();
    let region1 = open_openraft_region(
        identity1,
        &addr.to_string(),
        dir.path().join("region-7"),
        AppliedKvEngine::new(identity1.region_id, MvccStore::new()),
    )
    .await
    .unwrap();
    let region2 = open_openraft_region(
        identity2,
        &addr.to_string(),
        dir.path().join("region-8"),
        AppliedKvEngine::new(identity2.region_id, MvccStore::new()),
    )
    .await
    .unwrap();

    let req = coordinator_heartbeat_request_for_regions(
        11,
        &addr.to_string(),
        &[(identity1, region1), (identity2, region2)],
        None,
    );

    assert_eq!(req.store_id, 11);
    assert_eq!(req.region_num, 2);
    assert_eq!(req.leader_num, 2);
    assert_eq!(req.leader_region_ids, vec![7, 8]);
    assert_eq!(req.region_stats.len(), 2);
    assert_eq!(req.region_stats[0].region_id, 7);
    assert_eq!(req.region_stats[0].leader_store_id, 11);
    assert_eq!(req.region_stats[1].region_id, 8);
    assert_eq!(req.region_stats[1].leader_store_id, 11);
}

#[tokio::test]
async fn coordinator_heartbeat_reads_regions_inserted_after_registry_creation() {
    let dir = tempfile::tempdir().unwrap();
    let identity1 = ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    };
    let identity2 = ServerIdentity {
        region_id: 8,
        store_id: 11,
        peer_id: 102,
        bootstrap: true,
    };
    let addr: SocketAddr = "127.0.0.1:23880".parse().unwrap();
    let region1 = open_openraft_region(
        identity1,
        &addr.to_string(),
        dir.path().join("region-7"),
        AppliedKvEngine::new(identity1.region_id, MvccStore::new()),
    )
    .await
    .unwrap();
    let region2 = open_openraft_region(
        identity2,
        &addr.to_string(),
        dir.path().join("region-8"),
        AppliedKvEngine::new(identity2.region_id, MvccStore::new()),
    )
    .await
    .unwrap();
    let registry = HostedRegionRegistry::new([(identity1, region1)]).unwrap();

    registry.insert(identity2, region2).unwrap();
    let req =
        coordinator_heartbeat_request_for_hosted_regions(11, &addr.to_string(), &registry, None)
            .unwrap();

    assert_eq!(req.region_num, 2);
    assert_eq!(req.leader_num, 2);
    assert_eq!(req.leader_region_ids, vec![7, 8]);
    assert_eq!(req.region_stats.len(), 2);
}

#[tokio::test]
async fn coordinator_heartbeat_marks_pending_admin_for_unpublished_root_events() {
    let dir = tempfile::tempdir().unwrap();
    let identity = ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    };
    let addr: SocketAddr = "127.0.0.1:23880".parse().unwrap();
    let store = HoltMvccStore::open_memory().unwrap();
    store
        .enqueue_pending_root_event(&metapb::RootEvent {
            kind: metapb::RootEventKind::PeerAdded as i32,
            payload: Some(metapb::root_event::Payload::PeerChange(
                metapb::RootPeerChange {
                    region_id: identity.region_id,
                    store_id: 12,
                    peer_id: 102,
                    target: Some(default_region_descriptor(identity)),
                    ..Default::default()
                },
            )),
        })
        .unwrap();
    let region = open_openraft_region(
        identity,
        &addr.to_string(),
        dir.path().to_path_buf(),
        AppliedKvEngine::new(identity.region_id, MvccStore::new()),
    )
    .await
    .unwrap();

    let req = coordinator_heartbeat_request(identity, &addr.to_string(), &region, Some(&store));

    assert_eq!(req.region_stats.len(), 1);
    assert!(req.region_stats[0].pending_admin);
}

#[tokio::test]
async fn coordinator_heartbeat_marks_pending_admin_for_pending_scheduler_operations() {
    let dir = tempfile::tempdir().unwrap();
    let identity = ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    };
    let addr: SocketAddr = "127.0.0.1:23880".parse().unwrap();
    let store = HoltMvccStore::open_memory().unwrap();
    store
        .record_pending_scheduler_operation(&coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
            region_id: identity.region_id,
            split_key: b"m".to_vec(),
            split_child: Some(metapb::RegionDescriptor {
                region_id: 8,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
    let region = open_openraft_region(
        identity,
        &addr.to_string(),
        dir.path().to_path_buf(),
        AppliedKvEngine::new(identity.region_id, MvccStore::new()),
    )
    .await
    .unwrap();

    let req = coordinator_heartbeat_request(identity, &addr.to_string(), &region, Some(&store));

    assert_eq!(req.region_stats.len(), 1);
    assert!(req.region_stats[0].pending_admin);
}

#[tokio::test]
async fn coordinator_heartbeat_marks_pending_admin_for_blocked_scheduler_operations() {
    let dir = tempfile::tempdir().unwrap();
    let identity = ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    };
    let addr: SocketAddr = "127.0.0.1:23880".parse().unwrap();
    let store = HoltMvccStore::open_memory().unwrap();
    store
        .block_pending_scheduler_operation(
            &coordpb::SchedulerOperation {
                r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
                region_id: identity.region_id,
                split_key: b"m".to_vec(),
                split_child: Some(metapb::RegionDescriptor {
                    region_id: 8,
                    ..Default::default()
                }),
                ..Default::default()
            },
            MAX_PENDING_SCHEDULER_OPERATION_ATTEMPTS,
            "attempt limit reached",
        )
        .unwrap();
    let region = open_openraft_region(
        identity,
        &addr.to_string(),
        dir.path().to_path_buf(),
        AppliedKvEngine::new(identity.region_id, MvccStore::new()),
    )
    .await
    .unwrap();

    let req = coordinator_heartbeat_request(identity, &addr.to_string(), &region, Some(&store));

    assert_eq!(req.region_stats.len(), 1);
    assert!(req.region_stats[0].pending_admin);
}

#[test]
fn startup_root_events_publish_store_and_bootstrap_region() {
    let identity = ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    };

    let events = startup_root_events(identity, default_region_descriptor(identity));

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].kind, metapb::RootEventKind::StoreJoined as i32);
    match events[0].payload.as_ref().unwrap() {
        metapb::root_event::Payload::StoreMembership(membership) => {
            assert_eq!(membership.store_id, 11);
        }
        other => panic!("unexpected startup event payload: {other:?}"),
    }
    assert_eq!(
        events[1].kind,
        metapb::RootEventKind::RegionBootstrap as i32
    );
    let descriptor = match events[1].payload.as_ref().unwrap() {
        metapb::root_event::Payload::RegionDescriptor(record) => {
            record.descriptor.as_ref().unwrap()
        }
        other => panic!("unexpected startup event payload: {other:?}"),
    };
    assert_eq!(descriptor.region_id, 7);
    assert_eq!(descriptor.peers[0].store_id, 11);
    assert_eq!(descriptor.peers[0].peer_id, 101);
}

#[test]
fn startup_root_events_for_joining_peer_only_publish_store_membership() {
    let identity = ServerIdentity {
        region_id: 7,
        store_id: 12,
        peer_id: 102,
        bootstrap: false,
    };

    let events = startup_root_events(identity, default_region_descriptor(identity));

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, metapb::RootEventKind::StoreJoined as i32);
    match events[0].payload.as_ref().unwrap() {
        metapb::root_event::Payload::StoreMembership(membership) => {
            assert_eq!(membership.store_id, 12);
        }
        other => panic!("unexpected startup event payload: {other:?}"),
    }
}

#[test]
fn startup_root_events_for_regions_publish_store_once_and_bootstrap_regions() {
    let identity1 = ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    };
    let identity2 = ServerIdentity {
        region_id: 8,
        store_id: 11,
        peer_id: 102,
        bootstrap: true,
    };

    let events = startup_root_events_for_regions(
        &[identity1, identity2],
        &[
            default_region_descriptor(identity1),
            default_region_descriptor(identity2),
        ],
    );

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].kind, metapb::RootEventKind::StoreJoined as i32);
    assert!(matches!(
        events[0].payload.as_ref().unwrap(),
        metapb::root_event::Payload::StoreMembership(membership)
            if membership.store_id == 11
    ));
    let bootstrapped = events[1..]
        .iter()
        .map(|event| match event.payload.as_ref().unwrap() {
            metapb::root_event::Payload::RegionDescriptor(record) => {
                record.descriptor.as_ref().unwrap().region_id
            }
            other => panic!("unexpected startup event payload: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(bootstrapped, vec![7, 8]);
}

#[test]
fn default_region_descriptor_uses_configured_range() {
    let identity = ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    };
    let descriptor = default_region_descriptor_with_range(
        identity,
        Some(&RegionKeyRange {
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
        }),
    );

    assert_eq!(descriptor.start_key, b"a");
    assert_eq!(descriptor.end_key, b"z");
}

#[test]
fn build_merge_descriptor_extends_target_to_right_sibling() {
    let target = default_region_descriptor_with_range(
        ServerIdentity {
            region_id: 1,
            store_id: 11,
            peer_id: 101,
            bootstrap: true,
        },
        Some(&RegionKeyRange {
            start_key: b"a".to_vec(),
            end_key: b"m".to_vec(),
        }),
    );
    let source = default_region_descriptor_with_range(
        ServerIdentity {
            region_id: 2,
            store_id: 11,
            peer_id: 102,
            bootstrap: true,
        },
        Some(&RegionKeyRange {
            start_key: b"m".to_vec(),
            end_key: b"z".to_vec(),
        }),
    );

    let merged = build_merge_descriptor(&target, &source).unwrap();

    assert_eq!(merged.region_id, 1);
    assert_eq!(merged.start_key, b"a");
    assert_eq!(merged.end_key, b"z");
    assert_eq!(merged.epoch.unwrap().version, 2);
    assert_eq!(merged.lineage.len(), 1);
    assert_eq!(merged.lineage[0].region_id, 2);
    assert_eq!(
        merged.lineage[0].kind,
        metapb::DescriptorLineageKind::MergeSource as i32
    );
    assert!(merge_source_already_absorbed(&merged, 2));
    assert!(!merge_source_already_absorbed(&target, 2));
    assert_eq!(merge_region_ids(&target, &source), (1, 2));
}

#[test]
fn build_merge_descriptor_rejects_non_right_sibling_source() {
    let target = default_region_descriptor_with_range(
        ServerIdentity {
            region_id: 1,
            store_id: 11,
            peer_id: 101,
            bootstrap: true,
        },
        Some(&RegionKeyRange {
            start_key: b"m".to_vec(),
            end_key: b"z".to_vec(),
        }),
    );
    let source = default_region_descriptor_with_range(
        ServerIdentity {
            region_id: 2,
            store_id: 11,
            peer_id: 102,
            bootstrap: true,
        },
        Some(&RegionKeyRange {
            start_key: b"a".to_vec(),
            end_key: b"m".to_vec(),
        }),
    );

    let err = build_merge_descriptor(&target, &source).unwrap_err();

    assert_eq!(err.code(), tonic::Code::Unimplemented);
}

#[test]
fn merge_store_coverage_accepts_matching_store_sets() {
    let mut target = default_region_descriptor(ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    });
    target.peers.push(metapb::RegionPeer {
        store_id: 12,
        peer_id: 102,
    });
    let mut source = default_region_descriptor(ServerIdentity {
        region_id: 8,
        store_id: 11,
        peer_id: 201,
        bootstrap: true,
    });
    source.peers.push(metapb::RegionPeer {
        store_id: 12,
        peer_id: 202,
    });

    ensure_merge_store_coverage(&target, &source).unwrap();
}

#[test]
fn merge_store_coverage_rejects_mismatched_store_sets() {
    let mut target = default_region_descriptor(ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    });
    target.peers.push(metapb::RegionPeer {
        store_id: 12,
        peer_id: 102,
    });
    let source = default_region_descriptor(ServerIdentity {
        region_id: 8,
        store_id: 11,
        peer_id: 201,
        bootstrap: true,
    });

    let err = ensure_merge_store_coverage(&target, &source).unwrap_err();

    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert!(err.message().contains("must cover the same store set"));
}

#[test]
fn merged_source_region_ids_are_scoped_to_local_target_peers() {
    let mut merged = default_region_descriptor(ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    });
    merged.peers.push(metapb::RegionPeer {
        store_id: 12,
        peer_id: 102,
    });
    merged.lineage.push(metapb::DescriptorLineageRef {
        region_id: 8,
        kind: metapb::DescriptorLineageKind::MergeSource as i32,
        ..Default::default()
    });
    let source = default_region_descriptor(ServerIdentity {
        region_id: 8,
        store_id: 12,
        peer_id: 202,
        bootstrap: true,
    });

    let local = merged_source_region_ids_for_store(&[merged, source], 12);
    let unrelated = merged_source_region_ids_for_store(&[], 13);

    assert!(local.contains(&8));
    assert!(unrelated.is_empty());
}

#[test]
fn descriptor_membership_nodes_use_local_addr_and_configured_remote_endpoints() {
    let mut descriptor = default_region_descriptor(ServerIdentity {
        region_id: 7,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    });
    descriptor.peers.push(metapb::RegionPeer {
        store_id: 12,
        peer_id: 102,
    });
    let peer_endpoints = PeerEndpointCatalog::require_configured();
    peer_endpoints
        .insert_peer(102, "http://127.0.0.1:30202")
        .unwrap();
    let local_peer = descriptor.peers[0].clone();

    let members =
        descriptor_membership_nodes(&descriptor, &local_peer, "127.0.0.1:30101", &peer_endpoints)
            .unwrap();

    assert_eq!(members.len(), 2);
    assert_eq!(members.get(&101).unwrap().addr, "127.0.0.1:30101");
    assert_eq!(members.get(&102).unwrap().addr, "http://127.0.0.1:30202");
}

#[test]
fn recover_holt_hosted_identities_marks_multi_peer_descriptor_non_bootstrap() {
    let store = HoltMvccStore::open_memory().unwrap();
    store
        .put_region_descriptor(&metapb::RegionDescriptor {
            region_id: 2,
            peers: vec![
                metapb::RegionPeer {
                    store_id: 7,
                    peer_id: 20,
                },
                metapb::RegionPeer {
                    store_id: 8,
                    peer_id: 30,
                },
            ],
            ..default_region_descriptor(ServerIdentity {
                region_id: 2,
                store_id: 7,
                peer_id: 20,
                bootstrap: true,
            })
        })
        .unwrap();

    let identities = recover_holt_hosted_identities(
        &store,
        vec![ServerIdentity {
            region_id: 1,
            store_id: 7,
            peer_id: 10,
            bootstrap: true,
        }],
    )
    .unwrap();

    assert_eq!(identities.len(), 2);
    assert_eq!(identities[1].region_id, 2);
    assert_eq!(identities[1].peer_id, 20);
    assert!(!identities[1].bootstrap);
}

#[test]
fn recover_holt_hosted_identities_skips_merged_source_descriptor() {
    let store = HoltMvccStore::open_memory().unwrap();
    let mut merged = default_region_descriptor(ServerIdentity {
        region_id: 1,
        store_id: 7,
        peer_id: 10,
        bootstrap: true,
    });
    merged.lineage.push(metapb::DescriptorLineageRef {
        region_id: 2,
        kind: metapb::DescriptorLineageKind::MergeSource as i32,
        ..Default::default()
    });
    store.put_region_descriptor(&merged).unwrap();
    store
        .put_region_descriptor(&metapb::RegionDescriptor {
            region_id: 2,
            start_key: b"m".to_vec(),
            peers: vec![metapb::RegionPeer {
                store_id: 7,
                peer_id: 20,
            }],
            ..Default::default()
        })
        .unwrap();

    let identities = recover_holt_hosted_identities(
        &store,
        vec![ServerIdentity {
            region_id: 1,
            store_id: 7,
            peer_id: 10,
            bootstrap: true,
        }],
    )
    .unwrap();

    assert_eq!(
        identities
            .iter()
            .map(|identity| identity.region_id)
            .collect::<Vec<_>>(),
        vec![1]
    );
}

#[test]
fn recovered_descriptor_membership_init_only_runs_on_first_local_peer() {
    let descriptor = metapb::RegionDescriptor {
        region_id: 2,
        peers: vec![
            metapb::RegionPeer {
                store_id: 7,
                peer_id: 20,
            },
            metapb::RegionPeer {
                store_id: 8,
                peer_id: 30,
            },
        ],
        ..default_region_descriptor(ServerIdentity {
            region_id: 2,
            store_id: 7,
            peer_id: 20,
            bootstrap: true,
        })
    };
    let peer_endpoints = PeerEndpointCatalog::require_configured();
    peer_endpoints
        .insert_peer(30, "http://127.0.0.1:30303")
        .unwrap();

    let first = recovered_descriptor_membership_init(
        &descriptor,
        ServerIdentity {
            region_id: 2,
            store_id: 7,
            peer_id: 20,
            bootstrap: false,
        },
        "127.0.0.1:30202",
        &peer_endpoints,
    )
    .unwrap();
    assert!(first.is_some());

    let second = recovered_descriptor_membership_init(
        &descriptor,
        ServerIdentity {
            region_id: 2,
            store_id: 8,
            peer_id: 30,
            bootstrap: false,
        },
        "127.0.0.1:30303",
        &peer_endpoints,
    )
    .unwrap();
    assert!(second.is_none());
}

#[tokio::test]
async fn split_child_multi_peer_bootstrap_does_not_block_on_immediate_quorum() {
    let dir = tempfile::tempdir().unwrap();
    let mvcc = HoltMvccStore::open_memory().unwrap();
    let peer_endpoints = PeerEndpointCatalog::require_configured();
    peer_endpoints
        .insert_peer(202, "http://127.0.0.1:30202")
        .unwrap();
    let store_services = MultiRegionStoreKvService::new([]).unwrap();
    let admin_services = MultiRegionRaftAdminService::new([]).unwrap();
    let hosted_regions = HostedRegionRegistry::new([]).unwrap();
    let controller = HoltRangeController {
        store_id: 11,
        advertised_addr: "127.0.0.1:30101".to_owned(),
        persistent_root: dir.path().to_path_buf(),
        coordinator: None,
        mvcc: mvcc.clone(),
        transport: nokv_raftnode::TonicRaftTransportRegistry::default(),
        store_services,
        admin_services,
        hosted_regions: hosted_regions.clone(),
        peer_endpoints: peer_endpoints.clone(),
        topology_publisher: Arc::new(EmptyTopologyPublisher),
    };
    let identity = ServerIdentity {
        region_id: 8,
        store_id: 11,
        peer_id: 101,
        bootstrap: false,
    };
    let mut descriptor = default_region_descriptor_with_range(
        identity,
        Some(&RegionKeyRange {
            start_key: b"m".to_vec(),
            end_key: Vec::new(),
        }),
    );
    descriptor.peers.push(metapb::RegionPeer {
        store_id: 12,
        peer_id: 202,
    });
    let members = controller
        .child_membership_init(&descriptor, &descriptor.peers[0], true)
        .unwrap()
        .unwrap();

    let result = tokio::time::timeout(
        Duration::from_millis(500),
        controller.open_split_child_region(identity, descriptor.clone(), Some(members)),
    )
    .await;

    assert!(
        result.is_ok(),
        "multi-peer child bootstrap should not wait for quorum election"
    );
    result.unwrap().unwrap();
    assert!(hosted_regions.get(8).unwrap().is_some());
    assert_eq!(
        mvcc.get_region_descriptor(8).unwrap().unwrap().peers.len(),
        2
    );
}

#[test]
fn merge_root_event_carries_merged_descriptor() {
    let merged = default_region_descriptor(ServerIdentity {
        region_id: 1,
        store_id: 11,
        peer_id: 101,
        bootstrap: true,
    });

    let event = merge_root_event(metapb::RootEventKind::RegionMerged, 1, 2, &merged);

    assert_eq!(event.kind, metapb::RootEventKind::RegionMerged as i32);
    let Some(metapb::root_event::Payload::RangeMerge(merge)) = event.payload else {
        panic!("merge event should carry a range merge payload");
    };
    assert_eq!(merge.left_region_id, 1);
    assert_eq!(merge.right_region_id, 2);
    assert_eq!(merge.merged.unwrap().region_id, 1);
}

#[test]
fn region_log_dir_isolates_multi_region_logs() {
    assert_eq!(
        region_log_dir(PathBuf::from("/tmp/nokv-raftlog"), 7, false),
        PathBuf::from("/tmp/nokv-raftlog")
    );
    assert_eq!(
        region_log_dir(PathBuf::from("/tmp/nokv-raftlog"), 7, true),
        PathBuf::from("/tmp/nokv-raftlog/region-7")
    );
}

fn reserve_loopback_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn wait_for_server(addr: SocketAddr) {
    let endpoint = local_admin_endpoint(addr);
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
    panic!("rust raftstore test server at {addr} did not become ready");
}
