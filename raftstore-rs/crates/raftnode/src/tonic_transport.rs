use std::collections::BTreeMap;
use std::error::Error as StdErrorTrait;
use std::sync::{Arc, Mutex};

use openraft::{
    error::{RPCError, RaftError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    BasicNode, Raft,
};
use prost::Message;
use tonic::codegen::*;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::{
    decode_append_entries_request, decode_append_entries_response, decode_install_snapshot_request,
    decode_install_snapshot_response, decode_vote_request, decode_vote_response,
    encode_append_entries_request, encode_append_entries_response, encode_install_snapshot_request,
    encode_install_snapshot_response, encode_vote_request, encode_vote_response, Error, NodeId,
    RaftStoreConfig, RegionId,
};

const SERVICE_NAME: &str = "nokv.raftstore.internal.v1.RaftTransport";
const APPEND_ENTRIES_PATH: &str = "/nokv.raftstore.internal.v1.RaftTransport/AppendEntries";
const INSTALL_SNAPSHOT_PATH: &str = "/nokv.raftstore.internal.v1.RaftTransport/InstallSnapshot";
const VOTE_PATH: &str = "/nokv.raftstore.internal.v1.RaftTransport/Vote";

#[derive(Clone, PartialEq, Message)]
pub struct RaftTransportRequest {
    #[prost(uint64, tag = "1")]
    pub region_id: RegionId,
    #[prost(bytes, tag = "2")]
    pub payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct RaftTransportResponse {
    #[prost(bytes, tag = "1")]
    pub payload: Vec<u8>,
}

#[tonic::async_trait]
pub trait RaftTransport: Send + Sync + 'static {
    async fn append_entries(
        &self,
        request: Request<RaftTransportRequest>,
    ) -> Result<Response<RaftTransportResponse>, Status>;

    async fn install_snapshot(
        &self,
        request: Request<RaftTransportRequest>,
    ) -> Result<Response<RaftTransportResponse>, Status>;

    async fn vote(
        &self,
        request: Request<RaftTransportRequest>,
    ) -> Result<Response<RaftTransportResponse>, Status>;
}

#[derive(Clone, Default)]
pub struct TonicRaftTransportRegistry {
    peers: Arc<Mutex<BTreeMap<RegionId, Raft<RaftStoreConfig>>>>,
}

impl TonicRaftTransportRegistry {
    pub fn register(&self, region_id: RegionId, raft: Raft<RaftStoreConfig>) {
        self.peers
            .lock()
            .expect("tonic raft transport registry poisoned")
            .insert(region_id, raft);
    }

    pub fn service(&self) -> TonicRaftTransportService {
        TonicRaftTransportService {
            peers: self.peers.clone(),
        }
    }
}

#[derive(Clone)]
pub struct TonicRaftTransportService {
    peers: Arc<Mutex<BTreeMap<RegionId, Raft<RaftStoreConfig>>>>,
}

impl TonicRaftTransportService {
    fn target_raft(&self, region_id: RegionId) -> Result<Raft<RaftStoreConfig>, Status> {
        self.peers
            .lock()
            .expect("tonic raft transport registry poisoned")
            .get(&region_id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("raft region {region_id} is not hosted")))
    }
}

#[tonic::async_trait]
impl RaftTransport for TonicRaftTransportService {
    async fn append_entries(
        &self,
        request: Request<RaftTransportRequest>,
    ) -> Result<Response<RaftTransportResponse>, Status> {
        let request = request.into_inner();
        let (region_id, rpc) =
            decode_append_entries_request(&request.payload).map_err(invalid_transport_status)?;
        ensure_region_match(request.region_id, region_id)?;
        let response = self
            .target_raft(region_id)?
            .append_entries(rpc)
            .await
            .map_err(openraft_status)?;
        Ok(Response::new(RaftTransportResponse {
            payload: encode_append_entries_response(&response).map_err(invalid_transport_status)?,
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftTransportRequest>,
    ) -> Result<Response<RaftTransportResponse>, Status> {
        let request = request.into_inner();
        let (region_id, rpc) =
            decode_install_snapshot_request(&request.payload).map_err(invalid_transport_status)?;
        ensure_region_match(request.region_id, region_id)?;
        let response = self
            .target_raft(region_id)?
            .install_snapshot(rpc)
            .await
            .map_err(openraft_status)?;
        Ok(Response::new(RaftTransportResponse {
            payload: encode_install_snapshot_response(&response)
                .map_err(invalid_transport_status)?,
        }))
    }

    async fn vote(
        &self,
        request: Request<RaftTransportRequest>,
    ) -> Result<Response<RaftTransportResponse>, Status> {
        let request = request.into_inner();
        let rpc = decode_vote_request(&request.payload).map_err(invalid_transport_status)?;
        let response = self
            .target_raft(request.region_id)?
            .vote(rpc)
            .await
            .map_err(openraft_status)?;
        Ok(Response::new(RaftTransportResponse {
            payload: encode_vote_response(&response).map_err(invalid_transport_status)?,
        }))
    }
}

#[derive(Debug)]
pub struct RaftTransportServer<T> {
    inner: Arc<T>,
}

impl<T> RaftTransportServer<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<T, B> tonic::codegen::Service<http::Request<B>> for RaftTransportServer<T>
where
    T: RaftTransport,
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        match req.uri().path() {
            APPEND_ENTRIES_PATH => {
                struct AppendEntriesSvc<T: RaftTransport>(Arc<T>);
                impl<T: RaftTransport> tonic::server::UnaryService<RaftTransportRequest> for AppendEntriesSvc<T> {
                    type Response = RaftTransportResponse;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<RaftTransportRequest>) -> Self::Future {
                        let inner = self.0.clone();
                        Box::pin(async move { inner.append_entries(request).await })
                    }
                }
                serve_unary(req, self.inner.clone(), AppendEntriesSvc)
            }
            INSTALL_SNAPSHOT_PATH => {
                struct InstallSnapshotSvc<T: RaftTransport>(Arc<T>);
                impl<T: RaftTransport> tonic::server::UnaryService<RaftTransportRequest> for InstallSnapshotSvc<T> {
                    type Response = RaftTransportResponse;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<RaftTransportRequest>) -> Self::Future {
                        let inner = self.0.clone();
                        Box::pin(async move { inner.install_snapshot(request).await })
                    }
                }
                serve_unary(req, self.inner.clone(), InstallSnapshotSvc)
            }
            VOTE_PATH => {
                struct VoteSvc<T: RaftTransport>(Arc<T>);
                impl<T: RaftTransport> tonic::server::UnaryService<RaftTransportRequest> for VoteSvc<T> {
                    type Response = RaftTransportResponse;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<RaftTransportRequest>) -> Self::Future {
                        let inner = self.0.clone();
                        Box::pin(async move { inner.vote(request).await })
                    }
                }
                serve_unary(req, self.inner.clone(), VoteSvc)
            }
            _ => Box::pin(async move {
                let mut response = http::Response::new(empty_body());
                let headers = response.headers_mut();
                headers.insert(
                    tonic::Status::GRPC_STATUS,
                    (tonic::Code::Unimplemented as i32).into(),
                );
                headers.insert(
                    http::header::CONTENT_TYPE,
                    tonic::metadata::GRPC_CONTENT_TYPE,
                );
                Ok(response)
            }),
        }
    }
}

impl<T> Clone for RaftTransportServer<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> tonic::server::NamedService for RaftTransportServer<T> {
    const NAME: &'static str = SERVICE_NAME;
}

#[derive(Debug, Clone)]
pub struct TonicRaftNetworkFactory {
    region_id: RegionId,
    channels: Arc<Mutex<BTreeMap<NodeId, CachedRaftChannel>>>,
}

#[derive(Debug, Clone)]
struct CachedRaftChannel {
    endpoint: String,
    channel: Channel,
}

impl TonicRaftNetworkFactory {
    pub fn new(region_id: RegionId) -> Self {
        Self {
            region_id,
            channels: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl RaftNetworkFactory<RaftStoreConfig> for TonicRaftNetworkFactory {
    type Network = TonicRaftNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        TonicRaftNetwork {
            region_id: self.region_id,
            target,
            endpoint: node.addr.clone(),
            channels: self.channels.clone(),
        }
    }
}

pub struct TonicRaftNetwork {
    region_id: RegionId,
    target: NodeId,
    endpoint: String,
    channels: Arc<Mutex<BTreeMap<NodeId, CachedRaftChannel>>>,
}

impl RaftNetwork<RaftStoreConfig> for TonicRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<RaftStoreConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        let payload = encode_append_entries_request(self.region_id, &rpc).map_err(rpc_error)?;
        let response = self
            .call(APPEND_ENTRIES_PATH, "AppendEntries", payload)
            .await
            .map_err(rpc_error)?;
        decode_append_entries_response(&response.payload).map_err(rpc_error)
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<RaftStoreConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        let payload = encode_install_snapshot_request(self.region_id, &rpc).map_err(rpc_error)?;
        let response = self
            .call(INSTALL_SNAPSHOT_PATH, "InstallSnapshot", payload)
            .await
            .map_err(rpc_error)?;
        decode_install_snapshot_response(&response.payload).map_err(rpc_error)
    }

    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    {
        let payload = encode_vote_request(&rpc).map_err(rpc_error)?;
        let response = self
            .call(VOTE_PATH, "Vote", payload)
            .await
            .map_err(rpc_error)?;
        decode_vote_response(&response.payload).map_err(rpc_error)
    }
}

impl TonicRaftNetwork {
    async fn call(
        &self,
        path: &'static str,
        method: &'static str,
        payload: Vec<u8>,
    ) -> Result<RaftTransportResponse, TransportCallError> {
        let channel = self.channel().await?;
        let mut client = tonic::client::Grpc::new(channel);
        client
            .ready()
            .await
            .map_err(|err| TransportCallError::ServiceNotReady {
                target: self.target,
                detail: err.to_string(),
            })?;
        let mut request = tonic::Request::new(RaftTransportRequest {
            region_id: self.region_id,
            payload,
        });
        request
            .extensions_mut()
            .insert(tonic::GrpcMethod::new(SERVICE_NAME, method));
        let response = client
            .unary(
                request,
                http::uri::PathAndQuery::from_static(path),
                tonic::codec::ProstCodec::default(),
            )
            .await
            .map_err(TransportCallError::Status)?;
        Ok(response.into_inner())
    }

    async fn channel(&self) -> Result<Channel, TransportCallError> {
        let endpoint = normalize_endpoint(&self.endpoint);
        if let Some(channel) = self.cached_channel(&endpoint)? {
            return Ok(channel);
        }
        let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())
            .map_err(|err| TransportCallError::InvalidEndpoint {
                target: self.target,
                detail: err.to_string(),
            })?
            .connect()
            .await
            .map_err(|source| TransportCallError::Transport {
                target: self.target,
                source,
            })?;
        self.store_channel(endpoint, channel.clone())?;
        Ok(channel)
    }

    fn cached_channel(&self, endpoint: &str) -> Result<Option<Channel>, TransportCallError> {
        let channels = self
            .channels
            .lock()
            .map_err(|_| TransportCallError::ChannelCachePoisoned)?;
        Ok(channels
            .get(&self.target)
            .filter(|cached| cached.endpoint == endpoint)
            .map(|cached| cached.channel.clone()))
    }

    fn store_channel(&self, endpoint: String, channel: Channel) -> Result<(), TransportCallError> {
        self.channels
            .lock()
            .map_err(|_| TransportCallError::ChannelCachePoisoned)?
            .insert(self.target, CachedRaftChannel { endpoint, channel });
        Ok(())
    }
}

fn serve_unary<T, B, S>(
    req: http::Request<B>,
    inner: Arc<T>,
    service: fn(Arc<T>) -> S,
) -> BoxFuture<http::Response<tonic::body::BoxBody>, std::convert::Infallible>
where
    T: RaftTransport,
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
    S: tonic::server::UnaryService<RaftTransportRequest, Response = RaftTransportResponse>
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    Box::pin(async move {
        let codec = tonic::codec::ProstCodec::default();
        let mut grpc = tonic::server::Grpc::new(codec);
        let response = grpc.unary(service(inner), req).await;
        Ok(response)
    })
}

fn ensure_region_match(outer: RegionId, inner: RegionId) -> Result<(), Status> {
    if outer != inner {
        return Err(Status::invalid_argument(format!(
            "raft transport region {outer} does not match payload region {inner}",
        )));
    }
    Ok(())
}

fn invalid_transport_status(err: Error) -> Status {
    Status::invalid_argument(err.to_string())
}

fn openraft_status<E>(err: RaftError<NodeId, E>) -> Status
where
    E: StdErrorTrait,
{
    Status::failed_precondition(err.to_string())
}

fn rpc_error<E, S>(err: S) -> RPCError<NodeId, BasicNode, E>
where
    E: StdErrorTrait,
    S: StdErrorTrait + 'static,
{
    RPCError::Unreachable(Unreachable::new(&err))
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    }
}

#[derive(Debug, thiserror::Error)]
enum TransportCallError {
    #[error("invalid raft transport endpoint for node {target}: {detail}")]
    InvalidEndpoint { target: NodeId, detail: String },
    #[error("raft transport error for node {target}: {source}")]
    Transport {
        target: NodeId,
        source: tonic::transport::Error,
    },
    #[error("raft transport status: {0}")]
    Status(tonic::Status),
    #[error("raft transport for node {target} not ready: {detail}")]
    ServiceNotReady { target: NodeId, detail: String },
    #[error("raft transport channel cache poisoned")]
    ChannelCachePoisoned,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::time::Duration;

    use nokv_mvcc::{KvEngine, MvccStore};
    use nokv_proto::nokv::kv::v1 as kvpb;
    use nokv_proto::nokv::raft::v1 as raftpb;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::{
        AppliedKvEngine, ApplyStatusProvider, OpenRaftRegion, Proposal, RaftCommandExecutor,
        RegionLogStorage, RegionStateMachine, SegmentedEntryLog,
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
            let engine = AppliedKvEngine::new(7, MvccStore::new());
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

        let command = raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id: 7,
                request_id: 1,
                ..Default::default()
            }),
            requests: vec![raftpb::Request {
                cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                cmd: Some(raftpb::request::Cmd::TryAtomicMutate(
                    kvpb::TryAtomicMutateRequest {
                        mutations: vec![kvpb::Mutation {
                            key: b"tonic-network".to_vec(),
                            value: b"replicated".to_vec(),
                            op: kvpb::mutation::Op::Put as i32,
                            ..Default::default()
                        }],
                        commit_version: 10,
                        ..Default::default()
                    },
                )),
            }],
        };
        regions[0].execute_raft_command(&command).await.unwrap();
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
                .get(&kvpb::GetRequest {
                    key: b"tonic-network".to_vec(),
                    version: 10,
                })
                .unwrap();
            assert_eq!(response.value, b"replicated".to_vec());
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
        let leader_engine = AppliedKvEngine::new(7, MvccStore::new());
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
        for version in 1..=8 {
            let command = raftpb::RaftCmdRequest {
                header: Some(raftpb::CmdHeader {
                    region_id: 7,
                    request_id: version,
                    ..Default::default()
                }),
                requests: vec![raftpb::Request {
                    cmd_type: raftpb::CmdType::CmdTryAtomicMutate as i32,
                    cmd: Some(raftpb::request::Cmd::TryAtomicMutate(
                        kvpb::TryAtomicMutateRequest {
                            mutations: vec![kvpb::Mutation {
                                key: format!("k{version}").into_bytes(),
                                value: format!("v{version}").into_bytes(),
                                op: kvpb::mutation::Op::Put as i32,
                                ..Default::default()
                            }],
                            commit_version: version,
                            ..Default::default()
                        },
                    )),
                }],
            };
            last_applied = Some(
                leader
                    .propose(Proposal::from_raft_command(&command).unwrap())
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
        let joining_engine = AppliedKvEngine::new(7, MvccStore::new());
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
            .get(&kvpb::GetRequest {
                key: b"k8".to_vec(),
                version: 8,
            })
            .unwrap();
        assert_eq!(response.value, b"v8".to_vec());

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
}
