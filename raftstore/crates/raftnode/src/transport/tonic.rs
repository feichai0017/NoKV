use std::collections::BTreeMap;
use std::error::Error as StdErrorTrait;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use openraft::{
    error::{RPCError, RaftError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    BasicNode, Raft,
};
use prost::Message;
use tonic::codegen::*;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::metrics;
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

    pub fn unregister(&self, region_id: RegionId) {
        self.peers
            .lock()
            .expect("tonic raft transport registry poisoned")
            .remove(&region_id);
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
        let entries = rpc.entries.len() as u64;
        let started = Instant::now();
        let response = self
            .target_raft(region_id)?
            .append_entries(rpc)
            .await
            .map_err(openraft_status)?;
        metrics::record_append_entries_server(entries, started.elapsed());
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
        let entries = rpc.entries.len() as u64;
        let payload = encode_append_entries_request(self.region_id, &rpc).map_err(rpc_error)?;
        let started = Instant::now();
        let response = self
            .call(APPEND_ENTRIES_PATH, "AppendEntries", payload)
            .await;
        metrics::record_append_entries_client(entries, started.elapsed(), response.is_ok());
        let response = response.map_err(rpc_error)?;
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
#[path = "test.rs"]
mod test;
