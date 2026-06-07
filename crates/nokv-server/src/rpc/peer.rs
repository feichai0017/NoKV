use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpStream};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use nokv_cluster::{MetadataRaftError, MetadataRaftRpcClient};
use nokv_protocol::{
    MetadataRpcEnvelope, MetadataRpcRequest, MetadataRpcResult,
    WireMetadataRaftAppendEntriesRequest, WireMetadataRaftAppendEntriesResponse,
    WireMetadataRaftInstallSnapshotRequest, WireMetadataRaftInstallSnapshotResponse,
    WireMetadataRaftVoteRequest, WireMetadataRaftVoteResponse,
};

use super::transport::{call_framed_rpc_on_stream, open_framed_rpc_stream};
use crate::server::ServerError;

pub(crate) struct FramedRpcClient {
    address: SocketAddr,
    next_request_id: AtomicU64,
    stream: Mutex<Option<TcpStream>>,
}

#[derive(Clone, Default)]
pub(crate) struct MetadataRaftFramedRpcClient {
    peers: Arc<Mutex<BTreeMap<SocketAddr, Arc<FramedRpcClient>>>>,
}

impl FramedRpcClient {
    pub(crate) fn new(address: SocketAddr) -> Self {
        Self {
            address,
            next_request_id: AtomicU64::new(1),
            stream: Mutex::new(None),
        }
    }

    pub(crate) fn call(
        &self,
        request: &MetadataRpcRequest,
    ) -> Result<MetadataRpcEnvelope, ServerError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let mut stream = self.stream.lock().map_err(|_| {
            ServerError::Io(std::io::Error::other("metadata peer rpc mutex poisoned"))
        })?;
        if stream.is_none() {
            *stream = Some(open_framed_rpc_stream(self.address)?);
        }
        let result = call_framed_rpc_on_stream(
            stream.as_mut().expect("framed rpc stream was initialized"),
            request_id,
            request,
        );
        if result.is_err() {
            *stream = None;
        }
        result
    }
}

impl MetadataRaftFramedRpcClient {
    fn call_peer(
        &self,
        address: &str,
        request: &MetadataRpcRequest,
    ) -> Result<MetadataRpcResult, MetadataRaftError> {
        let address = address.parse::<SocketAddr>().map_err(|err| {
            MetadataRaftError::Backend(format!("metadata raft peer address {address:?}: {err}"))
        })?;
        let client = {
            let mut peers = self.peers.lock().map_err(|_| {
                MetadataRaftError::Backend("metadata raft peer client cache poisoned".to_owned())
            })?;
            Arc::clone(
                peers
                    .entry(address)
                    .or_insert_with(|| Arc::new(FramedRpcClient::new(address))),
            )
        };
        let envelope = client
            .call(request)
            .map_err(|err| MetadataRaftError::Backend(format!("metadata raft peer rpc: {err}")))?;
        if !envelope.ok {
            return Err(MetadataRaftError::Backend(format!(
                "metadata raft peer rejected rpc: {}",
                envelope.error.unwrap_or_else(|| "unknown error".to_owned())
            )));
        }
        envelope.result.ok_or_else(|| {
            MetadataRaftError::Backend("metadata raft peer returned no result".to_owned())
        })
    }
}

impl MetadataRaftRpcClient for MetadataRaftFramedRpcClient {
    fn vote_metadata_raft(
        &self,
        _target: u64,
        address: &str,
        request: WireMetadataRaftVoteRequest,
    ) -> Result<WireMetadataRaftVoteResponse, MetadataRaftError> {
        match self.call_peer(address, &MetadataRpcRequest::MetadataRaftVote { request })? {
            MetadataRpcResult::MetadataRaftVote { response } => Ok(response),
            other => Err(MetadataRaftError::Backend(format!(
                "metadata raft vote returned unexpected result: {other:?}"
            ))),
        }
    }

    fn append_metadata_raft_entries(
        &self,
        _target: u64,
        address: &str,
        request: WireMetadataRaftAppendEntriesRequest,
    ) -> Result<WireMetadataRaftAppendEntriesResponse, MetadataRaftError> {
        match self.call_peer(
            address,
            &MetadataRpcRequest::MetadataRaftAppendEntries { request },
        )? {
            MetadataRpcResult::MetadataRaftAppendEntries { response } => Ok(response),
            other => Err(MetadataRaftError::Backend(format!(
                "metadata raft append entries returned unexpected result: {other:?}"
            ))),
        }
    }

    fn install_metadata_raft_snapshot(
        &self,
        _target: u64,
        address: &str,
        request: WireMetadataRaftInstallSnapshotRequest,
    ) -> Result<WireMetadataRaftInstallSnapshotResponse, MetadataRaftError> {
        match self.call_peer(
            address,
            &MetadataRpcRequest::MetadataRaftInstallSnapshot { request },
        )? {
            MetadataRpcResult::MetadataRaftInstallSnapshot { response } => Ok(response),
            other => Err(MetadataRaftError::Backend(format!(
                "metadata raft install snapshot returned unexpected result: {other:?}"
            ))),
        }
    }
}
