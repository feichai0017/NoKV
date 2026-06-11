use std::net::{SocketAddr, TcpStream};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

use nokv_protocol::{MetadataRpcEnvelope, MetadataRpcRequest};

use super::transport::{call_framed_rpc_on_stream, open_framed_rpc_stream};
use crate::server::ServerError;

pub(crate) struct FramedRpcClient {
    address: SocketAddr,
    next_request_id: AtomicU64,
    stream: Mutex<Option<TcpStream>>,
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
