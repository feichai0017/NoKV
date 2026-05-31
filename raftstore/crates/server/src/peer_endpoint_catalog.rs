//! Peer endpoint catalog used by raft membership administration.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use nokv_raftnode::BasicNode;
use tonic::Status;

#[derive(Debug, Clone, Default)]
pub struct PeerEndpointCatalog {
    endpoints: Arc<Mutex<BTreeMap<u64, String>>>,
    require_configured: bool,
}

impl PeerEndpointCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn require_configured() -> Self {
        Self {
            endpoints: Arc::new(Mutex::new(BTreeMap::new())),
            require_configured: true,
        }
    }

    pub fn insert_peer(&self, peer_id: u64, endpoint: impl Into<String>) -> Result<(), Status> {
        if peer_id == 0 {
            return Err(Status::invalid_argument("peer_id is required"));
        }
        let endpoint = endpoint.into();
        if endpoint.is_empty() {
            return Err(Status::invalid_argument("peer endpoint is required"));
        }
        self.endpoints
            .lock()
            .map_err(|_| peer_endpoint_catalog_poisoned())?
            .insert(peer_id, endpoint);
        Ok(())
    }

    pub fn node_for_peer(&self, store_id: u64, peer_id: u64) -> Result<BasicNode, Status> {
        let endpoints = self
            .endpoints
            .lock()
            .map_err(|_| peer_endpoint_catalog_poisoned())?;
        if let Some(endpoint) = endpoints.get(&peer_id) {
            return Ok(BasicNode::new(endpoint.clone()));
        }
        if self.require_configured {
            return Err(Status::failed_precondition(format!(
                "endpoint for store {store_id} peer {peer_id} is not configured"
            )));
        }
        Ok(BasicNode::new(format!("store-{store_id}-peer-{peer_id}")))
    }

    pub(crate) fn endpoint_for_peer(&self, peer_id: u64) -> Result<Option<String>, Status> {
        self.endpoints
            .lock()
            .map_err(|_| peer_endpoint_catalog_poisoned())
            .map(|endpoints| endpoints.get(&peer_id).cloned())
    }
}

fn peer_endpoint_catalog_poisoned() -> Status {
    Status::internal("peer endpoint catalog mutex poisoned")
}
