use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::sync::{Arc, Mutex};

use openraft::{
    error::{RPCError, RaftError, RemoteError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    BasicNode, Raft,
};

use crate::{NodeId, RaftStoreConfig};

#[derive(Debug, thiserror::Error)]
#[error("raft node {0} is not registered in memory network")]
struct MemoryNetworkMissingNode(NodeId);

#[derive(Clone, Default)]
pub struct MemoryRaftNetworkRegistry {
    peers: Arc<Mutex<BTreeMap<NodeId, Raft<RaftStoreConfig>>>>,
}

impl MemoryRaftNetworkRegistry {
    pub fn factory(&self) -> MemoryRaftNetworkFactory {
        MemoryRaftNetworkFactory {
            peers: self.peers.clone(),
        }
    }

    pub fn register(&self, node_id: NodeId, raft: Raft<RaftStoreConfig>) {
        self.peers
            .lock()
            .expect("memory raft network poisoned")
            .insert(node_id, raft);
    }
}

#[derive(Clone)]
pub struct MemoryRaftNetworkFactory {
    peers: Arc<Mutex<BTreeMap<NodeId, Raft<RaftStoreConfig>>>>,
}

impl RaftNetworkFactory<RaftStoreConfig> for MemoryRaftNetworkFactory {
    type Network = MemoryRaftNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        MemoryRaftNetwork {
            target,
            peers: self.peers.clone(),
        }
    }
}

pub struct MemoryRaftNetwork {
    target: NodeId,
    peers: Arc<Mutex<BTreeMap<NodeId, Raft<RaftStoreConfig>>>>,
}

impl MemoryRaftNetwork {
    fn target_raft<E>(&self) -> Result<Raft<RaftStoreConfig>, RPCError<NodeId, BasicNode, E>>
    where
        E: StdError,
    {
        self.peers
            .lock()
            .expect("memory raft network poisoned")
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Unreachable(Unreachable::new(&MemoryNetworkMissingNode(self.target)))
            })
    }
}

impl RaftNetwork<RaftStoreConfig> for MemoryRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<RaftStoreConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        self.target_raft()?
            .append_entries(rpc)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<RaftStoreConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        self.target_raft()?
            .install_snapshot(rpc)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }

    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    {
        self.target_raft()?
            .vote(rpc)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }
}
