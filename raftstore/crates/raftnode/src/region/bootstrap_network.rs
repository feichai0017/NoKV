use std::error::Error as StdError;

use openraft::{
    error::{RPCError, RaftError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
};

use crate::{BasicNode, NodeId, RaftStoreConfig};

#[derive(Clone, Default)]
pub(super) struct BootstrapNetworkFactory;

pub(super) struct BootstrapNetwork;

#[derive(Debug, thiserror::Error)]
#[error("single-node raft network has no remote peers")]
struct BootstrapNetworkError;

impl RaftNetworkFactory<RaftStoreConfig> for BootstrapNetworkFactory {
    type Network = BootstrapNetwork;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        BootstrapNetwork
    }
}

impl RaftNetwork<RaftStoreConfig> for BootstrapNetwork {
    async fn append_entries(
        &mut self,
        _rpc: openraft::raft::AppendEntriesRequest<RaftStoreConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        Err(remote_unreachable())
    }

    async fn install_snapshot(
        &mut self,
        _rpc: openraft::raft::InstallSnapshotRequest<RaftStoreConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        Err(remote_unreachable())
    }

    async fn vote(
        &mut self,
        _rpc: openraft::raft::VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    {
        Err(remote_unreachable())
    }
}

fn remote_unreachable<NID, N, E>() -> RPCError<NID, N, E>
where
    NID: openraft::NodeId,
    N: openraft::Node,
    E: StdError,
{
    RPCError::Unreachable(Unreachable::new(&BootstrapNetworkError))
}
