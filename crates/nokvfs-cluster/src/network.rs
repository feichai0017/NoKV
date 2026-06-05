//! OpenRaft network adapter over storage-neutral metadata RPC DTOs.
//!
//! The cluster crate owns OpenRaft types. Server/client code only needs to
//! implement `MetadataRaftRpcClient` over the protocol DTOs.

use std::error::Error;

use nokvfs_protocol::{
    WireMetadataRaftAppendEntriesRequest, WireMetadataRaftAppendEntriesResponse,
    WireMetadataRaftInstallSnapshotRequest, WireMetadataRaftInstallSnapshotResponse,
    WireMetadataRaftVoteRequest, WireMetadataRaftVoteResponse,
};
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;

use crate::log::MetadataRaftConfig;
use crate::{wire, MetadataRaftError};

pub trait MetadataRaftRpcClient: Clone + Send + Sync + 'static {
    fn vote_metadata_raft(
        &self,
        target: u64,
        address: &str,
        request: WireMetadataRaftVoteRequest,
    ) -> Result<WireMetadataRaftVoteResponse, MetadataRaftError>;

    fn append_metadata_raft_entries(
        &self,
        target: u64,
        address: &str,
        request: WireMetadataRaftAppendEntriesRequest,
    ) -> Result<WireMetadataRaftAppendEntriesResponse, MetadataRaftError>;

    fn install_metadata_raft_snapshot(
        &self,
        target: u64,
        address: &str,
        request: WireMetadataRaftInstallSnapshotRequest,
    ) -> Result<WireMetadataRaftInstallSnapshotResponse, MetadataRaftError>;
}

#[derive(Clone)]
pub struct MetadataRaftRpcNetworkFactory<C> {
    client: C,
}

pub struct MetadataRaftRpcNetwork<C> {
    client: C,
    target: u64,
    address: String,
}

impl<C> MetadataRaftRpcNetworkFactory<C>
where
    C: MetadataRaftRpcClient,
{
    pub fn new(client: C) -> Self {
        Self { client }
    }
}

impl<C> RaftNetworkFactory<MetadataRaftConfig> for MetadataRaftRpcNetworkFactory<C>
where
    C: MetadataRaftRpcClient,
{
    type Network = MetadataRaftRpcNetwork<C>;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        MetadataRaftRpcNetwork {
            client: self.client.clone(),
            target,
            address: node.addr.clone(),
        }
    }
}

impl<C> RaftNetwork<MetadataRaftConfig> for MetadataRaftRpcNetwork<C>
where
    C: MetadataRaftRpcClient,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MetadataRaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let request = wire::wire_append_entries_request(&rpc).map_err(rpc_error)?;
        let response = call_blocking({
            let client = self.client.clone();
            let address = self.address.clone();
            let target = self.target;
            move || client.append_metadata_raft_entries(target, &address, request)
        })
        .await?;
        wire::append_entries_response(response).map_err(rpc_error)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MetadataRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let request = wire::wire_install_snapshot_request(&rpc);
        let response = call_blocking({
            let client = self.client.clone();
            let address = self.address.clone();
            let target = self.target;
            move || client.install_metadata_raft_snapshot(target, &address, request)
        })
        .await?;
        wire::install_snapshot_response(response).map_err(rpc_error)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let request = wire::wire_vote_request(&rpc);
        let response = call_blocking({
            let client = self.client.clone();
            let address = self.address.clone();
            let target = self.target;
            move || client.vote_metadata_raft(target, &address, request)
        })
        .await?;
        wire::vote_response(response).map_err(rpc_error)
    }
}

async fn call_blocking<R, E>(
    call: impl FnOnce() -> Result<R, MetadataRaftError> + Send + 'static,
) -> Result<R, RPCError<u64, BasicNode, E>>
where
    E: Error + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(call)
        .await
        .map_err(|err| {
            rpc_error(MetadataRaftError::Backend(format!(
                "metadata raft rpc: {err}"
            )))
        })?
        .map_err(rpc_error)
}

fn rpc_error<E>(err: MetadataRaftError) -> RPCError<u64, BasicNode, E>
where
    E: Error + 'static,
{
    RPCError::Network(NetworkError::new(&err))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use nokvfs_protocol::{
        WireMetadataRaftAppendEntriesResponse, WireMetadataRaftInstallSnapshotResponse,
    };
    use openraft::network::{RaftNetwork, RaftNetworkFactory};
    use openraft::raft::AppendEntriesRequest;
    use openraft::{CommittedLeaderId, LogId, Vote};

    use super::*;

    type RecordedRpcCall = (u64, String, &'static str);

    #[derive(Clone, Default)]
    struct RecordingRpcClient {
        calls: Arc<Mutex<Vec<RecordedRpcCall>>>,
    }

    impl MetadataRaftRpcClient for RecordingRpcClient {
        fn vote_metadata_raft(
            &self,
            target: u64,
            address: &str,
            request: WireMetadataRaftVoteRequest,
        ) -> Result<WireMetadataRaftVoteResponse, MetadataRaftError> {
            self.calls
                .lock()
                .unwrap()
                .push((target, address.to_owned(), "vote"));
            Ok(WireMetadataRaftVoteResponse {
                vote: request.vote,
                vote_granted: true,
                last_log_id: request.last_log_id,
            })
        }

        fn append_metadata_raft_entries(
            &self,
            target: u64,
            address: &str,
            _request: WireMetadataRaftAppendEntriesRequest,
        ) -> Result<WireMetadataRaftAppendEntriesResponse, MetadataRaftError> {
            self.calls
                .lock()
                .unwrap()
                .push((target, address.to_owned(), "append"));
            Ok(WireMetadataRaftAppendEntriesResponse::Success)
        }

        fn install_metadata_raft_snapshot(
            &self,
            target: u64,
            address: &str,
            request: WireMetadataRaftInstallSnapshotRequest,
        ) -> Result<WireMetadataRaftInstallSnapshotResponse, MetadataRaftError> {
            self.calls
                .lock()
                .unwrap()
                .push((target, address.to_owned(), "snapshot"));
            Ok(WireMetadataRaftInstallSnapshotResponse { vote: request.vote })
        }
    }

    #[test]
    fn rpc_network_factory_routes_vote_to_target_address() {
        let client = RecordingRpcClient::default();
        let calls = Arc::clone(&client.calls);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let response = runtime.block_on(async {
            let mut factory = MetadataRaftRpcNetworkFactory::new(client);
            let mut network = factory
                .new_client(
                    7,
                    &BasicNode {
                        addr: "127.0.0.1:17007".to_owned(),
                    },
                )
                .await;
            network
                .vote(
                    VoteRequest {
                        vote: Vote::new(3, 1),
                        last_log_id: Some(LogId::new(CommittedLeaderId::new(2, 1), 9)),
                    },
                    RPCOption::new(Duration::from_secs(1)),
                )
                .await
                .unwrap()
        });

        assert!(response.vote_granted);
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &[(7, "127.0.0.1:17007".to_owned(), "vote")]
        );
    }

    #[test]
    fn rpc_network_factory_routes_append_entries_to_target_address() {
        let client = RecordingRpcClient::default();
        let calls = Arc::clone(&client.calls);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            let mut factory = MetadataRaftRpcNetworkFactory::new(client);
            let mut network = factory
                .new_client(
                    9,
                    &BasicNode {
                        addr: "127.0.0.1:17009".to_owned(),
                    },
                )
                .await;
            network
                .append_entries(
                    AppendEntriesRequest {
                        vote: Vote::new_committed(4, 1),
                        prev_log_id: None,
                        entries: Vec::new(),
                        leader_commit: None,
                    },
                    RPCOption::new(Duration::from_secs(1)),
                )
                .await
                .unwrap();
        });

        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &[(9, "127.0.0.1:17009".to_owned(), "append")]
        );
    }
}
