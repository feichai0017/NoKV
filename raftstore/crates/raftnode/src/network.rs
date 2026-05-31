use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::sync::{Arc, Mutex};

use openraft::{
    error::{RPCError, RaftError, RemoteError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    BasicNode, Raft,
};

use crate::{
    decode_append_entries_request, decode_append_entries_response, decode_install_snapshot_request,
    decode_install_snapshot_response, decode_vote_request, decode_vote_response,
    encode_append_entries_request, encode_append_entries_response, encode_install_snapshot_request,
    encode_install_snapshot_response, encode_vote_request, encode_vote_response, Error, NodeId,
    RaftStoreConfig, RegionId,
};

#[derive(Debug, thiserror::Error)]
#[error("raft node {0} is not registered in memory network")]
struct MemoryNetworkMissingNode(NodeId);

#[derive(Debug, thiserror::Error)]
#[error("raft node {node_id} for region {region_id} is not registered in encoded network")]
struct EncodedNetworkMissingNode {
    region_id: RegionId,
    node_id: NodeId,
}

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

#[derive(Clone, Default)]
pub struct EncodedRaftNetworkRegistry {
    peers: Arc<Mutex<BTreeMap<(RegionId, NodeId), Raft<RaftStoreConfig>>>>,
}

impl EncodedRaftNetworkRegistry {
    pub fn factory(&self, region_id: RegionId) -> EncodedRaftNetworkFactory {
        EncodedRaftNetworkFactory {
            region_id,
            peers: self.peers.clone(),
        }
    }

    pub fn register(&self, region_id: RegionId, node_id: NodeId, raft: Raft<RaftStoreConfig>) {
        self.peers
            .lock()
            .expect("encoded raft network poisoned")
            .insert((region_id, node_id), raft);
    }
}

#[derive(Clone)]
pub struct EncodedRaftNetworkFactory {
    region_id: RegionId,
    peers: Arc<Mutex<BTreeMap<(RegionId, NodeId), Raft<RaftStoreConfig>>>>,
}

impl RaftNetworkFactory<RaftStoreConfig> for EncodedRaftNetworkFactory {
    type Network = EncodedRaftNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        EncodedRaftNetwork {
            region_id: self.region_id,
            target,
            peers: self.peers.clone(),
        }
    }
}

pub struct EncodedRaftNetwork {
    region_id: RegionId,
    target: NodeId,
    peers: Arc<Mutex<BTreeMap<(RegionId, NodeId), Raft<RaftStoreConfig>>>>,
}

impl EncodedRaftNetwork {
    fn target_raft<E>(&self) -> Result<Raft<RaftStoreConfig>, RPCError<NodeId, BasicNode, E>>
    where
        E: StdError,
    {
        self.peers
            .lock()
            .expect("encoded raft network poisoned")
            .get(&(self.region_id, self.target))
            .cloned()
            .ok_or_else(|| {
                RPCError::Unreachable(Unreachable::new(&EncodedNetworkMissingNode {
                    region_id: self.region_id,
                    node_id: self.target,
                }))
            })
    }
}

impl RaftNetwork<RaftStoreConfig> for EncodedRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<RaftStoreConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        let payload = encode_append_entries_request(self.region_id, &rpc).map_err(codec_error)?;
        let (region_id, request) = decode_append_entries_request(&payload).map_err(codec_error)?;
        if region_id != self.region_id {
            return Err(codec_error(Error::InvalidTransportPayload(format!(
                "append entries decoded region {region_id} does not match network region {}",
                self.region_id
            ))));
        }
        let response = self
            .target_raft()?
            .append_entries(request)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))?;
        let payload = encode_append_entries_response(&response).map_err(codec_error)?;
        decode_append_entries_response(&payload).map_err(codec_error)
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<RaftStoreConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        let payload = encode_install_snapshot_request(self.region_id, &rpc).map_err(codec_error)?;
        let (region_id, request) =
            decode_install_snapshot_request(&payload).map_err(codec_error)?;
        if region_id != self.region_id {
            return Err(codec_error(Error::InvalidTransportPayload(format!(
                "install snapshot decoded region {region_id} does not match network region {}",
                self.region_id
            ))));
        }
        let response = self
            .target_raft()?
            .install_snapshot(request)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))?;
        let payload = encode_install_snapshot_response(&response).map_err(codec_error)?;
        decode_install_snapshot_response(&payload).map_err(codec_error)
    }

    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    {
        let payload = encode_vote_request(&rpc).map_err(codec_error)?;
        let request = decode_vote_request(&payload).map_err(codec_error)?;
        let response = self
            .target_raft()?
            .vote(request)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))?;
        let payload = encode_vote_response(&response).map_err(codec_error)?;
        decode_vote_response(&payload).map_err(codec_error)
    }
}

fn codec_error<E>(err: Error) -> RPCError<NodeId, BasicNode, E>
where
    E: StdError,
{
    RPCError::Unreachable(Unreachable::new(&err))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nokv_mvcc::{KvEngine, MvccStore};
    use nokv_proto::nokv::kv::v1 as kvpb;
    use nokv_proto::nokv::metadata::v1 as metadatapb;

    use super::*;
    use crate::{
        AppliedKvEngine, ApplyStatusProvider, MetadataCommandExecutor, OpenRaftRegion,
        RegionLogStorage, RegionStateMachine,
    };

    #[tokio::test]
    async fn encoded_raft_network_replicates_proposal_through_codec() {
        let registry = EncodedRaftNetworkRegistry::default();
        let mut dirs = Vec::new();
        let mut regions = Vec::new();
        let mut engines = BTreeMap::new();

        for node_id in 1..=3 {
            let dir = tempfile::tempdir().unwrap();
            let log = crate::SegmentedEntryLog::open(7, dir.path()).unwrap();
            let log_store = RegionLogStorage::new(log);
            let engine = AppliedKvEngine::new(7, MvccStore::new());
            let region = OpenRaftRegion::open_with_network(
                node_id,
                7,
                log_store,
                RegionStateMachine::new(engine.clone()),
                registry.factory(7),
            )
            .await
            .unwrap();
            registry.register(7, node_id, region.raft_handle());
            dirs.push(dir);
            engines.insert(node_id, engine);
            regions.push(region);
        }

        regions[0]
            .initialize_members(BTreeMap::from([
                (1, BasicNode::new("node-1")),
                (2, BasicNode::new("node-2")),
                (3, BasicNode::new("node-3")),
            ]))
            .await
            .unwrap();
        regions[0].wait_for_leader(1).await.unwrap();

        let command = metadatapb::MetadataCommitRequest {
            context: Some(metadatapb::MetadataContext {
                region_id: 7,
                ..Default::default()
            }),
            command: Some(metadatapb::MetadataCommand {
                request_id: b"encoded-network".to_vec(),
                read_version: 9,
                commit_version: 10,
                mutations: vec![metadatapb::MetadataMutation {
                    key: b"encoded-network".to_vec(),
                    value: b"replicated".to_vec(),
                    op: metadatapb::metadata_mutation::Op::Put as i32,
                    ..Default::default()
                }],
                watch_keys: vec![b"encoded-network".to_vec()],
                ..Default::default()
            }),
        };
        regions[0].execute_metadata_command(&command).await.unwrap();
        let target_index = regions[0].apply_status().applied_index;

        for region in &regions {
            region
                .raft_handle()
                .wait(Some(std::time::Duration::from_secs(5)))
                .applied_index_at_least(Some(target_index), "encoded network replication")
                .await
                .unwrap();
        }

        for (_node_id, engine) in engines {
            let response = engine
                .get(&kvpb::GetRequest {
                    key: b"encoded-network".to_vec(),
                    version: 10,
                })
                .unwrap();
            assert_eq!(response.value, b"replicated".to_vec());
        }
    }
}
