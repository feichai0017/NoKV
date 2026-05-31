use std::collections::{BTreeMap, BTreeSet};
use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;

use nokv_metastore::MemoryMetadataStore;
use nokv_proto::nokv::error::v1 as errorpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_proto::nokv::raft::v1 as raftpb;
use openraft::{
    error::{Fatal, InitializeError, RPCError, RaftError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    Config, Raft,
};
use tokio::sync::broadcast;

use crate::{
    decode_metadata_response, AppliedMetadataEngine, AppliedProposal, ApplyStatus,
    ApplyStatusProvider, ApplyWatchProvider, ApplyWatchReplay, ApplyWatchReplayRequest, BasicNode,
    Error, MetadataCommandExecutor, MetadataReadExecutor, NodeId, Proposal, RaftStoreConfig,
    RegionId, RegionLogStorage, RegionSnapshotEngine, RegionStateMachine, RegionTrafficProvider,
    RegionTrafficSnapshot,
};

#[derive(Clone)]
pub struct OpenRaftRegion<E = AppliedMetadataEngine<MemoryMetadataStore>> {
    node_id: NodeId,
    raft: Raft<RaftStoreConfig>,
    apply_engine: E,
}

impl<E> OpenRaftRegion<E>
where
    E: RegionSnapshotEngine,
{
    pub async fn open_with_network<N>(
        node_id: NodeId,
        region_id: RegionId,
        log_store: RegionLogStorage,
        state_machine: RegionStateMachine<E>,
        network: N,
    ) -> Result<OpenRaftRegion<E>, Error>
    where
        N: RaftNetworkFactory<RaftStoreConfig>,
    {
        let config = default_openraft_config(region_id)?;
        Self::open_with_network_config(
            node_id,
            region_id,
            log_store,
            state_machine,
            network,
            config,
        )
        .await
    }

    async fn open_with_network_config<N>(
        node_id: NodeId,
        _region_id: RegionId,
        mut log_store: RegionLogStorage,
        mut state_machine: RegionStateMachine<E>,
        network: N,
        config: Arc<Config>,
    ) -> Result<OpenRaftRegion<E>, Error>
    where
        N: RaftNetworkFactory<RaftStoreConfig>,
    {
        if let Some(membership) = log_store
            .latest_membership()
            .map_err(|err| Error::OpenRaft(err.to_string()))?
        {
            state_machine.restore_membership(membership);
        }
        let apply_status = state_machine.apply_engine().apply_status();
        if apply_status.applied_index != 0 {
            state_machine.restore_last_applied(
                log_store
                    .log_id_at_index(apply_status.applied_index)
                    .map_err(|err| Error::OpenRaft(err.to_string()))?,
            );
        }
        let voters = state_machine.membership().voter_ids().collect::<Vec<_>>();
        if voters.as_slice() == [node_id] {
            log_store
                .seed_single_node_vote_above_log(node_id)
                .map_err(|err| Error::OpenRaft(err.to_string()))?;
        }
        let apply_engine = state_machine.apply_engine().clone();
        let raft = Raft::new(node_id, config, network, log_store, state_machine)
            .await
            .map_err(openraft_error)?;
        Ok(OpenRaftRegion {
            node_id,
            raft,
            apply_engine,
        })
    }

    #[cfg(test)]
    pub(crate) async fn open_with_network_for_test<N, F>(
        node_id: NodeId,
        region_id: RegionId,
        log_store: RegionLogStorage,
        state_machine: RegionStateMachine<E>,
        network: N,
        configure: F,
    ) -> Result<OpenRaftRegion<E>, Error>
    where
        N: RaftNetworkFactory<RaftStoreConfig>,
        F: FnOnce(&mut Config),
    {
        let mut config = Config {
            cluster_name: format!("nokv-region-{region_id}"),
            ..Default::default()
        };
        configure(&mut config);
        let config = Arc::new(
            config
                .validate()
                .map_err(|err| Error::OpenRaft(err.to_string()))?,
        );
        Self::open_with_network_config(
            node_id,
            region_id,
            log_store,
            state_machine,
            network,
            config,
        )
        .await
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub async fn bootstrap_single_node(
        node_id: NodeId,
        region_id: RegionId,
        log_store: RegionLogStorage,
        state_machine: RegionStateMachine<E>,
    ) -> Result<OpenRaftRegion<E>, Error> {
        Self::bootstrap_single_node_with_network(
            node_id,
            region_id,
            log_store,
            state_machine,
            NoopNetworkFactory,
            format!("local-{node_id}"),
        )
        .await
    }

    pub async fn bootstrap_single_node_with_network<N>(
        node_id: NodeId,
        region_id: RegionId,
        log_store: RegionLogStorage,
        state_machine: RegionStateMachine<E>,
        network: N,
        local_addr: impl Into<String>,
    ) -> Result<OpenRaftRegion<E>, Error>
    where
        N: RaftNetworkFactory<RaftStoreConfig>,
    {
        let region =
            Self::open_with_network(node_id, region_id, log_store, state_machine, network).await?;
        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::new(local_addr.into()));
        if region.initialize_members(members).await? {
            region.wait_for_leader(node_id).await?;
            region.ensure_linearizable().await?;
            return Ok(region);
        }

        let metrics = region.raft_handle().metrics().borrow().clone();
        let voters = metrics.membership_config.voter_ids().collect::<Vec<_>>();
        if voters.as_slice() == [node_id] {
            region.elect_and_wait(node_id).await?;
            region.ensure_linearizable().await?;
        }
        Ok(region)
    }

    pub async fn ensure_linearizable(&self) -> Result<(), Error> {
        self.raft
            .ensure_linearizable()
            .await
            .map(|_| ())
            .map_err(openraft_check_leader_error)
    }

    pub fn raft_handle(&self) -> Raft<RaftStoreConfig> {
        self.raft.clone()
    }

    pub fn region_descriptor(&self) -> nokv_metastore::Result<Option<metapb::RegionDescriptor>> {
        self.apply_engine.region_descriptor()
    }

    pub async fn initialize_members(
        &self,
        members: BTreeMap<NodeId, BasicNode>,
    ) -> Result<bool, Error> {
        match self.raft.initialize(members).await {
            Ok(()) => Ok(true),
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(false),
            Err(err) => return Err(openraft_api_error(err)),
        }
    }

    pub async fn elect_and_wait(&self, node_id: NodeId) -> Result<(), Error> {
        self.raft.trigger().elect().await.map_err(openraft_error)?;
        self.wait_for_leader(node_id).await
    }

    pub async fn wait_for_leader(&self, node_id: NodeId) -> Result<(), Error> {
        self.raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(
                |metrics| metrics.current_leader == Some(node_id),
                "raft leader election",
            )
            .await
            .map_err(|err| Error::OpenRaft(err.to_string()))?;
        Ok(())
    }

    pub async fn add_voter(&self, node_id: NodeId, node: BasicNode) -> Result<(), Error> {
        self.raft
            .add_learner(node_id, node, true)
            .await
            .map_err(openraft_api_error)?;
        self.raft
            .change_membership(
                openraft::ChangeMembers::AddVoterIds(BTreeSet::from([node_id])),
                true,
            )
            .await
            .map_err(openraft_api_error)?;
        self.wait_for_voter(node_id, true).await
    }

    pub async fn remove_voter(&self, node_id: NodeId, retain: bool) -> Result<(), Error> {
        self.raft
            .change_membership(
                openraft::ChangeMembers::RemoveVoters(BTreeSet::from([node_id])),
                retain,
            )
            .await
            .map_err(openraft_api_error)?;
        self.wait_for_voter(node_id, false).await
    }

    pub async fn transfer_leader(&self, node_id: NodeId) -> Result<(), Error> {
        if node_id == 0 {
            return Err(Error::InvalidLeaderTransferTarget {
                target: node_id,
                reason: "peer id is required",
            });
        }

        let metrics = self.raft.metrics().borrow().clone();
        let is_voter = metrics
            .membership_config
            .voter_ids()
            .any(|voter| voter == node_id);
        if !is_voter {
            return Err(Error::InvalidLeaderTransferTarget {
                target: node_id,
                reason: "target is not a voter",
            });
        }
        if metrics.current_leader == Some(node_id) {
            return Ok(());
        }
        if node_id == self.node_id {
            self.elect_and_wait(node_id).await?;
            self.ensure_linearizable().await?;
            return Ok(());
        }
        Err(Error::UnsupportedLeaderTransfer {
            local: self.node_id,
            target: node_id,
            reason: "remote directed transfer requires routing the request to the target peer",
        })
    }

    pub async fn wait_for_voter(&self, node_id: NodeId, present: bool) -> Result<(), Error> {
        self.raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(
                |metrics| {
                    let membership = &metrics.membership_config;
                    let uniform = membership.membership().get_joint_config().len() == 1;
                    let observed = membership.voter_ids().any(|voter| voter == node_id);
                    uniform && observed == present
                },
                "raft membership voter state",
            )
            .await
            .map_err(|err| Error::OpenRaft(err.to_string()))?;
        Ok(())
    }

    pub async fn propose(&self, proposal: Proposal) -> Result<AppliedProposal, Error> {
        let response = self
            .raft
            .client_write(proposal)
            .await
            .map_err(openraft_client_write_error)?;
        Ok(response.data)
    }

    pub async fn propose_region_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> Result<(), Error> {
        self.propose(Proposal::from_region_descriptor(descriptor)?)
            .await
            .map(|_| ())
    }

    pub async fn propose_admin_command(
        &self,
        region_id: RegionId,
        command: &raftpb::AdminCommand,
    ) -> Result<(), Error> {
        self.propose(Proposal::from_admin_command(region_id, command)?)
            .await
            .map(|_| ())
    }

    pub async fn trigger_snapshot(&self) -> Result<(), Error> {
        self.raft.trigger().snapshot().await.map_err(openraft_error)
    }

    pub async fn trigger_log_purge(&self, upto: u64) -> Result<(), Error> {
        self.raft
            .trigger()
            .purge_log(upto)
            .await
            .map_err(openraft_error)
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        self.raft
            .shutdown()
            .await
            .map_err(|err| Error::OpenRaft(err.to_string()))
    }
}

impl<E> ApplyStatusProvider for OpenRaftRegion<E>
where
    E: ApplyStatusProvider,
{
    fn apply_status(&self) -> ApplyStatus {
        self.apply_engine.apply_status()
    }
}

impl<E> ApplyWatchProvider for OpenRaftRegion<E>
where
    E: ApplyWatchProvider,
{
    fn subscribe_apply(&self) -> broadcast::Receiver<metadatapb::MetadataApplyWatchEvent> {
        self.apply_engine.subscribe_apply()
    }

    fn replay_apply(
        &self,
        request: ApplyWatchReplayRequest,
    ) -> nokv_metastore::Result<ApplyWatchReplay> {
        self.apply_engine.replay_apply(request)
    }
}

impl<E> RegionTrafficProvider for OpenRaftRegion<E>
where
    E: RegionTrafficProvider,
{
    fn traffic_snapshot(&self) -> RegionTrafficSnapshot {
        self.apply_engine.traffic_snapshot()
    }
}

impl<E> MetadataCommandExecutor for OpenRaftRegion<E>
where
    E: RegionSnapshotEngine + MetadataCommandExecutor,
{
    fn execute_metadata_command<'a>(
        &'a self,
        req: &'a metadatapb::MetadataCommitRequest,
    ) -> impl std::future::Future<Output = nokv_metastore::Result<metadatapb::MetadataCommitResponse>>
           + Send
           + 'a {
        async move {
            let proposal = Proposal::from_metadata_command(req)
                .map_err(|err| nokv_metastore::Error::Backend(err.to_string()))?;
            let applied = match self.propose(proposal).await {
                Ok(applied) => applied,
                Err(Error::NotLeader { leader_id }) => {
                    return Ok(self.not_leader_metadata_response(req, leader_id))
                }
                Err(err) => return Err(nokv_metastore::Error::Backend(err.to_string())),
            };
            decode_metadata_response(&applied.payload)
        }
    }
}

impl<E> MetadataReadExecutor for OpenRaftRegion<E>
where
    E: RegionSnapshotEngine + MetadataReadExecutor,
{
    fn execute_metadata_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataGetRequest,
    ) -> impl std::future::Future<Output = nokv_metastore::Result<metadatapb::MetadataGetResponse>>
           + Send
           + 'a {
        async move {
            let context = req.context.as_ref();
            if let Some(region_error) = self.metadata_read_gate(context).await? {
                return Ok(metadatapb::MetadataGetResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                });
            }
            self.apply_engine.execute_metadata_get(req).await
        }
    }

    fn execute_metadata_batch_get<'a>(
        &'a self,
        req: &'a metadatapb::MetadataBatchGetRequest,
    ) -> impl std::future::Future<
        Output = nokv_metastore::Result<metadatapb::MetadataBatchGetResponse>,
    > + Send
           + 'a {
        async move {
            let context = req.context.as_ref();
            if let Some(region_error) = self.metadata_read_gate(context).await? {
                return Ok(metadatapb::MetadataBatchGetResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                });
            }
            self.apply_engine.execute_metadata_batch_get(req).await
        }
    }

    fn execute_metadata_scan<'a>(
        &'a self,
        req: &'a metadatapb::MetadataScanRequest,
    ) -> impl std::future::Future<Output = nokv_metastore::Result<metadatapb::MetadataScanResponse>>
           + Send
           + 'a {
        async move {
            let context = req.context.as_ref();
            if let Some(region_error) = self.metadata_read_gate(context).await? {
                return Ok(metadatapb::MetadataScanResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                });
            }
            self.apply_engine.execute_metadata_scan(req).await
        }
    }
}

impl<E> OpenRaftRegion<E>
where
    E: RegionSnapshotEngine,
{
    async fn metadata_read_gate(
        &self,
        context: Option<&metadatapb::MetadataContext>,
    ) -> nokv_metastore::Result<Option<errorpb::RegionError>> {
        self.ensure_metadata_read_region(context)?;
        if metadata_read_consistency(context) == metadatapb::ReadConsistency::BoundedStale {
            if self.metadata_bounded_stale_read_admissible(context) {
                return Ok(None);
            }
            return Ok(Some(stale_metadata_region_error()));
        }
        if let Err(err) = ensure_linearizable_for_read(&self.raft).await {
            if let Error::NotLeader { leader_id } = err {
                return self
                    .not_leader_metadata_region_error(context, leader_id)
                    .map(Some);
            }
            return Err(nokv_metastore::Error::Backend(err.to_string()));
        }
        Ok(None)
    }

    fn not_leader_metadata_response(
        &self,
        req: &metadatapb::MetadataCommitRequest,
        leader_id: Option<NodeId>,
    ) -> metadatapb::MetadataCommitResponse {
        let descriptor = self.apply_engine.region_descriptor().ok().flatten();
        let region_id = descriptor
            .as_ref()
            .map(|descriptor| descriptor.region_id)
            .or_else(|| req.context.as_ref().map(|context| context.region_id))
            .unwrap_or_else(|| self.apply_engine.apply_status().region_id);
        let leader = leader_id.and_then(|leader_id| {
            descriptor.as_ref().and_then(|descriptor| {
                descriptor
                    .peers
                    .iter()
                    .find(|peer| peer.peer_id == leader_id)
                    .cloned()
            })
        });
        metadatapb::MetadataCommitResponse {
            region_error: Some(errorpb::RegionError {
                not_leader: Some(errorpb::NotLeader { region_id, leader }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn ensure_metadata_read_region(
        &self,
        context: Option<&metadatapb::MetadataContext>,
    ) -> nokv_metastore::Result<()> {
        let requested_region_id = context.map(|context| context.region_id).unwrap_or_default();
        let applied_region_id = self.apply_engine.apply_status().region_id;
        if requested_region_id != 0 && requested_region_id != applied_region_id {
            return Err(nokv_metastore::Error::Backend(
                Error::LogRegionMismatch {
                    record_region_id: applied_region_id,
                    proposal_region_id: requested_region_id,
                }
                .to_string(),
            ));
        }
        Ok(())
    }

    fn metadata_bounded_stale_read_admissible(
        &self,
        context: Option<&metadatapb::MetadataContext>,
    ) -> bool {
        if context.is_none() {
            return false;
        }
        let status = self.apply_engine.apply_status();
        if status.applied_index == 0 {
            return false;
        }
        let metrics = self.raft_handle().metrics();
        let metrics = metrics.borrow();
        let last_log_index = metrics.last_log_index.unwrap_or_default();
        last_log_index == status.applied_index
    }

    fn not_leader_metadata_region_error(
        &self,
        context: Option<&metadatapb::MetadataContext>,
        leader_id: Option<NodeId>,
    ) -> nokv_metastore::Result<errorpb::RegionError> {
        let descriptor = self.apply_engine.region_descriptor()?;
        let region_id = descriptor
            .as_ref()
            .map(|descriptor| descriptor.region_id)
            .or_else(|| context.map(|context| context.region_id))
            .unwrap_or_else(|| self.apply_engine.apply_status().region_id);
        let leader = leader_id.and_then(|leader_id| {
            descriptor.as_ref().and_then(|descriptor| {
                descriptor
                    .peers
                    .iter()
                    .find(|peer| peer.peer_id == leader_id)
                    .cloned()
            })
        });
        Ok(errorpb::RegionError {
            not_leader: Some(errorpb::NotLeader { region_id, leader }),
            ..Default::default()
        })
    }
}

fn metadata_read_consistency(
    context: Option<&metadatapb::MetadataContext>,
) -> metadatapb::ReadConsistency {
    context
        .and_then(|context| metadatapb::ReadConsistency::try_from(context.read_consistency).ok())
        .unwrap_or(metadatapb::ReadConsistency::Strong)
}

fn stale_metadata_region_error() -> errorpb::RegionError {
    errorpb::RegionError {
        stale_command: Some(errorpb::StaleCommand {}),
        ..Default::default()
    }
}

async fn ensure_linearizable_for_read(raft: &Raft<RaftStoreConfig>) -> Result<(), Error> {
    let mut last_error = None;
    for attempt in 1..=50 {
        match raft.ensure_linearizable().await {
            Ok(_) => return Ok(()),
            Err(err) => {
                let err = openraft_check_leader_error(err);
                if matches!(err, Error::NotLeader { leader_id: Some(_) }) {
                    return Err(err);
                }
                last_error = Some(err);
                if attempt < 50 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    let err = last_error
        .map(|err| err.to_string())
        .unwrap_or_else(|| "linearizable read did not complete".to_owned());
    Err(Error::OpenRaft(err))
}

#[derive(Clone, Default)]
struct NoopNetworkFactory;

struct NoopNetwork;

#[derive(Debug, thiserror::Error)]
#[error("single-node raft network has no remote peers")]
struct NoopNetworkError;

impl RaftNetworkFactory<RaftStoreConfig> for NoopNetworkFactory {
    type Network = NoopNetwork;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        NoopNetwork
    }
}

impl RaftNetwork<RaftStoreConfig> for NoopNetwork {
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
    RPCError::Unreachable(Unreachable::new(&NoopNetworkError))
}

fn openraft_error(err: Fatal<NodeId>) -> Error {
    Error::OpenRaft(err.to_string())
}

fn openraft_api_error<E>(err: RaftError<NodeId, E>) -> Error
where
    E: StdError,
{
    Error::OpenRaft(err.to_string())
}

fn openraft_client_write_error(
    err: RaftError<NodeId, openraft::error::ClientWriteError<NodeId, BasicNode>>,
) -> Error {
    if let Some(forward) = err.forward_to_leader() {
        return Error::NotLeader {
            leader_id: forward.leader_id,
        };
    }
    openraft_api_error(err)
}

fn openraft_check_leader_error(
    err: RaftError<NodeId, openraft::error::CheckIsLeaderError<NodeId, BasicNode>>,
) -> Error {
    if let Some(forward) = err.forward_to_leader() {
        return Error::NotLeader {
            leader_id: forward.leader_id,
        };
    }
    openraft_api_error(err)
}

fn default_openraft_config(region_id: RegionId) -> Result<Arc<Config>, Error> {
    Ok(Arc::new(
        Config {
            cluster_name: format!("nokv-region-{region_id}"),
            ..Default::default()
        }
        .validate()
        .map_err(|err| Error::OpenRaft(err.to_string()))?,
    ))
}
