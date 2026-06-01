use std::time::Duration;

use nokv_proto::nokv::error::v1 as errorpb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use openraft::Raft;

use super::{openraft_check_leader_error, OpenRaftRegion};
use crate::{
    decode_metadata_response, decode_metadata_response_batch, Error, MetadataCommandExecutor,
    MetadataReadExecutor, MetadataRetentionExecutor, NodeId, Proposal, RaftStoreConfig,
    RegionSnapshotEngine,
};

impl<E> MetadataCommandExecutor for OpenRaftRegion<E>
where
    E: RegionSnapshotEngine + MetadataCommandExecutor,
{
    fn execute_metadata_command<'a>(
        &'a self,
        req: &'a metadatapb::MetadataCommitRequest,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataCommitResponse>,
    > + Send
           + 'a {
        async move {
            let proposal = Proposal::from_metadata_command(req)
                .map_err(|err| nokv_metadata_state::Error::Backend(err.to_string()))?;
            let applied = match self.propose(proposal).await {
                Ok(applied) => applied,
                Err(Error::NotLeader { leader_id }) => {
                    return Ok(self.not_leader_metadata_response(req, leader_id));
                }
                Err(err) => return Err(nokv_metadata_state::Error::Backend(err.to_string())),
            };
            decode_metadata_response(&applied.payload)
        }
    }

    fn execute_metadata_commands<'a>(
        &'a self,
        reqs: &'a [metadatapb::MetadataCommitRequest],
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<Vec<metadatapb::MetadataCommitResponse>>,
    > + Send
           + 'a {
        async move {
            if reqs.is_empty() {
                return Ok(Vec::new());
            }
            if reqs.len() == 1 {
                return Ok(vec![self.execute_metadata_command(&reqs[0]).await?]);
            }
            let proposal = Proposal::from_metadata_command_batch(reqs)
                .map_err(|err| nokv_metadata_state::Error::Backend(err.to_string()))?;
            let applied = match self.propose(proposal).await {
                Ok(applied) => applied,
                Err(Error::NotLeader { leader_id }) => {
                    return Ok(reqs
                        .iter()
                        .map(|req| self.not_leader_metadata_response(req, leader_id))
                        .collect());
                }
                Err(err) => return Err(nokv_metadata_state::Error::Backend(err.to_string())),
            };
            let responses = decode_metadata_response_batch(&applied.payload)?;
            if responses.len() != reqs.len() {
                return Err(nokv_metadata_state::Error::Backend(
                    "metadata batch response length mismatch".to_owned(),
                ));
            }
            Ok(responses)
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
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataGetResponse>,
    > + Send
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
        Output = nokv_metadata_state::Result<metadatapb::MetadataBatchGetResponse>,
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
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<metadatapb::MetadataScanResponse>,
    > + Send
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

impl<E> MetadataRetentionExecutor for OpenRaftRegion<E>
where
    E: RegionSnapshotEngine + MetadataRetentionExecutor,
{
    fn prune_metadata_versions<'a>(
        &'a self,
        retention_floor: u64,
    ) -> impl std::future::Future<
        Output = nokv_metadata_state::Result<nokv_metadata_state::MetadataRetentionResult>,
    > + Send
           + 'a {
        async move {
            self.apply_engine
                .prune_metadata_versions(retention_floor)
                .await
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
    ) -> nokv_metadata_state::Result<Option<errorpb::RegionError>> {
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
            return Err(nokv_metadata_state::Error::Backend(err.to_string()));
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
    ) -> nokv_metadata_state::Result<()> {
        let requested_region_id = context.map(|context| context.region_id).unwrap_or_default();
        let applied_region_id = self.apply_engine.apply_status().region_id;
        if requested_region_id != 0 && requested_region_id != applied_region_id {
            return Err(nokv_metadata_state::Error::Backend(
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
    ) -> nokv_metadata_state::Result<errorpb::RegionError> {
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
