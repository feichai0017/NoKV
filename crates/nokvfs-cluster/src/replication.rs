use crate::{
    DurableReceipt, LearnerBootstrapPlan, LogIndex, LogPosition, MetadataLogEntry, NodeId,
    SharedLogError,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendMetadataBatchRequest {
    pub leader: NodeId,
    pub entry: MetadataLogEntry,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendMetadataBatchResponse {
    pub position: LogPosition,
    pub receipts: Vec<DurableReceipt>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadMetadataLogRequest {
    pub reader: NodeId,
    pub start: LogIndex,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadMetadataLogResponse {
    pub reader: NodeId,
    pub entries: Vec<MetadataLogEntry>,
    pub committed: Option<LogPosition>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstallCheckpointRequest {
    pub leader: NodeId,
    pub plan: LearnerBootstrapPlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstallCheckpointResponse {
    pub learner: NodeId,
    pub replay_start: LogIndex,
    pub replayed_index: LogIndex,
}

impl AppendMetadataBatchRequest {
    pub fn new(leader: NodeId, entry: MetadataLogEntry) -> Result<Self, SharedLogError> {
        if entry.commands.is_empty() {
            return Err(SharedLogError::EmptyBatch);
        }
        Ok(Self { leader, entry })
    }
}

impl AppendMetadataBatchResponse {
    pub fn from_receipts(receipts: Vec<DurableReceipt>) -> Result<Self, SharedLogError> {
        let position = receipts.first().ok_or(SharedLogError::EmptyBatch)?.position;
        Ok(Self { position, receipts })
    }
}

impl ReadMetadataLogRequest {
    pub fn new(reader: NodeId, start: LogIndex, limit: usize) -> Self {
        Self {
            reader,
            start,
            limit,
        }
    }
}

impl ReadMetadataLogResponse {
    pub fn new(
        reader: NodeId,
        entries: Vec<MetadataLogEntry>,
        committed: Option<LogPosition>,
    ) -> Self {
        Self {
            reader,
            entries,
            committed,
        }
    }
}

impl InstallCheckpointRequest {
    pub fn from_plan(leader: NodeId, plan: LearnerBootstrapPlan) -> Self {
        Self { leader, plan }
    }
}

impl InstallCheckpointResponse {
    pub fn from_plan(plan: &LearnerBootstrapPlan) -> Self {
        Self {
            learner: plan.node,
            replay_start: plan.replay_start,
            replayed_index: plan.replayed_index,
        }
    }
}
