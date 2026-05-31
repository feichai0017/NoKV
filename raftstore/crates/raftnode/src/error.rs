use crate::{NodeId, RegionId};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("raft command header region id is required")]
    MissingRegionHeader,
    #[error("raft proposal region {proposal_region_id} does not match command region {command_region_id}")]
    RegionMismatch {
        proposal_region_id: RegionId,
        command_region_id: RegionId,
    },
    #[error("raft log record region {record_region_id} does not match proposal region {proposal_region_id}")]
    LogRegionMismatch {
        record_region_id: RegionId,
        proposal_region_id: RegionId,
    },
    #[error("invalid raft log payload: {0}")]
    InvalidLogPayload(String),
    #[error("invalid raft transport payload: {0}")]
    InvalidTransportPayload(String),
    #[error("invalid region descriptor proposal: {0}")]
    InvalidRegionDescriptor(String),
    #[error("raft log error: {0}")]
    RaftLog(#[from] nokv_raftlog::Error),
    #[error("raft metadata io error: {0}")]
    MetadataIo(#[from] std::io::Error),
    #[error("corrupt raft metadata: {0}")]
    CorruptMetadata(&'static str),
    #[error("invalid leader transfer target {target}: {reason}")]
    InvalidLeaderTransferTarget {
        target: NodeId,
        reason: &'static str,
    },
    #[error("leader transfer to peer {target} is not supported from local peer {local}: {reason}")]
    UnsupportedLeaderTransfer {
        local: NodeId,
        target: NodeId,
        reason: &'static str,
    },
    #[error("not raft leader; forward to {leader_id:?}")]
    NotLeader { leader_id: Option<NodeId> },
    #[error("raft command encode error: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("raft command decode error: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("openraft error: {0}")]
    OpenRaft(String),
}
