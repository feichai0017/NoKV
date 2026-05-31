use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::meta::v1 as metapb;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("holt error: {0}")]
    Holt(#[from] holt::Error),
    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("protobuf encode error: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("invalid metadata record: {0}")]
    InvalidMetadata(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionApplyState {
    pub region_id: u64,
    pub term: u64,
    pub applied_index: u64,
    pub truncated_term: u64,
    pub truncated_index: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingRootEvent {
    pub sequence: u64,
    pub event: metapb::RootEvent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockedRootEvent {
    pub sequence: u64,
    pub event: metapb::RootEvent,
    pub transition_id: String,
    pub last_error: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingSchedulerOperation {
    pub operation: coordpb::SchedulerOperation,
    pub attempts: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockedSchedulerOperation {
    pub operation: coordpb::SchedulerOperation,
    pub attempts: u32,
    pub last_error: String,
}
