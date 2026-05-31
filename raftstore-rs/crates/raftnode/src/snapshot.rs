use nokv_proto::nokv::meta::v1 as metapb;
use prost::Message;

use crate::{ApplyStatus, RegionId};

pub(crate) fn decode_region_snapshot_payload(
    snapshot: &[u8],
) -> nokv_mvcc::Result<RegionSnapshotPayload> {
    RegionSnapshotPayload::decode(snapshot).map_err(|err| nokv_mvcc::Error::Decode(err.to_string()))
}

pub(crate) fn decode_region_snapshot_status(snapshot: &[u8]) -> nokv_mvcc::Result<ApplyStatus> {
    let payload = decode_region_snapshot_payload(snapshot)?;
    if payload.format_version != 1 {
        return Err(nokv_mvcc::Error::Decode(format!(
            "unsupported region snapshot format {}",
            payload.format_version
        )));
    }
    Ok(ApplyStatus {
        region_id: payload.region_id,
        term: payload.term,
        applied_index: payload.applied_index,
    })
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct RegionSnapshotPayload {
    #[prost(uint32, tag = "1")]
    pub(crate) format_version: u32,
    #[prost(uint64, tag = "2")]
    pub(crate) region_id: RegionId,
    #[prost(uint64, tag = "3")]
    pub(crate) term: u64,
    #[prost(uint64, tag = "4")]
    pub(crate) applied_index: u64,
    #[prost(bytes = "vec", tag = "5")]
    pub(crate) mvcc_snapshot: Vec<u8>,
    #[prost(message, optional, tag = "6")]
    pub(crate) region_descriptor: Option<metapb::RegionDescriptor>,
}
