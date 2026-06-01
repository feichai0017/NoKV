//! OpenRaft boundary for Rust raftstore.
//!
//! The crate intentionally exposes a small NoKV-owned trait instead of leaking
//! OpenRaft types into server, metadata store, or proto-facing code. The concrete
//! OpenRaft-backed implementation stays behind this boundary.

pub type NodeId = u64;
pub type RegionId = u64;

mod apply;
mod error;
mod log;
mod metadata_wire;
mod proposal;
mod region;
mod storage;
mod traffic;
mod transport;

pub use apply::{
    AppliedMetadataEngine, AppliedProposal, ApplyStatus, ApplyStatusProvider, ApplyWatchProvider,
    ApplyWatchReplay, ApplyWatchReplayRequest, MetadataCommandExecutor, MetadataReadExecutor,
    MetadataRetentionExecutor, PersistentAppliedMetadataEngine, RegionApplyEngine,
    RegionMetadataSink, RegionSnapshotEngine,
};
pub use error::Error;
pub use log::{decode_log_entry, encode_log_entry, RaftEntryLog, SegmentedEntryLog};
pub(crate) use metadata_wire::decode_metadata_response;
pub use openraft::BasicNode;
pub(crate) use proposal::ProposalPayloadKind;
pub use proposal::{Proposal, ProposalPayload};
pub use region::OpenRaftRegion;
pub use storage::{RegionLogStorage, RegionSnapshotBuilder, RegionStateMachine};
pub use traffic::{RegionTrafficProvider, RegionTrafficSnapshot};
pub use transport::{
    decode_append_entries_request, decode_append_entries_response, decode_install_snapshot_request,
    decode_install_snapshot_response, decode_vote_request, decode_vote_response,
    encode_append_entries_request, encode_append_entries_response, encode_install_snapshot_request,
    encode_install_snapshot_response, encode_vote_request, encode_vote_response,
    EncodedRaftNetworkFactory, EncodedRaftNetworkRegistry, MemoryRaftNetworkFactory,
    MemoryRaftNetworkRegistry, RaftTransportServer, TonicRaftNetworkFactory,
    TonicRaftTransportRegistry, TonicRaftTransportService,
};

openraft::declare_raft_types!(
    pub RaftStoreConfig:
        D = Proposal,
        R = AppliedProposal,
        NodeId = NodeId,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<RaftStoreConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub type OpenRaftEntry = openraft::Entry<RaftStoreConfig>;

#[cfg(test)]
mod tests;
