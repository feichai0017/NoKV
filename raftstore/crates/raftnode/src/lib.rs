//! OpenRaft boundary for Rust raftstore.
//!
//! The crate intentionally exposes a small NoKV-owned trait instead of leaking
//! OpenRaft types into server, metadata store, or proto-facing code. The concrete
//! OpenRaft-backed implementation stays behind this boundary.

pub type NodeId = u64;
pub type RegionId = u64;

mod apply;
mod error;
mod log_codec;
mod log_store;
mod metadata;
mod network;
mod proposal;
mod region;
mod region_storage;
mod snapshot;
mod tonic_transport;
mod traffic;
mod transport_codec;
mod watch;

pub use apply::{
    AppliedMetadataEngine, AppliedProposal, ApplyStatus, ApplyStatusProvider,
    MetadataCommandExecutor, MetadataReadExecutor, PersistentAppliedMetadataEngine,
    RegionApplyEngine, RegionDescriptorCatalog, RegionMetadataSink, RegionSnapshotEngine,
};
pub use error::Error;
pub use log_codec::{decode_log_entry, encode_log_entry};
pub use log_store::{RaftEntryLog, SegmentedEntryLog};
pub(crate) use metadata::decode_metadata_response;
pub use network::{
    EncodedRaftNetworkFactory, EncodedRaftNetworkRegistry, MemoryRaftNetworkFactory,
    MemoryRaftNetworkRegistry,
};
pub use openraft::BasicNode;
pub(crate) use proposal::ProposalPayloadKind;
pub use proposal::{Proposal, ProposalPayload};
pub use region::OpenRaftRegion;
pub use region_storage::{RegionLogStorage, RegionSnapshotBuilder, RegionStateMachine};
pub(crate) use snapshot::decode_region_snapshot_status;
pub use tonic_transport::{
    RaftTransportServer, TonicRaftNetworkFactory, TonicRaftTransportRegistry,
    TonicRaftTransportService,
};
pub use traffic::{RegionTrafficProvider, RegionTrafficSnapshot};
pub use transport_codec::{
    decode_append_entries_request, decode_append_entries_response, decode_install_snapshot_request,
    decode_install_snapshot_response, decode_vote_request, decode_vote_response,
    encode_append_entries_request, encode_append_entries_response, encode_install_snapshot_request,
    encode_install_snapshot_response, encode_vote_request, encode_vote_response,
};
pub use watch::{ApplyWatchProvider, ApplyWatchReplay, ApplyWatchReplayRequest};

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
