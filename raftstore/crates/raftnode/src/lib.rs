//! OpenRaft boundary for Rust raftstore.
//!
//! The crate intentionally exposes a small NoKV-owned trait instead of leaking
//! OpenRaft types into server, MVCC, or proto-facing code. The concrete
//! OpenRaft-backed implementation will fill this boundary as region replication
//! is brought up.

pub type NodeId = u64;
pub type RegionId = u64;

mod apply;
mod error;
mod log_codec;
mod log_store;
mod network;
mod proposal;
mod region;
mod region_storage;
mod snapshot;
mod tonic_transport;
mod transport_codec;

pub(crate) use apply::decode_metadata_response;
pub(crate) use apply::decode_raft_response;
pub use apply::{
    AppliedKvEngine, AppliedProposal, ApplyStatus, ApplyStatusProvider, ApplyWatchProvider,
    ApplyWatchReplay, ApplyWatchReplayRequest, MetadataCommandExecutor, MetadataReadExecutor,
    PersistentAppliedKvEngine, RaftCommandExecutor, RegionApplyEngine, RegionDescriptorCatalog,
    RegionMetadataSink, RegionSnapshotEngine, RegionTrafficProvider, RegionTrafficSnapshot,
};
pub use error::Error;
pub use log_codec::{decode_log_entry, encode_log_entry};
pub use log_store::{RaftEntryLog, SegmentedEntryLog};
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
pub use transport_codec::{
    decode_append_entries_request, decode_append_entries_response, decode_install_snapshot_request,
    decode_install_snapshot_response, decode_vote_request, decode_vote_response,
    encode_append_entries_request, encode_append_entries_response, encode_install_snapshot_request,
    encode_install_snapshot_response, encode_vote_request, encode_vote_response,
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
