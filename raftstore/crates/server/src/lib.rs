//! Tonic services for the Rust raftstore metadata data plane.
//!
//! This crate owns the external gRPC boundary for the metadata-native path.

mod admin;
mod admission;
mod diagnostics;
mod execution;
mod metadata;
mod region;
mod serve;
mod topology;

pub(crate) use admin::push_missing_topology_status;
pub use admin::{MultiRegionRaftAdminService, RaftAdminService};
pub use admission::RegionAdmission;
pub use diagnostics::{EmptyRestartDiagnostics, RestartDiagnosticsProvider};
pub use metadata::{
    metadata_batch_metrics_snapshot, MetadataBatchMetricsSnapshot, MetadataPlaneService,
    MultiRegionMetadataPlaneService,
};
pub use nokv_proto::nokv::admin::v1::raft_admin_server::RaftAdminServer;
pub use nokv_proto::nokv::metadata::v1::metadata_plane_server::MetadataPlaneServer;
pub use region::{
    apply_status_from_holt, AppliedRegionDescriptorProvider, EmptyApplyStatus,
    EmptyRegionDescriptorSink, HoltRegionMetadataSink, RaftMembershipAdmin, RaftRuntimeStatus,
    RaftRuntimeStatusProvider, RegionDescriptorSink,
};
pub use serve::{openraft_metadata_service_pair, serve_with_metadata_region_services};
pub use topology::{
    root_event_transition_id, EmptyTopologyPublisher, PeerEndpointCatalog, TopologyPublishOutcome,
    TopologyPublisher,
};

pub(crate) const DEFAULT_APPLY_WATCH_BUFFER: usize = 256;
pub(crate) const DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE: usize = 512;
pub(crate) const DEFAULT_APPLY_WATCH_MAX_KEY_BYTES_PER_MESSAGE: usize = 512 * 1024;

pub(crate) fn internal_error(err: nokv_metadata_state::Error) -> tonic::Status {
    tonic::Status::internal(err.to_string())
}

#[cfg(test)]
mod router_tests;
#[cfg(test)]
mod tests;
