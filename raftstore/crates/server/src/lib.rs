//! Tonic services for the Rust raftstore metadata data plane.
//!
//! This crate owns the external gRPC boundary for the metadata-native path.

mod admin;
mod admin_router;
mod admission;
mod admission_state;
mod diagnostics;
mod execution;
mod metadata;
mod metadata_plane;
mod metadata_router;
mod peer_endpoint_catalog;
mod publisher;
mod region_registry;
mod region_runtime;
mod serve;
mod topology;
mod watch_wire;

pub(crate) use admin::push_missing_topology_status;
pub use admin::RaftAdminService;
pub use admin_router::MultiRegionRaftAdminService;
pub use admission::RegionAdmission;
pub use diagnostics::{EmptyRestartDiagnostics, RestartDiagnosticsProvider};
pub use metadata::{
    apply_status_from_holt, EmptyRegionDescriptorSink, HoltRegionMetadataSink, RegionDescriptorSink,
};
pub use metadata_plane::MetadataPlaneService;
pub use metadata_router::MultiRegionMetadataPlaneService;
pub use nokv_proto::nokv::admin::v1::raft_admin_server::RaftAdminServer;
pub use nokv_proto::nokv::metadata::v1::metadata_plane_server::MetadataPlaneServer;
pub use peer_endpoint_catalog::PeerEndpointCatalog;
pub use publisher::{EmptyTopologyPublisher, TopologyPublishOutcome, TopologyPublisher};
pub use region_runtime::{
    AppliedRegionDescriptorProvider, EmptyApplyStatus, RaftMembershipAdmin, RaftRuntimeStatus,
    RaftRuntimeStatusProvider,
};
pub use serve::{openraft_metadata_service_pair, serve_with_metadata_region_services};
pub use topology::root_event_transition_id;

pub(crate) const DEFAULT_APPLY_WATCH_BUFFER: usize = 256;
pub(crate) const DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE: usize = 512;
pub(crate) const DEFAULT_APPLY_WATCH_MAX_KEY_BYTES_PER_MESSAGE: usize = 512 * 1024;

pub(crate) fn internal_error(err: nokv_metastore::Error) -> tonic::Status {
    tonic::Status::internal(err.to_string())
}

#[cfg(test)]
mod router_tests;
#[cfg(test)]
mod tests;
