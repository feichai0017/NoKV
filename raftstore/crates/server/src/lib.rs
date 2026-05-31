//! Tonic services for the Rust raftstore data plane.
//!
//! This crate owns the external gRPC boundary. It keeps the existing NoKV
//! protobuf contract intact while the Rust state-machine and replication layers
//! are brought up behind the service.

mod admin;
mod admission;
mod diagnostics;
mod execution;
mod metadata;
mod metadata_plane;
mod publisher;
mod region_router;
mod serve;
mod service;
mod topology;

pub(crate) use admin::push_missing_topology_status;
pub use admin::{
    AppliedRegionDescriptorProvider, EmptyApplyStatus, PeerEndpointCatalog, RaftAdminService,
    RaftMembershipAdmin, RaftRuntimeStatus, RaftRuntimeStatusProvider,
};
pub use admission::RegionAdmission;
pub use diagnostics::{EmptyRestartDiagnostics, RestartDiagnosticsProvider};
pub use metadata::{
    apply_status_from_holt, EmptyRegionDescriptorSink, HoltRegionMetadataSink, RegionDescriptorSink,
};
pub use metadata_plane::MetadataPlaneService;
pub use nokv_proto::nokv::admin::v1::raft_admin_server::RaftAdminServer;
pub use nokv_proto::nokv::kv::v1::store_kv_server::StoreKvServer;
pub use nokv_proto::nokv::metadata::v1::metadata_plane_server::MetadataPlaneServer;
pub use publisher::{EmptyTopologyPublisher, TopologyPublishOutcome, TopologyPublisher};
pub use region_router::{
    serve_with_multi_region_services, MultiRegionMetadataPlaneService, MultiRegionRaftAdminService,
    MultiRegionStoreKvService,
};
pub use serve::{
    openraft_region_service_pair, serve_with_openraft_region_admission_and_peer_endpoints,
    serve_with_openraft_region_admission_peer_endpoints_descriptor_sink_topology_publisher_and_restart_diagnostics,
};
pub use service::StoreKvService;
pub use topology::root_event_transition_id;

pub(crate) const DEFAULT_APPLY_WATCH_BUFFER: usize = 256;
pub(crate) const DEFAULT_APPLY_WATCH_MAX_KEYS_PER_MESSAGE: usize = 512;
pub(crate) const DEFAULT_APPLY_WATCH_MAX_KEY_BYTES_PER_MESSAGE: usize = 512 * 1024;

pub(crate) fn internal_error(err: nokv_mvcc::Error) -> tonic::Status {
    tonic::Status::internal(err.to_string())
}

#[cfg(test)]
mod tests;
