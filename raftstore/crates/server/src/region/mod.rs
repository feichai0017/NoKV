//! Region-local runtime status, service registry, and descriptor persistence.

mod descriptor_sink;
mod registry;
mod runtime;

pub use descriptor_sink::{
    apply_status_from_holt, EmptyRegionDescriptorSink, HoltRegionMetadataSink, RegionDescriptorSink,
};
pub use runtime::{
    AppliedRegionDescriptorProvider, EmptyApplyStatus, RaftMembershipAdmin, RaftRuntimeStatus,
    RaftRuntimeStatusProvider,
};

pub(crate) use registry::RegionServiceRegistry;
pub(crate) use registry::{admin_region_lookup, metadata_region_lookup, RegionServiceLookup};
