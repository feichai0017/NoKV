//! MetadataPlane request handling, routing, batching, and watch encoding.

mod batcher;
mod plane;
mod router;
mod watch;
mod watch_wire;

pub use batcher::{metadata_batch_metrics_snapshot, MetadataBatchMetricsSnapshot};
pub use plane::MetadataPlaneService;
pub use router::MultiRegionMetadataPlaneService;

pub(crate) use batcher::MetadataCommitBatcher;
pub(crate) use watch::{metadata_watch_apply_stream, MetadataWatchApplyStream};
pub(crate) use watch_wire::{
    chunk_apply_watch_keys, matching_apply_watch_events, matching_apply_watch_keys,
    watch_events_for_keys,
};
