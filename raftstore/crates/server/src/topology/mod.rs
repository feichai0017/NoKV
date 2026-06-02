//! Topology publication helpers and peer endpoint configuration.

mod peer_endpoint_catalog;
mod publisher;
mod transition;

pub use peer_endpoint_catalog::PeerEndpointCatalog;
pub use publisher::{EmptyTopologyPublisher, TopologyPublishOutcome, TopologyPublisher};
pub use transition::root_event_transition_id;

pub(crate) use transition::{
    peer_change_transition_id, scheduler_operation_action, scheduler_operation_transition_id,
};
