use nokv_proto::nokv::admin::v1 as adminpb;
use nokv_proto::nokv::meta::v1 as metapb;

#[tonic::async_trait]
pub trait TopologyPublisher: Send + Sync + 'static {
    async fn publish_peer_added(
        &self,
        _region_id: u64,
        _store_id: u64,
        _peer_id: u64,
        _region: &metapb::RegionDescriptor,
    ) -> TopologyPublishOutcome {
        TopologyPublishOutcome::not_required()
    }

    async fn publish_peer_removed(
        &self,
        _region_id: u64,
        _store_id: u64,
        _peer_id: u64,
        _region: &metapb::RegionDescriptor,
    ) -> TopologyPublishOutcome {
        TopologyPublishOutcome::not_required()
    }
}

#[derive(Debug, Clone)]
pub struct TopologyPublishOutcome {
    pub(crate) publish: adminpb::ExecutionPublishState,
    pub(crate) last_error: String,
}

impl TopologyPublishOutcome {
    pub fn publish_state(&self) -> adminpb::ExecutionPublishState {
        self.publish
    }

    pub fn last_error(&self) -> &str {
        &self.last_error
    }

    pub fn not_required() -> Self {
        Self {
            publish: adminpb::ExecutionPublishState::NotRequired,
            last_error: String::new(),
        }
    }

    pub fn terminal_published() -> Self {
        Self {
            publish: adminpb::ExecutionPublishState::TerminalPublished,
            last_error: String::new(),
        }
    }

    pub fn terminal_pending(error: impl Into<String>) -> Self {
        Self {
            publish: adminpb::ExecutionPublishState::TerminalPending,
            last_error: error.into(),
        }
    }

    pub fn terminal_failed(error: impl Into<String>) -> Self {
        Self {
            publish: adminpb::ExecutionPublishState::TerminalFailed,
            last_error: error.into(),
        }
    }

    pub fn terminal_blocked(error: impl Into<String>) -> Self {
        Self {
            publish: adminpb::ExecutionPublishState::TerminalBlocked,
            last_error: error.into(),
        }
    }
}

#[derive(Debug, Default)]
pub struct EmptyTopologyPublisher;

#[tonic::async_trait]
impl TopologyPublisher for EmptyTopologyPublisher {}
