use nokv_holtstore::{HoltMetadataStore, RegionApplyState, DEFAULT_WATCH_APPLY_REPLAY_LIMIT};
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_raftnode::{ApplyWatchReplay, ApplyWatchReplayRequest, RegionMetadataSink};
use tonic::Status;

/// Persists OpenRaft region metadata into Holt metadata trees.
#[derive(Clone)]
pub struct HoltRegionMetadataSink {
    store: HoltMetadataStore,
}

impl HoltRegionMetadataSink {
    pub fn new(store: HoltMetadataStore) -> Self {
        Self { store }
    }
}

impl RegionMetadataSink for HoltRegionMetadataSink {
    fn save_apply_status(&self, status: &nokv_raftnode::ApplyStatus) -> nokv_metastore::Result<()> {
        self.store
            .put_region_apply_state(&RegionApplyState {
                region_id: status.region_id,
                term: status.term,
                applied_index: status.applied_index,
                truncated_term: 0,
                truncated_index: 0,
            })
            .map_err(|err| nokv_metastore::Error::Backend(err.to_string()))
    }

    fn save_apply_watch_event(
        &self,
        event: &metadatapb::MetadataApplyWatchEvent,
    ) -> nokv_metastore::Result<()> {
        self.store
            .put_watch_apply_event(event)
            .map_err(|err| nokv_metastore::Error::Backend(err.to_string()))
    }

    fn replay_apply_watch(
        &self,
        request: &ApplyWatchReplayRequest,
    ) -> nokv_metastore::Result<Option<ApplyWatchReplay>> {
        if request.region_id == 0 {
            return Ok(Some(ApplyWatchReplay::default()));
        }
        let first = self
            .store
            .first_watch_apply_event(request.region_id)
            .map_err(|err| nokv_metastore::Error::Backend(err.to_string()))?;
        if first.is_none() {
            return Ok(None);
        }
        let events = self
            .store
            .watch_apply_events_after(
                request.region_id,
                request.term,
                request.index,
                &request.key_prefix,
                DEFAULT_WATCH_APPLY_REPLAY_LIMIT,
            )
            .map_err(|err| nokv_metastore::Error::Backend(err.to_string()))?;
        Ok(Some(ApplyWatchReplay {
            events,
            expired: false,
        }))
    }

    fn save_region_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> nokv_metastore::Result<()> {
        self.store
            .put_region_descriptor(descriptor)
            .and_then(|_| self.store.checkpoint())
            .map_err(|err| nokv_metastore::Error::Backend(err.to_string()))
    }
}

impl RegionDescriptorSink for HoltRegionMetadataSink {
    fn save_region_descriptor(&self, descriptor: &metapb::RegionDescriptor) -> Result<(), Status> {
        self.store
            .put_region_descriptor(descriptor)
            .and_then(|_| self.store.checkpoint())
            .map_err(|err| Status::internal(err.to_string()))
    }
}

pub trait RegionDescriptorSink: Clone + Send + Sync + 'static {
    fn save_region_descriptor(&self, descriptor: &metapb::RegionDescriptor) -> Result<(), Status>;
}

#[derive(Clone, Debug, Default)]
pub struct EmptyRegionDescriptorSink;

impl RegionDescriptorSink for EmptyRegionDescriptorSink {
    fn save_region_descriptor(&self, _descriptor: &metapb::RegionDescriptor) -> Result<(), Status> {
        Ok(())
    }
}

pub fn apply_status_from_holt(state: RegionApplyState) -> nokv_raftnode::ApplyStatus {
    nokv_raftnode::ApplyStatus {
        region_id: state.region_id,
        term: state.term,
        applied_index: state.applied_index,
    }
}
