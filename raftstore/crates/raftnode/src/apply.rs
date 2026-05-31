use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nokv_metastore::{MemoryMetadataStore, MetadataEngine, MetadataRetentionEngine};
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use tokio::sync::broadcast;

use crate::metadata::encode_metadata_response;
use crate::traffic::{RegionTrafficProvider, RegionTrafficSnapshot, RegionTrafficStats};
use crate::watch::{ApplyHistory, ApplyWatchProvider, ApplyWatchReplay, ApplyWatchReplayRequest};
use crate::{Error, OpenRaftEntry, ProposalPayloadKind, RegionId};

mod admin;
mod metadata;
mod snapshot;
mod types;

pub use types::{
    AppliedProposal, ApplyStatus, ApplyStatusProvider, MetadataCommandExecutor,
    MetadataReadExecutor, MetadataRetentionExecutor, RegionApplyEngine, RegionDescriptorCatalog,
    RegionMetadataSink, RegionSnapshotEngine,
};

#[derive(Debug)]
struct AppliedMetadataInner<E> {
    region_id: RegionId,
    term: AtomicU64,
    applied_index: AtomicU64,
    engine: Mutex<E>,
    descriptor: Mutex<Option<metapb::RegionDescriptor>>,
    topology_descriptors: Mutex<Vec<metapb::RegionDescriptor>>,
    topology_catalog: Mutex<Option<Arc<dyn RegionDescriptorCatalog>>>,
    traffic: RegionTrafficStats,
    watch: broadcast::Sender<metadatapb::MetadataApplyWatchEvent>,
    history: Mutex<ApplyHistory>,
}

/// Region-local metadata state-machine apply engine.
///
/// OpenRaft drives this engine through committed log entries; tests also use it
/// directly when Raft ordering is not the behavior under test. Reads go through
/// the current state-machine view, and writes advance the applied frontier under
/// the region apply mutex.
#[derive(Debug, Clone)]
pub struct AppliedMetadataEngine<E = MemoryMetadataStore> {
    inner: Arc<AppliedMetadataInner<E>>,
}

#[derive(Debug, Clone)]
pub struct PersistentAppliedMetadataEngine<E, S> {
    engine: AppliedMetadataEngine<E>,
    sink: S,
}

impl<E, S> PersistentAppliedMetadataEngine<E, S> {
    pub fn new(engine: AppliedMetadataEngine<E>, sink: S) -> Self {
        Self { engine, sink }
    }

    pub fn inner(&self) -> &AppliedMetadataEngine<E> {
        &self.engine
    }
}

impl<E> AppliedMetadataEngine<E> {
    pub fn new(region_id: RegionId, engine: E) -> Self {
        Self::with_status(
            ApplyStatus {
                region_id,
                term: 1,
                applied_index: 0,
            },
            engine,
        )
    }

    pub fn with_status(status: ApplyStatus, engine: E) -> Self {
        Self {
            inner: Arc::new(AppliedMetadataInner {
                region_id: status.region_id,
                term: AtomicU64::new(status.term),
                applied_index: AtomicU64::new(status.applied_index),
                engine: Mutex::new(engine),
                descriptor: Mutex::new(None),
                topology_descriptors: Mutex::new(Vec::new()),
                topology_catalog: Mutex::new(None),
                traffic: RegionTrafficStats::default(),
                watch: broadcast::channel(1024).0,
                history: Mutex::new(ApplyHistory::default()),
            }),
        }
    }

    pub fn set_region_descriptor(
        &self,
        descriptor: metapb::RegionDescriptor,
    ) -> nokv_metastore::Result<()> {
        self.validate_region_descriptor(&descriptor)?;
        let mut current = self.inner.descriptor.lock().map_err(|_| {
            nokv_metastore::Error::Backend("region descriptor mutex poisoned".to_owned())
        })?;
        *current = Some(descriptor);
        Ok(())
    }

    pub fn set_region_descriptor_catalog(
        &self,
        catalog: Arc<dyn RegionDescriptorCatalog>,
    ) -> nokv_metastore::Result<()> {
        let mut current = self.inner.topology_catalog.lock().map_err(|_| {
            nokv_metastore::Error::Backend("region descriptor catalog mutex poisoned".to_owned())
        })?;
        *current = Some(catalog);
        Ok(())
    }

    pub fn region_descriptor(&self) -> nokv_metastore::Result<Option<metapb::RegionDescriptor>> {
        self.inner
            .descriptor
            .lock()
            .map_err(|_| {
                nokv_metastore::Error::Backend("region descriptor mutex poisoned".to_owned())
            })
            .map(|descriptor| descriptor.clone())
    }

    pub(crate) fn record_topology_descriptor(
        &self,
        descriptor: metapb::RegionDescriptor,
    ) -> nokv_metastore::Result<()> {
        self.validate_topology_descriptor(&descriptor)?;
        self.inner
            .topology_descriptors
            .lock()
            .map_err(|_| {
                nokv_metastore::Error::Backend("topology descriptor mutex poisoned".to_owned())
            })?
            .push(descriptor);
        Ok(())
    }

    fn take_topology_descriptor(
        &self,
        region_id: RegionId,
    ) -> nokv_metastore::Result<Option<metapb::RegionDescriptor>> {
        let mut descriptors = self.inner.topology_descriptors.lock().map_err(|_| {
            nokv_metastore::Error::Backend("topology descriptor mutex poisoned".to_owned())
        })?;
        let Some(index) = descriptors
            .iter()
            .position(|descriptor| descriptor.region_id == region_id)
        else {
            return Ok(None);
        };
        Ok(Some(descriptors.remove(index)))
    }

    fn topology_descriptor(
        &self,
        region_id: RegionId,
    ) -> nokv_metastore::Result<Option<metapb::RegionDescriptor>> {
        let descriptor = {
            let descriptors = self.inner.topology_descriptors.lock().map_err(|_| {
                nokv_metastore::Error::Backend("topology descriptor mutex poisoned".to_owned())
            })?;
            descriptors
                .iter()
                .find(|descriptor| descriptor.region_id == region_id)
                .cloned()
        };
        if descriptor.is_some() {
            return Ok(descriptor);
        }
        let catalog = self
            .inner
            .topology_catalog
            .lock()
            .map_err(|_| {
                nokv_metastore::Error::Backend(
                    "region descriptor catalog mutex poisoned".to_owned(),
                )
            })?
            .clone();
        match catalog {
            Some(catalog) => catalog.region_descriptor(region_id),
            None => Ok(None),
        }
    }

    pub(crate) fn topology_descriptors(
        &self,
    ) -> nokv_metastore::Result<Vec<metapb::RegionDescriptor>> {
        self.inner
            .topology_descriptors
            .lock()
            .map_err(|_| {
                nokv_metastore::Error::Backend("topology descriptor mutex poisoned".to_owned())
            })
            .map(|descriptors| descriptors.clone())
    }

    fn clear_topology_descriptors(&self) -> nokv_metastore::Result<()> {
        self.inner
            .topology_descriptors
            .lock()
            .map_err(|_| {
                nokv_metastore::Error::Backend("topology descriptor mutex poisoned".to_owned())
            })?
            .clear();
        Ok(())
    }

    fn validate_region_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> nokv_metastore::Result<()> {
        if descriptor.region_id != self.inner.region_id {
            return Err(nokv_metastore::Error::Backend(
                Error::LogRegionMismatch {
                    record_region_id: self.inner.region_id,
                    proposal_region_id: descriptor.region_id,
                }
                .to_string(),
            ));
        }
        if descriptor.epoch.is_none() {
            return Err(nokv_metastore::Error::Backend(
                Error::InvalidRegionDescriptor("region descriptor epoch is required".to_owned())
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn validate_topology_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> nokv_metastore::Result<()> {
        if descriptor.region_id == 0 {
            return Err(nokv_metastore::Error::Backend(
                "topology descriptor region id is required".to_owned(),
            ));
        }
        if descriptor.peers.is_empty() {
            return Err(nokv_metastore::Error::Backend(format!(
                "topology descriptor for region {} has no peers",
                descriptor.region_id
            )));
        }
        Ok(())
    }

    pub fn status(&self) -> ApplyStatus {
        ApplyStatus {
            region_id: self.inner.region_id,
            term: self.inner.term.load(Ordering::Acquire),
            applied_index: self.inner.applied_index.load(Ordering::Acquire),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<metadatapb::MetadataApplyWatchEvent> {
        self.inner.watch.subscribe()
    }

    pub fn replay_apply(
        &self,
        request: ApplyWatchReplayRequest,
    ) -> nokv_metastore::Result<ApplyWatchReplay> {
        if request.region_id != 0 && request.region_id != self.inner.region_id {
            return Ok(ApplyWatchReplay {
                events: Vec::new(),
                expired: true,
            });
        }
        let history = self.inner.history.lock().map_err(|_| {
            nokv_metastore::Error::Backend("apply history mutex poisoned".to_owned())
        })?;
        Ok(history.replay(&request, self.inner.applied_index.load(Ordering::Acquire)))
    }

    pub fn traffic_snapshot(&self) -> RegionTrafficSnapshot {
        self.inner.traffic.snapshot()
    }
}

impl<E> ApplyStatusProvider for AppliedMetadataEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn apply_status(&self) -> ApplyStatus {
        self.status()
    }
}

impl<E, S> ApplyStatusProvider for PersistentAppliedMetadataEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: RegionMetadataSink,
{
    fn apply_status(&self) -> ApplyStatus {
        self.engine.status()
    }
}

impl<E> RegionTrafficProvider for AppliedMetadataEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn traffic_snapshot(&self) -> RegionTrafficSnapshot {
        self.traffic_snapshot()
    }
}

impl<E, S> RegionTrafficProvider for PersistentAppliedMetadataEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: RegionMetadataSink,
{
    fn traffic_snapshot(&self) -> RegionTrafficSnapshot {
        self.engine.traffic_snapshot()
    }
}

impl<E> ApplyWatchProvider for AppliedMetadataEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn subscribe_apply(&self) -> broadcast::Receiver<metadatapb::MetadataApplyWatchEvent> {
        self.subscribe()
    }

    fn replay_apply(
        &self,
        request: ApplyWatchReplayRequest,
    ) -> nokv_metastore::Result<ApplyWatchReplay> {
        self.replay_apply(request)
    }
}

impl<E, S> ApplyWatchProvider for PersistentAppliedMetadataEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: RegionMetadataSink,
{
    fn subscribe_apply(&self) -> broadcast::Receiver<metadatapb::MetadataApplyWatchEvent> {
        self.engine.subscribe()
    }

    fn replay_apply(
        &self,
        request: ApplyWatchReplayRequest,
    ) -> nokv_metastore::Result<ApplyWatchReplay> {
        let replay = self.engine.replay_apply(request.clone())?;
        if !replay.expired {
            return Ok(replay);
        }
        self.sink
            .replay_apply_watch(&request)?
            .map(Ok)
            .unwrap_or(Ok(replay))
    }
}

impl<E> AppliedMetadataEngine<E>
where
    E: MetadataEngine,
{
    pub fn apply_openraft_entries<I>(
        &self,
        entries: I,
    ) -> nokv_metastore::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>,
    {
        let mut applied = Vec::new();
        for entry in entries {
            let index = entry.log_id.index;
            let term = entry.log_id.leader_id.term;
            match entry.payload {
                openraft::EntryPayload::Blank | openraft::EntryPayload::Membership(_) => {
                    self.record_applied_status(term, index);
                    applied.push(AppliedProposal {
                        region_id: self.inner.region_id,
                        index,
                        term,
                        payload: Vec::new(),
                        descriptor_changed: false,
                    });
                }
                openraft::EntryPayload::Normal(proposal) => {
                    if proposal.region_id != self.inner.region_id {
                        return Err(nokv_metastore::Error::Backend(
                            Error::LogRegionMismatch {
                                record_region_id: self.inner.region_id,
                                proposal_region_id: proposal.region_id,
                            }
                            .to_string(),
                        ));
                    }
                    match proposal.payload_kind() {
                        ProposalPayloadKind::MetadataCommand => {
                            let req = proposal
                                .decode_metadata_command()
                                .map_err(|err| nokv_metastore::Error::Backend(err.to_string()))?;
                            let response =
                                self.execute_metadata_command_at(&req, Some((term, index)))?;
                            applied.push(AppliedProposal {
                                region_id: proposal.region_id,
                                index,
                                term,
                                payload: encode_metadata_response(&response)?,
                                descriptor_changed: false,
                            });
                        }
                        ProposalPayloadKind::RegionDescriptor => {
                            let descriptor = proposal
                                .decode_region_descriptor()
                                .map_err(|err| nokv_metastore::Error::Backend(err.to_string()))?;
                            self.apply_region_descriptor_at(term, index, descriptor)?;
                            applied.push(AppliedProposal {
                                region_id: proposal.region_id,
                                index,
                                term,
                                payload: Vec::new(),
                                descriptor_changed: true,
                            });
                        }
                        ProposalPayloadKind::AdminCommand => {
                            let command = proposal
                                .decode_admin_command()
                                .map_err(|err| nokv_metastore::Error::Backend(err.to_string()))?;
                            self.apply_admin_command_at(term, index, command)?;
                            applied.push(AppliedProposal {
                                region_id: proposal.region_id,
                                index,
                                term,
                                payload: Vec::new(),
                                descriptor_changed: true,
                            });
                        }
                    }
                }
            }
        }
        Ok(applied)
    }

    fn apply_region_descriptor_at(
        &self,
        term: u64,
        index: u64,
        descriptor: metapb::RegionDescriptor,
    ) -> nokv_metastore::Result<()> {
        self.set_region_descriptor(descriptor)?;
        self.record_applied_status(term, index);
        Ok(())
    }

    fn advance_apply_index(&self) -> (u64, u64) {
        let index = self.inner.applied_index.fetch_add(1, Ordering::AcqRel) + 1;
        let term = self.inner.term.load(Ordering::Acquire);
        (term, index)
    }

    fn record_applied_status(&self, term: u64, index: u64) {
        self.inner.term.store(term, Ordering::Release);
        let mut current = self.inner.applied_index.load(Ordering::Acquire);
        while current < index {
            match self.inner.applied_index.compare_exchange(
                current,
                index,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    fn publish_apply(
        &self,
        index: u64,
        term: u64,
        source: metadatapb::MetadataApplyWatchEventSource,
        commit_version: u64,
        keys: Vec<Vec<u8>>,
        atomic: bool,
    ) {
        if keys.is_empty() {
            return;
        }
        self.inner.traffic.record_apply(&keys, atomic);
        let event = metadatapb::MetadataApplyWatchEvent {
            region_id: self.inner.region_id,
            term,
            index,
            source: source as i32,
            commit_version,
            keys,
        };
        if let Ok(mut history) = self.inner.history.lock() {
            history.remember(event.clone());
        }
        let _ = self.inner.watch.send(event);
    }
}

impl<E> MetadataRetentionExecutor for AppliedMetadataEngine<E>
where
    E: MetadataRetentionEngine,
{
    fn prune_metadata_versions<'a>(
        &'a self,
        retention_floor: u64,
    ) -> impl std::future::Future<
        Output = nokv_metastore::Result<nokv_metastore::MetadataRetentionResult>,
    > + Send
           + 'a {
        async move {
            let engine = self.inner.engine.lock().map_err(|_| {
                nokv_metastore::Error::Backend("region apply mutex poisoned".to_owned())
            })?;
            engine.prune_metadata_versions(retention_floor)
        }
    }
}

impl<E, S> PersistentAppliedMetadataEngine<E, S>
where
    E: MetadataEngine,
    S: RegionMetadataSink,
{
    pub fn apply_openraft_entries<I>(
        &self,
        entries: I,
    ) -> nokv_metastore::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>,
    {
        let before = self.engine.status().applied_index;
        let applied = self.engine.apply_openraft_entries(entries)?;
        let descriptor_changed = applied.iter().any(|proposal| proposal.descriptor_changed);
        self.persist_if_advanced(before, descriptor_changed)?;
        Ok(applied)
    }

    fn persist_if_advanced(
        &self,
        before: u64,
        descriptor_changed: bool,
    ) -> nokv_metastore::Result<()> {
        let status = self.engine.status();
        if status.applied_index != before {
            let replay = self.engine.replay_apply(ApplyWatchReplayRequest {
                region_id: status.region_id,
                term: 0,
                index: before,
                key_prefix: Vec::new(),
            })?;
            for event in &replay.events {
                self.sink.save_apply_watch_event(event)?;
            }
            if descriptor_changed {
                if let Some(descriptor) = self.engine.region_descriptor()? {
                    self.sink.save_region_descriptor(&descriptor)?;
                }
            }
            let topology_descriptors = self.engine.topology_descriptors()?;
            for descriptor in &topology_descriptors {
                self.sink.save_region_descriptor(descriptor)?;
            }
            if !topology_descriptors.is_empty() {
                self.engine.clear_topology_descriptors()?;
            }
            self.sink.save_apply_status(&status)?;
        }
        Ok(())
    }
}

impl<E> RegionApplyEngine for AppliedMetadataEngine<E>
where
    E: MetadataEngine,
{
    fn apply_openraft_entries<I>(&self, entries: I) -> nokv_metastore::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>,
    {
        self.apply_openraft_entries(entries)
    }
}

impl<E, S> RegionApplyEngine for PersistentAppliedMetadataEngine<E, S>
where
    E: MetadataEngine,
    S: RegionMetadataSink,
{
    fn apply_openraft_entries<I>(&self, entries: I) -> nokv_metastore::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>,
    {
        self.apply_openraft_entries(entries)
    }
}

impl<E, S> MetadataRetentionExecutor for PersistentAppliedMetadataEngine<E, S>
where
    E: MetadataRetentionEngine,
    S: RegionMetadataSink,
{
    fn prune_metadata_versions<'a>(
        &'a self,
        retention_floor: u64,
    ) -> impl std::future::Future<
        Output = nokv_metastore::Result<nokv_metastore::MetadataRetentionResult>,
    > + Send
           + 'a {
        async move { self.engine.prune_metadata_versions(retention_floor).await }
    }
}

fn invalid_raft_command(detail: &str) -> nokv_metastore::Error {
    nokv_metastore::Error::Backend(format!("invalid raft command: {detail}"))
}
