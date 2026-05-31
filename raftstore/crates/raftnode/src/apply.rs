use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use nokv_mvcc::{KvEngine, MvccSnapshotEngine, MvccStore};
use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_proto::nokv::raft::v1 as raftpb;
use prost::Message;
use tokio::sync::broadcast;

use crate::{Error, OpenRaftEntry, ProposalPayloadKind, RegionId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedProposal {
    pub region_id: RegionId,
    pub index: u64,
    pub term: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyStatus {
    pub region_id: RegionId,
    pub term: u64,
    pub applied_index: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegionTrafficSnapshot {
    pub read_ops: u64,
    pub write_ops: u64,
    pub write_bytes: u64,
    pub atomic_ops: u64,
    pub elapsed_secs: u64,
}

pub trait ApplyStatusProvider: Clone + Send + Sync + 'static {
    fn apply_status(&self) -> ApplyStatus;
}

pub trait RegionTrafficProvider: Clone + Send + Sync + 'static {
    fn traffic_snapshot(&self) -> RegionTrafficSnapshot;
}

pub trait ApplyWatchProvider: Clone + Send + Sync + 'static {
    fn subscribe_apply(&self) -> broadcast::Receiver<kvpb::ApplyWatchEvent>;
}

pub trait RaftCommandExecutor: Clone + Send + Sync + 'static {
    fn execute_raft_command<'a>(
        &'a self,
        req: &'a raftpb::RaftCmdRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<raftpb::RaftCmdResponse>> + Send + 'a;
}

pub trait RegionApplyEngine: ApplyStatusProvider + ApplyWatchProvider {
    fn apply_openraft_entries<I>(&self, entries: I) -> nokv_mvcc::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>;
}

pub trait RegionSnapshotEngine: RegionApplyEngine {
    fn region_descriptor(&self) -> nokv_mvcc::Result<Option<metapb::RegionDescriptor>>;

    fn export_region_snapshot(&self) -> nokv_mvcc::Result<Vec<u8>>;
    fn install_region_snapshot(&self, snapshot: &[u8]) -> nokv_mvcc::Result<ApplyStatus>;
}

pub trait RegionMetadataSink: Clone + Send + Sync + 'static {
    fn save_apply_status(&self, status: &ApplyStatus) -> nokv_mvcc::Result<()>;

    fn save_region_descriptor(
        &self,
        _descriptor: &metapb::RegionDescriptor,
    ) -> nokv_mvcc::Result<()> {
        Ok(())
    }
}

pub trait RegionDescriptorCatalog: std::fmt::Debug + Send + Sync + 'static {
    fn region_descriptor(
        &self,
        region_id: RegionId,
    ) -> nokv_mvcc::Result<Option<metapb::RegionDescriptor>>;
}

#[derive(Debug)]
struct AppliedKvInner<E> {
    region_id: RegionId,
    term: AtomicU64,
    applied_index: AtomicU64,
    engine: Mutex<E>,
    descriptor: Mutex<Option<metapb::RegionDescriptor>>,
    topology_descriptors: Mutex<Vec<metapb::RegionDescriptor>>,
    topology_catalog: Mutex<Option<Arc<dyn RegionDescriptorCatalog>>>,
    traffic: RegionTrafficStats,
    watch: broadcast::Sender<kvpb::ApplyWatchEvent>,
}

#[derive(Debug)]
struct ApplyEvent {
    source: kvpb::ApplyWatchEventSource,
    commit_version: u64,
    keys: Vec<Vec<u8>>,
    atomic: bool,
}

#[derive(Debug)]
struct RegionTrafficStats {
    inner: Mutex<RegionTrafficInner>,
}

#[derive(Debug)]
struct RegionTrafficInner {
    last: Instant,
    read_ops: u64,
    write_ops: u64,
    write_bytes: u64,
    atomic_ops: u64,
}

impl Default for RegionTrafficStats {
    fn default() -> Self {
        Self {
            inner: Mutex::new(RegionTrafficInner {
                last: Instant::now(),
                read_ops: 0,
                write_ops: 0,
                write_bytes: 0,
                atomic_ops: 0,
            }),
        }
    }
}

impl RegionTrafficStats {
    fn record_read(&self, ops: u64) {
        if ops == 0 {
            return;
        }
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.read_ops = inner.read_ops.saturating_add(ops);
    }

    fn record_apply(&self, keys: &[Vec<u8>], atomic: bool) {
        if keys.is_empty() {
            return;
        }
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.write_ops = inner.write_ops.saturating_add(1);
        inner.write_bytes = inner
            .write_bytes
            .saturating_add(keys.iter().map(|key| key.len() as u64).sum::<u64>());
        if atomic {
            inner.atomic_ops = inner.atomic_ops.saturating_add(1);
        }
    }

    fn snapshot(&self) -> RegionTrafficSnapshot {
        let now = Instant::now();
        let Ok(mut inner) = self.inner.lock() else {
            return RegionTrafficSnapshot {
                elapsed_secs: 1,
                ..Default::default()
            };
        };
        let elapsed_secs = now.duration_since(inner.last).as_secs().max(1);
        let snapshot = RegionTrafficSnapshot {
            read_ops: inner.read_ops,
            write_ops: inner.write_ops,
            write_bytes: inner.write_bytes,
            atomic_ops: inner.atomic_ops,
            elapsed_secs,
        };
        inner.last = now;
        inner.read_ops = 0;
        inner.write_ops = 0;
        inner.write_bytes = 0;
        inner.atomic_ops = 0;
        snapshot
    }
}

/// Region-local apply boundary used before the OpenRaft-backed implementation
/// is complete. Reads go through the current state-machine view; writes advance
/// a monotonically increasing applied index under the region apply mutex.
#[derive(Debug, Clone)]
pub struct AppliedKvEngine<E = MvccStore> {
    inner: Arc<AppliedKvInner<E>>,
}

#[derive(Debug, Clone)]
pub struct PersistentAppliedKvEngine<E, S> {
    engine: AppliedKvEngine<E>,
    sink: S,
}

impl<E, S> PersistentAppliedKvEngine<E, S> {
    pub fn new(engine: AppliedKvEngine<E>, sink: S) -> Self {
        Self { engine, sink }
    }

    pub fn inner(&self) -> &AppliedKvEngine<E> {
        &self.engine
    }
}

impl<E> AppliedKvEngine<E> {
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
            inner: Arc::new(AppliedKvInner {
                region_id: status.region_id,
                term: AtomicU64::new(status.term),
                applied_index: AtomicU64::new(status.applied_index),
                engine: Mutex::new(engine),
                descriptor: Mutex::new(None),
                topology_descriptors: Mutex::new(Vec::new()),
                topology_catalog: Mutex::new(None),
                traffic: RegionTrafficStats::default(),
                watch: broadcast::channel(1024).0,
            }),
        }
    }

    pub fn set_region_descriptor(
        &self,
        descriptor: metapb::RegionDescriptor,
    ) -> nokv_mvcc::Result<()> {
        self.validate_region_descriptor(&descriptor)?;
        let mut current = self.inner.descriptor.lock().map_err(|_| {
            nokv_mvcc::Error::Backend("region descriptor mutex poisoned".to_owned())
        })?;
        *current = Some(descriptor);
        Ok(())
    }

    pub fn set_region_descriptor_catalog(
        &self,
        catalog: Arc<dyn RegionDescriptorCatalog>,
    ) -> nokv_mvcc::Result<()> {
        let mut current = self.inner.topology_catalog.lock().map_err(|_| {
            nokv_mvcc::Error::Backend("region descriptor catalog mutex poisoned".to_owned())
        })?;
        *current = Some(catalog);
        Ok(())
    }

    pub fn region_descriptor(&self) -> nokv_mvcc::Result<Option<metapb::RegionDescriptor>> {
        self.inner
            .descriptor
            .lock()
            .map_err(|_| nokv_mvcc::Error::Backend("region descriptor mutex poisoned".to_owned()))
            .map(|descriptor| descriptor.clone())
    }

    pub(crate) fn record_topology_descriptor(
        &self,
        descriptor: metapb::RegionDescriptor,
    ) -> nokv_mvcc::Result<()> {
        self.validate_topology_descriptor(&descriptor)?;
        self.inner
            .topology_descriptors
            .lock()
            .map_err(|_| {
                nokv_mvcc::Error::Backend("topology descriptor mutex poisoned".to_owned())
            })?
            .push(descriptor);
        Ok(())
    }

    fn take_topology_descriptor(
        &self,
        region_id: RegionId,
    ) -> nokv_mvcc::Result<Option<metapb::RegionDescriptor>> {
        let mut descriptors = self.inner.topology_descriptors.lock().map_err(|_| {
            nokv_mvcc::Error::Backend("topology descriptor mutex poisoned".to_owned())
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
    ) -> nokv_mvcc::Result<Option<metapb::RegionDescriptor>> {
        let descriptor = {
            let descriptors = self.inner.topology_descriptors.lock().map_err(|_| {
                nokv_mvcc::Error::Backend("topology descriptor mutex poisoned".to_owned())
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
                nokv_mvcc::Error::Backend("region descriptor catalog mutex poisoned".to_owned())
            })?
            .clone();
        match catalog {
            Some(catalog) => catalog.region_descriptor(region_id),
            None => Ok(None),
        }
    }

    pub(crate) fn topology_descriptors(&self) -> nokv_mvcc::Result<Vec<metapb::RegionDescriptor>> {
        self.inner
            .topology_descriptors
            .lock()
            .map_err(|_| nokv_mvcc::Error::Backend("topology descriptor mutex poisoned".to_owned()))
            .map(|descriptors| descriptors.clone())
    }

    fn clear_topology_descriptors(&self) -> nokv_mvcc::Result<()> {
        self.inner
            .topology_descriptors
            .lock()
            .map_err(|_| {
                nokv_mvcc::Error::Backend("topology descriptor mutex poisoned".to_owned())
            })?
            .clear();
        Ok(())
    }

    fn validate_region_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> nokv_mvcc::Result<()> {
        if descriptor.region_id != self.inner.region_id {
            return Err(nokv_mvcc::Error::Backend(
                Error::LogRegionMismatch {
                    record_region_id: self.inner.region_id,
                    proposal_region_id: descriptor.region_id,
                }
                .to_string(),
            ));
        }
        if descriptor.epoch.is_none() {
            return Err(nokv_mvcc::Error::Backend(
                Error::InvalidRegionDescriptor("region descriptor epoch is required".to_owned())
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn validate_topology_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> nokv_mvcc::Result<()> {
        if descriptor.region_id == 0 {
            return Err(nokv_mvcc::Error::Backend(
                "topology descriptor region id is required".to_owned(),
            ));
        }
        if descriptor.peers.is_empty() {
            return Err(nokv_mvcc::Error::Backend(format!(
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

    pub fn subscribe(&self) -> broadcast::Receiver<kvpb::ApplyWatchEvent> {
        self.inner.watch.subscribe()
    }

    pub fn traffic_snapshot(&self) -> RegionTrafficSnapshot {
        self.inner.traffic.snapshot()
    }
}

impl<E> ApplyStatusProvider for AppliedKvEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn apply_status(&self) -> ApplyStatus {
        self.status()
    }
}

impl<E, S> ApplyStatusProvider for PersistentAppliedKvEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: RegionMetadataSink,
{
    fn apply_status(&self) -> ApplyStatus {
        self.engine.status()
    }
}

impl<E> RegionTrafficProvider for AppliedKvEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn traffic_snapshot(&self) -> RegionTrafficSnapshot {
        self.traffic_snapshot()
    }
}

impl<E, S> RegionTrafficProvider for PersistentAppliedKvEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: RegionMetadataSink,
{
    fn traffic_snapshot(&self) -> RegionTrafficSnapshot {
        self.engine.traffic_snapshot()
    }
}

impl<E> ApplyWatchProvider for AppliedKvEngine<E>
where
    E: Clone + Send + Sync + 'static,
{
    fn subscribe_apply(&self) -> broadcast::Receiver<kvpb::ApplyWatchEvent> {
        self.subscribe()
    }
}

impl<E, S> ApplyWatchProvider for PersistentAppliedKvEngine<E, S>
where
    E: Clone + Send + Sync + 'static,
    S: RegionMetadataSink,
{
    fn subscribe_apply(&self) -> broadcast::Receiver<kvpb::ApplyWatchEvent> {
        self.engine.subscribe()
    }
}

impl<E> AppliedKvEngine<E>
where
    E: KvEngine,
{
    fn execute_raft_command_inner(
        &self,
        req: &raftpb::RaftCmdRequest,
    ) -> nokv_mvcc::Result<raftpb::RaftCmdResponse> {
        self.execute_raft_command_at(req, None)
    }

    pub fn apply_openraft_entries<I>(&self, entries: I) -> nokv_mvcc::Result<Vec<AppliedProposal>>
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
                    });
                }
                openraft::EntryPayload::Normal(proposal) => {
                    if proposal.region_id != self.inner.region_id {
                        return Err(nokv_mvcc::Error::Backend(
                            Error::LogRegionMismatch {
                                record_region_id: self.inner.region_id,
                                proposal_region_id: proposal.region_id,
                            }
                            .to_string(),
                        ));
                    }
                    match proposal.payload_kind() {
                        ProposalPayloadKind::RaftCommand => {
                            let req = proposal
                                .decode_raft_command()
                                .map_err(|err| nokv_mvcc::Error::Backend(err.to_string()))?;
                            let response =
                                self.execute_raft_command_at(&req, Some((term, index)))?;
                            applied.push(AppliedProposal {
                                region_id: proposal.region_id,
                                index,
                                term,
                                payload: encode_raft_response(&response)?,
                            });
                        }
                        ProposalPayloadKind::RegionDescriptor => {
                            let descriptor = proposal
                                .decode_region_descriptor()
                                .map_err(|err| nokv_mvcc::Error::Backend(err.to_string()))?;
                            self.apply_region_descriptor_at(term, index, descriptor)?;
                            applied.push(AppliedProposal {
                                region_id: proposal.region_id,
                                index,
                                term,
                                payload: Vec::new(),
                            });
                        }
                        ProposalPayloadKind::AdminCommand => {
                            let command = proposal
                                .decode_admin_command()
                                .map_err(|err| nokv_mvcc::Error::Backend(err.to_string()))?;
                            self.apply_admin_command_at(term, index, command)?;
                            applied.push(AppliedProposal {
                                region_id: proposal.region_id,
                                index,
                                term,
                                payload: Vec::new(),
                            });
                        }
                    }
                }
            }
        }
        Ok(applied)
    }

    fn apply_admin_command_at(
        &self,
        term: u64,
        index: u64,
        command: raftpb::AdminCommand,
    ) -> nokv_mvcc::Result<()> {
        let kind = raftpb::admin_command::Type::try_from(command.r#type)
            .unwrap_or(raftpb::admin_command::Type::Unknown);
        match kind {
            raftpb::admin_command::Type::Split => {
                let split = command
                    .split
                    .ok_or_else(|| invalid_raft_command("split admin payload is required"))?;
                self.apply_split_command_at(term, index, split)
            }
            raftpb::admin_command::Type::Merge => {
                let merge = command
                    .merge
                    .ok_or_else(|| invalid_raft_command("merge admin payload is required"))?;
                self.apply_merge_command_at(term, index, merge)
            }
            raftpb::admin_command::Type::Unknown => {
                Err(invalid_raft_command("unknown admin command type"))
            }
        }
    }

    fn apply_split_command_at(
        &self,
        term: u64,
        index: u64,
        split: raftpb::SplitCommand,
    ) -> nokv_mvcc::Result<()> {
        if split.parent_region_id != self.inner.region_id {
            return Err(invalid_raft_command(
                "split parent region does not match apply region",
            ));
        }
        if split.split_key.is_empty() {
            return Err(invalid_raft_command("split key is required"));
        }
        let parent = self.region_descriptor()?.ok_or_else(|| {
            invalid_raft_command("split parent descriptor must be installed before apply")
        })?;
        if split.split_key <= parent.start_key
            || (!parent.end_key.is_empty() && split.split_key >= parent.end_key)
        {
            return Err(invalid_raft_command(
                "split key must be inside parent descriptor range",
            ));
        }
        let mut child = split
            .child
            .ok_or_else(|| invalid_raft_command("split child descriptor is required"))?;
        if child.region_id == 0 {
            return Err(invalid_raft_command("split child region id is required"));
        }
        if child.start_key.is_empty() {
            child.start_key = split.split_key.clone();
        }
        if child.start_key != split.split_key {
            return Err(invalid_raft_command(
                "split child start key must equal split key",
            ));
        }
        if child.end_key != parent.end_key {
            return Err(invalid_raft_command(
                "split child end key must equal original parent end key",
            ));
        }

        let parent_epoch = parent.epoch.clone();
        let parent_hash = parent.hash.clone();
        let mut descriptor = parent.clone();
        descriptor.end_key = split.split_key;
        let epoch = descriptor.epoch.get_or_insert_with(Default::default);
        epoch.version = epoch.version.saturating_add(1);
        descriptor.hash.clear();
        if let Some(parent_epoch) = parent_epoch.clone() {
            push_split_lineage_once(
                &mut descriptor,
                parent.region_id,
                parent_epoch.clone(),
                &parent_hash,
            );
            if child.epoch.is_none() {
                child.epoch = Some(parent_epoch);
            }
        }
        if let Some(parent_epoch) = parent_epoch {
            push_split_lineage_once(&mut child, parent.region_id, parent_epoch, &parent_hash);
        }
        self.record_topology_descriptor(child)?;
        self.apply_region_descriptor_at(term, index, descriptor)
    }

    fn apply_merge_command_at(
        &self,
        term: u64,
        index: u64,
        merge: raftpb::MergeCommand,
    ) -> nokv_mvcc::Result<()> {
        if merge.target_region_id != self.inner.region_id {
            return Err(invalid_raft_command(
                "merge target region does not match apply region",
            ));
        }
        if merge.target_region_id == 0 || merge.source_region_id == 0 {
            return Err(invalid_raft_command(
                "merge target and source region ids are required",
            ));
        }
        if merge.target_region_id == merge.source_region_id {
            return Err(invalid_raft_command(
                "merge source region must differ from target region",
            ));
        }

        let target = self.region_descriptor()?.ok_or_else(|| {
            invalid_raft_command("merge target descriptor must be installed before apply")
        })?;
        if merge_source_already_absorbed(&target, merge.source_region_id) {
            let _ = self.take_topology_descriptor(merge.source_region_id)?;
            self.record_applied_status(term, index);
            return Ok(());
        }

        let source = self
            .topology_descriptor(merge.source_region_id)?
            .ok_or_else(|| {
                invalid_raft_command("merge source descriptor must be available before apply")
            })?;
        let descriptor = build_merge_descriptor_for_apply(&target, &source)?;
        let _ = self.take_topology_descriptor(merge.source_region_id)?;
        self.apply_region_descriptor_at(term, index, descriptor)
    }

    fn execute_raft_command_at(
        &self,
        req: &raftpb::RaftCmdRequest,
        forced_status: Option<(u64, u64)>,
    ) -> nokv_mvcc::Result<raftpb::RaftCmdResponse> {
        let (responses, writes, events) = {
            let engine =
                self.inner.engine.lock().map_err(|_| {
                    nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned())
                })?;
            let mut responses = Vec::with_capacity(req.requests.len());
            let mut writes = false;
            let mut events = Vec::new();
            for request in &req.requests {
                let (response, write, event) = Self::execute_raft_request_on(&*engine, request)?;
                writes |= write;
                if let Some(event) = event {
                    events.push(event);
                }
                responses.push(response);
            }
            (responses, writes, events)
        };

        let applied_status = if let Some((term, index)) = forced_status {
            self.record_applied_status(term, index);
            Some((term, index))
        } else if writes {
            Some(self.advance_apply_index())
        } else {
            None
        };
        if let Some((term, index)) = applied_status {
            for event in events {
                self.publish_apply(
                    index,
                    term,
                    event.source,
                    event.commit_version,
                    event.keys,
                    event.atomic,
                );
            }
        }
        let read_ops = req
            .requests
            .iter()
            .filter(|request| {
                matches!(
                    raftpb::CmdType::try_from(request.cmd_type),
                    Ok(raftpb::CmdType::CmdGet | raftpb::CmdType::CmdScan)
                )
            })
            .count() as u64;
        self.inner.traffic.record_read(read_ops);
        Ok(raftpb::RaftCmdResponse {
            header: req.header.clone(),
            responses,
            region_error: None,
        })
    }

    fn execute_raft_request_on(
        engine: &E,
        req: &raftpb::Request,
    ) -> nokv_mvcc::Result<(raftpb::Response, bool, Option<ApplyEvent>)> {
        use raftpb::request::Cmd as RequestCmd;
        use raftpb::response::Cmd as ResponseCmd;

        let cmd = raftpb::CmdType::try_from(req.cmd_type).unwrap_or(raftpb::CmdType::CmdInvalid);
        let (response, write, event) = match (cmd, req.cmd.as_ref()) {
            (raftpb::CmdType::CmdGet, Some(RequestCmd::Get(inner))) => {
                (ResponseCmd::Get(engine.get(inner)?), false, None)
            }
            (raftpb::CmdType::CmdScan, Some(RequestCmd::Scan(inner))) => {
                (ResponseCmd::Scan(engine.scan(inner)?), false, None)
            }
            (raftpb::CmdType::CmdPrewrite, Some(RequestCmd::Prewrite(inner))) => {
                (ResponseCmd::Prewrite(engine.prewrite(inner)?), true, None)
            }
            (raftpb::CmdType::CmdCommit, Some(RequestCmd::Commit(inner))) => {
                let response = engine.commit(inner)?;
                let event =
                    (response.error.is_none() && !inner.keys.is_empty()).then(|| ApplyEvent {
                        source: kvpb::ApplyWatchEventSource::Commit,
                        commit_version: inner.commit_version,
                        keys: inner.keys.clone(),
                        atomic: false,
                    });
                (ResponseCmd::Commit(response), true, event)
            }
            (raftpb::CmdType::CmdBatchRollback, Some(RequestCmd::BatchRollback(inner))) => (
                ResponseCmd::BatchRollback(engine.batch_rollback(inner)?),
                true,
                None,
            ),
            (raftpb::CmdType::CmdResolveLock, Some(RequestCmd::ResolveLock(inner))) => {
                let response = engine.resolve_lock(inner)?;
                let event = (response.error.is_none()
                    && inner.commit_version != 0
                    && !inner.keys.is_empty())
                .then(|| ApplyEvent {
                    source: kvpb::ApplyWatchEventSource::ResolveLock,
                    commit_version: inner.commit_version,
                    keys: inner.keys.clone(),
                    atomic: false,
                });
                (ResponseCmd::ResolveLock(response), true, event)
            }
            (raftpb::CmdType::CmdCheckTxnStatus, Some(RequestCmd::CheckTxnStatus(inner))) => (
                ResponseCmd::CheckTxnStatus(engine.check_txn_status(inner)?),
                true,
                None,
            ),
            (raftpb::CmdType::CmdMvccMaintenance, Some(RequestCmd::MvccMaintenance(inner))) => (
                ResponseCmd::MvccMaintenance(engine.mvcc_maintenance(inner)?),
                true,
                None,
            ),
            (raftpb::CmdType::CmdTxnHeartBeat, Some(RequestCmd::TxnHeartBeat(inner))) => (
                ResponseCmd::TxnHeartBeat(engine.txn_heartbeat(inner)?),
                true,
                None,
            ),
            (raftpb::CmdType::CmdTryAtomicMutate, Some(RequestCmd::TryAtomicMutate(inner))) => {
                let response = engine.try_atomic_mutate(inner)?;
                let event = (response.error.is_none()
                    && !response.fallback_to_two_phase_commit
                    && inner.commit_version != 0
                    && !inner.mutations.is_empty())
                .then(|| ApplyEvent {
                    source: kvpb::ApplyWatchEventSource::Commit,
                    commit_version: inner.commit_version,
                    keys: inner
                        .mutations
                        .iter()
                        .map(|mutation| mutation.key.clone())
                        .collect(),
                    atomic: true,
                });
                (ResponseCmd::TryAtomicMutate(response), true, event)
            }
            (
                raftpb::CmdType::CmdInstallPreparedMvcc,
                Some(RequestCmd::InstallPreparedMvcc(inner)),
            ) => {
                let response = engine.install_prepared(inner)?;
                let event = (response.error.is_none()
                    && inner.commit_version != 0
                    && !inner.watch_keys.is_empty())
                .then(|| ApplyEvent {
                    source: kvpb::ApplyWatchEventSource::Commit,
                    commit_version: inner.commit_version,
                    keys: inner.watch_keys.clone(),
                    atomic: false,
                });
                (ResponseCmd::InstallPreparedMvcc(response), true, event)
            }
            _ => {
                return Err(invalid_raft_command(
                    "command type and payload do not match",
                ))
            }
        };
        Ok((
            raftpb::Response {
                cmd: Some(response),
            },
            write,
            event,
        ))
    }

    fn read<T>(&self, f: impl FnOnce(&E) -> nokv_mvcc::Result<T>) -> nokv_mvcc::Result<T> {
        let engine = self
            .inner
            .engine
            .lock()
            .map_err(|_| nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned()))?;
        f(&engine)
    }

    fn apply_region_descriptor_at(
        &self,
        term: u64,
        index: u64,
        descriptor: metapb::RegionDescriptor,
    ) -> nokv_mvcc::Result<()> {
        self.set_region_descriptor(descriptor)?;
        self.record_applied_status(term, index);
        Ok(())
    }

    fn apply<T>(
        &self,
        f: impl FnOnce(&E) -> nokv_mvcc::Result<T>,
    ) -> nokv_mvcc::Result<(u64, u64, T)> {
        let engine = self
            .inner
            .engine
            .lock()
            .map_err(|_| nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned()))?;
        let result = f(&engine)?;
        let (term, index) = self.advance_apply_index();
        Ok((term, index, result))
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
        source: kvpb::ApplyWatchEventSource,
        commit_version: u64,
        keys: Vec<Vec<u8>>,
        atomic: bool,
    ) {
        if keys.is_empty() {
            return;
        }
        self.inner.traffic.record_apply(&keys, atomic);
        let _ = self.inner.watch.send(kvpb::ApplyWatchEvent {
            region_id: self.inner.region_id,
            term,
            index,
            source: source as i32,
            commit_version,
            keys,
        });
    }
}

impl<E> RaftCommandExecutor for AppliedKvEngine<E>
where
    E: KvEngine,
{
    fn execute_raft_command<'a>(
        &'a self,
        req: &'a raftpb::RaftCmdRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<raftpb::RaftCmdResponse>> + Send + 'a
    {
        async move { self.execute_raft_command_inner(req) }
    }
}

impl<E, S> RaftCommandExecutor for PersistentAppliedKvEngine<E, S>
where
    E: KvEngine,
    S: RegionMetadataSink,
{
    fn execute_raft_command<'a>(
        &'a self,
        req: &'a raftpb::RaftCmdRequest,
    ) -> impl std::future::Future<Output = nokv_mvcc::Result<raftpb::RaftCmdResponse>> + Send + 'a
    {
        async move {
            let before = self.engine.status().applied_index;
            let response = self.engine.execute_raft_command(req).await?;
            self.persist_if_advanced(before)?;
            Ok(response)
        }
    }
}

impl<E, S> PersistentAppliedKvEngine<E, S>
where
    E: KvEngine,
    S: RegionMetadataSink,
{
    pub fn apply_openraft_entries<I>(&self, entries: I) -> nokv_mvcc::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>,
    {
        let before = self.engine.status().applied_index;
        let applied = self.engine.apply_openraft_entries(entries)?;
        self.persist_if_advanced(before)?;
        Ok(applied)
    }

    fn persist_if_advanced(&self, before: u64) -> nokv_mvcc::Result<()> {
        let status = self.engine.status();
        if status.applied_index != before {
            if let Some(descriptor) = self.engine.region_descriptor()? {
                self.sink.save_region_descriptor(&descriptor)?;
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

impl<E> RegionSnapshotEngine for AppliedKvEngine<E>
where
    E: KvEngine + MvccSnapshotEngine,
{
    fn region_descriptor(&self) -> nokv_mvcc::Result<Option<metapb::RegionDescriptor>> {
        AppliedKvEngine::region_descriptor(self)
    }

    fn export_region_snapshot(&self) -> nokv_mvcc::Result<Vec<u8>> {
        let status = self.status();
        let mvcc_snapshot = {
            let engine =
                self.inner.engine.lock().map_err(|_| {
                    nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned())
                })?;
            engine.export_mvcc_snapshot()?
        };
        Ok(RegionSnapshotPayload {
            format_version: 1,
            region_id: status.region_id,
            term: status.term,
            applied_index: status.applied_index,
            mvcc_snapshot: nokv_mvcc::encode_mvcc_snapshot(&mvcc_snapshot),
            region_descriptor: self.region_descriptor()?,
        }
        .encode_to_vec())
    }

    fn install_region_snapshot(&self, snapshot: &[u8]) -> nokv_mvcc::Result<ApplyStatus> {
        let payload = decode_region_snapshot_payload(snapshot)?;
        if payload.format_version != 1 {
            return Err(nokv_mvcc::Error::Decode(format!(
                "unsupported region snapshot format {}",
                payload.format_version
            )));
        }
        if payload.region_id != self.inner.region_id {
            return Err(nokv_mvcc::Error::Backend(format!(
                "region snapshot {} cannot install into region {}",
                payload.region_id, self.inner.region_id
            )));
        }
        let current = self.status();
        if payload.applied_index < current.applied_index {
            return Err(nokv_mvcc::Error::Backend(format!(
                "stale region snapshot index {} is behind current applied index {}",
                payload.applied_index, current.applied_index
            )));
        }
        if payload.applied_index == current.applied_index
            && current.applied_index != 0
            && payload.term != current.term
        {
            return Err(nokv_mvcc::Error::Backend(format!(
                "region snapshot term {} conflicts with current applied term {} at index {}",
                payload.term, current.term, payload.applied_index
            )));
        }
        let mvcc_snapshot = nokv_mvcc::decode_mvcc_snapshot(&payload.mvcc_snapshot)?;
        if let Some(descriptor) = payload.region_descriptor {
            self.set_region_descriptor(descriptor)?;
        }
        {
            let engine =
                self.inner.engine.lock().map_err(|_| {
                    nokv_mvcc::Error::Backend("region apply mutex poisoned".to_owned())
                })?;
            engine.install_mvcc_snapshot(mvcc_snapshot)?;
        }
        self.record_applied_status(payload.term, payload.applied_index);
        Ok(self.status())
    }
}

impl<E, S> RegionSnapshotEngine for PersistentAppliedKvEngine<E, S>
where
    E: KvEngine + MvccSnapshotEngine,
    S: RegionMetadataSink,
{
    fn region_descriptor(&self) -> nokv_mvcc::Result<Option<metapb::RegionDescriptor>> {
        self.engine.region_descriptor()
    }

    fn export_region_snapshot(&self) -> nokv_mvcc::Result<Vec<u8>> {
        self.engine.export_region_snapshot()
    }

    fn install_region_snapshot(&self, snapshot: &[u8]) -> nokv_mvcc::Result<ApplyStatus> {
        let status = self.engine.install_region_snapshot(snapshot)?;
        if let Some(descriptor) = self.engine.region_descriptor()? {
            self.sink.save_region_descriptor(&descriptor)?;
        }
        self.sink.save_apply_status(&status)?;
        Ok(status)
    }
}

impl<E> RegionApplyEngine for AppliedKvEngine<E>
where
    E: KvEngine,
{
    fn apply_openraft_entries<I>(&self, entries: I) -> nokv_mvcc::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>,
    {
        self.apply_openraft_entries(entries)
    }
}

impl<E, S> RegionApplyEngine for PersistentAppliedKvEngine<E, S>
where
    E: KvEngine,
    S: RegionMetadataSink,
{
    fn apply_openraft_entries<I>(&self, entries: I) -> nokv_mvcc::Result<Vec<AppliedProposal>>
    where
        I: IntoIterator<Item = OpenRaftEntry>,
    {
        self.apply_openraft_entries(entries)
    }
}

fn invalid_raft_command(detail: &str) -> nokv_mvcc::Error {
    nokv_mvcc::Error::Backend(format!("invalid raft command: {detail}"))
}

fn push_split_lineage_once(
    descriptor: &mut metapb::RegionDescriptor,
    parent_region_id: RegionId,
    parent_epoch: metapb::RegionEpoch,
    parent_hash: &[u8],
) {
    let kind = metapb::DescriptorLineageKind::SplitParent as i32;
    if descriptor
        .lineage
        .iter()
        .any(|lineage| lineage.region_id == parent_region_id && lineage.kind == kind)
    {
        return;
    }
    descriptor.lineage.push(metapb::DescriptorLineageRef {
        region_id: parent_region_id,
        epoch: Some(parent_epoch),
        hash: parent_hash.to_vec(),
        kind,
    });
}

fn build_merge_descriptor_for_apply(
    target: &metapb::RegionDescriptor,
    source: &metapb::RegionDescriptor,
) -> nokv_mvcc::Result<metapb::RegionDescriptor> {
    if target.region_id == 0 || source.region_id == 0 {
        return Err(invalid_raft_command(
            "merge target and source region ids are required",
        ));
    }
    if target.end_key != source.start_key {
        return Err(invalid_raft_command(
            "merge source must be the target's right sibling",
        ));
    }
    ensure_merge_store_coverage_for_apply(target, source)?;
    let source_epoch = source
        .epoch
        .clone()
        .ok_or_else(|| invalid_raft_command("merge source epoch is required"))?;
    let target_epoch = target
        .epoch
        .clone()
        .ok_or_else(|| invalid_raft_command("merge target epoch is required"))?;

    let mut descriptor = target.clone();
    descriptor.end_key = source.end_key.clone();
    let epoch = descriptor.epoch.get_or_insert(target_epoch);
    epoch.version = epoch.version.saturating_add(1);
    descriptor.hash.clear();
    push_merge_lineage_once(
        &mut descriptor,
        source.region_id,
        source_epoch,
        &source.hash,
    );
    Ok(descriptor)
}

fn push_merge_lineage_once(
    descriptor: &mut metapb::RegionDescriptor,
    source_region_id: RegionId,
    source_epoch: metapb::RegionEpoch,
    source_hash: &[u8],
) {
    let kind = metapb::DescriptorLineageKind::MergeSource as i32;
    if descriptor
        .lineage
        .iter()
        .any(|lineage| lineage.region_id == source_region_id && lineage.kind == kind)
    {
        return;
    }
    descriptor.lineage.push(metapb::DescriptorLineageRef {
        region_id: source_region_id,
        epoch: Some(source_epoch),
        hash: source_hash.to_vec(),
        kind,
    });
}

fn merge_source_already_absorbed(
    target: &metapb::RegionDescriptor,
    source_region_id: RegionId,
) -> bool {
    target.lineage.iter().any(|lineage| {
        lineage.region_id == source_region_id
            && lineage.kind == metapb::DescriptorLineageKind::MergeSource as i32
    })
}

fn ensure_merge_store_coverage_for_apply(
    target: &metapb::RegionDescriptor,
    source: &metapb::RegionDescriptor,
) -> nokv_mvcc::Result<()> {
    let target_stores = region_peer_store_ids_for_apply(target)?;
    let source_stores = region_peer_store_ids_for_apply(source)?;
    if target_stores == source_stores {
        return Ok(());
    }
    Err(invalid_raft_command(
        "merge target and source must cover the same store set",
    ))
}

fn region_peer_store_ids_for_apply(
    descriptor: &metapb::RegionDescriptor,
) -> nokv_mvcc::Result<BTreeSet<u64>> {
    let mut stores = BTreeSet::new();
    for peer in &descriptor.peers {
        if peer.store_id == 0 || peer.peer_id == 0 {
            return Err(invalid_raft_command("merge descriptor has an invalid peer"));
        }
        if !stores.insert(peer.store_id) {
            return Err(invalid_raft_command(
                "merge descriptor has duplicate peer stores",
            ));
        }
    }
    if stores.is_empty() {
        return Err(invalid_raft_command("merge descriptor has no peers"));
    }
    Ok(stores)
}

fn encode_raft_response(response: &raftpb::RaftCmdResponse) -> nokv_mvcc::Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(response.encoded_len());
    response
        .encode(&mut payload)
        .map_err(|err| nokv_mvcc::Error::Backend(err.to_string()))?;
    Ok(payload)
}

pub(crate) fn decode_raft_response(payload: &[u8]) -> nokv_mvcc::Result<raftpb::RaftCmdResponse> {
    raftpb::RaftCmdResponse::decode(payload)
        .map_err(|err| nokv_mvcc::Error::Backend(err.to_string()))
}

use crate::snapshot::{decode_region_snapshot_payload, RegionSnapshotPayload};

impl<E> KvEngine for AppliedKvEngine<E>
where
    E: KvEngine,
{
    fn get(&self, req: &kvpb::GetRequest) -> nokv_mvcc::Result<kvpb::GetResponse> {
        let response = self.read(|engine| engine.get(req))?;
        self.inner.traffic.record_read(1);
        Ok(response)
    }

    fn batch_get(&self, req: &kvpb::BatchGetRequest) -> nokv_mvcc::Result<kvpb::BatchGetResponse> {
        let response = self.read(|engine| engine.batch_get(req))?;
        self.inner.traffic.record_read(req.requests.len() as u64);
        Ok(response)
    }

    fn scan(&self, req: &kvpb::ScanRequest) -> nokv_mvcc::Result<kvpb::ScanResponse> {
        let response = self.read(|engine| engine.scan(req))?;
        self.inner.traffic.record_read(1);
        Ok(response)
    }

    fn prewrite(&self, req: &kvpb::PrewriteRequest) -> nokv_mvcc::Result<kvpb::PrewriteResponse> {
        self.apply(|engine| engine.prewrite(req))
            .map(|(_, _, result)| result)
    }

    fn commit(&self, req: &kvpb::CommitRequest) -> nokv_mvcc::Result<kvpb::CommitResponse> {
        let (term, index, result) = self.apply(|engine| engine.commit(req))?;
        if result.error.is_none() && !req.keys.is_empty() {
            self.publish_apply(
                index,
                term,
                kvpb::ApplyWatchEventSource::Commit,
                req.commit_version,
                req.keys.clone(),
                false,
            );
        }
        Ok(result)
    }

    fn batch_rollback(
        &self,
        req: &kvpb::BatchRollbackRequest,
    ) -> nokv_mvcc::Result<kvpb::BatchRollbackResponse> {
        self.apply(|engine| engine.batch_rollback(req))
            .map(|(_, _, result)| result)
    }

    fn resolve_lock(
        &self,
        req: &kvpb::ResolveLockRequest,
    ) -> nokv_mvcc::Result<kvpb::ResolveLockResponse> {
        let (term, index, result) = self.apply(|engine| engine.resolve_lock(req))?;
        if result.error.is_none() && req.commit_version != 0 && !req.keys.is_empty() {
            self.publish_apply(
                index,
                term,
                kvpb::ApplyWatchEventSource::ResolveLock,
                req.commit_version,
                req.keys.clone(),
                false,
            );
        }
        Ok(result)
    }

    fn check_txn_status(
        &self,
        req: &kvpb::CheckTxnStatusRequest,
    ) -> nokv_mvcc::Result<kvpb::CheckTxnStatusResponse> {
        self.apply(|engine| engine.check_txn_status(req))
            .map(|(_, _, result)| result)
    }

    fn txn_heartbeat(
        &self,
        req: &kvpb::TxnHeartBeatRequest,
    ) -> nokv_mvcc::Result<kvpb::TxnHeartBeatResponse> {
        self.apply(|engine| engine.txn_heartbeat(req))
            .map(|(_, _, result)| result)
    }

    fn try_atomic_mutate(
        &self,
        req: &kvpb::TryAtomicMutateRequest,
    ) -> nokv_mvcc::Result<kvpb::TryAtomicMutateResponse> {
        let (term, index, result) = self.apply(|engine| engine.try_atomic_mutate(req))?;
        if result.error.is_none()
            && !result.fallback_to_two_phase_commit
            && req.commit_version != 0
            && !req.mutations.is_empty()
        {
            self.publish_apply(
                index,
                term,
                kvpb::ApplyWatchEventSource::Commit,
                req.commit_version,
                req.mutations
                    .iter()
                    .map(|mutation| mutation.key.clone())
                    .collect(),
                true,
            );
        }
        Ok(result)
    }

    fn install_prepared(
        &self,
        req: &kvpb::InstallPreparedMvccEntriesRequest,
    ) -> nokv_mvcc::Result<kvpb::InstallPreparedMvccEntriesResponse> {
        let (term, index, result) = self.apply(|engine| engine.install_prepared(req))?;
        if result.error.is_none() && req.commit_version != 0 && !req.watch_keys.is_empty() {
            self.publish_apply(
                index,
                term,
                kvpb::ApplyWatchEventSource::Commit,
                req.commit_version,
                req.watch_keys.clone(),
                false,
            );
        }
        Ok(result)
    }

    fn mvcc_maintenance(
        &self,
        req: &kvpb::MvccMaintenanceRequest,
    ) -> nokv_mvcc::Result<kvpb::MvccMaintenanceResponse> {
        self.apply(|engine| engine.mvcc_maintenance(req))
            .map(|(_, _, result)| result)
    }
}
