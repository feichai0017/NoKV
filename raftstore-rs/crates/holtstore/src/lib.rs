//! Holt multi-tree adapter for Rust raftstore.
//!
//! The adapter fixes the tree layout used by the Rust data plane while keeping
//! Holt internals out of the raftstore service and MVCC protocol crates.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use holt::{RangeEntry, Tree, TreeConfig, DB};
use nokv_mvcc as mvcc;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::meta::v1 as metapb;
use prost::Message;

pub const DATA_TREE: &str = "data";
pub const WRITE_TREE: &str = "write";
pub const LOCK_TREE: &str = "lock";
pub const REGION_META_TREE: &str = "region_meta";
pub const APPLY_STATE_TREE: &str = "apply_state";
pub const WATCH_APPLY_TREE: &str = "watch_apply";

const REQUIRED_TREES: [&str; 6] = [
    DATA_TREE,
    WRITE_TREE,
    LOCK_TREE,
    REGION_META_TREE,
    APPLY_STATE_TREE,
    WATCH_APPLY_TREE,
];

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("holt error: {0}")]
    Holt(#[from] holt::Error),
    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("protobuf encode error: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("invalid metadata record: {0}")]
    InvalidMetadata(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionApplyState {
    pub region_id: u64,
    pub term: u64,
    pub applied_index: u64,
    pub truncated_term: u64,
    pub truncated_index: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingRootEvent {
    pub sequence: u64,
    pub event: metapb::RootEvent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockedRootEvent {
    pub sequence: u64,
    pub event: metapb::RootEvent,
    pub transition_id: String,
    pub last_error: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingSchedulerOperation {
    pub operation: coordpb::SchedulerOperation,
    pub attempts: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockedSchedulerOperation {
    pub operation: coordpb::SchedulerOperation,
    pub attempts: u32,
    pub last_error: String,
}

#[derive(Clone, PartialEq, Message)]
struct PendingSchedulerOperationRecord {
    #[prost(message, optional, tag = "1")]
    operation: Option<coordpb::SchedulerOperation>,
    #[prost(uint32, tag = "2")]
    attempts: u32,
}

#[derive(Clone, PartialEq, Message)]
struct BlockedSchedulerOperationRecord {
    #[prost(message, optional, tag = "1")]
    operation: Option<coordpb::SchedulerOperation>,
    #[prost(uint32, tag = "2")]
    attempts: u32,
    #[prost(string, tag = "3")]
    last_error: String,
}

#[derive(Clone)]
pub struct HoltStore {
    db: DB,
}

impl HoltStore {
    pub fn open_memory() -> Result<Self> {
        Self::open(TreeConfig::memory())
    }

    pub fn open_file(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(TreeConfig::new(path.as_ref()))
    }

    pub fn open(cfg: TreeConfig) -> Result<Self> {
        let db = DB::open(cfg)?;
        let store = Self { db };
        store.ensure_required_trees()?;
        Ok(store)
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.db.checkpoint()?;
        Ok(())
    }

    pub fn atomic<F>(&self, build: F) -> Result<bool>
    where
        F: FnOnce(&mut holt::DBAtomicBatch),
    {
        Ok(self.db.atomic(build)?)
    }

    pub fn data(&self) -> Result<Tree> {
        self.tree(DATA_TREE)
    }

    pub fn write(&self) -> Result<Tree> {
        self.tree(WRITE_TREE)
    }

    pub fn lock(&self) -> Result<Tree> {
        self.tree(LOCK_TREE)
    }

    pub fn region_meta(&self) -> Result<Tree> {
        self.tree(REGION_META_TREE)
    }

    pub fn apply_state(&self) -> Result<Tree> {
        self.tree(APPLY_STATE_TREE)
    }

    pub fn watch_apply(&self) -> Result<Tree> {
        self.tree(WATCH_APPLY_TREE)
    }

    pub fn put_region_descriptor(&self, descriptor: &metapb::RegionDescriptor) -> Result<()> {
        let mut bytes = Vec::with_capacity(descriptor.encoded_len());
        descriptor.encode(&mut bytes)?;
        self.region_meta()?
            .put(&region_descriptor_key(descriptor.region_id), &bytes)?;
        Ok(())
    }

    pub fn get_region_descriptor(
        &self,
        region_id: u64,
    ) -> Result<Option<metapb::RegionDescriptor>> {
        let Some(bytes) = self.region_meta()?.get(&region_descriptor_key(region_id))? else {
            return Ok(None);
        };
        Ok(Some(metapb::RegionDescriptor::decode(bytes.as_slice())?))
    }

    pub fn delete_region_descriptor(&self, region_id: u64) -> Result<()> {
        self.region_meta()?
            .delete(&region_descriptor_key(region_id))?;
        Ok(())
    }

    pub fn region_descriptors(&self) -> Result<Vec<metapb::RegionDescriptor>> {
        let mut out = Vec::new();
        for entry in self.region_meta()?.range().prefix(REGION_DESCRIPTOR_PREFIX) {
            let entry = entry?;
            let RangeEntry::Key { value, .. } = entry else {
                continue;
            };
            out.push(metapb::RegionDescriptor::decode(value.as_slice())?);
        }
        out.sort_by_key(|descriptor| descriptor.region_id);
        Ok(out)
    }

    pub fn load_or_bootstrap_region_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> Result<metapb::RegionDescriptor> {
        if descriptor.region_id == 0 {
            return Err(Error::InvalidMetadata(
                "region descriptor id is required".to_owned(),
            ));
        }
        if let Some(existing) = self.get_region_descriptor(descriptor.region_id)? {
            return Ok(existing);
        }
        self.put_region_descriptor(descriptor)?;
        Ok(descriptor.clone())
    }

    pub fn put_region_apply_state(&self, state: &RegionApplyState) -> Result<()> {
        self.apply_state()?.put(
            &region_apply_state_key(state.region_id),
            &encode_apply_state(state),
        )?;
        Ok(())
    }

    pub fn get_region_apply_state(&self, region_id: u64) -> Result<Option<RegionApplyState>> {
        let Some(bytes) = self
            .apply_state()?
            .get(&region_apply_state_key(region_id))?
        else {
            return Ok(None);
        };
        decode_apply_state(&bytes).map(Some)
    }

    pub fn put_pending_root_event(&self, sequence: u64, event: &metapb::RootEvent) -> Result<()> {
        let mut bytes = Vec::with_capacity(event.encoded_len());
        event.encode(&mut bytes)?;
        self.region_meta()?
            .put(&pending_root_event_key(sequence), &bytes)?;
        Ok(())
    }

    pub fn delete_pending_root_event(&self, sequence: u64) -> Result<()> {
        self.region_meta()?
            .delete(&pending_root_event_key(sequence))?;
        Ok(())
    }

    pub fn pending_root_events(&self) -> Result<Vec<PendingRootEvent>> {
        let mut out = Vec::new();
        for entry in self
            .region_meta()?
            .range()
            .prefix(PENDING_ROOT_EVENT_PREFIX)
        {
            let entry = entry?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some(sequence) = pending_root_event_sequence(&key) else {
                continue;
            };
            out.push(PendingRootEvent {
                sequence,
                event: metapb::RootEvent::decode(value.as_slice())?,
            });
        }
        out.sort_by_key(|event| event.sequence);
        Ok(out)
    }

    pub fn block_pending_root_event(
        &self,
        sequence: u64,
        event: &metapb::RootEvent,
        transition_id: &str,
        last_error: &str,
    ) -> Result<()> {
        let record = metapb::BlockedRootEvent {
            sequence,
            event: Some(event.clone()),
            transition_id: transition_id.to_owned(),
            last_error: last_error.to_owned(),
        };
        let mut bytes = Vec::with_capacity(record.encoded_len());
        record.encode(&mut bytes)?;
        self.atomic(|batch| {
            batch.put(REGION_META_TREE, &blocked_root_event_key(sequence), &bytes);
            batch.delete(REGION_META_TREE, &pending_root_event_key(sequence));
        })?;
        Ok(())
    }

    pub fn blocked_root_events(&self) -> Result<Vec<BlockedRootEvent>> {
        let mut out = Vec::new();
        for entry in self
            .region_meta()?
            .range()
            .prefix(BLOCKED_ROOT_EVENT_PREFIX)
        {
            let entry = entry?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some(sequence) = blocked_root_event_sequence(&key) else {
                continue;
            };
            let record = metapb::BlockedRootEvent::decode(value.as_slice())?;
            let event = record.event.ok_or_else(|| {
                Error::InvalidMetadata(format!("blocked root event {sequence} missing event"))
            })?;
            out.push(BlockedRootEvent {
                sequence,
                event,
                transition_id: record.transition_id,
                last_error: record.last_error,
            });
        }
        out.sort_by_key(|event| event.sequence);
        Ok(out)
    }

    pub fn put_pending_scheduler_operation(
        &self,
        operation: &coordpb::SchedulerOperation,
    ) -> Result<()> {
        let key = pending_scheduler_operation_key(operation)?;
        let attempts = self
            .read_pending_scheduler_operation_record(&key)?
            .map(|record| record.attempts)
            .unwrap_or_default();
        let record = PendingSchedulerOperationRecord {
            operation: Some(operation.clone()),
            attempts,
        };
        let mut bytes = Vec::with_capacity(record.encoded_len());
        record.encode(&mut bytes)?;
        self.region_meta()?.put(&key, &bytes)?;
        Ok(())
    }

    pub fn increment_pending_scheduler_operation_attempts(
        &self,
        operation: &coordpb::SchedulerOperation,
    ) -> Result<u32> {
        let key = pending_scheduler_operation_key(operation)?;
        let attempts = self
            .read_pending_scheduler_operation_record(&key)?
            .map(|record| record.attempts)
            .unwrap_or_default()
            .saturating_add(1);
        let record = PendingSchedulerOperationRecord {
            operation: Some(operation.clone()),
            attempts,
        };
        let mut bytes = Vec::with_capacity(record.encoded_len());
        record.encode(&mut bytes)?;
        self.region_meta()?.put(&key, &bytes)?;
        Ok(attempts)
    }

    pub fn delete_pending_scheduler_operation(
        &self,
        operation: &coordpb::SchedulerOperation,
    ) -> Result<()> {
        let key = pending_scheduler_operation_key(operation)?;
        self.region_meta()?.delete(&key)?;
        Ok(())
    }

    pub fn pending_scheduler_operations(&self) -> Result<Vec<PendingSchedulerOperation>> {
        let mut out = Vec::new();
        for entry in self
            .region_meta()?
            .range()
            .prefix(PENDING_SCHEDULER_OPERATION_PREFIX)
        {
            let entry = entry?;
            let RangeEntry::Key { value, .. } = entry else {
                continue;
            };
            let record = PendingSchedulerOperationRecord::decode(value.as_slice())?;
            let operation = record.operation.ok_or_else(|| {
                Error::InvalidMetadata("pending scheduler operation missing operation".to_owned())
            })?;
            out.push(PendingSchedulerOperation {
                operation,
                attempts: record.attempts,
            });
        }
        out.sort_by_key(|pending| {
            (
                pending.operation.region_id,
                pending.operation.r#type,
                pending.operation.source_region_id,
                pending.operation.split_key.clone(),
                pending
                    .operation
                    .split_child
                    .as_ref()
                    .map(|child| child.region_id)
                    .unwrap_or_default(),
                pending.operation.source_peer_id,
                pending.operation.target_peer_id,
            )
        });
        Ok(out)
    }

    pub fn block_pending_scheduler_operation(
        &self,
        operation: &coordpb::SchedulerOperation,
        attempts: u32,
        last_error: &str,
    ) -> Result<()> {
        let pending_key = pending_scheduler_operation_key(operation)?;
        let blocked_key = blocked_scheduler_operation_key(operation)?;
        let record = BlockedSchedulerOperationRecord {
            operation: Some(operation.clone()),
            attempts,
            last_error: last_error.to_owned(),
        };
        let mut bytes = Vec::with_capacity(record.encoded_len());
        record.encode(&mut bytes)?;
        let tree = self.region_meta()?;
        tree.put(&blocked_key, &bytes)?;
        tree.delete(&pending_key)?;
        Ok(())
    }

    pub fn blocked_scheduler_operations(&self) -> Result<Vec<BlockedSchedulerOperation>> {
        let mut out = Vec::new();
        for entry in self
            .region_meta()?
            .range()
            .prefix(BLOCKED_SCHEDULER_OPERATION_PREFIX)
        {
            let entry = entry?;
            let RangeEntry::Key { value, .. } = entry else {
                continue;
            };
            let record = BlockedSchedulerOperationRecord::decode(value.as_slice())?;
            let operation = record.operation.ok_or_else(|| {
                Error::InvalidMetadata("blocked scheduler operation missing operation".to_owned())
            })?;
            out.push(BlockedSchedulerOperation {
                operation,
                attempts: record.attempts,
                last_error: record.last_error,
            });
        }
        out.sort_by_key(|blocked| {
            (
                blocked.operation.region_id,
                blocked.operation.r#type,
                blocked.operation.source_region_id,
                blocked.operation.split_key.clone(),
            )
        });
        Ok(out)
    }

    fn read_pending_scheduler_operation_record(
        &self,
        key: &[u8],
    ) -> Result<Option<PendingSchedulerOperationRecord>> {
        let Some(value) = self.region_meta()?.get(key)? else {
            return Ok(None);
        };
        Ok(Some(PendingSchedulerOperationRecord::decode(
            value.as_slice(),
        )?))
    }

    pub fn next_pending_root_event_sequence(&self) -> Result<u64> {
        Ok(self
            .pending_root_events()?
            .into_iter()
            .map(|event| event.sequence)
            .chain(
                self.blocked_root_events()?
                    .into_iter()
                    .map(|event| event.sequence),
            )
            .max()
            .map(|sequence| sequence.saturating_add(1))
            .unwrap_or(1))
    }

    pub fn put_data(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.data()?.put(key, value)?;
        Ok(())
    }

    pub fn get_data(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data()?.get(key)?)
    }

    fn tree(&self, name: &str) -> Result<Tree> {
        Ok(self.db.open_tree(name)?)
    }

    fn ensure_required_trees(&self) -> Result<()> {
        for name in REQUIRED_TREES {
            self.db.open_or_create_tree(name)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct HoltMvccStore {
    store: HoltStore,
    gate: Arc<Mutex<()>>,
}

impl HoltMvccStore {
    pub fn open_memory() -> Result<Self> {
        Ok(Self::new(HoltStore::open_memory()?))
    }

    pub fn open_file(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(HoltStore::open_file(path)?))
    }

    pub fn new(store: HoltStore) -> Self {
        Self {
            store,
            gate: Arc::new(Mutex::new(())),
        }
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.store.checkpoint()
    }

    pub fn put_region_descriptor(&self, descriptor: &metapb::RegionDescriptor) -> Result<()> {
        self.store.put_region_descriptor(descriptor)
    }

    pub fn get_region_descriptor(
        &self,
        region_id: u64,
    ) -> Result<Option<metapb::RegionDescriptor>> {
        self.store.get_region_descriptor(region_id)
    }

    pub fn delete_region_descriptor(&self, region_id: u64) -> Result<()> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| Error::InvalidMetadata("holt metadata mutex poisoned".to_owned()))?;
        self.store
            .delete_region_descriptor(region_id)
            .and_then(|_| self.store.checkpoint())
    }

    pub fn region_descriptors(&self) -> Result<Vec<metapb::RegionDescriptor>> {
        self.store.region_descriptors()
    }

    pub fn load_or_bootstrap_region_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> Result<metapb::RegionDescriptor> {
        self.store.load_or_bootstrap_region_descriptor(descriptor)
    }

    pub fn put_region_apply_state(&self, state: &RegionApplyState) -> Result<()> {
        self.store.put_region_apply_state(state)
    }

    pub fn get_region_apply_state(&self, region_id: u64) -> Result<Option<RegionApplyState>> {
        self.store.get_region_apply_state(region_id)
    }

    pub fn enqueue_pending_root_event(&self, event: &metapb::RootEvent) -> Result<u64> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| Error::InvalidMetadata("holt metadata mutex poisoned".to_owned()))?;
        let sequence = self.store.next_pending_root_event_sequence()?;
        self.store
            .put_pending_root_event(sequence, event)
            .and_then(|_| self.store.checkpoint())?;
        Ok(sequence)
    }

    pub fn delete_pending_root_event(&self, sequence: u64) -> Result<()> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| Error::InvalidMetadata("holt metadata mutex poisoned".to_owned()))?;
        self.store
            .delete_pending_root_event(sequence)
            .and_then(|_| self.store.checkpoint())
    }

    pub fn pending_root_events(&self) -> Result<Vec<PendingRootEvent>> {
        self.store.pending_root_events()
    }

    pub fn block_pending_root_event(
        &self,
        sequence: u64,
        event: &metapb::RootEvent,
        transition_id: &str,
        last_error: &str,
    ) -> Result<()> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| Error::InvalidMetadata("holt metadata mutex poisoned".to_owned()))?;
        self.store
            .block_pending_root_event(sequence, event, transition_id, last_error)
            .and_then(|_| self.store.checkpoint())
    }

    pub fn blocked_root_events(&self) -> Result<Vec<BlockedRootEvent>> {
        self.store.blocked_root_events()
    }

    pub fn record_pending_scheduler_operation(
        &self,
        operation: &coordpb::SchedulerOperation,
    ) -> Result<()> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| Error::InvalidMetadata("holt metadata mutex poisoned".to_owned()))?;
        self.store
            .put_pending_scheduler_operation(operation)
            .and_then(|_| self.store.checkpoint())
    }

    pub fn increment_pending_scheduler_operation_attempts(
        &self,
        operation: &coordpb::SchedulerOperation,
    ) -> Result<u32> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| Error::InvalidMetadata("holt metadata mutex poisoned".to_owned()))?;
        self.store
            .increment_pending_scheduler_operation_attempts(operation)
            .and_then(|attempts| self.store.checkpoint().map(|_| attempts))
    }

    pub fn delete_pending_scheduler_operation(
        &self,
        operation: &coordpb::SchedulerOperation,
    ) -> Result<()> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| Error::InvalidMetadata("holt metadata mutex poisoned".to_owned()))?;
        self.store
            .delete_pending_scheduler_operation(operation)
            .and_then(|_| self.store.checkpoint())
    }

    pub fn pending_scheduler_operations(&self) -> Result<Vec<PendingSchedulerOperation>> {
        self.store.pending_scheduler_operations()
    }

    pub fn block_pending_scheduler_operation(
        &self,
        operation: &coordpb::SchedulerOperation,
        attempts: u32,
        last_error: &str,
    ) -> Result<()> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| Error::InvalidMetadata("holt metadata mutex poisoned".to_owned()))?;
        self.store
            .block_pending_scheduler_operation(operation, attempts, last_error)
            .and_then(|_| self.store.checkpoint())
    }

    pub fn blocked_scheduler_operations(&self) -> Result<Vec<BlockedSchedulerOperation>> {
        self.store.blocked_scheduler_operations()
    }

    fn lock(&self) -> mvcc::Result<std::sync::MutexGuard<'_, ()>> {
        self.gate
            .lock()
            .map_err(|_| mvcc::Error::Backend("holt mvcc mutex poisoned".to_owned()))
    }

    fn atomic<F>(&self, build: F) -> mvcc::Result<bool>
    where
        F: FnOnce(&mut holt::DBAtomicBatch),
    {
        self.store
            .atomic(build)
            .map_err(|err| mvcc::Error::Backend(err.to_string()))
    }

    fn get_lock(&self, key: &[u8]) -> mvcc::Result<Option<mvcc::LockRecord>> {
        let Some(bytes) = self
            .store
            .lock()
            .map_err(to_backend_error)?
            .get(key)
            .map_err(to_backend_error)?
        else {
            return Ok(None);
        };
        decode_lock(&bytes).map(Some)
    }

    fn read_committed(
        &self,
        key: &[u8],
        version: u64,
    ) -> mvcc::Result<Option<(u64, mvcc::VersionedValue)>> {
        let prefix = write_prefix(key);
        let mut best = None;
        for entry in self
            .store
            .write()
            .map_err(to_backend_error)?
            .range()
            .prefix(&prefix)
        {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((_user_key, commit_ts)) = decode_write_key(&key)? else {
                continue;
            };
            if commit_ts <= version {
                let decoded = decode_value(&value)?;
                if decoded.kind == kvpb::mutation::Op::Lock
                    || decoded.kind == kvpb::mutation::Op::Rollback
                {
                    continue;
                }
                if best.as_ref().is_none_or(|(ts, _)| commit_ts > *ts) {
                    best = Some((commit_ts, decoded));
                }
            }
        }
        Ok(best)
    }

    fn write_by_start_version(
        &self,
        key: &[u8],
        start_version: u64,
    ) -> mvcc::Result<Option<(u64, mvcc::VersionedValue)>> {
        let prefix = write_prefix(key);
        for entry in self
            .store
            .write()
            .map_err(to_backend_error)?
            .range()
            .prefix(&prefix)
        {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((_user_key, commit_ts)) = decode_write_key(&key)? else {
                continue;
            };
            let decoded = decode_value(&value)?;
            if decoded.start_version == start_version {
                return Ok(Some((commit_ts, decoded)));
            }
        }
        Ok(None)
    }

    fn first_write_after_or_at(
        &self,
        key: &[u8],
        version: u64,
    ) -> mvcc::Result<Option<(u64, mvcc::VersionedValue)>> {
        self.first_write_matching(key, |commit_ts| commit_ts >= version)
    }

    fn first_write_matching(
        &self,
        key: &[u8],
        matches: impl Fn(u64) -> bool,
    ) -> mvcc::Result<Option<(u64, mvcc::VersionedValue)>> {
        let prefix = write_prefix(key);
        let mut best = None;
        for entry in self
            .store
            .write()
            .map_err(to_backend_error)?
            .range()
            .prefix(&prefix)
        {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((_user_key, commit_ts)) = decode_write_key(&key)? else {
                continue;
            };
            if matches(commit_ts) && best.as_ref().is_none_or(|(ts, _)| commit_ts < *ts) {
                best = Some((commit_ts, decode_value(&value)?));
            }
        }
        Ok(best)
    }

    fn atomic_mutate_already_applied(
        &self,
        req: &kvpb::TryAtomicMutateRequest,
    ) -> mvcc::Result<bool> {
        let mut any_present = false;
        let mut all_present = true;
        for mutation in &req.mutations {
            let Some((commit_version, value)) =
                self.write_by_start_version(&mutation.key, req.start_version)?
            else {
                all_present = false;
                continue;
            };
            any_present = true;
            if commit_version != req.commit_version
                || !mvcc::validation::atomic_mutation_matches_value(mutation, &value)
            {
                return Ok(false);
            }
        }
        Ok(any_present && all_present)
    }

    fn scan_write_user_keys(&self) -> mvcc::Result<Vec<Vec<u8>>> {
        let mut keys = std::collections::BTreeSet::new();
        for entry in self.store.write().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, .. } = entry else {
                continue;
            };
            if let Some((user_key, _commit_ts)) = decode_write_key(&key)? {
                keys.insert(user_key);
            }
        }
        Ok(keys.into_iter().collect())
    }
}

impl mvcc::KvEngine for HoltMvccStore {
    fn get(&self, req: &kvpb::GetRequest) -> mvcc::Result<kvpb::GetResponse> {
        let _guard = self.lock()?;
        if let Some(lock) = self.get_lock(&req.key)? {
            if lock.start_version <= req.version {
                return Ok(kvpb::GetResponse {
                    error: Some(mvcc::errors::locked(&req.key, &lock)),
                    ..Default::default()
                });
            }
        }
        Ok(match self.read_committed(&req.key, req.version)? {
            Some((_commit, value)) => {
                if mvcc::value_is_expired(value.expires_at) {
                    return Ok(kvpb::GetResponse {
                        not_found: true,
                        ..Default::default()
                    });
                }
                let not_found = value.value.is_none();
                kvpb::GetResponse {
                    value: value.value.unwrap_or_default(),
                    not_found,
                    expires_at: value.expires_at,
                    ..Default::default()
                }
            }
            None => kvpb::GetResponse {
                not_found: true,
                ..Default::default()
            },
        })
    }

    fn batch_get(&self, req: &kvpb::BatchGetRequest) -> mvcc::Result<kvpb::BatchGetResponse> {
        let mut responses = Vec::with_capacity(req.requests.len());
        for get in &req.requests {
            responses.push(self.get(get)?);
        }
        Ok(kvpb::BatchGetResponse { responses })
    }

    fn scan(&self, req: &kvpb::ScanRequest) -> mvcc::Result<kvpb::ScanResponse> {
        let _guard = self.lock()?;
        let read_version = mvcc::scan_read_version(req.version);
        let mut keys = self.scan_write_user_keys()?;
        if req.reverse {
            keys.reverse();
        }
        let mut kvs = Vec::new();
        for key in keys {
            if !req.reverse
                && (key.as_slice() < req.start_key.as_slice()
                    || (!req.include_start && key == req.start_key))
            {
                continue;
            }
            if req.reverse
                && (key.as_slice() > req.start_key.as_slice()
                    || (!req.include_start && key == req.start_key))
            {
                continue;
            }
            if let Some(lock) = self.get_lock(&key)? {
                if lock.start_version <= read_version {
                    return Ok(kvpb::ScanResponse {
                        error: Some(mvcc::errors::locked(&key, &lock)),
                        ..Default::default()
                    });
                }
            }
            if let Some((_commit_version, value)) = self.read_committed(&key, read_version)? {
                if mvcc::value_is_expired(value.expires_at) {
                    continue;
                }
                if let Some(bytes) = value.value {
                    kvs.push(kvpb::Kv {
                        key,
                        value: bytes,
                        version: read_version,
                        expires_at: value.expires_at,
                        ..Default::default()
                    });
                    if req.limit > 0 && kvs.len() >= req.limit as usize {
                        break;
                    }
                }
            }
        }
        Ok(kvpb::ScanResponse {
            kvs,
            ..Default::default()
        })
    }

    fn prewrite(&self, req: &kvpb::PrewriteRequest) -> mvcc::Result<kvpb::PrewriteResponse> {
        let _guard = self.lock()?;
        let mut errors = Vec::new();
        for mutation in &req.mutations {
            if let Some(error) = mvcc::validation::prewrite_mutation(mutation) {
                errors.push(error);
                continue;
            }
            if let Some(existing) = self.get_lock(&mutation.key)? {
                if existing.start_version != req.start_version {
                    errors.push(mvcc::errors::locked(&mutation.key, &existing));
                    continue;
                }
            }
            if let Some((commit_ts, value)) =
                self.first_write_after_or_at(&mutation.key, req.start_version)?
            {
                errors.push(mvcc::errors::write_conflict(
                    &mutation.key,
                    &req.primary_lock,
                    commit_ts,
                    value.start_version,
                    req.start_version,
                ));
                continue;
            }
            if mutation.assertion_not_exist
                && self
                    .read_committed(&mutation.key, req.start_version)?
                    .and_then(|(_, value)| value.value)
                    .is_some()
            {
                errors.push(mvcc::errors::already_exists(&mutation.key));
            }
        }
        if !errors.is_empty() {
            return Ok(kvpb::PrewriteResponse { errors });
        }
        let locks = req
            .mutations
            .iter()
            .map(|mutation| {
                let op =
                    kvpb::mutation::Op::try_from(mutation.op).unwrap_or(kvpb::mutation::Op::Put);
                let lock = mvcc::LockRecord {
                    primary: req.primary_lock.clone(),
                    start_version: req.start_version,
                    start_time: current_physical_time_millis(),
                    ttl: req.lock_ttl,
                    min_commit_ts: req.min_commit_ts,
                    op,
                    value: mutation.value.clone(),
                    expires_at: mutation.expires_at,
                };
                encode_lock(&lock).map(|encoded| (mutation.key.clone(), encoded))
            })
            .collect::<mvcc::Result<Vec<_>>>()?;
        self.atomic(|batch| {
            for (key, encoded) in &locks {
                batch.put(LOCK_TREE, key, encoded);
            }
        })?;
        Ok(kvpb::PrewriteResponse::default())
    }

    fn commit(&self, req: &kvpb::CommitRequest) -> mvcc::Result<kvpb::CommitResponse> {
        let _guard = self.lock()?;
        if let Some(err) = validate_commit_version(req.start_version, req.commit_version) {
            return Ok(kvpb::CommitResponse { error: Some(err) });
        }
        let mut locks = Vec::new();
        for key in &req.keys {
            if key.is_empty() {
                return Ok(kvpb::CommitResponse {
                    error: Some(mvcc::errors::empty_commit_key()),
                });
            }
            let Some(lock) = self.get_lock(key)? else {
                if let Some((_commit_version, value)) =
                    self.write_by_start_version(key, req.start_version)?
                {
                    if value.kind == kvpb::mutation::Op::Rollback {
                        return Ok(kvpb::CommitResponse {
                            error: Some(kvpb::KeyError {
                                abort: "transaction already rolled back".to_owned(),
                                ..Default::default()
                            }),
                        });
                    }
                    continue;
                }
                return Ok(kvpb::CommitResponse {
                    error: Some(kvpb::KeyError {
                        abort: "transaction lock not found".to_owned(),
                        ..Default::default()
                    }),
                });
            };
            if lock.start_version != req.start_version {
                return Ok(kvpb::CommitResponse {
                    error: Some(mvcc::errors::locked(key, &lock)),
                });
            }
            if req.commit_version < lock.min_commit_ts {
                return Ok(kvpb::CommitResponse {
                    error: Some(mvcc::errors::commit_ts_expired(
                        key,
                        req.commit_version,
                        lock.min_commit_ts,
                    )),
                });
            }
            locks.push((key.clone(), lock));
        }
        self.atomic(|batch| {
            for (key, lock) in &locks {
                let value = lock_value(lock);
                apply_committed(batch, key, req.commit_version, &value);
                batch.delete(LOCK_TREE, key);
            }
        })?;
        Ok(kvpb::CommitResponse::default())
    }

    fn batch_rollback(
        &self,
        req: &kvpb::BatchRollbackRequest,
    ) -> mvcc::Result<kvpb::BatchRollbackResponse> {
        let _guard = self.lock()?;
        if req.keys.iter().any(Vec::is_empty) {
            return Ok(kvpb::BatchRollbackResponse {
                error: Some(mvcc::errors::empty_rollback_key()),
            });
        }
        let mut delete_locks = Vec::new();
        let mut rollbacks = Vec::new();
        for key in &req.keys {
            if self
                .write_by_start_version(key, req.start_version)?
                .is_some()
            {
                continue;
            }
            if self
                .get_lock(key)?
                .is_some_and(|lock| lock.start_version == req.start_version)
            {
                delete_locks.push(key.clone());
            }
            rollbacks.push(key.clone());
        }
        self.atomic(|batch| {
            for key in &delete_locks {
                batch.delete(LOCK_TREE, key);
            }
            for key in &rollbacks {
                let value = rollback_value(req.start_version);
                apply_committed(batch, key, req.start_version, &value);
            }
        })?;
        Ok(kvpb::BatchRollbackResponse::default())
    }

    fn resolve_lock(
        &self,
        req: &kvpb::ResolveLockRequest,
    ) -> mvcc::Result<kvpb::ResolveLockResponse> {
        let _guard = self.lock()?;
        if req.commit_version != 0 {
            if let Some(err) = validate_commit_version(req.start_version, req.commit_version) {
                return Ok(kvpb::ResolveLockResponse {
                    error: Some(err),
                    ..Default::default()
                });
            }
        }
        let keys = mvcc::validation::resolve_lock_keys(req);
        let mut locks = Vec::new();
        for key in keys {
            let Some(lock) = self.get_lock(&key)? else {
                continue;
            };
            if lock.start_version == req.start_version {
                if req.commit_version != 0 && req.commit_version < lock.min_commit_ts {
                    return Ok(kvpb::ResolveLockResponse {
                        error: Some(mvcc::errors::commit_ts_expired(
                            &key,
                            req.commit_version,
                            lock.min_commit_ts,
                        )),
                        ..Default::default()
                    });
                }
                locks.push((key, lock));
            }
        }
        let resolved = locks.len() as u64;
        self.atomic(|batch| {
            for (key, lock) in &locks {
                if req.commit_version == 0 {
                    batch.delete(LOCK_TREE, key);
                    let value = rollback_value(req.start_version);
                    apply_committed(batch, key, req.start_version, &value);
                } else {
                    let value = lock_value(lock);
                    apply_committed(batch, key, req.commit_version, &value);
                    batch.delete(LOCK_TREE, key);
                }
            }
        })?;
        Ok(kvpb::ResolveLockResponse {
            resolved_locks: resolved,
            ..Default::default()
        })
    }

    fn check_txn_status(
        &self,
        req: &kvpb::CheckTxnStatusRequest,
    ) -> mvcc::Result<kvpb::CheckTxnStatusResponse> {
        let _guard = self.lock()?;
        if let Some(lock) = self.get_lock(&req.primary_key)? {
            if lock.start_version == req.lock_ts {
                if is_lock_expired(&lock, req.current_time) {
                    if req.primary_key.is_empty() {
                        return Ok(kvpb::CheckTxnStatusResponse {
                            error: Some(mvcc::errors::empty_rollback_key()),
                            ..Default::default()
                        });
                    }
                    self.atomic(|batch| {
                        batch.delete(LOCK_TREE, &req.primary_key);
                        let value = rollback_value(req.lock_ts);
                        apply_committed(batch, &req.primary_key, req.lock_ts, &value);
                    })?;
                    return Ok(kvpb::CheckTxnStatusResponse {
                        action: kvpb::CheckTxnStatusAction::CheckTxnStatusTtlExpireRollback as i32,
                        ..Default::default()
                    });
                }
                let mut action = kvpb::CheckTxnStatusAction::CheckTxnStatusNoAction;
                let mut current = lock;
                if req.caller_start_ts > 0 && current.min_commit_ts < req.caller_start_ts + 1 {
                    current.min_commit_ts = req.caller_start_ts + 1;
                    let encoded = encode_lock(&current)?;
                    self.atomic(|batch| {
                        batch.put(LOCK_TREE, &req.primary_key, &encoded);
                    })?;
                    action = kvpb::CheckTxnStatusAction::CheckTxnStatusMinCommitTsPushed;
                }
                return Ok(kvpb::CheckTxnStatusResponse {
                    lock_ttl: current.ttl,
                    action: action as i32,
                    ..Default::default()
                });
            } else {
                return Ok(kvpb::CheckTxnStatusResponse {
                    error: Some(mvcc::errors::locked(&req.primary_key, &lock)),
                    ..Default::default()
                });
            }
        }
        if let Some((commit_version, value)) =
            self.write_by_start_version(&req.primary_key, req.lock_ts)?
        {
            if value.kind == kvpb::mutation::Op::Rollback {
                return Ok(kvpb::CheckTxnStatusResponse {
                    action: kvpb::CheckTxnStatusAction::CheckTxnStatusLockNotExistRollback as i32,
                    ..Default::default()
                });
            }
            return Ok(kvpb::CheckTxnStatusResponse {
                commit_version,
                action: kvpb::CheckTxnStatusAction::CheckTxnStatusNoAction as i32,
                ..Default::default()
            });
        }
        if req.rollback_if_not_exist {
            if req.primary_key.is_empty() {
                return Ok(kvpb::CheckTxnStatusResponse {
                    error: Some(mvcc::errors::empty_rollback_key()),
                    ..Default::default()
                });
            }
            self.atomic(|batch| {
                let value = rollback_value(req.lock_ts);
                apply_committed(batch, &req.primary_key, req.lock_ts, &value);
            })?;
            return Ok(kvpb::CheckTxnStatusResponse {
                action: kvpb::CheckTxnStatusAction::CheckTxnStatusLockNotExistRollback as i32,
                ..Default::default()
            });
        }
        Ok(kvpb::CheckTxnStatusResponse::default())
    }

    fn txn_heartbeat(
        &self,
        req: &kvpb::TxnHeartBeatRequest,
    ) -> mvcc::Result<kvpb::TxnHeartBeatResponse> {
        let _guard = self.lock()?;
        if let Some(error) = mvcc::errors::txn_heartbeat_validation(req) {
            return Ok(kvpb::TxnHeartBeatResponse {
                error: Some(error),
                ..Default::default()
            });
        }
        let Some(mut lock) = self.get_lock(&req.primary_key)? else {
            if let Some((commit_version, value)) =
                self.write_by_start_version(&req.primary_key, req.start_version)?
            {
                if value.kind != kvpb::mutation::Op::Rollback {
                    return Ok(kvpb::TxnHeartBeatResponse {
                        commit_version,
                        action: kvpb::TxnHeartBeatAction::TxnHeartBeatNoAction as i32,
                        ..Default::default()
                    });
                }
            }
            self.atomic(|batch| {
                let value = rollback_value(req.start_version);
                apply_committed(batch, &req.primary_key, req.start_version, &value);
            })?;
            return Ok(kvpb::TxnHeartBeatResponse {
                action: kvpb::TxnHeartBeatAction::TxnHeartBeatLockNotExistRollback as i32,
                ..Default::default()
            });
        };
        if lock.start_version != req.start_version {
            return Ok(kvpb::TxnHeartBeatResponse {
                error: Some(mvcc::errors::locked(&req.primary_key, &lock)),
                ..Default::default()
            });
        }
        if lock.primary.as_slice() != req.primary_key.as_slice() {
            return Ok(kvpb::TxnHeartBeatResponse {
                error: Some(mvcc::errors::txn_heartbeat_primary_mismatch()),
                ..Default::default()
            });
        }
        if is_lock_expired(&lock, req.current_time) {
            self.atomic(|batch| {
                batch.delete(LOCK_TREE, &req.primary_key);
                let value = rollback_value(req.start_version);
                apply_committed(batch, &req.primary_key, req.start_version, &value);
            })?;
            return Ok(kvpb::TxnHeartBeatResponse {
                action: kvpb::TxnHeartBeatAction::TxnHeartBeatTtlExpireRollback as i32,
                ..Default::default()
            });
        }
        let desired_ttl = if req.current_time > lock.start_time {
            req.current_time - lock.start_time + req.ttl_extension
        } else {
            req.ttl_extension
        };
        let mut action = kvpb::TxnHeartBeatAction::TxnHeartBeatNoAction;
        if desired_ttl > lock.ttl {
            lock.ttl = desired_ttl;
            let encoded = encode_lock(&lock)?;
            self.atomic(|batch| {
                batch.put(LOCK_TREE, &req.primary_key, &encoded);
            })?;
            action = kvpb::TxnHeartBeatAction::TxnHeartBeatTtlExtended;
        }
        Ok(kvpb::TxnHeartBeatResponse {
            lock_ttl: lock.ttl,
            lock_expire_time: lock_expire_time(&lock),
            action: action as i32,
            ..Default::default()
        })
    }

    fn try_atomic_mutate(
        &self,
        req: &kvpb::TryAtomicMutateRequest,
    ) -> mvcc::Result<kvpb::TryAtomicMutateResponse> {
        let _guard = self.lock()?;
        if self.atomic_mutate_already_applied(req)? {
            return Ok(kvpb::TryAtomicMutateResponse {
                applied_keys: req.mutations.len() as u64,
                ..Default::default()
            });
        }
        for predicate in &req.predicates {
            if predicate.key.is_empty() {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(mvcc::errors::empty_mutation_key()),
                    ..Default::default()
                });
            }
            let read_version = if predicate.read_version == 0 {
                req.start_version
            } else {
                predicate.read_version
            };
            if let Some(lock) = self.get_lock(&predicate.key)? {
                if lock.start_version <= read_version {
                    return Ok(kvpb::TryAtomicMutateResponse {
                        error: Some(mvcc::errors::locked(&predicate.key, &lock)),
                        ..Default::default()
                    });
                }
            }
            let observed = self
                .read_committed(&predicate.key, read_version)?
                .and_then(|(_, value)| value.value);
            if let Some(error) =
                mvcc::validation::atomic_predicate_observation(predicate, observed.as_deref())
            {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(error),
                    ..Default::default()
                });
            }
        }
        let primary = req
            .mutations
            .first()
            .map(|mutation| mutation.key.as_slice())
            .unwrap_or_default();
        for mutation in &req.mutations {
            if let Some(error) = mvcc::validation::atomic_mutation(mutation) {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(error),
                    ..Default::default()
                });
            }
            if let Some(lock) = self.get_lock(&mutation.key)? {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(mvcc::errors::locked(&mutation.key, &lock)),
                    ..Default::default()
                });
            }
            if let Some((commit_ts, value)) =
                self.first_write_after_or_at(&mutation.key, req.start_version)?
            {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(mvcc::errors::write_conflict(
                        &mutation.key,
                        primary,
                        commit_ts,
                        value.start_version,
                        req.start_version,
                    )),
                    ..Default::default()
                });
            }
            if mutation.assertion_not_exist
                && self
                    .read_committed(&mutation.key, req.start_version)?
                    .and_then(|(_, value)| value.value)
                    .is_some()
            {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(mvcc::errors::already_exists(&mutation.key)),
                    ..Default::default()
                });
            }
        }
        let values = req
            .mutations
            .iter()
            .map(|mutation| {
                (
                    mutation.key.clone(),
                    mutation_value(mutation, req.start_version),
                )
            })
            .collect::<Vec<_>>();
        self.atomic(|batch| {
            for (key, value) in &values {
                apply_committed(batch, key, req.commit_version, value);
            }
        })?;
        Ok(kvpb::TryAtomicMutateResponse {
            applied_keys: req.mutations.len() as u64,
            ..Default::default()
        })
    }

    fn install_prepared(
        &self,
        req: &kvpb::InstallPreparedMvccEntriesRequest,
    ) -> mvcc::Result<kvpb::InstallPreparedMvccEntriesResponse> {
        let _guard = self.lock()?;
        let mut applied = 0;
        self.atomic(|batch| {
            for entry in &req.entries {
                match kvpb::prepared_mvcc_entry::ColumnFamily::try_from(entry.column_family)
                    .unwrap_or(kvpb::prepared_mvcc_entry::ColumnFamily::Default)
                {
                    kvpb::prepared_mvcc_entry::ColumnFamily::Default
                    | kvpb::prepared_mvcc_entry::ColumnFamily::Write => {
                        let value = mvcc::VersionedValue {
                            kind: if entry.has_value {
                                kvpb::mutation::Op::Put
                            } else {
                                kvpb::mutation::Op::Delete
                            },
                            start_version: entry.version,
                            value: entry.has_value.then(|| entry.value.clone()),
                            expires_at: entry.expires_at,
                        };
                        apply_committed(batch, &entry.key, entry.version, &value);
                        applied += 1;
                    }
                    kvpb::prepared_mvcc_entry::ColumnFamily::Lock => {
                        batch.delete(LOCK_TREE, &entry.key);
                        applied += 1;
                    }
                }
            }
        })?;
        Ok(kvpb::InstallPreparedMvccEntriesResponse {
            applied_entries: applied,
            commit_version: req.commit_version,
            ..Default::default()
        })
    }

    fn mvcc_maintenance(
        &self,
        req: &kvpb::MvccMaintenanceRequest,
    ) -> mvcc::Result<kvpb::MvccMaintenanceResponse> {
        let _guard = self.lock()?;
        let mut applied = 0;
        self.atomic(|batch| {
            for tombstone in &req.tombstones {
                match kvpb::internal_entry_tombstone::ColumnFamily::try_from(
                    tombstone.column_family,
                )
                .unwrap_or(kvpb::internal_entry_tombstone::ColumnFamily::Default)
                {
                    kvpb::internal_entry_tombstone::ColumnFamily::Default
                    | kvpb::internal_entry_tombstone::ColumnFamily::Write => {
                        batch.delete(WRITE_TREE, &write_key(&tombstone.key, tombstone.version));
                        applied += 1;
                    }
                }
            }
        })?;
        Ok(kvpb::MvccMaintenanceResponse {
            applied_entries: applied,
            ..Default::default()
        })
    }
}

impl mvcc::MvccSnapshotEngine for HoltMvccStore {
    fn export_mvcc_snapshot(&self) -> mvcc::Result<mvcc::MvccSnapshot> {
        let _guard = self.lock()?;
        let mut writes = Vec::new();
        for entry in self.store.write().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((user_key, commit_version)) = decode_write_key(&key)? else {
                continue;
            };
            writes.push(mvcc::MvccSnapshotWrite {
                key: user_key,
                commit_version,
                value: decode_value(&value)?,
            });
        }
        writes.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then(left.commit_version.cmp(&right.commit_version))
        });

        let mut locks = Vec::new();
        for entry in self.store.lock().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            locks.push(mvcc::MvccSnapshotLock {
                key,
                lock: decode_lock(&value)?,
            });
        }
        locks.sort_by(|left, right| left.key.cmp(&right.key));

        let rollbacks = writes
            .iter()
            .filter(|write| write.value.kind == kvpb::mutation::Op::Rollback)
            .map(|write| mvcc::MvccSnapshotRollback {
                key: write.key.clone(),
                start_version: write.value.start_version,
            })
            .collect();
        Ok(mvcc::MvccSnapshot {
            writes,
            locks,
            rollbacks,
        })
    }

    fn install_mvcc_snapshot(&self, mut snapshot: mvcc::MvccSnapshot) -> mvcc::Result<()> {
        let _guard = self.lock()?;
        let mut rollback_writes = snapshot
            .writes
            .iter()
            .filter(|write| write.value.kind == kvpb::mutation::Op::Rollback)
            .map(|write| (write.key.clone(), write.value.start_version))
            .collect::<std::collections::BTreeSet<_>>();
        for rollback in &snapshot.rollbacks {
            if rollback_writes.insert((rollback.key.clone(), rollback.start_version)) {
                snapshot.writes.push(mvcc::MvccSnapshotWrite {
                    key: rollback.key.clone(),
                    commit_version: rollback.start_version,
                    value: mvcc::VersionedValue {
                        kind: kvpb::mutation::Op::Rollback,
                        start_version: rollback.start_version,
                        value: None,
                        expires_at: 0,
                    },
                });
            }
        }
        snapshot.writes.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then(left.commit_version.cmp(&right.commit_version))
        });
        let encoded_locks = snapshot
            .locks
            .iter()
            .map(|lock| encode_lock(&lock.lock).map(|encoded| (lock.key.clone(), encoded)))
            .collect::<mvcc::Result<Vec<_>>>()?;

        let mut data_keys = Vec::new();
        for entry in self.store.data().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            if let RangeEntry::Key { key, .. } = entry {
                data_keys.push(key);
            }
        }
        let mut write_keys = Vec::new();
        for entry in self.store.write().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            if let RangeEntry::Key { key, .. } = entry {
                write_keys.push(key);
            }
        }
        let mut lock_keys = Vec::new();
        for entry in self.store.lock().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            if let RangeEntry::Key { key, .. } = entry {
                lock_keys.push(key);
            }
        }

        self.atomic(|batch| {
            for key in &data_keys {
                batch.delete(DATA_TREE, key);
            }
            for key in &write_keys {
                batch.delete(WRITE_TREE, key);
            }
            for key in &lock_keys {
                batch.delete(LOCK_TREE, key);
            }
            for write in &snapshot.writes {
                apply_committed(batch, &write.key, write.commit_version, &write.value);
            }
            for (key, encoded) in &encoded_locks {
                batch.put(LOCK_TREE, key, encoded);
            }
        })?;
        Ok(())
    }
}

fn lock_value(lock: &mvcc::LockRecord) -> mvcc::VersionedValue {
    let value = match lock.op {
        kvpb::mutation::Op::Put | kvpb::mutation::Op::Lock => Some(lock.value.clone()),
        kvpb::mutation::Op::Delete | kvpb::mutation::Op::Rollback => None,
    };
    mvcc::VersionedValue {
        kind: lock.op,
        start_version: lock.start_version,
        value,
        expires_at: lock.expires_at,
    }
}

fn mutation_value(mutation: &kvpb::Mutation, start_version: u64) -> mvcc::VersionedValue {
    let op = kvpb::mutation::Op::try_from(mutation.op).unwrap_or(kvpb::mutation::Op::Put);
    let value = match op {
        kvpb::mutation::Op::Put | kvpb::mutation::Op::Lock => Some(mutation.value.clone()),
        kvpb::mutation::Op::Delete | kvpb::mutation::Op::Rollback => None,
    };
    mvcc::VersionedValue {
        kind: op,
        start_version,
        value,
        expires_at: mutation.expires_at,
    }
}

fn rollback_value(start_version: u64) -> mvcc::VersionedValue {
    mvcc::VersionedValue {
        kind: kvpb::mutation::Op::Rollback,
        start_version,
        value: None,
        expires_at: 0,
    }
}

fn apply_committed(
    batch: &mut holt::DBAtomicBatch,
    key: &[u8],
    commit_ts: u64,
    value: &mvcc::VersionedValue,
) {
    let encoded = encode_value(value);
    batch.put(WRITE_TREE, &write_key(key, commit_ts), &encoded);
    match value.kind {
        kvpb::mutation::Op::Put => {
            if let Some(bytes) = &value.value {
                batch.put(DATA_TREE, key, bytes);
            }
        }
        kvpb::mutation::Op::Delete => {
            batch.delete(DATA_TREE, key);
        }
        kvpb::mutation::Op::Lock | kvpb::mutation::Op::Rollback => {}
    }
}

fn write_prefix(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + key.len());
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out
}

fn write_key(key: &[u8], commit_ts: u64) -> Vec<u8> {
    let mut out = write_prefix(key);
    out.extend_from_slice(&(u64::MAX - commit_ts).to_be_bytes());
    out
}

fn decode_write_key(key: &[u8]) -> mvcc::Result<Option<(Vec<u8>, u64)>> {
    if key.len() < 12 {
        return Ok(None);
    }
    let user_len = u32::from_be_bytes(key[0..4].try_into().unwrap()) as usize;
    if key.len() != 4 + user_len + 8 {
        return Ok(None);
    }
    let user_key = key[4..4 + user_len].to_vec();
    let inverted = u64::from_be_bytes(key[4 + user_len..].try_into().unwrap());
    Ok(Some((user_key, u64::MAX - inverted)))
}

fn encode_value(value: &mvcc::VersionedValue) -> Vec<u8> {
    let bytes = value.value.as_deref().unwrap_or_default();
    let mut out = Vec::with_capacity(1 + 4 + 8 + 8 + 4 + bytes.len());
    out.push(1);
    out.extend_from_slice(&(value.kind as i32).to_be_bytes());
    out.extend_from_slice(&value.start_version.to_be_bytes());
    out.extend_from_slice(&value.expires_at.to_be_bytes());
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
    out
}

fn decode_value(bytes: &[u8]) -> mvcc::Result<mvcc::VersionedValue> {
    if bytes.len() < 25 {
        return Err(mvcc::Error::Decode("short mvcc value".to_owned()));
    }
    if bytes[0] != 1 {
        return Err(mvcc::Error::Decode(
            "unsupported mvcc value version".to_owned(),
        ));
    }
    let kind_raw = i32::from_be_bytes(bytes[1..5].try_into().unwrap());
    let start_version = u64::from_be_bytes(bytes[5..13].try_into().unwrap());
    let expires_at = u64::from_be_bytes(bytes[13..21].try_into().unwrap());
    let len = u32::from_be_bytes(bytes[21..25].try_into().unwrap()) as usize;
    if bytes.len() != 25 + len {
        return Err(mvcc::Error::Decode("invalid mvcc value length".to_owned()));
    }
    let kind = kvpb::mutation::Op::try_from(kind_raw).unwrap_or(kvpb::mutation::Op::Put);
    Ok(mvcc::VersionedValue {
        kind,
        start_version,
        value: (kind == kvpb::mutation::Op::Put || kind == kvpb::mutation::Op::Lock)
            .then(|| bytes[25..].to_vec()),
        expires_at,
    })
}

fn encode_lock(lock: &mvcc::LockRecord) -> mvcc::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(4 + lock.primary.len() + 8 * 4 + 4 + 4 + lock.value.len());
    out.extend_from_slice(&(lock.primary.len() as u32).to_be_bytes());
    out.extend_from_slice(&lock.primary);
    out.extend_from_slice(&lock.start_version.to_be_bytes());
    out.extend_from_slice(&lock.start_time.to_be_bytes());
    out.extend_from_slice(&lock.ttl.to_be_bytes());
    out.extend_from_slice(&lock.min_commit_ts.to_be_bytes());
    out.extend_from_slice(&(lock.op as i32).to_be_bytes());
    out.extend_from_slice(&lock.expires_at.to_be_bytes());
    out.extend_from_slice(&(lock.value.len() as u32).to_be_bytes());
    out.extend_from_slice(&lock.value);
    Ok(out)
}

fn decode_lock(bytes: &[u8]) -> mvcc::Result<mvcc::LockRecord> {
    let mut cursor = Cursor::new(bytes);
    let primary = cursor.read_vec()?;
    let start_version = cursor.read_u64()?;
    let start_time = cursor.read_u64()?;
    let ttl = cursor.read_u64()?;
    let min_commit_ts = cursor.read_u64()?;
    let op_raw = cursor.read_i32()?;
    let expires_at = cursor.read_u64()?;
    let value = cursor.read_vec()?;
    if !cursor.done() {
        return Err(mvcc::Error::Decode("trailing lock bytes".to_owned()));
    }
    Ok(mvcc::LockRecord {
        primary,
        start_version,
        start_time,
        ttl,
        min_commit_ts,
        op: kvpb::mutation::Op::try_from(op_raw).unwrap_or(kvpb::mutation::Op::Put),
        value,
        expires_at,
    })
}

fn current_physical_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn is_lock_expired(lock: &mvcc::LockRecord, current_time: u64) -> bool {
    lock.ttl != 0
        && lock.start_time != 0
        && current_time != 0
        && current_time >= lock.start_time
        && current_time - lock.start_time >= lock.ttl
}

fn lock_expire_time(lock: &mvcc::LockRecord) -> u64 {
    if lock.start_time == 0 || lock.ttl == 0 {
        return 0;
    }
    lock.start_time.saturating_add(lock.ttl)
}

fn validate_commit_version(start_version: u64, commit_version: u64) -> Option<kvpb::KeyError> {
    (commit_version <= start_version).then(|| kvpb::KeyError {
        abort: "commit version must be greater than start version".to_owned(),
        ..Default::default()
    })
}

const REGION_DESCRIPTOR_PREFIX: &[u8] = b"descriptor/";
const PENDING_ROOT_EVENT_PREFIX: &[u8] = b"pending-root-event/";
const BLOCKED_ROOT_EVENT_PREFIX: &[u8] = b"blocked-root-event/";
const PENDING_SCHEDULER_OPERATION_PREFIX: &[u8] = b"pending-scheduler-operation/";
const BLOCKED_SCHEDULER_OPERATION_PREFIX: &[u8] = b"blocked-scheduler-operation/";

fn region_descriptor_key(region_id: u64) -> Vec<u8> {
    region_meta_key(REGION_DESCRIPTOR_PREFIX, region_id)
}

fn region_apply_state_key(region_id: u64) -> Vec<u8> {
    region_meta_key(b"apply-state/", region_id)
}

fn pending_root_event_key(sequence: u64) -> Vec<u8> {
    region_meta_key(PENDING_ROOT_EVENT_PREFIX, sequence)
}

fn pending_root_event_sequence(key: &[u8]) -> Option<u64> {
    let rest = key.strip_prefix(PENDING_ROOT_EVENT_PREFIX)?;
    if rest.len() != 8 {
        return None;
    }
    Some(u64::from_be_bytes(rest.try_into().ok()?))
}

fn blocked_root_event_key(sequence: u64) -> Vec<u8> {
    region_meta_key(BLOCKED_ROOT_EVENT_PREFIX, sequence)
}

fn blocked_root_event_sequence(key: &[u8]) -> Option<u64> {
    let rest = key.strip_prefix(BLOCKED_ROOT_EVENT_PREFIX)?;
    if rest.len() != 8 {
        return None;
    }
    Some(u64::from_be_bytes(rest.try_into().ok()?))
}

fn pending_scheduler_operation_key(operation: &coordpb::SchedulerOperation) -> Result<Vec<u8>> {
    let kind = coordpb::SchedulerOperationType::try_from(operation.r#type)
        .unwrap_or(coordpb::SchedulerOperationType::None);
    if kind == coordpb::SchedulerOperationType::None {
        return Err(Error::InvalidMetadata(
            "scheduler operation type is required".to_owned(),
        ));
    }
    if operation.region_id == 0 {
        return Err(Error::InvalidMetadata(
            "scheduler operation region is required".to_owned(),
        ));
    }
    let mut encoded = Vec::with_capacity(operation.encoded_len());
    operation.encode(&mut encoded)?;
    let mut key =
        Vec::with_capacity(PENDING_SCHEDULER_OPERATION_PREFIX.len() + 4 + 8 + encoded.len());
    key.extend_from_slice(PENDING_SCHEDULER_OPERATION_PREFIX);
    key.extend_from_slice(&(kind as i32).to_be_bytes());
    key.extend_from_slice(&operation.region_id.to_be_bytes());
    key.extend_from_slice(&encoded);
    Ok(key)
}

fn blocked_scheduler_operation_key(operation: &coordpb::SchedulerOperation) -> Result<Vec<u8>> {
    let pending = pending_scheduler_operation_key(operation)?;
    let rest = pending
        .strip_prefix(PENDING_SCHEDULER_OPERATION_PREFIX)
        .expect("pending scheduler key has scheduler prefix");
    let mut key = Vec::with_capacity(BLOCKED_SCHEDULER_OPERATION_PREFIX.len() + rest.len());
    key.extend_from_slice(BLOCKED_SCHEDULER_OPERATION_PREFIX);
    key.extend_from_slice(rest);
    Ok(key)
}

fn region_meta_key(prefix: &[u8], region_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 8);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&region_id.to_be_bytes());
    key
}

fn encode_apply_state(state: &RegionApplyState) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8 * 5);
    out.push(1);
    out.extend_from_slice(&state.region_id.to_be_bytes());
    out.extend_from_slice(&state.term.to_be_bytes());
    out.extend_from_slice(&state.applied_index.to_be_bytes());
    out.extend_from_slice(&state.truncated_term.to_be_bytes());
    out.extend_from_slice(&state.truncated_index.to_be_bytes());
    out
}

fn decode_apply_state(bytes: &[u8]) -> Result<RegionApplyState> {
    if bytes.len() != 1 + 8 * 5 {
        return Err(Error::InvalidMetadata(
            "invalid apply state length".to_owned(),
        ));
    }
    if bytes[0] != 1 {
        return Err(Error::InvalidMetadata(
            "unsupported apply state version".to_owned(),
        ));
    }
    Ok(RegionApplyState {
        region_id: u64::from_be_bytes(bytes[1..9].try_into().unwrap()),
        term: u64::from_be_bytes(bytes[9..17].try_into().unwrap()),
        applied_index: u64::from_be_bytes(bytes[17..25].try_into().unwrap()),
        truncated_term: u64::from_be_bytes(bytes[25..33].try_into().unwrap()),
        truncated_index: u64::from_be_bytes(bytes[33..41].try_into().unwrap()),
    })
}

fn to_backend_error(err: impl std::fmt::Display) -> mvcc::Error {
    mvcc::Error::Backend(err.to_string())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_vec(&mut self) -> mvcc::Result<Vec<u8>> {
        let len = self.read_u32()? as usize;
        if self.bytes.len().saturating_sub(self.offset) < len {
            return Err(mvcc::Error::Decode("short vector field".to_owned()));
        }
        let out = self.bytes[self.offset..self.offset + len].to_vec();
        self.offset += len;
        Ok(out)
    }

    fn read_u32(&mut self) -> mvcc::Result<u32> {
        if self.bytes.len().saturating_sub(self.offset) < 4 {
            return Err(mvcc::Error::Decode("short u32 field".to_owned()));
        }
        let out = u32::from_be_bytes(self.bytes[self.offset..self.offset + 4].try_into().unwrap());
        self.offset += 4;
        Ok(out)
    }

    fn read_u64(&mut self) -> mvcc::Result<u64> {
        if self.bytes.len().saturating_sub(self.offset) < 8 {
            return Err(mvcc::Error::Decode("short u64 field".to_owned()));
        }
        let out = u64::from_be_bytes(self.bytes[self.offset..self.offset + 8].try_into().unwrap());
        self.offset += 8;
        Ok(out)
    }

    fn read_i32(&mut self) -> mvcc::Result<i32> {
        if self.bytes.len().saturating_sub(self.offset) < 4 {
            return Err(mvcc::Error::Decode("short i32 field".to_owned()));
        }
        let out = i32::from_be_bytes(self.bytes[self.offset..self.offset + 4].try_into().unwrap());
        self.offset += 4;
        Ok(out)
    }

    fn done(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mvcc::{KvEngine, MvccSnapshotEngine};

    fn assert_abort_contains(error: Option<kvpb::KeyError>, needle: &str) {
        let err = error.expect("expected key error");
        assert!(
            err.abort.contains(needle),
            "expected abort containing {needle:?}, got {err:?}"
        );
    }

    #[test]
    fn opens_required_multi_tree_layout() {
        let store = HoltStore::open_memory().unwrap();
        store.data().unwrap();
        store.write().unwrap();
        store.lock().unwrap();
        store.region_meta().unwrap();
        store.apply_state().unwrap();
        store.watch_apply().unwrap();
    }

    #[test]
    fn stores_data_tree_values() {
        let store = HoltStore::open_memory().unwrap();
        store.put_data(b"/workspace/a", b"meta").unwrap();
        assert_eq!(store.get_data(b"/workspace/a").unwrap().unwrap(), b"meta");
    }

    #[test]
    fn applies_cross_tree_atomic_batch() {
        let store = HoltStore::open_memory().unwrap();
        let applied = store
            .atomic(|batch| {
                batch.put(DATA_TREE, b"k", b"v");
                batch.put(LOCK_TREE, b"k", b"lock");
            })
            .unwrap();
        assert!(applied);
        assert_eq!(store.data().unwrap().get(b"k").unwrap().unwrap(), b"v");
        assert_eq!(store.lock().unwrap().get(b"k").unwrap().unwrap(), b"lock");
    }

    #[test]
    fn region_descriptor_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = metapb::RegionDescriptor {
            region_id: 42,
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 7,
                conf_version: 3,
            }),
            peers: vec![metapb::RegionPeer {
                store_id: 5,
                peer_id: 55,
            }],
            ..Default::default()
        };
        {
            let store = HoltStore::open_file(dir.path()).unwrap();
            assert!(store.get_region_descriptor(42).unwrap().is_none());
            let bootstrapped = store
                .load_or_bootstrap_region_descriptor(&descriptor)
                .unwrap();
            assert_eq!(bootstrapped, descriptor);
            store.checkpoint().unwrap();
        }
        let reopened = HoltStore::open_file(dir.path()).unwrap();
        assert_eq!(
            reopened.get_region_descriptor(42).unwrap().unwrap(),
            descriptor
        );
        reopened.delete_region_descriptor(42).unwrap();
        assert!(reopened.get_region_descriptor(42).unwrap().is_none());
    }

    #[test]
    fn region_descriptors_list_persisted_descriptors_in_region_order() {
        let store = HoltStore::open_memory().unwrap();
        let first = metapb::RegionDescriptor {
            region_id: 2,
            start_key: b"m".to_vec(),
            peers: vec![metapb::RegionPeer {
                store_id: 7,
                peer_id: 20,
            }],
            ..Default::default()
        };
        let second = metapb::RegionDescriptor {
            region_id: 1,
            end_key: b"m".to_vec(),
            peers: vec![metapb::RegionPeer {
                store_id: 7,
                peer_id: 10,
            }],
            ..Default::default()
        };
        store.put_region_descriptor(&first).unwrap();
        store.put_region_descriptor(&second).unwrap();

        let descriptors = store.region_descriptors().unwrap();
        assert_eq!(
            descriptors
                .iter()
                .map(|descriptor| descriptor.region_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(descriptors[0], second);
        assert_eq!(descriptors[1], first);
    }

    #[test]
    fn region_apply_state_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let state = RegionApplyState {
            region_id: 42,
            term: 9,
            applied_index: 123,
            truncated_term: 8,
            truncated_index: 99,
        };
        {
            let store = HoltStore::open_file(dir.path()).unwrap();
            store.put_region_apply_state(&state).unwrap();
            store.checkpoint().unwrap();
        }
        let reopened = HoltStore::open_file(dir.path()).unwrap();
        assert_eq!(reopened.get_region_apply_state(42).unwrap().unwrap(), state);
    }

    #[test]
    fn pending_root_events_survive_reopen_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let event = metapb::RootEvent {
            kind: metapb::RootEventKind::PeerAdded as i32,
            payload: Some(metapb::root_event::Payload::PeerChange(
                metapb::RootPeerChange {
                    region_id: 42,
                    store_id: 7,
                    peer_id: 9,
                    target: Some(metapb::RegionDescriptor {
                        region_id: 42,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };
        {
            let store = HoltMvccStore::open_file(dir.path()).unwrap();
            assert_eq!(store.enqueue_pending_root_event(&event).unwrap(), 1);
            assert_eq!(store.enqueue_pending_root_event(&event).unwrap(), 2);
            store.checkpoint().unwrap();
        }

        let reopened = HoltMvccStore::open_file(dir.path()).unwrap();
        let pending = reopened.pending_root_events().unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].sequence, 1);
        assert_eq!(pending[0].event, event);

        reopened.delete_pending_root_event(1).unwrap();
        let pending = reopened.pending_root_events().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].sequence, 2);
    }

    #[test]
    fn blocked_root_events_survive_reopen_and_advance_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let event = metapb::RootEvent {
            kind: metapb::RootEventKind::PeerAdded as i32,
            payload: Some(metapb::root_event::Payload::PeerChange(
                metapb::RootPeerChange {
                    region_id: 42,
                    store_id: 7,
                    peer_id: 9,
                    target: Some(metapb::RegionDescriptor {
                        region_id: 42,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };
        {
            let store = HoltMvccStore::open_file(dir.path()).unwrap();
            let sequence = store.enqueue_pending_root_event(&event).unwrap();
            store
                .block_pending_root_event(
                    sequence,
                    &event,
                    "peer:42:add:7:9",
                    "catalog precondition",
                )
                .unwrap();
            store.checkpoint().unwrap();
        }

        let reopened = HoltMvccStore::open_file(dir.path()).unwrap();
        assert!(reopened.pending_root_events().unwrap().is_empty());
        let blocked = reopened.blocked_root_events().unwrap();
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].sequence, 1);
        assert_eq!(blocked[0].event, event);
        assert_eq!(blocked[0].transition_id, "peer:42:add:7:9");
        assert_eq!(blocked[0].last_error, "catalog precondition");
        assert_eq!(reopened.enqueue_pending_root_event(&event).unwrap(), 2);
    }

    #[test]
    fn pending_scheduler_operations_survive_reopen_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let split = coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
            region_id: 42,
            split_key: b"m".to_vec(),
            split_child: Some(metapb::RegionDescriptor {
                region_id: 43,
                ..Default::default()
            }),
            ..Default::default()
        };
        let merge = coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::MergeRegion as i32,
            region_id: 42,
            source_region_id: 43,
            ..Default::default()
        };
        {
            let store = HoltMvccStore::open_file(dir.path()).unwrap();
            store.record_pending_scheduler_operation(&split).unwrap();
            store.record_pending_scheduler_operation(&merge).unwrap();
            store.checkpoint().unwrap();
        }

        let reopened = HoltMvccStore::open_file(dir.path()).unwrap();
        let pending = reopened.pending_scheduler_operations().unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].operation, split);
        assert_eq!(pending[0].attempts, 0);
        assert_eq!(pending[1].operation, merge);
        assert_eq!(pending[1].attempts, 0);

        reopened.delete_pending_scheduler_operation(&split).unwrap();
        let pending = reopened.pending_scheduler_operations().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].operation, merge);
        assert_eq!(pending[0].attempts, 0);
    }

    #[test]
    fn pending_scheduler_operations_dedupe_by_full_operation_identity() {
        let store = HoltMvccStore::open_memory().unwrap();
        let split_m = coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
            region_id: 42,
            split_key: b"m".to_vec(),
            split_child: Some(metapb::RegionDescriptor {
                region_id: 43,
                ..Default::default()
            }),
            ..Default::default()
        };
        let split_n = coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
            region_id: 42,
            split_key: b"n".to_vec(),
            split_child: Some(metapb::RegionDescriptor {
                region_id: 44,
                ..Default::default()
            }),
            ..Default::default()
        };
        let merge = coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::MergeRegion as i32,
            region_id: 42,
            source_region_id: 43,
            ..Default::default()
        };

        store.record_pending_scheduler_operation(&split_m).unwrap();
        store.record_pending_scheduler_operation(&split_m).unwrap();
        store.record_pending_scheduler_operation(&split_n).unwrap();
        store.record_pending_scheduler_operation(&merge).unwrap();

        let pending = store.pending_scheduler_operations().unwrap();
        assert_eq!(pending.len(), 3);
        assert_eq!(pending[0].operation, split_m);
        assert_eq!(pending[0].attempts, 0);
        assert_eq!(pending[1].operation, split_n);
        assert_eq!(pending[1].attempts, 0);
        assert_eq!(pending[2].operation, merge);
        assert_eq!(pending[2].attempts, 0);
    }

    #[test]
    fn pending_scheduler_operation_attempts_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let operation = coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::LeaderTransfer as i32,
            region_id: 42,
            source_peer_id: 1,
            target_peer_id: 2,
            ..Default::default()
        };
        {
            let store = HoltMvccStore::open_file(dir.path()).unwrap();
            store
                .record_pending_scheduler_operation(&operation)
                .unwrap();
            assert_eq!(
                store
                    .increment_pending_scheduler_operation_attempts(&operation)
                    .unwrap(),
                1
            );
            store
                .record_pending_scheduler_operation(&operation)
                .unwrap();
            assert_eq!(
                store
                    .increment_pending_scheduler_operation_attempts(&operation)
                    .unwrap(),
                2
            );
            store.checkpoint().unwrap();
        }

        let reopened = HoltMvccStore::open_file(dir.path()).unwrap();
        let pending = reopened.pending_scheduler_operations().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].operation, operation);
        assert_eq!(pending[0].attempts, 2);
    }

    #[test]
    fn blocked_scheduler_operations_survive_reopen_and_clear_pending() {
        let dir = tempfile::tempdir().unwrap();
        let operation = coordpb::SchedulerOperation {
            r#type: coordpb::SchedulerOperationType::SplitRegion as i32,
            region_id: 42,
            split_key: b"m".to_vec(),
            split_child: Some(metapb::RegionDescriptor {
                region_id: 43,
                ..Default::default()
            }),
            ..Default::default()
        };
        {
            let store = HoltMvccStore::open_file(dir.path()).unwrap();
            store
                .record_pending_scheduler_operation(&operation)
                .unwrap();
            store
                .block_pending_scheduler_operation(&operation, 8, "attempt limit reached")
                .unwrap();
            store.checkpoint().unwrap();
        }

        let reopened = HoltMvccStore::open_file(dir.path()).unwrap();
        assert!(reopened.pending_scheduler_operations().unwrap().is_empty());
        let blocked = reopened.blocked_scheduler_operations().unwrap();
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].operation, operation);
        assert_eq!(blocked[0].attempts, 8);
        assert_eq!(blocked[0].last_error, "attempt limit reached");
    }

    #[test]
    fn holt_mvcc_prewrite_commit_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = HoltMvccStore::open_file(dir.path()).unwrap();
            store
                .prewrite(&kvpb::PrewriteRequest {
                    mutations: vec![kvpb::Mutation {
                        key: b"k".to_vec(),
                        value: b"v1".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    }],
                    primary_lock: b"k".to_vec(),
                    start_version: 10,
                    lock_ttl: 30_000,
                    ..Default::default()
                })
                .unwrap();
            store
                .commit(&kvpb::CommitRequest {
                    keys: vec![b"k".to_vec()],
                    start_version: 10,
                    commit_version: 20,
                })
                .unwrap();
            store.checkpoint().unwrap();
        }
        let reopened = HoltMvccStore::open_file(dir.path()).unwrap();
        let current = reopened
            .get(&kvpb::GetRequest {
                key: b"k".to_vec(),
                version: 20,
            })
            .unwrap();
        assert_eq!(current.value, b"v1");
    }

    #[test]
    fn holt_mvcc_empty_key_txn_requests_abort_without_partial_apply() {
        let store = HoltMvccStore::open_memory().unwrap();

        let prewrite = store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![
                    kvpb::Mutation {
                        key: b"prewrite-valid".to_vec(),
                        value: b"v".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    },
                    kvpb::Mutation {
                        key: Vec::new(),
                        value: b"bad".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    },
                ],
                primary_lock: b"prewrite-valid".to_vec(),
                start_version: 10,
                lock_ttl: 30_000,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(prewrite.errors.len(), 1);
        assert!(prewrite.errors[0].abort.contains("empty key in mutation"));
        let missing_lock = store
            .commit(&kvpb::CommitRequest {
                keys: vec![b"prewrite-valid".to_vec()],
                start_version: 10,
                commit_version: 20,
            })
            .unwrap();
        assert_abort_contains(missing_lock.error, "lock not found");

        store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"commit-valid".to_vec(),
                    value: b"v".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"commit-valid".to_vec(),
                start_version: 30,
                lock_ttl: 30_000,
                ..Default::default()
            })
            .unwrap();
        let commit = store
            .commit(&kvpb::CommitRequest {
                keys: vec![b"commit-valid".to_vec(), Vec::new()],
                start_version: 30,
                commit_version: 40,
            })
            .unwrap();
        assert_abort_contains(commit.error, "empty key in commit");
        let not_committed = store
            .get(&kvpb::GetRequest {
                key: b"commit-valid".to_vec(),
                version: 40,
            })
            .unwrap();
        assert!(not_committed.error.unwrap().locked.is_some());

        store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"rollback-valid".to_vec(),
                    value: b"v".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"rollback-valid".to_vec(),
                start_version: 50,
                lock_ttl: 30_000,
                ..Default::default()
            })
            .unwrap();
        let rollback = store
            .batch_rollback(&kvpb::BatchRollbackRequest {
                keys: vec![b"rollback-valid".to_vec(), Vec::new()],
                start_version: 50,
            })
            .unwrap();
        assert_abort_contains(rollback.error, "empty key in rollback");
        let still_committable = store
            .commit(&kvpb::CommitRequest {
                keys: vec![b"rollback-valid".to_vec()],
                start_version: 50,
                commit_version: 60,
            })
            .unwrap();
        assert!(still_committable.error.is_none());
    }

    #[test]
    fn holt_mvcc_empty_key_atomic_mutate_aborts_without_partial_apply() {
        let store = HoltMvccStore::open_memory().unwrap();

        let empty_predicate = store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                predicates: vec![kvpb::AtomicPredicate {
                    key: Vec::new(),
                    kind: kvpb::AtomicPredicateKind::NotExists as i32,
                    read_version: 1,
                    ..Default::default()
                }],
                mutations: vec![kvpb::Mutation {
                    key: b"predicate-valid".to_vec(),
                    value: b"v".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 2,
            })
            .unwrap();
        assert_abort_contains(empty_predicate.error, "empty key in mutation");
        assert!(
            store
                .get(&kvpb::GetRequest {
                    key: b"predicate-valid".to_vec(),
                    version: 2,
                })
                .unwrap()
                .not_found
        );

        let empty_mutation = store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![
                    kvpb::Mutation {
                        key: b"mutation-valid".to_vec(),
                        value: b"v".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    },
                    kvpb::Mutation {
                        key: Vec::new(),
                        value: b"bad".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    },
                ],
                predicates: Vec::new(),
                start_version: 3,
                commit_version: 4,
            })
            .unwrap();
        assert_abort_contains(empty_mutation.error, "empty key in mutation");
        assert!(
            store
                .get(&kvpb::GetRequest {
                    key: b"mutation-valid".to_vec(),
                    version: 4,
                })
                .unwrap()
                .not_found
        );
    }

    #[test]
    fn holt_mvcc_atomic_predicate_rejects_existing_key() {
        let store = HoltMvccStore::open_memory().unwrap();
        let first = store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                predicates: vec![kvpb::AtomicPredicate {
                    key: b"k".to_vec(),
                    kind: kvpb::AtomicPredicateKind::NotExists as i32,
                    read_version: 1,
                    ..Default::default()
                }],
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 2,
            })
            .unwrap();
        assert_eq!(first.applied_keys, 1);

        let second = store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                predicates: vec![kvpb::AtomicPredicate {
                    key: b"k".to_vec(),
                    kind: kvpb::AtomicPredicateKind::NotExists as i32,
                    read_version: 2,
                    ..Default::default()
                }],
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v2".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 3,
                commit_version: 4,
            })
            .unwrap();
        assert!(second.error.unwrap().already_exists.is_some());
    }

    #[test]
    fn holt_mvcc_atomic_mutate_matches_go_validation_and_idempotency() {
        let store = HoltMvccStore::open_memory().unwrap();
        let request = kvpb::TryAtomicMutateRequest {
            predicates: vec![kvpb::AtomicPredicate {
                key: b"atomic-idempotent".to_vec(),
                kind: kvpb::AtomicPredicateKind::NotExists as i32,
                ..Default::default()
            }],
            mutations: vec![kvpb::Mutation {
                key: b"atomic-idempotent".to_vec(),
                value: b"v1".to_vec(),
                op: kvpb::mutation::Op::Put as i32,
                ..Default::default()
            }],
            start_version: 10,
            commit_version: 11,
        };
        let first = store.try_atomic_mutate(&request).unwrap();
        assert_eq!(first.applied_keys, 1);
        let retry = store.try_atomic_mutate(&request).unwrap();
        assert_eq!(retry.applied_keys, 1);
        assert!(retry.error.is_none());

        let mismatch = store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                predicates: vec![kvpb::AtomicPredicate {
                    key: b"atomic-idempotent".to_vec(),
                    kind: kvpb::AtomicPredicateKind::ValueEquals as i32,
                    expected_value: b"old".to_vec(),
                    read_version: 11,
                }],
                mutations: vec![kvpb::Mutation {
                    key: b"atomic-idempotent".to_vec(),
                    value: b"bad".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 12,
                commit_version: 13,
            })
            .unwrap();
        assert!(mismatch
            .error
            .unwrap()
            .retryable
            .contains("atomic predicate mismatch"));

        let unsupported = store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"atomic-lock".to_vec(),
                    value: b"bad".to_vec(),
                    op: kvpb::mutation::Op::Lock as i32,
                    ..Default::default()
                }],
                start_version: 14,
                commit_version: 15,
                ..Default::default()
            })
            .unwrap();
        assert_abort_contains(unsupported.error, "unsupported mutation op");
    }

    #[test]
    fn holt_mvcc_resolve_lock_matches_go_key_set_boundary() {
        let store = HoltMvccStore::open_memory().unwrap();
        let key = b"resolve-key-boundary".to_vec();
        let prewrite = store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: key.clone(),
                    value: b"resolve-value".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: key.clone(),
                start_version: 40,
                lock_ttl: 10_000,
                ..Default::default()
            })
            .unwrap();
        assert!(prewrite.errors.is_empty());

        let empty = store
            .resolve_lock(&kvpb::ResolveLockRequest {
                start_version: 40,
                commit_version: 50,
                ..Default::default()
            })
            .unwrap();
        assert!(empty.error.is_none());
        assert_eq!(empty.resolved_locks, 0);

        let duplicate = store
            .resolve_lock(&kvpb::ResolveLockRequest {
                keys: vec![Vec::new(), key.clone(), key.clone()],
                start_version: 40,
                commit_version: 50,
            })
            .unwrap();
        assert!(duplicate.error.is_none());
        assert_eq!(duplicate.resolved_locks, 1);

        let retry = store
            .resolve_lock(&kvpb::ResolveLockRequest {
                keys: vec![key.clone()],
                start_version: 40,
                commit_version: 50,
            })
            .unwrap();
        assert!(retry.error.is_none());
        assert_eq!(retry.resolved_locks, 0);

        let got = store
            .get(&kvpb::GetRequest {
                key: key.clone(),
                version: 60,
            })
            .unwrap();
        assert_eq!(got.value, b"resolve-value");
    }

    #[test]
    fn holt_mvcc_prewrite_rejects_unsupported_ops_without_partial_apply() {
        let store = HoltMvccStore::open_memory().unwrap();
        let valid_key = b"prewrite-valid-before-unsupported".to_vec();
        let invalid_key = b"prewrite-unsupported".to_vec();
        let response = store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![
                    kvpb::Mutation {
                        key: valid_key.clone(),
                        value: b"valid".to_vec(),
                        op: kvpb::mutation::Op::Put as i32,
                        ..Default::default()
                    },
                    kvpb::Mutation {
                        key: invalid_key,
                        op: kvpb::mutation::Op::Rollback as i32,
                        ..Default::default()
                    },
                ],
                primary_lock: valid_key.clone(),
                start_version: 70,
                lock_ttl: 10_000,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(response.errors.len(), 1);
        assert_abort_contains(
            response.errors.into_iter().next(),
            "unsupported mutation op",
        );

        let got = store
            .get(&kvpb::GetRequest {
                key: valid_key,
                version: 80,
            })
            .unwrap();
        assert!(got.not_found);
    }

    #[test]
    fn holt_mvcc_prewrite_write_conflict_matches_go_fields_and_rollback_fence() {
        let store = HoltMvccStore::open_memory().unwrap();
        let key = b"prewrite-conflict-fields".to_vec();
        assert!(store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: key.clone(),
                    value: b"old".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: key.clone(),
                start_version: 10,
                lock_ttl: 10_000,
                ..Default::default()
            })
            .unwrap()
            .errors
            .is_empty());
        assert!(store
            .commit(&kvpb::CommitRequest {
                keys: vec![key.clone()],
                start_version: 10,
                commit_version: 20,
            })
            .unwrap()
            .error
            .is_none());

        let conflict = store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: key.clone(),
                    value: b"new".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: key,
                start_version: 15,
                lock_ttl: 10_000,
                ..Default::default()
            })
            .unwrap();
        let conflict = conflict.errors[0].write_conflict.as_ref().unwrap();
        assert_eq!(conflict.conflict_ts, 20);
        assert_eq!(conflict.start_ts, 10);
        assert_eq!(conflict.commit_ts, 15);

        let rollback_key = b"prewrite-rollback-fence".to_vec();
        assert!(store
            .batch_rollback(&kvpb::BatchRollbackRequest {
                keys: vec![rollback_key.clone()],
                start_version: 30,
            })
            .unwrap()
            .error
            .is_none());
        let fenced = store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: rollback_key,
                    value: b"new".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"prewrite-rollback-fence".to_vec(),
                start_version: 30,
                lock_ttl: 10_000,
                ..Default::default()
            })
            .unwrap();
        let fenced = fenced.errors[0].write_conflict.as_ref().unwrap();
        assert_eq!(fenced.conflict_ts, 30);
        assert_eq!(fenced.start_ts, 30);
        assert_eq!(fenced.commit_ts, 30);
    }

    #[test]
    fn holt_mvcc_rollback_marker_does_not_hide_older_visible_put() {
        let store = HoltMvccStore::open_memory().unwrap();
        store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 10,
                ..Default::default()
            })
            .unwrap();
        store
            .batch_rollback(&kvpb::BatchRollbackRequest {
                keys: vec![b"k".to_vec()],
                start_version: 20,
            })
            .unwrap();

        let current = store
            .get(&kvpb::GetRequest {
                key: b"k".to_vec(),
                version: 20,
            })
            .unwrap();
        assert!(!current.not_found);
        assert_eq!(current.value, b"v1");
    }

    #[test]
    fn holt_mvcc_scan_reports_read_version_and_skips_marker_writes() {
        let store = HoltMvccStore::open_memory().unwrap();
        store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 10,
                ..Default::default()
            })
            .unwrap();
        store
            .batch_rollback(&kvpb::BatchRollbackRequest {
                keys: vec![b"k".to_vec()],
                start_version: 20,
            })
            .unwrap();

        let scan = store
            .scan(&kvpb::ScanRequest {
                start_key: b"k".to_vec(),
                limit: 1,
                version: 30,
                include_start: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(scan.kvs.len(), 1);
        assert_eq!(scan.kvs[0].key, b"k");
        assert_eq!(scan.kvs[0].value, b"v1");
        assert_eq!(scan.kvs[0].version, 30);

        let latest = store
            .scan(&kvpb::ScanRequest {
                start_key: b"k".to_vec(),
                limit: 1,
                include_start: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(latest.kvs.len(), 1);
        assert_eq!(latest.kvs[0].version, u64::MAX);
    }

    #[test]
    fn holt_mvcc_expired_values_are_not_visible_to_get_or_scan() {
        let store = HoltMvccStore::open_memory().unwrap();
        store
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"expired".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    expires_at: 1,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 10,
                ..Default::default()
            })
            .unwrap();

        let got = store
            .get(&kvpb::GetRequest {
                key: b"k".to_vec(),
                version: 20,
            })
            .unwrap();
        assert!(got.not_found);

        let scan = store
            .scan(&kvpb::ScanRequest {
                start_key: b"k".to_vec(),
                limit: 1,
                version: 20,
                include_start: true,
                ..Default::default()
            })
            .unwrap();
        assert!(scan.kvs.is_empty());
    }

    #[test]
    fn holt_mvcc_snapshot_replaces_write_and_lock_trees() {
        let source = HoltMvccStore::open_memory().unwrap();
        source
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                start_version: 1,
                commit_version: 10,
                ..Default::default()
            })
            .unwrap();
        source
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"locked".to_vec(),
                    value: b"pending".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"locked".to_vec(),
                start_version: 20,
                lock_ttl: 30_000,
                ..Default::default()
            })
            .unwrap();
        source
            .batch_rollback(&kvpb::BatchRollbackRequest {
                keys: vec![b"rolled-back".to_vec()],
                start_version: 30,
            })
            .unwrap();

        let target = HoltMvccStore::open_memory().unwrap();
        target
            .try_atomic_mutate(&kvpb::TryAtomicMutateRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"old".to_vec(),
                    value: b"gone".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                commit_version: 5,
                ..Default::default()
            })
            .unwrap();
        target
            .install_mvcc_snapshot(source.export_mvcc_snapshot().unwrap())
            .unwrap();

        let current = target
            .get(&kvpb::GetRequest {
                key: b"k".to_vec(),
                version: 10,
            })
            .unwrap();
        assert_eq!(current.value, b"v1");
        let old = target
            .get(&kvpb::GetRequest {
                key: b"old".to_vec(),
                version: 10,
            })
            .unwrap();
        assert!(old.not_found);
        let locked = target
            .get(&kvpb::GetRequest {
                key: b"locked".to_vec(),
                version: 20,
            })
            .unwrap();
        assert!(locked.error.unwrap().locked.is_some());
        let rolled_back = target
            .commit(&kvpb::CommitRequest {
                keys: vec![b"rolled-back".to_vec()],
                start_version: 30,
                commit_version: 40,
            })
            .unwrap();
        assert!(rolled_back.error.unwrap().abort.contains("rolled back"));
    }

    #[test]
    fn holt_mvcc_check_txn_status_ttl_expire_rolls_back_primary() {
        let store = HoltMvccStore::open_memory().unwrap();
        store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"k".to_vec(),
                start_version: 10,
                lock_ttl: 1,
                ..Default::default()
            })
            .unwrap();

        let status = store
            .check_txn_status(&kvpb::CheckTxnStatusRequest {
                primary_key: b"k".to_vec(),
                lock_ts: 10,
                current_time: u64::MAX,
                rollback_if_not_exist: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            status.action,
            kvpb::CheckTxnStatusAction::CheckTxnStatusTtlExpireRollback as i32
        );

        let committed = store
            .commit(&kvpb::CommitRequest {
                keys: vec![b"k".to_vec()],
                start_version: 10,
                commit_version: 20,
            })
            .unwrap();
        assert!(committed.error.unwrap().abort.contains("rolled back"));
    }

    #[test]
    fn holt_mvcc_check_txn_status_empty_primary_rollback_aborts_without_marker() {
        let store = HoltMvccStore::open_memory().unwrap();
        let status = store
            .check_txn_status(&kvpb::CheckTxnStatusRequest {
                primary_key: Vec::new(),
                lock_ts: 10,
                current_time: 1,
                rollback_if_not_exist: true,
                ..Default::default()
            })
            .unwrap();
        assert_abort_contains(status.error, "empty key in rollback");

        let snapshot = store.export_mvcc_snapshot().unwrap();
        assert!(snapshot
            .rollbacks
            .iter()
            .all(|rollback| !rollback.key.is_empty()));
    }

    #[test]
    fn holt_mvcc_txn_heartbeat_validates_request_like_go_percolator() {
        let store = HoltMvccStore::open_memory().unwrap();
        let cases = [
            (
                kvpb::TxnHeartBeatRequest {
                    primary_key: Vec::new(),
                    start_version: 10,
                    ttl_extension: 1,
                    current_time: 1,
                },
                "heartbeat primary key is required",
            ),
            (
                kvpb::TxnHeartBeatRequest {
                    primary_key: b"k".to_vec(),
                    start_version: 0,
                    ttl_extension: 1,
                    current_time: 1,
                },
                "heartbeat start version is required",
            ),
            (
                kvpb::TxnHeartBeatRequest {
                    primary_key: b"k".to_vec(),
                    start_version: 10,
                    ttl_extension: 0,
                    current_time: 1,
                },
                "heartbeat ttl extension is required",
            ),
            (
                kvpb::TxnHeartBeatRequest {
                    primary_key: b"k".to_vec(),
                    start_version: 10,
                    ttl_extension: 1,
                    current_time: 0,
                },
                "heartbeat current time is required",
            ),
        ];
        for (request, needle) in cases {
            let heartbeat = store.txn_heartbeat(&request).unwrap();
            assert_abort_contains(heartbeat.error, needle);
        }
    }

    #[test]
    fn holt_mvcc_txn_heartbeat_rejects_secondary_lock_primary_mismatch() {
        let store = HoltMvccStore::open_memory().unwrap();
        store
            .prewrite(&kvpb::PrewriteRequest {
                mutations: vec![kvpb::Mutation {
                    key: b"secondary".to_vec(),
                    value: b"v1".to_vec(),
                    op: kvpb::mutation::Op::Put as i32,
                    ..Default::default()
                }],
                primary_lock: b"primary".to_vec(),
                start_version: 10,
                lock_ttl: 10,
                ..Default::default()
            })
            .unwrap();

        let heartbeat = store
            .txn_heartbeat(&kvpb::TxnHeartBeatRequest {
                primary_key: b"secondary".to_vec(),
                start_version: 10,
                ttl_extension: 100,
                current_time: current_physical_time_millis(),
            })
            .unwrap();
        assert_abort_contains(heartbeat.error, "primary key does not match lock primary");
    }
}
