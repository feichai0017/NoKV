use holt::RangeEntry;
use nokv_proto::nokv::coordinator::v1 as coordpb;
use nokv_proto::nokv::meta::v1 as metapb;
use prost::Message;

use crate::trees::{
    blocked_root_event_key, blocked_root_event_sequence, blocked_scheduler_operation_key,
    pending_root_event_key, pending_root_event_sequence, pending_scheduler_operation_key,
    BLOCKED_ROOT_EVENT_PREFIX, BLOCKED_SCHEDULER_OPERATION_PREFIX, PENDING_ROOT_EVENT_PREFIX,
    PENDING_SCHEDULER_OPERATION_PREFIX, REGION_META_TREE,
};
use crate::{
    BlockedRootEvent, BlockedSchedulerOperation, Error, HoltMetadataStore, HoltStore,
    PendingRootEvent, PendingSchedulerOperation, Result,
};

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

impl HoltStore {
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

    pub fn clear_scheduler_operation_diagnostic(
        &self,
        operation: &coordpb::SchedulerOperation,
    ) -> Result<()> {
        let pending_key = pending_scheduler_operation_key(operation)?;
        let blocked_key = blocked_scheduler_operation_key(operation)?;
        self.atomic(|batch| {
            batch.delete(REGION_META_TREE, &pending_key);
            batch.delete(REGION_META_TREE, &blocked_key);
        })?;
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
        self.atomic(|batch| {
            batch.put(REGION_META_TREE, &blocked_key, &bytes);
            batch.delete(REGION_META_TREE, &pending_key);
        })?;
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
                blocked.operation.source_peer_id,
                blocked.operation.target_peer_id,
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
}

impl HoltMetadataStore {
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

    pub fn clear_scheduler_operation_diagnostic(
        &self,
        operation: &coordpb::SchedulerOperation,
    ) -> Result<()> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| Error::InvalidMetadata("holt metadata mutex poisoned".to_owned()))?;
        self.store
            .clear_scheduler_operation_diagnostic(operation)
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
}
