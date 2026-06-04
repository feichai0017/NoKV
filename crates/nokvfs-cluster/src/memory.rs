use std::collections::VecDeque;
use std::sync::Mutex;

use nokvfs_meta::command::MetadataCommand;
use nokvfs_types::MountId;

use crate::{
    DurableReceipt, LogIndex, LogPosition, LogTerm, MetadataLogEntry, SharedLogError,
    SharedMetadataLog,
};

#[derive(Debug)]
pub struct InMemorySharedLog {
    inner: Mutex<InMemoryState>,
}

#[derive(Debug)]
struct InMemoryState {
    next_index: u64,
    committed_position: Option<LogPosition>,
    compacted_through: LogIndex,
    entries: VecDeque<MetadataLogEntry>,
}

impl Default for InMemorySharedLog {
    fn default() -> Self {
        Self {
            inner: Mutex::new(InMemoryState {
                next_index: 1,
                committed_position: None,
                compacted_through: LogIndex::ZERO,
                entries: VecDeque::new(),
            }),
        }
    }
}

impl InMemorySharedLog {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SharedMetadataLog for InMemorySharedLog {
    fn append_batch(
        &self,
        term: LogTerm,
        mount: MountId,
        commands: &[MetadataCommand],
    ) -> Result<Vec<DurableReceipt>, SharedLogError> {
        if commands.is_empty() {
            return Err(SharedLogError::EmptyBatch);
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| SharedLogError::Backend("shared log mutex poisoned".to_owned()))?;
        let index = LogIndex::new(inner.next_index)?;
        inner.next_index = inner.next_index.saturating_add(1);
        let position = LogPosition { term, index };
        inner.committed_position = Some(position);
        let receipts = commands
            .iter()
            .enumerate()
            .map(|(batch_position, command)| DurableReceipt {
                position,
                mount,
                batch_position,
                request_id: command.request_id.clone(),
                commit_version: command.commit_version,
            })
            .collect::<Vec<_>>();
        inner.entries.push_back(MetadataLogEntry {
            position,
            mount,
            commands: commands.to_vec(),
        });
        Ok(receipts)
    }

    fn read_from(
        &self,
        start: LogIndex,
        limit: usize,
    ) -> Result<Vec<MetadataLogEntry>, SharedLogError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| SharedLogError::Backend("shared log mutex poisoned".to_owned()))?;
        if start <= inner.compacted_through {
            return Err(SharedLogError::Compacted {
                requested: start,
                compacted: inner.compacted_through,
            });
        }
        let limit = if limit == 0 { usize::MAX } else { limit };
        Ok(inner
            .entries
            .iter()
            .filter(|entry| entry.position.index >= start)
            .take(limit)
            .cloned()
            .collect())
    }

    fn compact_through(&self, index: LogIndex) -> Result<(), SharedLogError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| SharedLogError::Backend("shared log mutex poisoned".to_owned()))?;
        inner.compacted_through = inner.compacted_through.max(index);
        while inner
            .entries
            .front()
            .is_some_and(|entry| entry.position.index <= inner.compacted_through)
        {
            inner.entries.pop_front();
        }
        Ok(())
    }

    fn committed_position(&self) -> Option<LogPosition> {
        self.inner
            .lock()
            .map(|inner| inner.committed_position)
            .unwrap_or(None)
    }
}
