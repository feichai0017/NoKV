use std::sync::Arc;

use crate::{DurableReceipt, LogIndex, LogPosition, LogTerm, MetadataLogEntry, SharedLogError};
use nokvfs_meta::command::MetadataCommand;
use nokvfs_types::MountId;

pub trait SharedMetadataLog {
    fn append_batch(
        &self,
        term: LogTerm,
        mount: MountId,
        commands: &[MetadataCommand],
    ) -> Result<Vec<DurableReceipt>, SharedLogError>;

    fn read_from(
        &self,
        start: LogIndex,
        limit: usize,
    ) -> Result<Vec<MetadataLogEntry>, SharedLogError>;

    fn compact_through(&self, index: LogIndex) -> Result<(), SharedLogError>;

    fn committed_position(&self) -> Option<LogPosition>;

    fn committed_index(&self) -> LogIndex {
        self.committed_position()
            .map(|position| position.index)
            .unwrap_or(LogIndex::ZERO)
    }
}

impl<L> SharedMetadataLog for Arc<L>
where
    L: SharedMetadataLog + ?Sized,
{
    fn append_batch(
        &self,
        term: LogTerm,
        mount: MountId,
        commands: &[MetadataCommand],
    ) -> Result<Vec<DurableReceipt>, SharedLogError> {
        (**self).append_batch(term, mount, commands)
    }

    fn read_from(
        &self,
        start: LogIndex,
        limit: usize,
    ) -> Result<Vec<MetadataLogEntry>, SharedLogError> {
        (**self).read_from(start, limit)
    }

    fn compact_through(&self, index: LogIndex) -> Result<(), SharedLogError> {
        (**self).compact_through(index)
    }

    fn committed_position(&self) -> Option<LogPosition> {
        (**self).committed_position()
    }
}
