use nokvfs_meta::command::MetadataCommand;
use nokvfs_types::MountId;

use crate::{
    AppliedMetadataCommand, ApplyFrontier, DurableReceipt, LogTerm, MetadataLogSink, ReplayError,
    SharedMetadataLog,
};

pub struct MetadataGroup<'a, L, S> {
    log: &'a L,
    sink: &'a S,
    term: LogTerm,
    mount: MountId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataGroupCommit {
    pub durable_receipts: Vec<DurableReceipt>,
    pub applied: Vec<AppliedMetadataCommand>,
    pub frontier: Option<ApplyFrontier>,
}

impl<'a, L, S> MetadataGroup<'a, L, S>
where
    L: SharedMetadataLog,
    S: MetadataLogSink,
{
    pub fn new(log: &'a L, sink: &'a S, term: LogTerm, mount: MountId) -> Self {
        Self {
            log,
            sink,
            term,
            mount,
        }
    }

    pub fn commit_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Result<MetadataGroupCommit, ReplayError> {
        let durable_receipts = self.log.append_batch(self.term, self.mount, commands)?;
        let mut applied = Vec::with_capacity(durable_receipts.len());
        for (receipt, command) in durable_receipts
            .iter()
            .cloned()
            .zip(commands.iter().cloned())
        {
            applied.push(self.sink.apply_command(receipt, command)?);
        }
        let frontier = applied.last().map(|applied| ApplyFrontier {
            position: applied.receipt.position,
            commit_version: applied.receipt.commit_version,
        });
        Ok(MetadataGroupCommit {
            durable_receipts,
            applied,
            frontier,
        })
    }
}
