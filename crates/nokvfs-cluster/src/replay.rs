use nokvfs_meta::command::MetadataCommand;

use crate::{
    AppliedMetadataCommand, ApplyFrontier, DurableReceipt, LogIndex, MetadataLogEntry, ReplayError,
    SharedMetadataLog,
};

pub trait MetadataLogSink {
    fn apply_command(
        &self,
        receipt: DurableReceipt,
        command: MetadataCommand,
    ) -> Result<AppliedMetadataCommand, ReplayError>;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplayOutcome {
    pub entries: usize,
    pub commands: usize,
    pub frontier: Option<ApplyFrontier>,
}

pub struct ReplayDriver<'a, L, S> {
    log: &'a L,
    sink: &'a S,
}

impl<'a, L, S> ReplayDriver<'a, L, S>
where
    L: SharedMetadataLog,
    S: MetadataLogSink,
{
    pub fn new(log: &'a L, sink: &'a S) -> Self {
        Self { log, sink }
    }

    pub fn replay_from(&self, start: LogIndex, limit: usize) -> Result<ReplayOutcome, ReplayError> {
        let entries = self.log.read_from(start, limit)?;
        replay_entries(self.sink, start, &entries)
    }
}

pub fn replay_entries<S>(
    sink: &S,
    start: LogIndex,
    entries: &[MetadataLogEntry],
) -> Result<ReplayOutcome, ReplayError>
where
    S: MetadataLogSink,
{
    let mut expected = start;
    let mut outcome = ReplayOutcome::default();
    for entry in entries {
        if entry.position.index != expected {
            return Err(ReplayError::NonContiguousLog {
                expected,
                actual: entry.position.index,
            });
        }
        if entry.commands.is_empty() {
            return Err(ReplayError::EmptyEntry {
                position: entry.position,
            });
        }
        outcome.entries += 1;
        for (batch_position, command) in entry.commands.iter().cloned().enumerate() {
            let receipt = DurableReceipt {
                position: entry.position,
                mount: entry.mount,
                batch_position,
                request_id: command.request_id.clone(),
                commit_version: command.commit_version,
            };
            let applied = sink.apply_command(receipt, command)?;
            outcome.commands += 1;
            outcome.frontier = Some(ApplyFrontier {
                position: applied.receipt.position,
                commit_version: applied.receipt.commit_version,
            });
        }
        expected = next_index(expected)?;
    }
    Ok(outcome)
}

fn next_index(index: LogIndex) -> Result<LogIndex, ReplayError> {
    let next = index
        .get()
        .checked_add(1)
        .ok_or(ReplayError::IndexOverflow(index))?;
    LogIndex::new(next).map_err(ReplayError::from)
}
