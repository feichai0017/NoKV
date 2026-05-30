use std::ops::{Bound, RangeBounds};
use std::path::Path;

use nokv_raftlog::SegmentedRaftLog;

use crate::{decode_log_entry, encode_log_entry, Error, NodeId, OpenRaftEntry, RegionId};

pub trait RaftEntryLog {
    fn append_entries(&mut self, entries: &[OpenRaftEntry]) -> Result<(), Error>;
    fn sync(&self) -> Result<(), Error>;
    fn recover_entries(&self) -> Result<Vec<OpenRaftEntry>, Error>;
    fn last_log_id(&self) -> Result<Option<openraft::LogId<NodeId>>, Error>;
    fn read_entries<R>(&self, range: R) -> Result<Vec<OpenRaftEntry>, Error>
    where
        R: RangeBounds<u64>;
}

pub struct SegmentedEntryLog {
    region_id: RegionId,
    inner: SegmentedRaftLog,
}

impl SegmentedEntryLog {
    pub fn open(region_id: RegionId, dir: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self {
            region_id,
            inner: SegmentedRaftLog::open(dir)?,
        })
    }
}

impl RaftEntryLog for SegmentedEntryLog {
    fn append_entries(&mut self, entries: &[OpenRaftEntry]) -> Result<(), Error> {
        let encoded = entries
            .iter()
            .map(|entry| encode_log_entry(self.region_id, entry))
            .collect::<Result<Vec<_>, _>>()?;
        self.inner.append(&encoded)?;
        Ok(())
    }

    fn sync(&self) -> Result<(), Error> {
        self.inner.sync()?;
        Ok(())
    }

    fn recover_entries(&self) -> Result<Vec<OpenRaftEntry>, Error> {
        self.inner
            .recover()?
            .iter()
            .map(decode_log_entry)
            .collect::<Result<Vec<_>, _>>()
    }

    fn last_log_id(&self) -> Result<Option<openraft::LogId<NodeId>>, Error> {
        Ok(self.recover_entries()?.last().map(|entry| entry.log_id))
    }

    fn read_entries<R>(&self, range: R) -> Result<Vec<OpenRaftEntry>, Error>
    where
        R: RangeBounds<u64>,
    {
        self.recover_entries().map(|entries| {
            entries
                .into_iter()
                .filter(|entry| range_contains(&range, entry.log_id.index))
                .collect()
        })
    }
}

fn range_contains<R>(range: &R, index: u64) -> bool
where
    R: RangeBounds<u64>,
{
    let after_start = match range.start_bound() {
        Bound::Included(start) => index >= *start,
        Bound::Excluded(start) => index > *start,
        Bound::Unbounded => true,
    };
    let before_end = match range.end_bound() {
        Bound::Included(end) => index <= *end,
        Bound::Excluded(end) => index < *end,
        Bound::Unbounded => true,
    };
    after_start && before_end
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeId, Proposal, RaftStoreConfig};
    use nokv_proto::nokv::kv::v1 as kvpb;
    use nokv_proto::nokv::raft::v1 as raftpb;
    use openraft::{CommittedLeaderId, EntryPayload, LogId};

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    fn normal_entry(region_id: RegionId, index: u64) -> OpenRaftEntry {
        let command = raftpb::RaftCmdRequest {
            header: Some(raftpb::CmdHeader {
                region_id,
                request_id: index,
                ..Default::default()
            }),
            requests: vec![raftpb::Request {
                cmd_type: raftpb::CmdType::CmdGet as i32,
                cmd: Some(raftpb::request::Cmd::Get(kvpb::GetRequest {
                    key: b"k".to_vec(),
                    version: 9,
                })),
            }],
        };
        OpenRaftEntry {
            log_id: log_id(3, index),
            payload: EntryPayload::Normal(Proposal::from_raft_command(&command).unwrap()),
        }
    }

    #[test]
    fn segmented_entry_log_recovers_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        log.append_entries(&[normal_entry(7, 1), normal_entry(7, 2)])
            .unwrap();
        log.sync().unwrap();

        let recovered = log.recover_entries().unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[1].log_id.index, 2);
        assert!(matches!(
            recovered[1].payload,
            EntryPayload::<RaftStoreConfig>::Normal(_)
        ));
    }

    #[test]
    fn segmented_entry_log_reads_index_range_and_last_log_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        log.append_entries(&[normal_entry(7, 1), normal_entry(7, 2), normal_entry(7, 3)])
            .unwrap();

        let entries = log.read_entries(2..4).unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.log_id.index)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(log.last_log_id().unwrap().unwrap().index, 3);
    }

    #[test]
    fn segmented_entry_log_rejects_region_mismatch_without_partial_append() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentedEntryLog::open(7, dir.path()).unwrap();

        let err = log
            .append_entries(&[normal_entry(7, 1), normal_entry(8, 2)])
            .unwrap_err();
        assert!(matches!(err, Error::LogRegionMismatch { .. }));
        assert!(log.recover_entries().unwrap().is_empty());
    }
}
