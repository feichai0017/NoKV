use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};

use nokv_raftlog::{LogMarker, SegmentedRaftLog};

use crate::{decode_log_entry, encode_log_entry, Error, NodeId, OpenRaftEntry, RegionId};

pub trait RaftEntryLog {
    fn append_entries(&mut self, entries: &[OpenRaftEntry]) -> Result<(), Error>;
    fn sync(&self) -> Result<(), Error>;
    fn recover_entries(&self) -> Result<Vec<OpenRaftEntry>, Error>;
    fn last_purged_log_id(&self) -> Result<Option<openraft::LogId<NodeId>>, Error>;
    fn last_log_id(&self) -> Result<Option<openraft::LogId<NodeId>>, Error>;
    fn read_entries<R>(&self, range: R) -> Result<Vec<OpenRaftEntry>, Error>
    where
        R: RangeBounds<u64>;
    fn truncate_since(&mut self, log_id: openraft::LogId<NodeId>) -> Result<(), Error>;
    fn purge_upto(&mut self, log_id: openraft::LogId<NodeId>) -> Result<(), Error>;
}

pub struct SegmentedEntryLog {
    region_id: RegionId,
    dir: PathBuf,
    inner: SegmentedRaftLog,
}

impl SegmentedEntryLog {
    pub fn open(region_id: RegionId, dir: impl AsRef<Path>) -> Result<Self, Error> {
        let dir = dir.as_ref().to_path_buf();
        Ok(Self {
            region_id,
            inner: SegmentedRaftLog::open(&dir)?,
            dir,
        })
    }

    pub fn save_vote(&self, vote: openraft::Vote<NodeId>) -> Result<(), Error> {
        write_vote(&self.vote_path(), vote)
    }

    pub fn read_vote(&self) -> Result<Option<openraft::Vote<NodeId>>, Error> {
        read_vote(&self.vote_path())
    }

    pub fn save_committed(&self, committed: Option<openraft::LogId<NodeId>>) -> Result<(), Error> {
        match committed {
            Some(log_id) => write_log_id(&self.committed_path(), log_id),
            None => remove_file_if_exists(&self.committed_path()),
        }
    }

    pub fn read_committed(&self) -> Result<Option<openraft::LogId<NodeId>>, Error> {
        read_log_id(&self.committed_path())
    }

    fn vote_path(&self) -> PathBuf {
        self.dir.join("vote.meta")
    }

    fn committed_path(&self) -> PathBuf {
        self.dir.join("committed.meta")
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

    fn last_purged_log_id(&self) -> Result<Option<openraft::LogId<NodeId>>, Error> {
        Ok(self.inner.last_purged()?.map(marker_to_log_id))
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

    fn truncate_since(&mut self, log_id: openraft::LogId<NodeId>) -> Result<(), Error> {
        self.inner.truncate_since(log_id.index)?;
        Ok(())
    }

    fn purge_upto(&mut self, log_id: openraft::LogId<NodeId>) -> Result<(), Error> {
        self.inner.purge_upto(LogMarker {
            region_id: self.region_id,
            index: log_id.index,
            term: log_id.leader_id.term,
            node_id: log_id.leader_id.node_id,
        })?;
        Ok(())
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

fn marker_to_log_id(marker: LogMarker) -> openraft::LogId<NodeId> {
    openraft::LogId::new(
        openraft::CommittedLeaderId::new(marker.term, marker.node_id),
        marker.index,
    )
}

const VOTE_META_MAGIC: u32 = 0x4e4b_5654; // NKVT
const COMMITTED_META_MAGIC: u32 = 0x4e4b_434d; // NKCM
const META_VERSION: u16 = 1;
const VOTE_META_LEN: usize = 4 + 2 + 2 + 8 + 8 + 1 + 1 + 4;
const LOG_ID_META_LEN: usize = 4 + 2 + 2 + 8 + 8 + 8 + 4;

fn write_vote(path: &Path, vote: openraft::Vote<NodeId>) -> Result<(), Error> {
    let mut bytes = Vec::with_capacity(VOTE_META_LEN);
    bytes.extend_from_slice(&VOTE_META_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&META_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&vote.leader_id.term.to_le_bytes());
    bytes.extend_from_slice(&vote.leader_id.voted_for().unwrap_or_default().to_le_bytes());
    bytes.push(u8::from(vote.leader_id.voted_for().is_some()));
    bytes.push(u8::from(vote.committed));
    append_crc(&mut bytes);
    write_meta_file(path, &bytes)
}

fn read_vote(path: &Path) -> Result<Option<openraft::Vote<NodeId>>, Error> {
    let Some(bytes) = read_meta_file(path, VOTE_META_LEN)? else {
        return Ok(None);
    };
    validate_meta(&bytes, VOTE_META_MAGIC)?;
    let term = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let node_id = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let committed = bytes[25] != 0;
    Ok(Some(openraft::Vote {
        leader_id: openraft::LeaderId::new(term, node_id),
        committed,
    }))
}

fn write_log_id(path: &Path, log_id: openraft::LogId<NodeId>) -> Result<(), Error> {
    let mut bytes = Vec::with_capacity(LOG_ID_META_LEN);
    bytes.extend_from_slice(&COMMITTED_META_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&META_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&log_id.leader_id.term.to_le_bytes());
    bytes.extend_from_slice(&log_id.leader_id.node_id.to_le_bytes());
    bytes.extend_from_slice(&log_id.index.to_le_bytes());
    append_crc(&mut bytes);
    write_meta_file(path, &bytes)
}

fn read_log_id(path: &Path) -> Result<Option<openraft::LogId<NodeId>>, Error> {
    let Some(bytes) = read_meta_file(path, LOG_ID_META_LEN)? else {
        return Ok(None);
    };
    validate_meta(&bytes, COMMITTED_META_MAGIC)?;
    let term = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let node_id = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let index = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
    Ok(Some(openraft::LogId::new(
        openraft::CommittedLeaderId::new(term, node_id),
        index,
    )))
}

fn append_crc(bytes: &mut Vec<u8>) {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(bytes);
    bytes.extend_from_slice(&hasher.finalize().to_le_bytes());
}

fn validate_meta(bytes: &[u8], magic: u32) -> Result<(), Error> {
    let observed = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[..bytes.len() - 4]);
    if observed != hasher.finalize() {
        return Err(Error::CorruptMetadata("metadata crc mismatch"));
    }
    if u32::from_le_bytes(bytes[0..4].try_into().unwrap()) != magic {
        return Err(Error::CorruptMetadata("metadata magic mismatch"));
    }
    if u16::from_le_bytes(bytes[4..6].try_into().unwrap()) != META_VERSION {
        return Err(Error::CorruptMetadata("metadata version mismatch"));
    }
    Ok(())
}

fn write_meta_file(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let tmp_path = path.with_extension("tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(bytes)?;
        file.sync_data()?;
    }
    std::fs::rename(&tmp_path, path)?;
    sync_parent(path)?;
    Ok(())
}

fn read_meta_file(path: &Path, len: usize) -> Result<Option<Vec<u8>>, Error> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let mut bytes = vec![0u8; len];
    file.read_exact(&mut bytes)
        .map_err(|_| Error::CorruptMetadata("metadata length mismatch"))?;
    let mut trailing = [0u8; 1];
    match file.read(&mut trailing) {
        Ok(0) => {}
        Ok(_) => return Err(Error::CorruptMetadata("metadata trailing bytes")),
        Err(err) => return Err(err.into()),
    }
    Ok(Some(bytes))
}

fn remove_file_if_exists(path: &Path) -> Result<(), Error> {
    match std::fs::remove_file(path) {
        Ok(()) => {
            sync_parent(path)?;
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn sync_parent(path: &Path) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeId, Proposal, RaftStoreConfig};
    use nokv_proto::nokv::metadata::v1 as metadatapb;
    use openraft::{CommittedLeaderId, EntryPayload, LogId};

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    fn normal_entry(region_id: RegionId, index: u64) -> OpenRaftEntry {
        let command = metadatapb::MetadataCommitRequest {
            context: Some(metadatapb::MetadataContext {
                region_id,
                ..Default::default()
            }),
            command: Some(metadatapb::MetadataCommand {
                request_id: index.to_be_bytes().to_vec(),
                read_version: 8,
                commit_version: 9,
                mutations: vec![metadatapb::MetadataMutation {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    op: metadatapb::metadata_mutation::Op::Put as i32,
                    ..Default::default()
                }],
                watch_keys: vec![b"k".to_vec()],
                ..Default::default()
            }),
        };
        OpenRaftEntry {
            log_id: log_id(3, index),
            payload: EntryPayload::Normal(Proposal::from_metadata_command(&command).unwrap()),
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
    fn segmented_entry_log_truncates_conflicting_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        log.append_entries(&[normal_entry(7, 1), normal_entry(7, 2), normal_entry(7, 3)])
            .unwrap();

        log.truncate_since(log_id(3, 3)).unwrap();
        assert_eq!(
            log.recover_entries()
                .unwrap()
                .iter()
                .map(|entry| entry.log_id.index)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn segmented_entry_log_persists_purged_log_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        log.append_entries(&[normal_entry(7, 1), normal_entry(7, 2), normal_entry(7, 3)])
            .unwrap();
        log.purge_upto(log_id(3, 2)).unwrap();
        drop(log);

        let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        let purged = log.last_purged_log_id().unwrap().unwrap();
        assert_eq!(purged.index, 2);
        assert_eq!(purged.leader_id.node_id, 1);
        assert_eq!(
            log.recover_entries()
                .unwrap()
                .iter()
                .map(|entry| entry.log_id.index)
                .collect::<Vec<_>>(),
            vec![3]
        );
    }

    #[test]
    fn segmented_entry_log_persists_vote_and_committed_log_id() {
        let dir = tempfile::tempdir().unwrap();
        let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        log.save_vote(openraft::Vote::new_committed(11, 2)).unwrap();
        log.save_committed(Some(log_id(11, 9))).unwrap();
        drop(log);

        let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        let vote = log.read_vote().unwrap().unwrap();
        assert_eq!(vote.leader_id.term, 11);
        assert_eq!(vote.leader_id.voted_for(), Some(2));
        assert!(vote.committed);
        let committed = log.read_committed().unwrap().unwrap();
        assert_eq!(committed.leader_id.term, 11);
        assert_eq!(committed.index, 9);

        log.save_committed(None).unwrap();
        drop(log);
        let log = SegmentedEntryLog::open(7, dir.path()).unwrap();
        assert!(log.read_committed().unwrap().is_none());
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
