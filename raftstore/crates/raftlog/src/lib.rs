//! Segmented append-only Raft log for Rust raftstore.
//!
//! Raft log durability is kept separate from Holt's state-machine WAL because
//! consensus log append/truncate and metadata tree updates have different
//! access patterns and recovery boundaries.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

const MAGIC: u32 = 0x4e4b_524c; // NKRL
const MARKER_MAGIC: u32 = 0x4e4b_524d; // NKRM
const VERSION: u16 = 1;
const HEADER_LEN: usize = 4 + 2 + 2 + 8 + 8 + 8 + 4;
const MARKER_LEN: usize = 4 + 2 + 2 + 8 + 8 + 8 + 8 + 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub region_id: u64,
    pub index: u64,
    pub term: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogMarker {
    pub region_id: u64,
    pub index: u64,
    pub term: u64,
    pub node_id: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt raft log record at offset {offset}")]
    Corrupt { offset: u64 },
    #[error("corrupt raft log marker")]
    CorruptMarker,
    #[error("raft log append has purged entry index {index}, last purged index {purged_index}")]
    AppendPurgedEntry { index: u64, purged_index: u64 },
    #[error("raft log append is not consecutive: expected index {expected}, got {actual}")]
    NonConsecutiveAppend { expected: u64, actual: u64 },
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct SegmentedRaftLog {
    path: PathBuf,
    marker_path: PathBuf,
    file: File,
}

impl SegmentedRaftLog {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(dir.as_ref())?;
        let path = dir.as_ref().join("000001.log");
        let marker_path = dir.as_ref().join("purged.meta");
        let file = open_log_file(&path)?;
        Ok(Self {
            path,
            marker_path,
            file,
        })
    }

    pub fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
        let rewrite_prefix = self.prepare_append(entries)?;
        if let Some(prefix) = rewrite_prefix {
            self.rewrite_entries(&prefix)?;
        }
        for entry in entries {
            write_entry(&mut self.file, entry)?;
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    pub fn recover(&self) -> Result<Vec<LogEntry>> {
        let mut file = File::open(&self.path)?;
        let mut out = Vec::new();
        let purged = self.last_purged()?.map(|marker| marker.index);
        loop {
            let offset = file.stream_position()?;
            match read_entry(&mut file, offset)? {
                Some(entry) if purged.is_none_or(|index| entry.index > index) => out.push(entry),
                Some(_) => {}
                None => break,
            }
        }
        Ok(out)
    }

    pub fn truncate_since(&mut self, index: u64) -> Result<()> {
        let retained = self
            .recover()?
            .into_iter()
            .filter(|entry| entry.index < index)
            .collect::<Vec<_>>();
        self.rewrite_entries(&retained)
    }

    pub fn purge_upto(&mut self, marker: LogMarker) -> Result<()> {
        let marker = match self.last_purged()? {
            Some(current) if current.index >= marker.index => current,
            _ => marker,
        };
        write_marker(&self.marker_path, marker)?;
        let retained = self
            .recover()?
            .into_iter()
            .filter(|entry| entry.index > marker.index)
            .collect::<Vec<_>>();
        self.rewrite_entries(&retained)
    }

    pub fn last_purged(&self) -> Result<Option<LogMarker>> {
        read_marker(&self.marker_path)
    }

    fn prepare_append(&self, entries: &[LogEntry]) -> Result<Option<Vec<LogEntry>>> {
        let Some(first) = entries.first() else {
            return Ok(None);
        };
        let marker = self.last_purged()?;
        if let Some(marker) = marker {
            if first.index <= marker.index {
                return Err(Error::AppendPurgedEntry {
                    index: first.index,
                    purged_index: marker.index,
                });
            }
        }

        for pair in entries.windows(2) {
            let expected = pair[0].index + 1;
            let actual = pair[1].index;
            if actual != expected {
                return Err(Error::NonConsecutiveAppend { expected, actual });
            }
        }

        let recovered = self.recover()?;
        let prefix = recovered
            .iter()
            .take_while(|entry| entry.index < first.index)
            .cloned()
            .collect::<Vec<_>>();
        if let Some(last) = prefix.last() {
            let expected = last.index + 1;
            if first.index != expected {
                return Err(Error::NonConsecutiveAppend {
                    expected,
                    actual: first.index,
                });
            }
        } else if let Some(marker) = marker {
            let expected = marker.index + 1;
            if first.index != expected {
                return Err(Error::NonConsecutiveAppend {
                    expected,
                    actual: first.index,
                });
            }
        }
        if prefix.len() != recovered.len() {
            Ok(Some(prefix))
        } else {
            Ok(None)
        }
    }

    fn rewrite_entries(&mut self, entries: &[LogEntry]) -> Result<()> {
        let tmp_path = self.path.with_extension("rewrite");
        {
            let mut tmp = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            for entry in entries {
                write_entry(&mut tmp, entry)?;
            }
            tmp.sync_data()?;
        }
        std::fs::rename(&tmp_path, &self.path)?;
        sync_parent(&self.path)?;
        self.file = open_log_file(&self.path)?;
        Ok(())
    }
}

fn open_log_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
        .map_err(Into::into)
}

fn write_entry(file: &mut File, entry: &LogEntry) -> Result<()> {
    let payload_len = entry.payload.len() as u32;
    let mut header = Vec::with_capacity(HEADER_LEN);
    header.extend_from_slice(&MAGIC.to_le_bytes());
    header.extend_from_slice(&VERSION.to_le_bytes());
    header.extend_from_slice(&0u16.to_le_bytes());
    header.extend_from_slice(&entry.region_id.to_le_bytes());
    header.extend_from_slice(&entry.index.to_le_bytes());
    header.extend_from_slice(&entry.term.to_le_bytes());
    header.extend_from_slice(&payload_len.to_le_bytes());

    let crc = {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header);
        hasher.update(&entry.payload);
        hasher.finalize()
    };
    file.write_all(&header)?;
    file.write_all(&entry.payload)?;
    file.write_all(&crc.to_le_bytes())?;
    Ok(())
}

fn read_entry(file: &mut File, offset: u64) -> Result<Option<LogEntry>> {
    let mut header = [0u8; HEADER_LEN];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if magic != MAGIC || version != VERSION {
        return Err(Error::Corrupt { offset });
    }
    let region_id = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let index = u64::from_le_bytes(header[16..24].try_into().unwrap());
    let term = u64::from_le_bytes(header[24..32].try_into().unwrap());
    let payload_len = u32::from_le_bytes(header[32..36].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; payload_len];
    file.read_exact(&mut payload)?;
    let mut crc_bytes = [0u8; 4];
    file.read_exact(&mut crc_bytes)?;
    let observed = u32::from_le_bytes(crc_bytes);
    let expected = {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header);
        hasher.update(&payload);
        hasher.finalize()
    };
    if observed != expected {
        return Err(Error::Corrupt { offset });
    }
    Ok(Some(LogEntry {
        region_id,
        index,
        term,
        payload,
    }))
}

fn write_marker(path: &Path, marker: LogMarker) -> Result<()> {
    let tmp_path = path.with_extension("tmp");
    let mut bytes = Vec::with_capacity(MARKER_LEN);
    bytes.extend_from_slice(&MARKER_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&marker.region_id.to_le_bytes());
    bytes.extend_from_slice(&marker.index.to_le_bytes());
    bytes.extend_from_slice(&marker.term.to_le_bytes());
    bytes.extend_from_slice(&marker.node_id.to_le_bytes());
    let crc = {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&bytes);
        hasher.finalize()
    };
    bytes.extend_from_slice(&crc.to_le_bytes());
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_data()?;
    }
    std::fs::rename(tmp_path, path)?;
    sync_parent(path)?;
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn read_marker(path: &Path) -> Result<Option<LogMarker>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let mut bytes = [0u8; MARKER_LEN];
    file.read_exact(&mut bytes)
        .map_err(|_| Error::CorruptMarker)?;
    let observed = u32::from_le_bytes(bytes[MARKER_LEN - 4..MARKER_LEN].try_into().unwrap());
    let expected = {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&bytes[..MARKER_LEN - 4]);
        hasher.finalize()
    };
    if observed != expected {
        return Err(Error::CorruptMarker);
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    if magic != MARKER_MAGIC || version != VERSION {
        return Err(Error::CorruptMarker);
    }
    Ok(Some(LogMarker {
        region_id: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        index: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        term: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        node_id: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::SeekFrom;

    #[test]
    fn append_and_recover_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
        log.append(&[
            LogEntry {
                region_id: 7,
                index: 1,
                term: 3,
                payload: b"a".to_vec(),
            },
            LogEntry {
                region_id: 7,
                index: 2,
                term: 3,
                payload: b"b".to_vec(),
            },
        ])
        .unwrap();
        log.sync().unwrap();
        let recovered = log.recover().unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[1].payload, b"b");
    }

    #[test]
    fn detects_corrupt_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
        log.append(&[LogEntry {
            region_id: 1,
            index: 1,
            term: 1,
            payload: b"payload".to_vec(),
        }])
        .unwrap();
        drop(log);
        let path = dir.path().join("000001.log");
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        file.write_all(&[0xff]).unwrap();
        drop(file);
        let log = SegmentedRaftLog::open(dir.path()).unwrap();
        assert!(matches!(log.recover(), Err(Error::Corrupt { .. })));
    }

    #[test]
    fn truncate_since_removes_conflicting_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
        log.append(&[
            LogEntry {
                region_id: 7,
                index: 1,
                term: 1,
                payload: b"a".to_vec(),
            },
            LogEntry {
                region_id: 7,
                index: 2,
                term: 1,
                payload: b"b".to_vec(),
            },
            LogEntry {
                region_id: 7,
                index: 3,
                term: 2,
                payload: b"c".to_vec(),
            },
        ])
        .unwrap();
        log.truncate_since(3).unwrap();
        drop(log);

        let log = SegmentedRaftLog::open(dir.path()).unwrap();
        let recovered = log.recover().unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[1].index, 2);
    }

    #[test]
    fn purge_upto_persists_marker_and_filters_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
        log.append(&[
            LogEntry {
                region_id: 7,
                index: 1,
                term: 1,
                payload: b"a".to_vec(),
            },
            LogEntry {
                region_id: 7,
                index: 2,
                term: 1,
                payload: b"b".to_vec(),
            },
            LogEntry {
                region_id: 7,
                index: 3,
                term: 2,
                payload: b"c".to_vec(),
            },
        ])
        .unwrap();
        log.purge_upto(LogMarker {
            region_id: 7,
            index: 2,
            term: 1,
            node_id: 1,
        })
        .unwrap();
        drop(log);

        let log = SegmentedRaftLog::open(dir.path()).unwrap();
        assert_eq!(
            log.last_purged().unwrap(),
            Some(LogMarker {
                region_id: 7,
                index: 2,
                term: 1,
                node_id: 1,
            })
        );
        let recovered = log.recover().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].index, 3);
    }

    #[test]
    fn purge_beyond_tail_makes_marker_the_log_frontier() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
        log.append(&[
            LogEntry {
                region_id: 7,
                index: 1,
                term: 1,
                payload: b"a".to_vec(),
            },
            LogEntry {
                region_id: 7,
                index: 2,
                term: 1,
                payload: b"b".to_vec(),
            },
        ])
        .unwrap();
        log.purge_upto(LogMarker {
            region_id: 7,
            index: 5,
            term: 3,
            node_id: 1,
        })
        .unwrap();

        assert!(log.recover().unwrap().is_empty());
        assert_eq!(
            log.last_purged().unwrap(),
            Some(LogMarker {
                region_id: 7,
                index: 5,
                term: 3,
                node_id: 1,
            })
        );
        log.append(&[LogEntry {
            region_id: 7,
            index: 6,
            term: 4,
            payload: b"next".to_vec(),
        }])
        .unwrap();
        assert_eq!(log.recover().unwrap()[0].index, 6);
    }

    #[test]
    fn append_rejects_holes_after_existing_log() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
        log.append(&[LogEntry {
            region_id: 1,
            index: 1,
            term: 1,
            payload: b"a".to_vec(),
        }])
        .unwrap();

        let err = log
            .append(&[LogEntry {
                region_id: 1,
                index: 3,
                term: 1,
                payload: b"c".to_vec(),
            }])
            .unwrap_err();
        assert!(matches!(
            err,
            Error::NonConsecutiveAppend {
                expected: 2,
                actual: 3
            }
        ));
    }

    #[test]
    fn append_replaces_overlapping_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentedRaftLog::open(dir.path()).unwrap();
        log.append(&[
            LogEntry {
                region_id: 1,
                index: 1,
                term: 1,
                payload: b"a".to_vec(),
            },
            LogEntry {
                region_id: 1,
                index: 2,
                term: 1,
                payload: b"old".to_vec(),
            },
        ])
        .unwrap();
        log.append(&[
            LogEntry {
                region_id: 1,
                index: 2,
                term: 2,
                payload: b"new".to_vec(),
            },
            LogEntry {
                region_id: 1,
                index: 3,
                term: 2,
                payload: b"c".to_vec(),
            },
        ])
        .unwrap();

        let recovered = log.recover().unwrap();
        assert_eq!(recovered.len(), 3);
        assert_eq!(recovered[1].term, 2);
        assert_eq!(recovered[1].payload, b"new");
    }
}
