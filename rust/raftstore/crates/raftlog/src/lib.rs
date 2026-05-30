//! Segmented append-only Raft log for Rust raftstore.
//!
//! Raft log durability is kept separate from Holt's state-machine WAL because
//! consensus log append/truncate and metadata tree updates have different
//! access patterns and recovery boundaries.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

const MAGIC: u32 = 0x4e4b_524c; // NKRL
const VERSION: u16 = 1;
const HEADER_LEN: usize = 4 + 2 + 2 + 8 + 8 + 8 + 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub region_id: u64,
    pub index: u64,
    pub term: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt raft log record at offset {offset}")]
    Corrupt { offset: u64 },
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct SegmentedRaftLog {
    path: PathBuf,
    file: File,
}

impl SegmentedRaftLog {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(dir.as_ref())?;
        let path = dir.as_ref().join("000001.log");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        Ok(Self { path, file })
    }

    pub fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
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
        loop {
            let offset = file.stream_position()?;
            match read_entry(&mut file, offset)? {
                Some(entry) => out.push(entry),
                None => break,
            }
        }
        Ok(out)
    }
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
}
