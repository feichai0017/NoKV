use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::Path;

use crate::{Error, LogEntry, LogMarker, Result};

const MAGIC: u32 = 0x4e4b_524c; // NKRL
const MARKER_MAGIC: u32 = 0x4e4b_524d; // NKRM
const VERSION: u16 = 1;
const HEADER_LEN: usize = 4 + 2 + 2 + 8 + 8 + 8 + 4;
const MARKER_LEN: usize = 4 + 2 + 2 + 8 + 8 + 8 + 8 + 4;

pub(crate) fn read_entries_from_path(
    path: &Path,
    purged_index: Option<u64>,
) -> Result<Vec<LogEntry>> {
    let mut file = File::open(path)?;
    let mut out = Vec::new();
    loop {
        let offset = file.stream_position()?;
        match read_entry(&mut file, offset)? {
            Some(entry) if purged_index.is_none_or(|index| entry.index > index) => out.push(entry),
            Some(_) => {}
            None => break,
        }
    }
    Ok(out)
}

pub(crate) fn open_log_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
        .map_err(Into::into)
}

pub(crate) fn write_entry(file: &mut File, entry: &LogEntry) -> Result<()> {
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

pub(crate) fn write_marker(path: &Path, marker: LogMarker) -> Result<()> {
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

pub(crate) fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

pub(crate) fn read_marker(path: &Path) -> Result<Option<LogMarker>> {
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
