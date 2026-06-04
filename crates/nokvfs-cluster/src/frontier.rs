use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::{ApplyFrontier, LogIndex, LogPosition, LogTerm, SharedLogError};
use nokvfs_meta::Version;

const FRONTIER_MAGIC: &[u8; 8] = b"NKFSAPF1";
const FRONTIER_BYTES: usize = 32;

pub trait AppliedFrontierStore {
    fn load(&self) -> Result<Option<ApplyFrontier>, SharedLogError>;
    fn save(&self, frontier: ApplyFrontier) -> Result<(), SharedLogError>;
}

#[derive(Debug, Default)]
pub struct MemoryAppliedFrontierStore {
    inner: Mutex<Option<ApplyFrontier>>,
}

#[derive(Debug)]
pub struct FileAppliedFrontierStore {
    path: PathBuf,
    inner: Mutex<Option<ApplyFrontier>>,
}

impl MemoryAppliedFrontierStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AppliedFrontierStore for MemoryAppliedFrontierStore {
    fn load(&self) -> Result<Option<ApplyFrontier>, SharedLogError> {
        self.inner
            .lock()
            .map(|frontier| *frontier)
            .map_err(|_| SharedLogError::Backend("applied frontier mutex poisoned".to_owned()))
    }

    fn save(&self, frontier: ApplyFrontier) -> Result<(), SharedLogError> {
        *self
            .inner
            .lock()
            .map_err(|_| SharedLogError::Backend("applied frontier mutex poisoned".to_owned()))? =
            Some(frontier);
        Ok(())
    }
}

impl FileAppliedFrontierStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SharedLogError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(to_backend_error)?;
            }
        }
        let frontier = read_frontier(&path)?;
        Ok(Self {
            path,
            inner: Mutex::new(frontier),
        })
    }
}

impl AppliedFrontierStore for FileAppliedFrontierStore {
    fn load(&self) -> Result<Option<ApplyFrontier>, SharedLogError> {
        self.inner
            .lock()
            .map(|frontier| *frontier)
            .map_err(|_| SharedLogError::Backend("applied frontier mutex poisoned".to_owned()))
    }

    fn save(&self, frontier: ApplyFrontier) -> Result<(), SharedLogError> {
        write_frontier(&self.path, frontier)?;
        *self
            .inner
            .lock()
            .map_err(|_| SharedLogError::Backend("applied frontier mutex poisoned".to_owned()))? =
            Some(frontier);
        Ok(())
    }
}

fn read_frontier(path: &Path) -> Result<Option<ApplyFrontier>, SharedLogError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(to_backend_error(err)),
    };
    let mut encoded = Vec::new();
    file.read_to_end(&mut encoded).map_err(to_backend_error)?;
    if encoded.is_empty() {
        return Ok(None);
    }
    decode_frontier(&encoded).map(Some)
}

fn write_frontier(path: &Path, frontier: ApplyFrontier) -> Result<(), SharedLogError> {
    let tmp = frontier_temp_path(path);
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(to_backend_error)?;
        file.write_all(&encode_frontier(frontier))
            .map_err(to_backend_error)?;
        file.flush().map_err(to_backend_error)?;
        file.sync_data().map_err(to_backend_error)?;
    }
    fs::rename(&tmp, path).map_err(to_backend_error)?;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_data();
            }
        }
    }
    Ok(())
}

fn frontier_temp_path(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf();
    let file_name = path
        .file_name()
        .map(|name| {
            let mut name = name.to_os_string();
            name.push(".tmp");
            name
        })
        .unwrap_or_else(|| "applied.frontier.tmp".into());
    tmp.set_file_name(file_name);
    tmp
}

fn encode_frontier(frontier: ApplyFrontier) -> [u8; FRONTIER_BYTES] {
    let mut out = [0_u8; FRONTIER_BYTES];
    out[0..8].copy_from_slice(FRONTIER_MAGIC);
    out[8..16].copy_from_slice(&frontier.position.term.get().to_be_bytes());
    out[16..24].copy_from_slice(&frontier.position.index.get().to_be_bytes());
    out[24..32].copy_from_slice(&frontier.commit_version.get().to_be_bytes());
    out
}

fn decode_frontier(encoded: &[u8]) -> Result<ApplyFrontier, SharedLogError> {
    if encoded.len() != FRONTIER_BYTES {
        return Err(SharedLogError::Backend(
            "applied frontier marker has invalid length".to_owned(),
        ));
    }
    if &encoded[0..8] != FRONTIER_MAGIC {
        return Err(SharedLogError::Backend(
            "applied frontier marker has invalid magic".to_owned(),
        ));
    }
    let term = LogTerm::new(u64::from_be_bytes(encoded[8..16].try_into().unwrap()))?;
    let index = LogIndex::new(u64::from_be_bytes(encoded[16..24].try_into().unwrap()))?;
    let commit_version = Version::new(u64::from_be_bytes(encoded[24..32].try_into().unwrap()))
        .map_err(|err| SharedLogError::Backend(err.to_string()))?;
    Ok(ApplyFrontier {
        position: LogPosition { term, index },
        commit_version,
    })
}

fn to_backend_error(err: impl std::fmt::Display) -> SharedLogError {
    SharedLogError::Backend(err.to_string())
}
