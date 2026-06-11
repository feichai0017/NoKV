use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::io::Write;
#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::chunk::ObjectReadBlock;
use crate::store::{
    ObjectBytes, ObjectCapabilities, ObjectError, ObjectGetRequest, ObjectInfo, ObjectKey,
    ObjectRange, ObjectStore,
};

use super::pending::LOCAL_HOT_INTERNAL_DIR;
use super::placement::{resolve_block_placements, BlockPlacement};
use super::timing::duration_ns;

static LOCAL_OBJECT_TMP_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalObjectStoreOptions {
    pub root: PathBuf,
    pub max_bytes: Option<u64>,
    pub sync_on_put: bool,
}

#[derive(Clone, Debug)]
pub struct LocalObjectStore {
    root: Arc<PathBuf>,
    max_bytes: Option<u64>,
    sync_on_put: bool,
    state: Arc<Mutex<LocalObjectStoreState>>,
    created_dirs: Arc<Mutex<BTreeSet<PathBuf>>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LocalObjectStoreStats {
    pub resident_objects: u64,
    pub resident_bytes: u64,
    pub max_bytes: Option<u64>,
    pub evictions: u64,
    pub eviction_bytes: u64,
    pub admission_rejections: u64,
    pub puts: u64,
    pub put_bytes: u64,
    pub put_total_ns: u64,
    pub put_prepare_ns: u64,
    pub put_write_ns: u64,
    pub put_sync_ns: u64,
    pub put_rename_ns: u64,
    pub put_record_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LocalObjectResidency {
    bytes: u64,
    access_seq: u64,
}

#[derive(Clone, Debug, Default)]
struct LocalObjectStoreState {
    residents: BTreeMap<String, LocalObjectResidency>,
    resident_bytes: u64,
    evictions: u64,
    eviction_bytes: u64,
    admission_rejections: u64,
    access_seq: u64,
    puts: u64,
    put_bytes: u64,
    put_total_ns: u64,
    put_prepare_ns: u64,
    put_write_ns: u64,
    put_sync_ns: u64,
    put_rename_ns: u64,
    put_record_ns: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct LocalObjectPutTiming {
    total_ns: u64,
    prepare_ns: u64,
    write_ns: u64,
    sync_ns: u64,
    rename_ns: u64,
    record_ns: u64,
}
impl LocalObjectStoreOptions {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_bytes: None,
            sync_on_put: false,
        }
    }

    pub fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    pub fn with_sync_on_put(mut self, sync_on_put: bool) -> Self {
        self.sync_on_put = sync_on_put;
        self
    }
}

impl LocalObjectStore {
    pub fn new(options: LocalObjectStoreOptions) -> Result<Self, ObjectError> {
        fs::create_dir_all(&options.root).map_err(ObjectError::from_backend)?;
        let state = scan_local_hot_residency(&options.root)?;
        let store = Self {
            root: Arc::new(options.root),
            max_bytes: options.max_bytes,
            sync_on_put: options.sync_on_put,
            state: Arc::new(Mutex::new(state)),
            created_dirs: Arc::new(Mutex::new(BTreeSet::new())),
        };
        store.enforce_capacity()?;
        Ok(store)
    }

    fn object_path(&self, key: &ObjectKey) -> PathBuf {
        self.root.join(key.as_str())
    }

    fn ensure_parent_dir(&self, path: &Path) -> Result<(), ObjectError> {
        let Some(parent) = path.parent() else {
            return Ok(());
        };
        {
            let dirs = self
                .created_dirs
                .lock()
                .map_err(ObjectError::from_poisoned_lock)?;
            if dirs.contains(parent) {
                return Ok(());
            }
        }
        fs::create_dir_all(parent).map_err(ObjectError::from_backend)?;
        let mut dirs = self
            .created_dirs
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?;
        dirs.insert(parent.to_path_buf());
        Ok(())
    }

    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    pub fn stats(&self) -> Result<LocalObjectStoreStats, ObjectError> {
        let state = self.state.lock().map_err(ObjectError::from_poisoned_lock)?;
        Ok(LocalObjectStoreStats {
            resident_objects: state.residents.len() as u64,
            resident_bytes: state.resident_bytes,
            max_bytes: self.max_bytes,
            evictions: state.evictions,
            eviction_bytes: state.eviction_bytes,
            admission_rejections: state.admission_rejections,
            puts: state.puts,
            put_bytes: state.put_bytes,
            put_total_ns: state.put_total_ns,
            put_prepare_ns: state.put_prepare_ns,
            put_write_ns: state.put_write_ns,
            put_sync_ns: state.put_sync_ns,
            put_rename_ns: state.put_rename_ns,
            put_record_ns: state.put_record_ns,
        })
    }

    fn admit(&self, bytes: u64) -> Result<(), ObjectError> {
        if let Some(max_bytes) = self.max_bytes {
            if bytes > max_bytes {
                let mut state = self.state.lock().map_err(ObjectError::from_poisoned_lock)?;
                state.admission_rejections = state.admission_rejections.saturating_add(1);
                return Err(ObjectError::Backend(format!(
                    "local hot object exceeds capacity: object_bytes={bytes} max_bytes={max_bytes}"
                )));
            }
        }
        Ok(())
    }

    fn record_resident(&self, key: &ObjectKey, bytes: u64) -> Result<(), ObjectError> {
        let key = key.as_str().to_owned();
        let mut state = self.state.lock().map_err(ObjectError::from_poisoned_lock)?;
        state.access_seq = state.access_seq.saturating_add(1);
        let access_seq = state.access_seq;
        if let Some(old) = state
            .residents
            .insert(key, LocalObjectResidency { bytes, access_seq })
        {
            state.resident_bytes = state.resident_bytes.saturating_sub(old.bytes);
        }
        state.resident_bytes = state.resident_bytes.saturating_add(bytes);
        let over_capacity = self
            .max_bytes
            .is_some_and(|max_bytes| state.resident_bytes > max_bytes);
        drop(state);
        if over_capacity {
            self.enforce_capacity()
        } else {
            Ok(())
        }
    }

    fn forget_resident(&self, key: &ObjectKey) -> Result<(), ObjectError> {
        let mut state = self.state.lock().map_err(ObjectError::from_poisoned_lock)?;
        if let Some(old) = state.residents.remove(key.as_str()) {
            state.resident_bytes = state.resident_bytes.saturating_sub(old.bytes);
        }
        Ok(())
    }

    fn touch_resident(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        let mut state = self.state.lock().map_err(ObjectError::from_poisoned_lock)?;
        state.access_seq = state.access_seq.saturating_add(1);
        let access_seq = state.access_seq;
        if let Some(resident) = state.residents.get_mut(key.as_str()) {
            resident.access_seq = access_seq;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn enforce_capacity(&self) -> Result<(), ObjectError> {
        let Some(max_bytes) = self.max_bytes else {
            return Ok(());
        };
        let victims = {
            let mut state = self.state.lock().map_err(ObjectError::from_poisoned_lock)?;
            let mut victims = Vec::new();
            while state.resident_bytes > max_bytes {
                let Some(victim) = state
                    .residents
                    .iter()
                    .min_by_key(|(_, resident)| resident.access_seq)
                    .map(|(key, resident)| (key.clone(), resident.bytes))
                else {
                    break;
                };
                let (key, bytes) = victim;
                state.residents.remove(&key);
                state.resident_bytes = state.resident_bytes.saturating_sub(bytes);
                state.evictions = state.evictions.saturating_add(1);
                state.eviction_bytes = state.eviction_bytes.saturating_add(bytes);
                victims.push(key);
            }
            victims
        };
        for key in victims {
            match fs::remove_file(self.root.join(&key)) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(ObjectError::from_backend(err)),
            }
        }
        Ok(())
    }

    fn record_put_timing(
        &self,
        bytes: u64,
        timing: LocalObjectPutTiming,
    ) -> Result<(), ObjectError> {
        let mut state = self.state.lock().map_err(ObjectError::from_poisoned_lock)?;
        state.puts = state.puts.saturating_add(1);
        state.put_bytes = state.put_bytes.saturating_add(bytes);
        state.put_total_ns = state.put_total_ns.saturating_add(timing.total_ns);
        state.put_prepare_ns = state.put_prepare_ns.saturating_add(timing.prepare_ns);
        state.put_write_ns = state.put_write_ns.saturating_add(timing.write_ns);
        state.put_sync_ns = state.put_sync_ns.saturating_add(timing.sync_ns);
        state.put_rename_ns = state.put_rename_ns.saturating_add(timing.rename_ns);
        state.put_record_ns = state.put_record_ns.saturating_add(timing.record_ns);
        Ok(())
    }

    fn read_if_present(
        &self,
        key: &ObjectKey,
        range: Option<ObjectRange>,
    ) -> Result<Option<Vec<u8>>, ObjectError> {
        let path = self.object_path(key);
        match range {
            Some(range) => {
                let Some(file) = open_local_object_if_present(&path)? else {
                    self.forget_resident(key)?;
                    return Ok(None);
                };
                let bytes = read_local_object_range(&file, range)?;
                if self.max_bytes.is_some() && !self.touch_resident(key)? {
                    self.record_resident(
                        key,
                        file.metadata().map_err(ObjectError::from_backend)?.len(),
                    )?;
                }
                Ok(Some(bytes))
            }
            None => match fs::read(path) {
                Ok(bytes) => {
                    self.record_resident(key, bytes.len() as u64)?;
                    Ok(Some(bytes))
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    self.forget_resident(key)?;
                    Ok(None)
                }
                Err(err) => Err(ObjectError::from_backend(err)),
            },
        }
    }
}

fn scan_local_hot_residency(root: &Path) -> Result<LocalObjectStoreState, ObjectError> {
    let mut state = LocalObjectStoreState::default();
    scan_local_hot_dir(root, root, &mut state)?;
    Ok(state)
}

fn scan_local_hot_dir(
    root: &Path,
    dir: &Path,
    state: &mut LocalObjectStoreState,
) -> Result<(), ObjectError> {
    for entry in fs::read_dir(dir).map_err(ObjectError::from_backend)? {
        let entry = entry.map_err(ObjectError::from_backend)?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(ObjectError::from_backend)?;
        if file_type.is_dir() {
            if entry.file_name().to_str() == Some(LOCAL_HOT_INTERNAL_DIR) {
                continue;
            }
            scan_local_hot_dir(root, &path, state)?;
        } else if file_type.is_file() {
            if is_local_hot_temp_file(&path) {
                continue;
            }
            let Ok(relative) = path.strip_prefix(root) else {
                continue;
            };
            let Some(key) = relative.to_str().map(|path| path.replace('\\', "/")) else {
                continue;
            };
            if ObjectKey::new(key.clone()).is_err() {
                continue;
            }
            let bytes = entry.metadata().map_err(ObjectError::from_backend)?.len();
            state.access_seq = state.access_seq.saturating_add(1);
            state.residents.insert(
                key,
                LocalObjectResidency {
                    bytes,
                    access_seq: state.access_seq,
                },
            );
            state.resident_bytes = state.resident_bytes.saturating_add(bytes);
        }
    }
    Ok(())
}

fn is_local_hot_temp_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.starts_with('.') && name.ends_with(".tmp"))
        .unwrap_or(false)
}

impl ObjectStore for LocalObjectStore {
    fn capabilities(&self) -> ObjectCapabilities {
        ObjectCapabilities {
            range_get: true,
            ..ObjectCapabilities::default()
        }
    }

    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        let total_start = Instant::now();
        let prepare_start = Instant::now();
        let bytes = bytes.into();
        let size = bytes.len() as u64;
        self.admit(size)?;
        let path = self.object_path(key);
        self.ensure_parent_dir(&path)?;
        let id = LOCAL_OBJECT_TMP_ID.fetch_add(1, Ordering::Relaxed);
        let file_name = path
            .file_name()
            .ok_or_else(|| ObjectError::Backend("local object path has no file name".to_owned()))?
            .to_string_lossy();
        let tmp = path.with_file_name(format!(".{file_name}.{id:016x}.tmp"));
        let prepare_ns = duration_ns(prepare_start.elapsed());

        let write_start = Instant::now();
        let mut file = fs::File::create(&tmp).map_err(ObjectError::from_backend)?;
        file.write_all(bytes.as_slice())
            .map_err(ObjectError::from_backend)?;
        let write_ns = duration_ns(write_start.elapsed());

        let mut sync_ns = 0_u64;
        if self.sync_on_put {
            let sync_start = Instant::now();
            file.sync_all().map_err(ObjectError::from_backend)?;
            sync_ns = sync_ns.saturating_add(duration_ns(sync_start.elapsed()));
        }
        drop(file);

        let rename_start = Instant::now();
        fs::rename(&tmp, &path).map_err(ObjectError::from_backend)?;
        let rename_ns = duration_ns(rename_start.elapsed());

        if self.sync_on_put {
            let sync_start = Instant::now();
            sync_parent_dir(&path)?;
            sync_ns = sync_ns.saturating_add(duration_ns(sync_start.elapsed()));
        }

        let record_start = Instant::now();
        self.record_resident(key, size)?;
        let record_ns = duration_ns(record_start.elapsed());
        self.record_put_timing(
            size,
            LocalObjectPutTiming {
                total_ns: duration_ns(total_start.elapsed()),
                prepare_ns,
                write_ns,
                sync_ns,
                rename_ns,
                record_ns,
            },
        )?;
        Ok(ObjectInfo {
            key: key.clone(),
            size,
        })
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.read_if_present(key, range)?
            .ok_or_else(object_not_found)
    }

    fn get_if_present(
        &self,
        key: &ObjectKey,
        range: Option<ObjectRange>,
    ) -> Result<Option<Vec<u8>>, ObjectError> {
        self.read_if_present(key, range)
    }

    fn get_many_if_present(
        &self,
        requests: &[ObjectGetRequest],
    ) -> Result<Vec<Option<Vec<u8>>>, ObjectError> {
        requests
            .iter()
            .map(|request| self.read_if_present(&request.key, request.range))
            .collect()
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        match fs::metadata(self.object_path(key)) {
            Ok(metadata) => Ok(Some(ObjectInfo {
                key: key.clone(),
                size: {
                    let size = metadata.len();
                    self.record_resident(key, size)?;
                    size
                },
            })),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                self.forget_resident(key)?;
                Ok(None)
            }
            Err(err) => Err(ObjectError::from_backend(err)),
        }
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        match fs::remove_file(self.object_path(key)) {
            Ok(()) => {
                self.forget_resident(key)?;
                Ok(true)
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                self.forget_resident(key)?;
                Ok(false)
            }
            Err(err) => Err(ObjectError::from_backend(err)),
        }
    }

    fn resolve_read_placements(
        &self,
        blocks: &[ObjectReadBlock],
    ) -> Result<Vec<BlockPlacement>, ObjectError> {
        resolve_block_placements(self, blocks)
    }

    fn local_hot_stats(&self) -> Result<Option<LocalObjectStoreStats>, ObjectError> {
        self.stats().map(Some)
    }
}

fn sync_parent_dir(path: &Path) -> Result<(), ObjectError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let dir = fs::File::open(parent).map_err(ObjectError::from_backend)?;
    dir.sync_all().map_err(ObjectError::from_backend)
}

fn open_local_object_if_present(path: &Path) -> Result<Option<fs::File>, ObjectError> {
    match fs::File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(ObjectError::from_backend(err)),
    }
}

fn object_not_found() -> ObjectError {
    ObjectError::Backend("object not found".to_owned())
}

fn read_local_object_range(file: &fs::File, range: ObjectRange) -> Result<Vec<u8>, ObjectError> {
    let mut bytes = vec![0_u8; range.len];
    let mut filled = 0_usize;
    while filled < bytes.len() {
        let offset = range
            .offset
            .checked_add(u64::try_from(filled).map_err(|_| ObjectError::InvalidRange)?)
            .ok_or(ObjectError::InvalidRange)?;
        let read = read_local_object_at(file, &mut bytes[filled..], offset)
            .map_err(ObjectError::from_backend)?;
        if read == 0 {
            break;
        }
        filled = filled.checked_add(read).ok_or(ObjectError::InvalidRange)?;
    }
    bytes.truncate(filled);
    Ok(bytes)
}

#[cfg(unix)]
fn read_local_object_at(file: &fs::File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
    file.read_at(bytes, offset)
}

#[cfg(not(unix))]
fn read_local_object_at(file: &fs::File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(offset))?;
    file.read(bytes)
}
