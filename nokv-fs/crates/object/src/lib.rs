//! Object storage boundary for NoKV-FS file bodies.
//!
//! This crate owns body-object keys and the local filesystem object backend used
//! by demos and contract tests. It does not own namespace metadata, Holt state,
//! Raft replication, FUSE, or wire types.

use std::fmt;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

pub trait ObjectStore {
    fn put(&self, key: &ObjectKey, bytes: &[u8]) -> Result<ObjectInfo, ObjectError>;
    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError>;
    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError>;
    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError>;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectKey(String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectInfo {
    pub key: ObjectKey,
    pub size: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectRange {
    pub offset: u64,
    pub len: usize,
}

#[derive(Clone, Debug)]
pub struct LocalObjectStore {
    root: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectError {
    EmptyKey,
    AbsoluteKey,
    ParentTraversal,
    CurrentDirectory,
    ContainsNul,
    InvalidRange,
    Io(String),
}

impl ObjectKey {
    pub fn new(raw: impl Into<String>) -> Result<Self, ObjectError> {
        let raw = raw.into();
        validate_key(&raw)?;
        Ok(Self(raw))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl ObjectRange {
    pub fn new(offset: u64, len: usize) -> Result<Self, ObjectError> {
        if len == 0 {
            return Err(ObjectError::InvalidRange);
        }
        Ok(Self { offset, len })
    }
}

impl LocalObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, ObjectError> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(ObjectError::from_io)?;
        Ok(Self { root })
    }

    fn path_for(&self, key: &ObjectKey) -> PathBuf {
        let mut path = self.root.clone();
        for component in key.as_str().split('/') {
            path.push(component);
        }
        path
    }
}

impl ObjectStore for LocalObjectStore {
    fn put(&self, key: &ObjectKey, bytes: &[u8]) -> Result<ObjectInfo, ObjectError> {
        let final_path = self.path_for(key);
        let parent = final_path
            .parent()
            .ok_or_else(|| ObjectError::Io("object path has no parent".to_owned()))?;
        fs::create_dir_all(parent).map_err(ObjectError::from_io)?;

        let tmp_path = temp_path(parent, &final_path);
        {
            let mut file = File::create(&tmp_path).map_err(ObjectError::from_io)?;
            file.write_all(bytes).map_err(ObjectError::from_io)?;
            file.sync_all().map_err(ObjectError::from_io)?;
        }
        fs::rename(&tmp_path, &final_path).map_err(ObjectError::from_io)?;
        sync_dir(parent)?;

        Ok(ObjectInfo {
            key: key.clone(),
            size: bytes.len() as u64,
        })
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        let mut file = File::open(self.path_for(key)).map_err(ObjectError::from_io)?;
        match range {
            Some(range) => {
                file.seek(SeekFrom::Start(range.offset))
                    .map_err(ObjectError::from_io)?;
                let mut buf = vec![0; range.len];
                let read = file.read(&mut buf).map_err(ObjectError::from_io)?;
                buf.truncate(read);
                Ok(buf)
            }
            None => {
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).map_err(ObjectError::from_io)?;
                Ok(buf)
            }
        }
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        match fs::metadata(self.path_for(key)) {
            Ok(meta) if meta.is_file() => Ok(Some(ObjectInfo {
                key: key.clone(),
                size: meta.len(),
            })),
            Ok(_) => Ok(None),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(ObjectError::from_io(err)),
        }
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        match fs::remove_file(self.path_for(key)) {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(ObjectError::from_io(err)),
        }
    }
}

fn validate_key(raw: &str) -> Result<(), ObjectError> {
    if raw.is_empty() {
        return Err(ObjectError::EmptyKey);
    }
    if raw.as_bytes().contains(&0) {
        return Err(ObjectError::ContainsNul);
    }
    let path = Path::new(raw);
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => return Err(ObjectError::AbsoluteKey),
            Component::ParentDir => return Err(ObjectError::ParentTraversal),
            Component::CurDir => return Err(ObjectError::CurrentDirectory),
            Component::Normal(_) => {}
        }
    }
    Ok(())
}

fn temp_path(parent: &Path, final_path: &Path) -> PathBuf {
    let name = final_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("object");
    parent.join(format!(".{name}.tmp-{}", std::process::id()))
}

fn sync_dir(path: &Path) -> Result<(), ObjectError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(ObjectError::from_io)
}

impl ObjectError {
    fn from_io(err: std::io::Error) -> Self {
        Self::Io(err.to_string())
    }
}

impl fmt::Display for ObjectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyKey => write!(f, "object key is empty"),
            Self::AbsoluteKey => write!(f, "object key must be relative"),
            Self::ParentTraversal => write!(f, "object key contains '..'"),
            Self::CurrentDirectory => write!(f, "object key contains '.'"),
            Self::ContainsNul => write!(f, "object key contains NUL"),
            Self::InvalidRange => write!(f, "object range must have non-zero length"),
            Self::Io(err) => write!(f, "object store io error: {err}"),
        }
    }
}

impl std::error::Error for ObjectError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_key_rejects_unsafe_paths() {
        assert_eq!(ObjectKey::new(""), Err(ObjectError::EmptyKey));
        assert_eq!(ObjectKey::new("/abs"), Err(ObjectError::AbsoluteKey));
        assert_eq!(
            ObjectKey::new("../escape"),
            Err(ObjectError::ParentTraversal)
        );
        assert_eq!(
            ObjectKey::new("./current"),
            Err(ObjectError::CurrentDirectory)
        );
        assert_eq!(ObjectKey::new("bad\0key"), Err(ObjectError::ContainsNul));
    }

    #[test]
    fn local_object_store_put_head_get_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path()).unwrap();
        let key = ObjectKey::new("runs/1/artifact.bin").unwrap();

        let info = store.put(&key, b"abcdef").unwrap();
        assert_eq!(info.size, 6);
        assert_eq!(store.head(&key).unwrap().unwrap().size, 6);
        assert_eq!(store.get(&key, None).unwrap(), b"abcdef");
        assert_eq!(
            store
                .get(&key, Some(ObjectRange::new(2, 3).unwrap()))
                .unwrap(),
            b"cde"
        );
        assert!(store.delete(&key).unwrap());
        assert!(!store.delete(&key).unwrap());
        assert!(store.head(&key).unwrap().is_none());
    }
}
