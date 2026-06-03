//! Object storage boundary for NoKV-FS file bodies.
//!
//! This crate owns body-object keys, an S3-compatible object backend, and an
//! in-memory object store for package tests. It does not own namespace metadata,
//! Holt state, Raft replication, FUSE, or wire types.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Component, Path};
use std::sync::{Arc, LazyLock, Mutex};

use opendal::blocking::Operator as BlockingOperator;
use opendal::services::S3;
use opendal::{ErrorKind, Operator};

static OPENDAL_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("nokvfs-object")
        .build()
        .expect("create NoKV-FS object runtime")
});

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStoreConfig {
    options: S3ObjectStoreOptions,
}

#[derive(Clone, Debug)]
pub struct MemoryObjectStore {
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3ObjectStoreOptions {
    pub bucket: String,
    pub root: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub virtual_host_style: bool,
    pub skip_signature: bool,
}

#[derive(Clone, Debug)]
pub struct S3ObjectStore {
    operator: BlockingOperator,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectError {
    EmptyKey,
    AbsoluteKey,
    ParentTraversal,
    CurrentDirectory,
    ContainsNul,
    InvalidRange,
    MissingBucket,
    MissingRegion,
    Backend(String),
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

impl ObjectStoreConfig {
    pub fn s3(options: S3ObjectStoreOptions) -> Self {
        Self { options }
    }

    pub fn rustfs(
        bucket: impl Into<String>,
        endpoint: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        Self {
            options: S3ObjectStoreOptions::rustfs(
                bucket,
                endpoint,
                access_key_id,
                secret_access_key,
            ),
        }
    }

    pub fn open(&self) -> Result<S3ObjectStore, ObjectError> {
        S3ObjectStore::new(self.options.clone())
    }

    pub fn options(&self) -> &S3ObjectStoreOptions {
        &self.options
    }
}

impl MemoryObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl Default for MemoryObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

impl S3ObjectStoreOptions {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            root: "/".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            virtual_host_style: false,
            skip_signature: false,
        }
    }

    pub fn rustfs(
        bucket: impl Into<String>,
        endpoint: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            root: "/".to_owned(),
            region: "auto".to_owned(),
            endpoint: Some(endpoint.into()),
            access_key_id: Some(access_key_id.into()),
            secret_access_key: Some(secret_access_key.into()),
            session_token: None,
            virtual_host_style: false,
            skip_signature: false,
        }
    }

    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.bucket.is_empty() {
            return Err(ObjectError::MissingBucket);
        }
        if self.region.is_empty() {
            return Err(ObjectError::MissingRegion);
        }
        Ok(())
    }
}

impl S3ObjectStore {
    pub fn new(options: S3ObjectStoreOptions) -> Result<Self, ObjectError> {
        options.validate()?;
        let mut builder = S3::default()
            .bucket(&options.bucket)
            .root(&options.root)
            .region(&options.region);
        if let Some(endpoint) = &options.endpoint {
            builder = builder.endpoint(endpoint);
        }
        if let Some(access_key_id) = &options.access_key_id {
            builder = builder.access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = &options.secret_access_key {
            builder = builder.secret_access_key(secret_access_key);
        }
        if let Some(session_token) = &options.session_token {
            builder = builder.session_token(session_token);
        }
        if options.virtual_host_style {
            builder = builder.enable_virtual_host_style();
        }
        if options.skip_signature {
            builder = builder.skip_signature();
        }

        let operator = Operator::new(builder)
            .map_err(ObjectError::from_backend)?
            .finish();
        let _guard = OPENDAL_RUNTIME.enter();
        let operator = BlockingOperator::new(operator).map_err(ObjectError::from_backend)?;
        Ok(Self { operator })
    }
}

impl ObjectStore for S3ObjectStore {
    fn put(&self, key: &ObjectKey, bytes: &[u8]) -> Result<ObjectInfo, ObjectError> {
        self.operator
            .write(key.as_str(), bytes.to_vec())
            .map_err(ObjectError::from_backend)?;
        Ok(ObjectInfo {
            key: key.clone(),
            size: bytes.len() as u64,
        })
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        let buffer = match range {
            Some(range) => {
                let end = range
                    .offset
                    .checked_add(range.len as u64)
                    .ok_or(ObjectError::InvalidRange)?;
                self.operator
                    .reader(key.as_str())
                    .and_then(|reader| reader.read(range.offset..end))
            }
            None => self.operator.read(key.as_str()),
        }
        .map_err(ObjectError::from_backend)?;
        Ok(buffer.to_vec())
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        match self.operator.stat(key.as_str()) {
            Ok(meta) => Ok(Some(ObjectInfo {
                key: key.clone(),
                size: meta.content_length(),
            })),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(ObjectError::from_backend(err)),
        }
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        let existed = self.head(key)?.is_some();
        self.operator
            .delete(key.as_str())
            .map_err(ObjectError::from_backend)?;
        Ok(existed)
    }
}

impl ObjectStore for MemoryObjectStore {
    fn put(&self, key: &ObjectKey, bytes: &[u8]) -> Result<ObjectInfo, ObjectError> {
        self.objects
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?
            .insert(key.as_str().to_owned(), bytes.to_vec());
        Ok(ObjectInfo {
            key: key.clone(),
            size: bytes.len() as u64,
        })
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        let objects = self
            .objects
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?;
        let Some(bytes) = objects.get(key.as_str()) else {
            return Err(ObjectError::Backend("object not found".to_owned()));
        };
        match range {
            Some(range) => slice_range(bytes, range),
            None => Ok(bytes.clone()),
        }
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        Ok(self
            .objects
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?
            .get(key.as_str())
            .map(|bytes| ObjectInfo {
                key: key.clone(),
                size: bytes.len() as u64,
            }))
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        Ok(self
            .objects
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?
            .remove(key.as_str())
            .is_some())
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

fn slice_range(bytes: &[u8], range: ObjectRange) -> Result<Vec<u8>, ObjectError> {
    let offset = usize::try_from(range.offset).map_err(|_| ObjectError::InvalidRange)?;
    if offset >= bytes.len() {
        return Ok(Vec::new());
    }
    let end = offset
        .checked_add(range.len)
        .ok_or(ObjectError::InvalidRange)?
        .min(bytes.len());
    Ok(bytes[offset..end].to_vec())
}

impl ObjectError {
    fn from_backend(err: impl fmt::Display) -> Self {
        Self::Backend(err.to_string())
    }

    fn from_poisoned_lock(err: impl fmt::Display) -> Self {
        Self::Backend(format!("object store lock poisoned: {err}"))
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
            Self::MissingBucket => write!(f, "S3 object store bucket is required"),
            Self::MissingRegion => write!(f, "S3 object store region is required"),
            Self::Backend(err) => write!(f, "object store backend error: {err}"),
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
    fn memory_object_store_put_head_get_delete() {
        let store = MemoryObjectStore::new();
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

    #[test]
    fn s3_options_validate_required_fields_and_rustfs_defaults() {
        let mut options = S3ObjectStoreOptions::new("");
        assert_eq!(options.validate(), Err(ObjectError::MissingBucket));

        options.bucket = "nokv".to_owned();
        options.region.clear();
        assert_eq!(options.validate(), Err(ObjectError::MissingRegion));

        let options =
            S3ObjectStoreOptions::rustfs("nokv", "http://127.0.0.1:9000", "access", "secret");
        assert_eq!(options.bucket, "nokv");
        assert_eq!(options.region, "auto");
        assert_eq!(options.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert!(!options.virtual_host_style);
    }

    #[test]
    fn object_store_config_requires_valid_s3_backend() {
        let config = ObjectStoreConfig::s3(S3ObjectStoreOptions::new(""));
        assert_eq!(config.open().unwrap_err(), ObjectError::MissingBucket);
    }

    #[test]
    fn s3_object_store_contract_from_env() {
        let Ok(bucket) = std::env::var("NOKV_FS_S3_BUCKET") else {
            return;
        };
        let mut options = S3ObjectStoreOptions::new(bucket);
        options.region = std::env::var("NOKV_FS_S3_REGION").unwrap_or_else(|_| "auto".to_owned());
        options.endpoint = std::env::var("NOKV_FS_S3_ENDPOINT").ok();
        options.access_key_id = std::env::var("NOKV_FS_S3_ACCESS_KEY_ID").ok();
        options.secret_access_key = std::env::var("NOKV_FS_S3_SECRET_ACCESS_KEY").ok();
        options.session_token = std::env::var("NOKV_FS_S3_SESSION_TOKEN").ok();
        options.virtual_host_style =
            std::env::var("NOKV_FS_S3_VIRTUAL_HOST_STYLE").as_deref() == Ok("1");
        options.skip_signature = std::env::var("NOKV_FS_S3_SKIP_SIGNATURE").as_deref() == Ok("1");

        let store = S3ObjectStore::new(options).unwrap();
        let key = ObjectKey::new(format!("nokv-fs-test/{}.bin", std::process::id())).unwrap();

        store.put(&key, b"abcdef").unwrap();
        assert_eq!(store.head(&key).unwrap().unwrap().size, 6);
        assert_eq!(
            store
                .get(&key, Some(ObjectRange::new(1, 3).unwrap()))
                .unwrap(),
            b"bcd"
        );
        assert!(store.delete(&key).unwrap());
        assert!(store.head(&key).unwrap().is_none());
    }
}
