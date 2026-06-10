use std::collections::BTreeMap;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, LazyLock, Mutex};
use std::thread;

use opendal::blocking::Operator as BlockingOperator;
use opendal::options::WriteOptions;
use opendal::services::S3;
use opendal::{ErrorKind, Operator};

use crate::chunk::{ObjectReadBlock, StagedObjectSet};
use crate::fabric::{
    default_pending_cold_put_root, BlockPlacement, DataTransport, LocalObjectStore,
    LocalObjectStoreOptions, LocalObjectStoreStats, TieredObjectStore, TieredObjectStoreOptions,
    TieredObjectStoreStats, TieredPutPolicy,
};

pub const DEFAULT_S3_MULTIPART_CHUNK_SIZE: usize = 8 * 1024 * 1024;
pub const DEFAULT_S3_MULTIPART_CONCURRENCY: usize = 8;
pub const DEFAULT_S3_GET_CONCURRENCY: usize = 16;

static OPENDAL_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("nokv-object")
        .build()
        .expect("create NoKV object runtime")
});

pub trait ObjectStore {
    fn capabilities(&self) -> ObjectCapabilities {
        ObjectCapabilities::default()
    }

    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError>;
    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError>;
    fn get_if_present(
        &self,
        key: &ObjectKey,
        range: Option<ObjectRange>,
    ) -> Result<Option<Vec<u8>>, ObjectError> {
        if self.head(key)?.is_some() {
            self.get(key, range).map(Some)
        } else {
            Ok(None)
        }
    }

    fn get_many_if_present(
        &self,
        requests: &[ObjectGetRequest],
    ) -> Result<Vec<Option<Vec<u8>>>, ObjectError> {
        requests
            .iter()
            .map(|request| self.get_if_present(&request.key, request.range))
            .collect()
    }

    fn get_many(&self, requests: &[ObjectGetRequest]) -> Result<Vec<Vec<u8>>, ObjectError> {
        requests
            .iter()
            .map(|request| self.get(&request.key, request.range))
            .collect()
    }
    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError>;
    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError>;
    fn resolve_read_placements(
        &self,
        blocks: &[ObjectReadBlock],
    ) -> Result<Vec<BlockPlacement>, ObjectError> {
        resolve_object_read_placements(blocks)
    }

    fn tiered_stats(&self) -> Result<Option<TieredObjectStoreStats>, ObjectError> {
        Ok(None)
    }

    fn local_hot_stats(&self) -> Result<Option<LocalObjectStoreStats>, ObjectError> {
        Ok(None)
    }
}

fn resolve_object_read_placements(
    blocks: &[ObjectReadBlock],
) -> Result<Vec<BlockPlacement>, ObjectError> {
    blocks
        .iter()
        .map(|block| {
            Ok(BlockPlacement {
                object_key: ObjectKey::new(block.object_key.clone())?,
                transport: DataTransport::ObjectTcpGet,
            })
        })
        .collect()
}

impl<T> ObjectStore for Arc<T>
where
    T: ObjectStore + ?Sized,
{
    fn capabilities(&self) -> ObjectCapabilities {
        (**self).capabilities()
    }

    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        (**self).put(key, bytes)
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        (**self).get(key, range)
    }

    fn get_if_present(
        &self,
        key: &ObjectKey,
        range: Option<ObjectRange>,
    ) -> Result<Option<Vec<u8>>, ObjectError> {
        (**self).get_if_present(key, range)
    }

    fn get_many_if_present(
        &self,
        requests: &[ObjectGetRequest],
    ) -> Result<Vec<Option<Vec<u8>>>, ObjectError> {
        (**self).get_many_if_present(requests)
    }

    fn get_many(&self, requests: &[ObjectGetRequest]) -> Result<Vec<Vec<u8>>, ObjectError> {
        (**self).get_many(requests)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        (**self).head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        (**self).delete(key)
    }

    fn resolve_read_placements(
        &self,
        blocks: &[ObjectReadBlock],
    ) -> Result<Vec<BlockPlacement>, ObjectError> {
        (**self).resolve_read_placements(blocks)
    }

    fn tiered_stats(&self) -> Result<Option<TieredObjectStoreStats>, ObjectError> {
        (**self).tiered_stats()
    }

    fn local_hot_stats(&self) -> Result<Option<LocalObjectStoreStats>, ObjectError> {
        (**self).local_hot_stats()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectCapabilities {
    pub range_get: bool,
    pub multipart_put: bool,
    pub server_side_copy: bool,
    pub max_single_put_bytes: Option<u64>,
    pub multipart_min_part_bytes: Option<u64>,
    pub multipart_max_part_bytes: Option<u64>,
    pub multipart_max_parts: Option<u64>,
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
pub enum ObjectBytes {
    Owned(Vec<u8>),
    Shared(Arc<[u8]>),
    SharedVec(Arc<Vec<u8>>),
    SharedVecSlice {
        bytes: Arc<Vec<u8>>,
        offset: usize,
        len: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectGetRequest {
    pub key: ObjectKey,
    pub range: Option<ObjectRange>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStoreConfig {
    kind: ObjectStoreConfigKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ObjectStoreConfigKind {
    S3(S3ObjectStoreOptions),
    TieredLocal {
        hot: LocalObjectStoreOptions,
        cold: S3ObjectStoreOptions,
        options: TieredObjectStoreOptions,
    },
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

#[derive(Clone, Debug)]
pub enum ConfiguredObjectStore {
    S3(Arc<S3ObjectStore>),
    TieredLocal(Arc<TieredObjectStore<LocalObjectStore, S3ObjectStore>>),
}

impl ConfiguredObjectStore {
    pub fn tiered_stats(&self) -> Result<Option<TieredObjectStoreStats>, ObjectError> {
        match self {
            Self::S3(_) => Ok(None),
            Self::TieredLocal(store) => store.stats().map(Some),
        }
    }

    pub fn local_hot_stats(&self) -> Result<Option<LocalObjectStoreStats>, ObjectError> {
        match self {
            Self::S3(_) => Ok(None),
            Self::TieredLocal(store) => store.hot().stats().map(Some),
        }
    }
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
    InvalidChunkLayout,
    Backend(String),
    StagedWriteFailed {
        source: String,
        staged: StagedObjectSet,
    },
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

impl ObjectBytes {
    pub fn shared(bytes: Arc<[u8]>) -> Self {
        Self::Shared(bytes)
    }

    pub fn shared_vec(bytes: Arc<Vec<u8>>) -> Self {
        Self::SharedVec(bytes)
    }

    pub fn shared_vec_slice(
        bytes: Arc<Vec<u8>>,
        offset: usize,
        len: usize,
    ) -> Result<Self, ObjectError> {
        let end = offset.checked_add(len).ok_or(ObjectError::InvalidRange)?;
        if end > bytes.len() {
            return Err(ObjectError::InvalidRange);
        }
        if offset == 0 && len == bytes.len() {
            Ok(Self::SharedVec(bytes))
        } else {
            Ok(Self::SharedVecSlice { bytes, offset, len })
        }
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared(bytes) => bytes,
            Self::SharedVec(bytes) => bytes,
            Self::SharedVecSlice { bytes, offset, len } => {
                let end = offset
                    .checked_add(*len)
                    .expect("shared object byte slice offset overflows");
                &bytes[*offset..end]
            }
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.as_slice().as_ptr()
    }

    pub fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared(bytes) => bytes.to_vec(),
            Self::SharedVec(bytes) => {
                Arc::try_unwrap(bytes).unwrap_or_else(|bytes| (*bytes).clone())
            }
            Self::SharedVecSlice { bytes, offset, len } => {
                let end = offset
                    .checked_add(len)
                    .expect("shared object byte slice offset overflows");
                bytes[offset..end].to_vec()
            }
        }
    }

    pub(crate) fn into_shared_vec_window(
        self,
    ) -> Result<(Arc<Vec<u8>>, usize, usize), ObjectError> {
        match self {
            Self::Owned(bytes) => {
                let len = bytes.len();
                Ok((Arc::new(bytes), 0, len))
            }
            Self::Shared(bytes) => {
                let len = bytes.len();
                Ok((Arc::new(bytes.to_vec()), 0, len))
            }
            Self::SharedVec(bytes) => {
                let len = bytes.len();
                Ok((bytes, 0, len))
            }
            Self::SharedVecSlice { bytes, offset, len } => {
                let end = offset.checked_add(len).ok_or(ObjectError::InvalidRange)?;
                if end > bytes.len() {
                    return Err(ObjectError::InvalidRange);
                }
                Ok((bytes, offset, len))
            }
        }
    }
}

impl Default for ObjectBytes {
    fn default() -> Self {
        Self::Owned(Vec::new())
    }
}

impl From<Vec<u8>> for ObjectBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Owned(bytes)
    }
}

impl From<Arc<[u8]>> for ObjectBytes {
    fn from(bytes: Arc<[u8]>) -> Self {
        Self::Shared(bytes)
    }
}

impl From<Arc<Vec<u8>>> for ObjectBytes {
    fn from(bytes: Arc<Vec<u8>>) -> Self {
        Self::SharedVec(bytes)
    }
}

impl AsRef<[u8]> for ObjectBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl ObjectGetRequest {
    pub fn new(key: ObjectKey, range: Option<ObjectRange>) -> Self {
        Self { key, range }
    }
}

impl Default for ObjectCapabilities {
    fn default() -> Self {
        Self {
            range_get: true,
            multipart_put: false,
            server_side_copy: false,
            max_single_put_bytes: None,
            multipart_min_part_bytes: None,
            multipart_max_part_bytes: None,
            multipart_max_parts: None,
        }
    }
}

impl ObjectStoreConfig {
    pub fn s3(options: S3ObjectStoreOptions) -> Self {
        Self {
            kind: ObjectStoreConfigKind::S3(options),
        }
    }

    pub fn rustfs(
        bucket: impl Into<String>,
        endpoint: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        Self {
            kind: ObjectStoreConfigKind::S3(S3ObjectStoreOptions::rustfs(
                bucket,
                endpoint,
                access_key_id,
                secret_access_key,
            )),
        }
    }

    pub fn tiered_local(hot_root: impl Into<PathBuf>, cold: S3ObjectStoreOptions) -> Self {
        Self::tiered_local_with_options(
            LocalObjectStoreOptions::new(hot_root),
            cold,
            TieredObjectStoreOptions::default(),
        )
    }

    pub fn tiered_local_with_options(
        hot: LocalObjectStoreOptions,
        cold: S3ObjectStoreOptions,
        options: TieredObjectStoreOptions,
    ) -> Self {
        Self {
            kind: ObjectStoreConfigKind::TieredLocal { hot, cold, options },
        }
    }

    pub fn open(&self) -> Result<ConfiguredObjectStore, ObjectError> {
        match &self.kind {
            ObjectStoreConfigKind::S3(options) => Ok(ConfiguredObjectStore::S3(Arc::new(
                S3ObjectStore::new(options.clone())?,
            ))),
            ObjectStoreConfigKind::TieredLocal { hot, cold, options } => {
                let mut options = options.clone();
                if options.put_policy == TieredPutPolicy::HotThenBackgroundCold
                    && options.pending_cold_put_root.is_none()
                {
                    options.pending_cold_put_root = Some(default_pending_cold_put_root(&hot.root));
                }
                let hot = LocalObjectStore::new(hot.clone())?;
                let cold = S3ObjectStore::new(cold.clone())?;
                let store = Arc::new(TieredObjectStore::new(hot, cold, options));
                store.recover_pending_cold_puts()?;
                Ok(ConfiguredObjectStore::TieredLocal(store))
            }
        }
    }

    pub fn options(&self) -> &S3ObjectStoreOptions {
        match &self.kind {
            ObjectStoreConfigKind::S3(options) => options,
            ObjectStoreConfigKind::TieredLocal { cold, .. } => cold,
        }
    }

    pub fn local_hot_root(&self) -> Option<&Path> {
        match &self.kind {
            ObjectStoreConfigKind::S3(_) => None,
            ObjectStoreConfigKind::TieredLocal { hot, .. } => Some(hot.root.as_path()),
        }
    }

    pub fn local_hot_options(&self) -> Option<&LocalObjectStoreOptions> {
        match &self.kind {
            ObjectStoreConfigKind::S3(_) => None,
            ObjectStoreConfigKind::TieredLocal { hot, .. } => Some(hot),
        }
    }

    pub fn tiered_options(&self) -> Option<TieredObjectStoreOptions> {
        match &self.kind {
            ObjectStoreConfigKind::S3(_) => None,
            ObjectStoreConfigKind::TieredLocal { options, .. } => Some(options.clone()),
        }
    }
}

impl MemoryObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Number of objects currently held. Useful for asserting that a
    /// copy-on-write clone shared blocks (count unchanged) instead of copying
    /// them.
    pub fn object_count(&self) -> usize {
        self.objects
            .lock()
            .map(|objects| objects.len())
            .unwrap_or(0)
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

impl ObjectStore for ConfiguredObjectStore {
    fn capabilities(&self) -> ObjectCapabilities {
        match self {
            Self::S3(store) => store.capabilities(),
            Self::TieredLocal(store) => store.capabilities(),
        }
    }

    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        match self {
            Self::S3(store) => store.put(key, bytes),
            Self::TieredLocal(store) => store.put(key, bytes),
        }
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        match self {
            Self::S3(store) => store.get(key, range),
            Self::TieredLocal(store) => store.get(key, range),
        }
    }

    fn get_if_present(
        &self,
        key: &ObjectKey,
        range: Option<ObjectRange>,
    ) -> Result<Option<Vec<u8>>, ObjectError> {
        match self {
            Self::S3(store) => store.get_if_present(key, range),
            Self::TieredLocal(store) => store.get_if_present(key, range),
        }
    }

    fn get_many_if_present(
        &self,
        requests: &[ObjectGetRequest],
    ) -> Result<Vec<Option<Vec<u8>>>, ObjectError> {
        match self {
            Self::S3(store) => store.get_many_if_present(requests),
            Self::TieredLocal(store) => store.get_many_if_present(requests),
        }
    }

    fn get_many(&self, requests: &[ObjectGetRequest]) -> Result<Vec<Vec<u8>>, ObjectError> {
        match self {
            Self::S3(store) => store.get_many(requests),
            Self::TieredLocal(store) => store.get_many(requests),
        }
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        match self {
            Self::S3(store) => store.head(key),
            Self::TieredLocal(store) => store.head(key),
        }
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        match self {
            Self::S3(store) => store.delete(key),
            Self::TieredLocal(store) => store.delete(key),
        }
    }

    fn resolve_read_placements(
        &self,
        blocks: &[ObjectReadBlock],
    ) -> Result<Vec<BlockPlacement>, ObjectError> {
        match self {
            Self::S3(store) => store.resolve_read_placements(blocks),
            Self::TieredLocal(store) => store.resolve_read_placements(blocks),
        }
    }

    fn tiered_stats(&self) -> Result<Option<TieredObjectStoreStats>, ObjectError> {
        match self {
            Self::S3(store) => store.tiered_stats(),
            Self::TieredLocal(store) => store.tiered_stats(),
        }
    }

    fn local_hot_stats(&self) -> Result<Option<LocalObjectStoreStats>, ObjectError> {
        match self {
            Self::S3(store) => store.local_hot_stats(),
            Self::TieredLocal(store) => store.local_hot_stats(),
        }
    }
}

impl ObjectStore for S3ObjectStore {
    fn capabilities(&self) -> ObjectCapabilities {
        ObjectCapabilities {
            range_get: true,
            multipart_put: true,
            server_side_copy: true,
            max_single_put_bytes: None,
            multipart_min_part_bytes: Some(5 * 1024 * 1024),
            multipart_max_part_bytes: Some(5 * 1024 * 1024 * 1024),
            multipart_max_parts: Some(10_000),
        }
    }

    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        let bytes = bytes.into();
        let size = bytes.len() as u64;
        let mut writer = self
            .operator
            .writer_options(
                key.as_str(),
                WriteOptions {
                    chunk: Some(DEFAULT_S3_MULTIPART_CHUNK_SIZE),
                    concurrent: DEFAULT_S3_MULTIPART_CONCURRENCY,
                    ..WriteOptions::default()
                },
            )
            .map_err(ObjectError::from_backend)?;
        writer
            .write(bytes.into_vec())
            .map_err(ObjectError::from_backend)?;
        writer.close().map_err(ObjectError::from_backend)?;
        Ok(ObjectInfo {
            key: key.clone(),
            size,
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

    fn get_many(&self, requests: &[ObjectGetRequest]) -> Result<Vec<Vec<u8>>, ObjectError> {
        parallel_get_many(requests.len(), DEFAULT_S3_GET_CONCURRENCY, |index| {
            let request = &requests[index];
            self.get(&request.key, request.range)
        })
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

fn parallel_get_many<F>(
    len: usize,
    concurrency: usize,
    fetch: F,
) -> Result<Vec<Vec<u8>>, ObjectError>
where
    F: Fn(usize) -> Result<Vec<u8>, ObjectError> + Sync,
{
    if len == 0 {
        return Ok(Vec::new());
    }
    if len == 1 || concurrency <= 1 {
        return (0..len).map(fetch).collect();
    }

    let workers = concurrency.min(len);
    let next = AtomicUsize::new(0);
    let (tx, rx) = mpsc::channel();
    thread::scope(|scope| {
        for _ in 0..workers {
            let tx = tx.clone();
            let fetch = &fetch;
            let next = &next;
            scope.spawn(move || loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= len {
                    break;
                }
                if tx.send((index, fetch(index))).is_err() {
                    break;
                }
            });
        }
        drop(tx);
    });

    let mut results = std::iter::repeat_with(|| None)
        .take(len)
        .collect::<Vec<Option<Result<Vec<u8>, ObjectError>>>>();
    for (index, result) in rx {
        if index >= len {
            return Err(ObjectError::Backend(
                "object store returned out-of-range batch index".to_owned(),
            ));
        }
        results[index] = Some(result);
    }

    results
        .into_iter()
        .enumerate()
        .map(|(index, result)| {
            result.ok_or_else(|| {
                ObjectError::Backend(format!("object store batch read missed index {index}"))
            })?
        })
        .collect()
}

impl ObjectStore for MemoryObjectStore {
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
        let bytes = bytes.into();
        let size = bytes.len() as u64;
        self.objects
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?
            .insert(key.as_str().to_owned(), bytes.into_vec());
        Ok(ObjectInfo {
            key: key.clone(),
            size,
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

pub(crate) fn validate_key(raw: &str) -> Result<(), ObjectError> {
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
    pub(crate) fn from_backend(err: impl fmt::Display) -> Self {
        Self::Backend(err.to_string())
    }

    pub(crate) fn from_poisoned_lock(err: impl fmt::Display) -> Self {
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
            Self::InvalidChunkLayout => write!(f, "invalid object chunk layout"),
            Self::Backend(err) => write!(f, "object store backend error: {err}"),
            Self::StagedWriteFailed { source, staged } => write!(
                f,
                "object write failed after staging {} objects: {source}",
                staged.len()
            ),
        }
    }
}

impl std::error::Error for ObjectError {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    #[test]
    fn parallel_get_many_preserves_order_and_uses_bounded_concurrency() {
        let active = AtomicUsize::new(0);
        let max_active = AtomicUsize::new(0);

        let result = parallel_get_many(8, 3, |index| {
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            max_active.fetch_max(now, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(10));
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(vec![index as u8])
        })
        .unwrap();

        assert_eq!(
            result,
            (0_u8..8).map(|index| vec![index]).collect::<Vec<_>>()
        );
        assert!(
            max_active.load(Ordering::SeqCst) > 1,
            "batch helper should issue concurrent reads"
        );
        assert!(
            max_active.load(Ordering::SeqCst) <= 3,
            "batch helper must honor the concurrency limit"
        );
    }

    #[test]
    fn parallel_get_many_returns_ordered_error() {
        let err = parallel_get_many(4, 4, |index| {
            if index == 2 {
                Err(ObjectError::Backend("boom".to_owned()))
            } else {
                Ok(vec![index as u8])
            }
        })
        .unwrap_err();

        assert_eq!(err, ObjectError::Backend("boom".to_owned()));
    }
}
