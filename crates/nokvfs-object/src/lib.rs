//! Object storage boundary for NoKV file bodies.
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

pub const DEFAULT_CHUNK_SIZE: u64 = 64 * 1024 * 1024;
pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024 * 1024;

static OPENDAL_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("nokvfs-object")
        .build()
        .expect("create NoKV object runtime")
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
pub struct ChunkWriteOptions {
    pub manifest_id: String,
    pub mount: u64,
    pub inode: u64,
    pub generation: u64,
    pub chunk_size: u64,
    pub block_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkWriteRange {
    pub logical_offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkedWrite {
    pub manifest_id: String,
    pub size: u64,
    pub chunk_size: u64,
    pub block_size: u64,
    pub chunks: Vec<StoredChunk>,
    pub object_puts: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredChunk {
    pub chunk_index: u64,
    pub logical_offset: u64,
    pub len: u64,
    pub blocks: Vec<StoredBlock>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredBlock {
    pub object_key: String,
    pub logical_offset: u64,
    pub object_offset: u64,
    pub len: u64,
    pub digest_uri: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedObject {
    pub key: ObjectKey,
    pub size: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StagedObjectSet {
    objects: Vec<StagedObject>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ObjectCleanupOutcome {
    pub attempted: usize,
    pub deleted: usize,
    pub missing: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectReadBlock {
    pub object_key: String,
    pub object_offset: u64,
    pub len: usize,
    pub output_offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockReadOutcome {
    pub bytes: Vec<u8>,
    pub object_gets: usize,
    pub cache_hits: usize,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryBlockCache {
    blocks: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
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

impl ChunkWriteOptions {
    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.manifest_id.is_empty() {
            return Err(ObjectError::InvalidChunkLayout);
        }
        validate_key(&self.manifest_id)?;
        if self.mount == 0 || self.inode == 0 || self.generation == 0 {
            return Err(ObjectError::InvalidChunkLayout);
        }
        if self.chunk_size == 0 || self.block_size == 0 {
            return Err(ObjectError::InvalidChunkLayout);
        }
        if self.block_size as u64 > self.chunk_size {
            return Err(ObjectError::InvalidChunkLayout);
        }
        Ok(())
    }
}

impl ChunkedWrite {
    pub fn staged_objects(&self) -> Result<StagedObjectSet, ObjectError> {
        let mut objects = Vec::new();
        for chunk in &self.chunks {
            for block in &chunk.blocks {
                objects.push(StagedObject {
                    key: ObjectKey::new(block.object_key.clone())?,
                    size: block.len,
                });
            }
        }
        Ok(StagedObjectSet::new(objects))
    }
}

impl StagedObjectSet {
    pub fn new(objects: Vec<StagedObject>) -> Self {
        Self { objects }
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    pub fn len(&self) -> usize {
        self.objects.len()
    }

    pub fn objects(&self) -> &[StagedObject] {
        &self.objects
    }
}

impl MemoryBlockCache {
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ObjectError> {
        Ok(self
            .blocks
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?
            .get(key)
            .cloned())
    }

    pub fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), ObjectError> {
        self.blocks
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?
            .insert(key, bytes);
        Ok(())
    }
}

pub fn put_chunked_object<O: ObjectStore>(
    store: &O,
    bytes: &[u8],
    options: ChunkWriteOptions,
) -> Result<ChunkedWrite, ObjectError> {
    options.validate()?;
    let mut chunks = Vec::new();
    let mut object_puts = 0_usize;
    let mut staged = Vec::new();
    let mut offset = 0_usize;
    while offset < bytes.len() {
        let chunk_index = (offset as u64) / options.chunk_size;
        let chunk_start = offset;
        let chunk_end = bytes
            .len()
            .min((chunk_index + 1).saturating_mul(options.chunk_size) as usize);
        let mut blocks = Vec::new();
        let mut block_offset = chunk_start;
        let mut block_index = 0_u64;
        while block_offset < chunk_end {
            let block_end = chunk_end.min(block_offset.saturating_add(options.block_size));
            let object_key = block_object_key(&options, chunk_index, block_index);
            let key = ObjectKey::new(object_key.clone())?;
            let block = &bytes[block_offset..block_end];
            let info = store
                .put(&key, block)
                .map_err(|err| ObjectError::StagedWriteFailed {
                    source: err.to_string(),
                    staged: StagedObjectSet::new(staged.clone()),
                })?;
            object_puts += 1;
            staged.push(StagedObject {
                key: info.key,
                size: info.size,
            });
            blocks.push(StoredBlock {
                object_key,
                logical_offset: block_offset as u64,
                object_offset: 0,
                len: block.len() as u64,
                digest_uri: block_digest_uri(block),
            });
            block_offset = block_end;
            block_index += 1;
        }
        chunks.push(StoredChunk {
            chunk_index,
            logical_offset: chunk_start as u64,
            len: (chunk_end - chunk_start) as u64,
            blocks,
        });
        offset = chunk_end;
    }
    Ok(ChunkedWrite {
        manifest_id: options.manifest_id,
        size: bytes.len() as u64,
        chunk_size: options.chunk_size,
        block_size: options.block_size as u64,
        chunks,
        object_puts,
    })
}

pub fn put_chunked_ranges<O: ObjectStore>(
    store: &O,
    ranges: &[ChunkWriteRange],
    options: ChunkWriteOptions,
) -> Result<ChunkedWrite, ObjectError> {
    put_chunked_ranges_with_block_index_base(store, ranges, options, 0)
}

pub fn put_chunked_ranges_with_block_index_base<O: ObjectStore>(
    store: &O,
    ranges: &[ChunkWriteRange],
    options: ChunkWriteOptions,
    block_index_base: u64,
) -> Result<ChunkedWrite, ObjectError> {
    options.validate()?;
    let mut chunks = BTreeMap::<u64, StoredChunk>::new();
    let mut object_puts = 0_usize;
    let mut staged = Vec::new();
    let mut max_end = 0_u64;
    let mut block_indexes = BTreeMap::<u64, u64>::new();
    for range in ranges {
        if range.bytes.is_empty() {
            continue;
        }
        let mut range_offset = 0_usize;
        while range_offset < range.bytes.len() {
            let logical_offset = range
                .logical_offset
                .checked_add(u64::try_from(range_offset).map_err(|_| ObjectError::InvalidRange)?)
                .ok_or(ObjectError::InvalidRange)?;
            let chunk_index = logical_offset / options.chunk_size;
            let chunk_start = chunk_index.saturating_mul(options.chunk_size);
            let next_chunk = chunk_start
                .checked_add(options.chunk_size)
                .ok_or(ObjectError::InvalidRange)?;
            let remaining_in_chunk = usize::try_from(next_chunk - logical_offset)
                .map_err(|_| ObjectError::InvalidRange)?;
            let write_len = options
                .block_size
                .min(remaining_in_chunk)
                .min(range.bytes.len() - range_offset);
            let block_index = block_indexes.entry(chunk_index).or_insert(block_index_base);
            let block_index_value = *block_index;
            let object_key = block_object_key(&options, chunk_index, block_index_value);
            *block_index = block_index_value.saturating_add(1);
            let key = ObjectKey::new(object_key.clone())?;
            let block = &range.bytes[range_offset..range_offset + write_len];
            let info = store
                .put(&key, block)
                .map_err(|err| ObjectError::StagedWriteFailed {
                    source: err.to_string(),
                    staged: StagedObjectSet::new(staged.clone()),
                })?;
            object_puts += 1;
            staged.push(StagedObject {
                key: info.key,
                size: info.size,
            });
            chunks
                .entry(chunk_index)
                .or_insert_with(|| StoredChunk {
                    chunk_index,
                    logical_offset: chunk_start,
                    len: 0,
                    blocks: Vec::new(),
                })
                .blocks
                .push(StoredBlock {
                    object_key,
                    logical_offset,
                    object_offset: 0,
                    len: write_len as u64,
                    digest_uri: block_digest_uri(block),
                });
            let block_end = logical_offset
                .checked_add(write_len as u64)
                .ok_or(ObjectError::InvalidRange)?;
            max_end = max_end.max(block_end);
            range_offset += write_len;
        }
    }
    let mut chunks = chunks.into_values().collect::<Vec<_>>();
    for chunk in &mut chunks {
        let chunk_end = max_end.min(chunk.logical_offset.saturating_add(options.chunk_size));
        chunk.len = chunk_end.saturating_sub(chunk.logical_offset);
    }
    Ok(ChunkedWrite {
        manifest_id: options.manifest_id,
        size: max_end,
        chunk_size: options.chunk_size,
        block_size: options.block_size as u64,
        chunks,
        object_puts,
    })
}

pub fn delete_staged_objects<O: ObjectStore>(
    store: &O,
    staged: &StagedObjectSet,
) -> Result<ObjectCleanupOutcome, ObjectError> {
    let mut outcome = ObjectCleanupOutcome {
        attempted: staged.len(),
        deleted: 0,
        missing: 0,
    };
    for object in staged.objects() {
        if store.delete(&object.key)? {
            outcome.deleted += 1;
        } else {
            outcome.missing += 1;
        }
    }
    Ok(outcome)
}

pub fn read_object_blocks<O: ObjectStore>(
    store: &O,
    cache: Option<&MemoryBlockCache>,
    output_len: usize,
    blocks: &[ObjectReadBlock],
) -> Result<BlockReadOutcome, ObjectError> {
    let mut out = vec![0_u8; output_len];
    let mut object_gets = 0_usize;
    let mut cache_hits = 0_usize;
    for block in blocks {
        let key = ObjectKey::new(block.object_key.clone())?;
        let cache_key = format!("{}:{}:{}", key.as_str(), block.object_offset, block.len);
        let bytes = if let Some(cache) = cache {
            if let Some(cached) = cache.get(&cache_key)? {
                cache_hits += 1;
                cached
            } else {
                let fetched = store.get(
                    &key,
                    Some(ObjectRange::new(block.object_offset, block.len)?),
                )?;
                cache.put(cache_key, fetched.clone())?;
                object_gets += 1;
                fetched
            }
        } else {
            object_gets += 1;
            store.get(
                &key,
                Some(ObjectRange::new(block.object_offset, block.len)?),
            )?
        };
        let end = block
            .output_offset
            .checked_add(bytes.len())
            .ok_or(ObjectError::InvalidRange)?;
        if end > out.len() {
            return Err(ObjectError::InvalidRange);
        }
        out[block.output_offset..end].copy_from_slice(&bytes);
    }
    Ok(BlockReadOutcome {
        bytes: out,
        object_gets,
        cache_hits,
    })
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

fn block_object_key(options: &ChunkWriteOptions, chunk_index: u64, block_index: u64) -> String {
    format!(
        "blocks/{}/{}/{}/{}/{}",
        options.mount, options.inode, options.generation, chunk_index, block_index
    )
}

fn block_digest_uri(bytes: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    format!("fnv64:{hash:016x}")
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
    fn chunked_put_and_read_cross_block_range() {
        let store = MemoryObjectStore::new();
        let bytes = b"abcdefghijklmnop".to_vec();
        let written = put_chunked_object(
            &store,
            &bytes,
            ChunkWriteOptions {
                manifest_id: "artifacts/checkpoint".to_owned(),
                mount: 1,
                inode: 2,
                generation: 3,
                chunk_size: 8,
                block_size: 4,
            },
        )
        .unwrap();
        assert_eq!(written.size, 16);
        assert_eq!(written.object_puts, 4);
        assert_eq!(written.chunks.len(), 2);
        assert_eq!(written.chunks[0].blocks.len(), 2);
        assert_eq!(written.chunks[0].blocks[0].object_key, "blocks/1/2/3/0/0");
        let staged = written.staged_objects().unwrap();
        assert_eq!(staged.len(), 4);

        let blocks = vec![
            ObjectReadBlock {
                object_key: "blocks/1/2/3/0/1".to_owned(),
                object_offset: 1,
                len: 3,
                output_offset: 0,
            },
            ObjectReadBlock {
                object_key: "blocks/1/2/3/1/0".to_owned(),
                object_offset: 0,
                len: 2,
                output_offset: 3,
            },
        ];
        let read = read_object_blocks(&store, None, 5, &blocks).unwrap();
        assert_eq!(read.bytes, b"fghij");
        assert_eq!(read.object_gets, 2);
        assert_eq!(read.cache_hits, 0);

        let cleanup = delete_staged_objects(&store, &staged).unwrap();
        assert_eq!(cleanup.attempted, 4);
        assert_eq!(cleanup.deleted, 4);
        assert_eq!(cleanup.missing, 0);
        let cleanup = delete_staged_objects(&store, &staged).unwrap();
        assert_eq!(cleanup.attempted, 4);
        assert_eq!(cleanup.deleted, 0);
        assert_eq!(cleanup.missing, 4);
    }

    #[test]
    fn chunked_ranges_put_only_dirty_blocks() {
        let store = MemoryObjectStore::new();
        let written = put_chunked_ranges(
            &store,
            &[
                ChunkWriteRange {
                    logical_offset: 3,
                    bytes: b"XYZ".to_vec(),
                },
                ChunkWriteRange {
                    logical_offset: 10,
                    bytes: b"mnopq".to_vec(),
                },
            ],
            ChunkWriteOptions {
                manifest_id: "artifacts/checkpoint".to_owned(),
                mount: 1,
                inode: 2,
                generation: 4,
                chunk_size: 8,
                block_size: 4,
            },
        )
        .unwrap();
        assert_eq!(written.size, 15);
        assert_eq!(written.object_puts, 3);
        assert_eq!(written.chunks.len(), 2);
        assert_eq!(written.chunks[0].chunk_index, 0);
        assert_eq!(written.chunks[0].blocks[0].logical_offset, 3);
        assert_eq!(written.chunks[0].blocks[0].object_key, "blocks/1/2/4/0/0");
        assert_eq!(written.chunks[1].chunk_index, 1);
        assert_eq!(written.chunks[1].blocks.len(), 2);
        assert_eq!(
            store
                .get(
                    &ObjectKey::new("blocks/1/2/4/1/0").unwrap(),
                    Some(ObjectRange::new(0, 4).unwrap())
                )
                .unwrap(),
            b"mnop"
        );
    }

    #[test]
    fn block_cache_reuses_object_reads() {
        let store = MemoryObjectStore::new();
        let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
        store.put(&key, b"abcd").unwrap();
        let cache = MemoryBlockCache::default();
        let blocks = vec![ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            object_offset: 0,
            len: 4,
            output_offset: 0,
        }];
        let first = read_object_blocks(&store, Some(&cache), 4, &blocks).unwrap();
        let second = read_object_blocks(&store, Some(&cache), 4, &blocks).unwrap();
        assert_eq!(first.object_gets, 1);
        assert_eq!(first.cache_hits, 0);
        assert_eq!(second.object_gets, 0);
        assert_eq!(second.cache_hits, 1);
    }

    #[derive(Clone)]
    struct FailAfterFirstPut {
        inner: MemoryObjectStore,
        puts: Arc<Mutex<usize>>,
    }

    impl FailAfterFirstPut {
        fn new() -> Self {
            Self {
                inner: MemoryObjectStore::new(),
                puts: Arc::new(Mutex::new(0)),
            }
        }
    }

    impl ObjectStore for FailAfterFirstPut {
        fn put(&self, key: &ObjectKey, bytes: &[u8]) -> Result<ObjectInfo, ObjectError> {
            let mut puts = self.puts.lock().unwrap();
            if *puts >= 1 {
                return Err(ObjectError::Backend("injected put failure".to_owned()));
            }
            *puts += 1;
            self.inner.put(key, bytes)
        }

        fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
            self.inner.get(key, range)
        }

        fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
            self.inner.head(key)
        }

        fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
            self.inner.delete(key)
        }
    }

    #[test]
    fn chunked_put_failure_returns_staged_objects_for_cleanup() {
        let store = FailAfterFirstPut::new();
        let err = put_chunked_object(
            &store,
            b"abcdefgh",
            ChunkWriteOptions {
                manifest_id: "artifacts/checkpoint".to_owned(),
                mount: 1,
                inode: 2,
                generation: 3,
                chunk_size: 8,
                block_size: 4,
            },
        )
        .unwrap_err();
        let staged = match err {
            ObjectError::StagedWriteFailed { source, staged } => {
                assert!(source.contains("injected put failure"));
                staged
            }
            err => panic!("unexpected object error: {err:?}"),
        };
        assert_eq!(staged.len(), 1);
        assert!(store.head(&staged.objects()[0].key).unwrap().is_some());

        let cleanup = delete_staged_objects(&store, &staged).unwrap();
        assert_eq!(cleanup.deleted, 1);
        assert!(store.head(&staged.objects()[0].key).unwrap().is_none());
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
