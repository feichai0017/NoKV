use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::digest::sha256_hex;
use crate::store::ObjectError;

pub trait BlockCache {
    fn get_block(&self, key: &str) -> Result<Option<Vec<u8>>, ObjectError>;
    fn get_block_range(
        &self,
        object_key: &str,
        object_offset: u64,
        len: usize,
    ) -> Result<Option<Vec<u8>>, ObjectError> {
        self.get_block(&block_range_cache_key(object_key, object_offset, len))
    }
    fn put_block(&self, key: String, bytes: Vec<u8>) -> Result<(), ObjectError>;
    fn stats(&self) -> Result<BlockCacheStats, ObjectError>;
}

pub(crate) fn block_range_cache_key(object_key: &str, object_offset: u64, len: usize) -> String {
    format!("{object_key}:{object_offset}:{len}")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryBlockCacheOptions {
    pub max_bytes: u64,
    pub max_items: usize,
    pub ttl: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiskBlockCacheOptions {
    pub root: PathBuf,
    pub max_bytes: u64,
    pub max_items: usize,
    pub ttl: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockCacheStats {
    pub items: usize,
    pub bytes: u64,
    pub hits: u64,
    pub hit_bytes: u64,
    pub misses: u64,
    pub puts: u64,
    pub put_bytes: u64,
    pub evictions: u64,
    pub eviction_bytes: u64,
    pub expired: u64,
}

#[derive(Clone, Debug)]
pub struct MemoryBlockCache {
    inner: Arc<Mutex<MemoryBlockCacheState>>,
}

#[derive(Clone, Debug)]
pub struct DiskBlockCache {
    inner: Arc<Mutex<DiskBlockCacheState>>,
}

#[derive(Clone, Debug)]
pub enum ObjectBlockCache {
    Memory(MemoryBlockCache),
    Disk(DiskBlockCache),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockCachePolicy {
    Off,
    Memory(MemoryBlockCacheOptions),
    Disk(DiskBlockCacheOptions),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WritebackCacheOptions {
    pub root: PathBuf,
    pub max_bytes: u64,
    pub max_items: usize,
}

#[derive(Clone, Debug)]
pub struct WritebackCache {
    inner: Arc<Mutex<WritebackCacheState>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WritebackTicket {
    id: u64,
    key: String,
    file_name: String,
    len: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WritebackCacheStats {
    pub active_items: usize,
    pub active_bytes: u64,
    pub staged: u64,
    pub staged_bytes: u64,
    pub removed: u64,
    pub removed_bytes: u64,
}

#[derive(Clone, Debug)]
struct MemoryBlockCacheState {
    options: MemoryBlockCacheOptions,
    blocks: BTreeMap<String, CachedBlock>,
    order: VecDeque<String>,
    bytes: u64,
    stats: BlockCacheStats,
}

#[derive(Clone, Debug)]
struct CachedBlock {
    bytes: Vec<u8>,
    inserted_at: Instant,
}

#[derive(Clone, Debug)]
struct DiskBlockCacheState {
    options: DiskBlockCacheOptions,
    blocks: BTreeMap<String, DiskCachedBlock>,
    order: VecDeque<String>,
    bytes: u64,
    stats: BlockCacheStats,
}

#[derive(Clone, Debug)]
struct DiskCachedBlock {
    file_name: String,
    bytes: u64,
    inserted_at: Instant,
}

#[derive(Clone, Debug)]
struct WritebackCacheState {
    options: WritebackCacheOptions,
    next_id: u64,
    entries: BTreeMap<u64, WritebackEntry>,
    bytes: u64,
    stats: WritebackCacheStats,
}

#[derive(Clone, Debug)]
struct WritebackEntry {
    key: String,
    file_name: String,
    bytes: u64,
}

impl Default for MemoryBlockCacheOptions {
    fn default() -> Self {
        Self {
            max_bytes: 512 * 1024 * 1024,
            max_items: 16 * 1024,
            ttl: None,
        }
    }
}

impl Default for BlockCachePolicy {
    fn default() -> Self {
        Self::Memory(MemoryBlockCacheOptions::default())
    }
}

impl BlockCachePolicy {
    pub fn memory(options: MemoryBlockCacheOptions) -> Self {
        Self::Memory(options)
    }

    pub fn disk(options: DiskBlockCacheOptions) -> Self {
        Self::Disk(options)
    }

    pub fn open(self) -> Result<Option<ObjectBlockCache>, ObjectError> {
        match self {
            Self::Off => Ok(None),
            Self::Memory(options) => Ok(Some(ObjectBlockCache::Memory(MemoryBlockCache::new(
                options,
            )))),
            Self::Disk(options) => DiskBlockCache::new(options)
                .map(ObjectBlockCache::Disk)
                .map(Some),
        }
    }
}

impl MemoryBlockCache {
    pub fn new(options: MemoryBlockCacheOptions) -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemoryBlockCacheState {
                options,
                blocks: BTreeMap::new(),
                order: VecDeque::new(),
                bytes: 0,
                stats: BlockCacheStats::default(),
            })),
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ObjectError> {
        self.get_block(key)
    }

    pub fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), ObjectError> {
        self.put_block(key, bytes)
    }
}

impl Default for MemoryBlockCache {
    fn default() -> Self {
        Self::new(MemoryBlockCacheOptions::default())
    }
}

impl DiskBlockCache {
    pub fn new(options: DiskBlockCacheOptions) -> Result<Self, ObjectError> {
        fs::create_dir_all(&options.root).map_err(ObjectError::from_backend)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(DiskBlockCacheState {
                options,
                blocks: BTreeMap::new(),
                order: VecDeque::new(),
                bytes: 0,
                stats: BlockCacheStats::default(),
            })),
        })
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ObjectError> {
        self.get_block(key)
    }

    pub fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), ObjectError> {
        self.put_block(key, bytes)
    }
}

impl WritebackCache {
    pub fn new(options: WritebackCacheOptions) -> Result<Self, ObjectError> {
        fs::create_dir_all(&options.root).map_err(ObjectError::from_backend)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(WritebackCacheState {
                options,
                next_id: 1,
                entries: BTreeMap::new(),
                bytes: 0,
                stats: WritebackCacheStats::default(),
            })),
        })
    }

    pub fn stage(&self, key: String, bytes: &[u8]) -> Result<WritebackTicket, ObjectError> {
        let len = bytes.len() as u64;
        // Reserve capacity under the mutex, then perform the large payload write
        // without holding it so concurrent writeback staging can make progress.
        let (id, file_name, tmp_path, path) = {
            let mut inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
            if inner.options.max_items == 0 || inner.options.max_bytes == 0 {
                return Err(ObjectError::Backend(
                    "writeback cache is disabled".to_owned(),
                ));
            }
            if len > inner.options.max_bytes
                || inner.entries.len() >= inner.options.max_items
                || inner.bytes.saturating_add(len) > inner.options.max_bytes
            {
                return Err(ObjectError::Backend(
                    "writeback cache capacity exceeded".to_owned(),
                ));
            }
            let id = inner.next_id;
            inner.next_id = inner.next_id.saturating_add(1);
            let file_name = format!("{}-{id:016x}.writeback", sha256_hex(key.as_bytes()));
            let tmp_path = inner.file_path(&format!("{file_name}.tmp"));
            let path = inner.file_path(&file_name);
            inner.entries.insert(
                id,
                WritebackEntry {
                    key: key.clone(),
                    file_name: file_name.clone(),
                    bytes: len,
                },
            );
            inner.bytes = inner.bytes.saturating_add(len);
            (id, file_name, tmp_path, path)
        };
        if let Err(err) = write_cache_file(&tmp_path, &key, bytes)
            .and_then(|()| fs::rename(&tmp_path, &path).map_err(ObjectError::from_backend))
        {
            let _ = fs::remove_file(&tmp_path);
            let _ = fs::remove_file(&path);
            if let Ok(mut inner) = self.inner.lock() {
                if inner
                    .entries
                    .remove(&id)
                    .filter(|entry| entry.key == key && entry.file_name == file_name)
                    .is_some()
                {
                    inner.bytes = inner.bytes.saturating_sub(len);
                }
            }
            return Err(err);
        }
        let mut inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        inner.stats.staged = inner.stats.staged.saturating_add(1);
        inner.stats.staged_bytes = inner.stats.staged_bytes.saturating_add(len);
        Ok(WritebackTicket {
            id,
            key,
            file_name,
            len,
        })
    }

    pub fn read(&self, ticket: &WritebackTicket) -> Result<Vec<u8>, ObjectError> {
        let (key, path) = {
            let inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
            let Some(entry) = inner.entries.get(&ticket.id) else {
                return Err(ObjectError::Backend(
                    "writeback ticket is not active".to_owned(),
                ));
            };
            if entry.key != ticket.key
                || entry.file_name != ticket.file_name
                || entry.bytes != ticket.len
            {
                return Err(ObjectError::Backend("writeback ticket mismatch".to_owned()));
            }
            (entry.key.clone(), inner.file_path(&entry.file_name))
        };
        let encoded = fs::read(path).map_err(ObjectError::from_backend)?;
        decode_cache_file(&key, &encoded)
            .ok_or_else(|| ObjectError::Backend("writeback cache record is corrupt".to_owned()))
    }

    pub fn remove(&self, ticket: &WritebackTicket) -> Result<bool, ObjectError> {
        let mut inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        let Some(entry) = inner.entries.remove(&ticket.id) else {
            return Ok(false);
        };
        if entry.key != ticket.key
            || entry.file_name != ticket.file_name
            || entry.bytes != ticket.len
        {
            return Err(ObjectError::Backend("writeback ticket mismatch".to_owned()));
        }
        match fs::remove_file(inner.file_path(&entry.file_name)) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(ObjectError::from_backend(err)),
        }
        inner.bytes = inner.bytes.saturating_sub(entry.bytes);
        inner.stats.removed = inner.stats.removed.saturating_add(1);
        inner.stats.removed_bytes = inner.stats.removed_bytes.saturating_add(entry.bytes);
        Ok(true)
    }

    pub fn stats(&self) -> Result<WritebackCacheStats, ObjectError> {
        let inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        let mut stats = inner.stats;
        stats.active_items = inner.entries.len();
        stats.active_bytes = inner.bytes;
        Ok(stats)
    }
}

impl WritebackTicket {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Default for ObjectBlockCache {
    fn default() -> Self {
        Self::Memory(MemoryBlockCache::default())
    }
}

impl From<MemoryBlockCache> for ObjectBlockCache {
    fn from(cache: MemoryBlockCache) -> Self {
        Self::Memory(cache)
    }
}

impl From<DiskBlockCache> for ObjectBlockCache {
    fn from(cache: DiskBlockCache) -> Self {
        Self::Disk(cache)
    }
}

impl BlockCache for MemoryBlockCache {
    fn get_block(&self, key: &str) -> Result<Option<Vec<u8>>, ObjectError> {
        let mut inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        let Some(block) = inner.blocks.get(key) else {
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        };
        if inner.is_expired(block.inserted_at) {
            inner.remove(key);
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            inner.stats.expired = inner.stats.expired.saturating_add(1);
            return Ok(None);
        }
        let bytes = block.bytes.clone();
        inner.stats.hits = inner.stats.hits.saturating_add(1);
        inner.stats.hit_bytes = inner.stats.hit_bytes.saturating_add(bytes.len() as u64);
        Ok(Some(bytes))
    }

    fn get_block_range(
        &self,
        object_key: &str,
        object_offset: u64,
        len: usize,
    ) -> Result<Option<Vec<u8>>, ObjectError> {
        if len == 0 {
            return Err(ObjectError::InvalidRange);
        }
        let mut inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        let prefix = format!("{object_key}:");
        let mut expired = Vec::new();
        let mut hit = None;
        for (key, block) in inner.blocks.range(prefix.clone()..) {
            if !key.starts_with(&prefix) {
                break;
            }
            let Some((cached_offset, cached_len)) = parse_block_range_cache_key(object_key, key)
            else {
                continue;
            };
            let Some(relative) =
                covered_range_offset(cached_offset, cached_len, object_offset, len)
            else {
                continue;
            };
            if inner.is_expired(block.inserted_at) {
                expired.push(key.clone());
                continue;
            }
            let relative_end = relative.checked_add(len).ok_or(ObjectError::InvalidRange)?;
            if relative_end <= block.bytes.len() {
                hit = Some(block.bytes[relative..relative_end].to_vec());
                break;
            }
        }
        for key in expired {
            if inner.remove(&key).is_some() {
                inner.stats.expired = inner.stats.expired.saturating_add(1);
            }
        }
        let Some(bytes) = hit else {
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        };
        inner.stats.hits = inner.stats.hits.saturating_add(1);
        inner.stats.hit_bytes = inner.stats.hit_bytes.saturating_add(bytes.len() as u64);
        Ok(Some(bytes))
    }

    fn put_block(&self, key: String, bytes: Vec<u8>) -> Result<(), ObjectError> {
        let mut inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        let len = bytes.len() as u64;
        if let Some(previous) = inner.blocks.remove(&key) {
            inner.bytes = inner.bytes.saturating_sub(previous.bytes.len() as u64);
        }
        inner.blocks.insert(
            key.clone(),
            CachedBlock {
                bytes,
                inserted_at: Instant::now(),
            },
        );
        inner.order.push_back(key);
        inner.bytes = inner.bytes.saturating_add(len);
        inner.stats.puts = inner.stats.puts.saturating_add(1);
        inner.stats.put_bytes = inner.stats.put_bytes.saturating_add(len);
        inner.evict_over_limit();
        Ok(())
    }

    fn stats(&self) -> Result<BlockCacheStats, ObjectError> {
        let inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        let mut stats = inner.stats;
        stats.items = inner.blocks.len();
        stats.bytes = inner.bytes;
        Ok(stats)
    }
}

impl BlockCache for ObjectBlockCache {
    fn get_block(&self, key: &str) -> Result<Option<Vec<u8>>, ObjectError> {
        match self {
            Self::Memory(cache) => cache.get_block(key),
            Self::Disk(cache) => cache.get_block(key),
        }
    }

    fn get_block_range(
        &self,
        object_key: &str,
        object_offset: u64,
        len: usize,
    ) -> Result<Option<Vec<u8>>, ObjectError> {
        match self {
            Self::Memory(cache) => cache.get_block_range(object_key, object_offset, len),
            Self::Disk(cache) => cache.get_block_range(object_key, object_offset, len),
        }
    }

    fn put_block(&self, key: String, bytes: Vec<u8>) -> Result<(), ObjectError> {
        match self {
            Self::Memory(cache) => cache.put_block(key, bytes),
            Self::Disk(cache) => cache.put_block(key, bytes),
        }
    }

    fn stats(&self) -> Result<BlockCacheStats, ObjectError> {
        match self {
            Self::Memory(cache) => cache.stats(),
            Self::Disk(cache) => cache.stats(),
        }
    }
}

impl BlockCache for DiskBlockCache {
    fn get_block(&self, key: &str) -> Result<Option<Vec<u8>>, ObjectError> {
        let mut inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        let Some(block) = inner.blocks.get(key).cloned() else {
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        };
        if inner.is_expired(block.inserted_at) {
            inner.remove(key)?;
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            inner.stats.expired = inner.stats.expired.saturating_add(1);
            return Ok(None);
        }
        let path = inner.file_path(&block.file_name);
        let encoded = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                inner.remove(key)?;
                inner.stats.misses = inner.stats.misses.saturating_add(1);
                return Ok(None);
            }
            Err(err) => return Err(ObjectError::from_backend(err)),
        };
        let Some(bytes) = decode_cache_file(key, &encoded) else {
            inner.remove(key)?;
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        };
        inner.stats.hits = inner.stats.hits.saturating_add(1);
        inner.stats.hit_bytes = inner.stats.hit_bytes.saturating_add(bytes.len() as u64);
        Ok(Some(bytes))
    }

    fn get_block_range(
        &self,
        object_key: &str,
        object_offset: u64,
        len: usize,
    ) -> Result<Option<Vec<u8>>, ObjectError> {
        if len == 0 {
            return Err(ObjectError::InvalidRange);
        }
        let mut inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        let prefix = format!("{object_key}:");
        let mut expired = Vec::new();
        let mut hit = None;
        for (key, block) in inner.blocks.range(prefix.clone()..) {
            if !key.starts_with(&prefix) {
                break;
            }
            let Some((cached_offset, cached_len)) = parse_block_range_cache_key(object_key, key)
            else {
                continue;
            };
            let Some(relative) =
                covered_range_offset(cached_offset, cached_len, object_offset, len)
            else {
                continue;
            };
            if inner.is_expired(block.inserted_at) {
                expired.push(key.clone());
                continue;
            }
            hit = Some((key.clone(), block.clone(), relative));
            break;
        }
        for key in expired {
            if inner.remove(&key)?.is_some() {
                inner.stats.expired = inner.stats.expired.saturating_add(1);
            }
        }
        let Some((key, block, relative)) = hit else {
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        };
        let path = inner.file_path(&block.file_name);
        let encoded = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                inner.remove(&key)?;
                inner.stats.misses = inner.stats.misses.saturating_add(1);
                return Ok(None);
            }
            Err(err) => return Err(ObjectError::from_backend(err)),
        };
        let Some(cached) = decode_cache_file(&key, &encoded) else {
            inner.remove(&key)?;
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        };
        let end = relative.checked_add(len).ok_or(ObjectError::InvalidRange)?;
        if end > cached.len() {
            inner.remove(&key)?;
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        }
        let bytes = cached[relative..end].to_vec();
        inner.stats.hits = inner.stats.hits.saturating_add(1);
        inner.stats.hit_bytes = inner.stats.hit_bytes.saturating_add(bytes.len() as u64);
        Ok(Some(bytes))
    }

    fn put_block(&self, key: String, bytes: Vec<u8>) -> Result<(), ObjectError> {
        let mut inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        if inner.options.max_bytes == 0 || inner.options.max_items == 0 {
            inner.remove(&key)?;
            return Ok(());
        }
        let bytes_len = bytes.len() as u64;
        if bytes_len > inner.options.max_bytes {
            inner.remove(&key)?;
            return Ok(());
        }
        let file_name = cache_file_name(&key);
        let path = inner.file_path(&file_name);
        let tmp_path = inner.file_path(&format!("{file_name}.tmp"));
        let encoded = encode_cache_file(&key, &bytes)?;
        fs::write(&tmp_path, encoded).map_err(ObjectError::from_backend)?;
        fs::rename(&tmp_path, &path).map_err(ObjectError::from_backend)?;
        inner.remove(&key)?;
        inner.order.retain(|cached_key| cached_key != &key);
        inner.blocks.insert(
            key.clone(),
            DiskCachedBlock {
                file_name,
                bytes: bytes_len,
                inserted_at: Instant::now(),
            },
        );
        inner.order.push_back(key);
        inner.bytes = inner.bytes.saturating_add(bytes_len);
        inner.stats.puts = inner.stats.puts.saturating_add(1);
        inner.stats.put_bytes = inner.stats.put_bytes.saturating_add(bytes_len);
        inner.evict_over_limit()?;
        Ok(())
    }

    fn stats(&self) -> Result<BlockCacheStats, ObjectError> {
        let inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        let mut stats = inner.stats;
        stats.items = inner.blocks.len();
        stats.bytes = inner.bytes;
        Ok(stats)
    }
}

impl MemoryBlockCacheState {
    fn is_expired(&self, inserted_at: Instant) -> bool {
        self.options
            .ttl
            .is_some_and(|ttl| inserted_at.elapsed() >= ttl)
    }

    fn remove(&mut self, key: &str) -> Option<u64> {
        let removed = self.blocks.remove(key)?;
        let len = removed.bytes.len() as u64;
        self.bytes = self.bytes.saturating_sub(len);
        Some(len)
    }

    fn evict_over_limit(&mut self) {
        while self.blocks.len() > self.options.max_items || self.bytes > self.options.max_bytes {
            let Some(key) = self.order.pop_front() else {
                break;
            };
            if let Some(len) = self.remove(&key) {
                self.stats.evictions = self.stats.evictions.saturating_add(1);
                self.stats.eviction_bytes = self.stats.eviction_bytes.saturating_add(len);
            }
        }
    }
}

impl DiskBlockCacheState {
    fn file_path(&self, file_name: &str) -> PathBuf {
        self.options.root.join(file_name)
    }

    fn is_expired(&self, inserted_at: Instant) -> bool {
        self.options
            .ttl
            .is_some_and(|ttl| inserted_at.elapsed() >= ttl)
    }

    fn remove(&mut self, key: &str) -> Result<Option<u64>, ObjectError> {
        let Some(removed) = self.blocks.remove(key) else {
            return Ok(None);
        };
        self.bytes = self.bytes.saturating_sub(removed.bytes);
        match fs::remove_file(self.file_path(&removed.file_name)) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(ObjectError::from_backend(err)),
        }
        Ok(Some(removed.bytes))
    }

    fn evict_over_limit(&mut self) -> Result<(), ObjectError> {
        while self.bytes > self.options.max_bytes || self.blocks.len() > self.options.max_items {
            let Some(key) = self.order.pop_front() else {
                break;
            };
            if let Some(len) = self.remove(&key)? {
                self.stats.evictions = self.stats.evictions.saturating_add(1);
                self.stats.eviction_bytes = self.stats.eviction_bytes.saturating_add(len);
            }
        }
        Ok(())
    }
}

impl WritebackCacheState {
    fn file_path(&self, file_name: &str) -> PathBuf {
        self.options.root.join(file_name)
    }
}

fn cache_file_name(key: &str) -> String {
    format!("{}.block", sha256_hex(key.as_bytes()))
}

fn parse_block_range_cache_key(object_key: &str, key: &str) -> Option<(u64, usize)> {
    let suffix = key.strip_prefix(object_key)?.strip_prefix(':')?;
    let (offset, len) = suffix.split_once(':')?;
    Some((offset.parse().ok()?, len.parse().ok()?))
}

fn covered_range_offset(
    cached_offset: u64,
    cached_len: usize,
    object_offset: u64,
    len: usize,
) -> Option<usize> {
    let cached_end = cached_offset.checked_add(cached_len as u64)?;
    let requested_end = object_offset.checked_add(len as u64)?;
    if cached_offset <= object_offset && requested_end <= cached_end {
        usize::try_from(object_offset - cached_offset).ok()
    } else {
        None
    }
}

fn encode_cache_file(key: &str, bytes: &[u8]) -> Result<Vec<u8>, ObjectError> {
    let key_len = u64::try_from(key.len()).map_err(|_| ObjectError::InvalidRange)?;
    let mut encoded = Vec::with_capacity(8 + key.len() + bytes.len());
    encoded.extend_from_slice(&key_len.to_be_bytes());
    encoded.extend_from_slice(key.as_bytes());
    encoded.extend_from_slice(bytes);
    Ok(encoded)
}

fn write_cache_file(path: &Path, key: &str, bytes: &[u8]) -> Result<(), ObjectError> {
    let key_len = u64::try_from(key.len()).map_err(|_| ObjectError::InvalidRange)?;
    let mut file = fs::File::create(path).map_err(ObjectError::from_backend)?;
    file.write_all(&key_len.to_be_bytes())
        .map_err(ObjectError::from_backend)?;
    file.write_all(key.as_bytes())
        .map_err(ObjectError::from_backend)?;
    file.write_all(bytes).map_err(ObjectError::from_backend)
}

fn decode_cache_file(expected_key: &str, encoded: &[u8]) -> Option<Vec<u8>> {
    let len_bytes: [u8; 8] = encoded.get(..8)?.try_into().ok()?;
    let key_len = usize::try_from(u64::from_be_bytes(len_bytes)).ok()?;
    let key_start = 8_usize;
    let key_end = key_start.checked_add(key_len)?;
    let key = encoded.get(key_start..key_end)?;
    if key != expected_key.as_bytes() {
        return None;
    }
    Some(encoded.get(key_end..)?.to_vec())
}
