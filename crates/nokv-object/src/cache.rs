use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::digest::sha256_hex;
use crate::store::ObjectError;

const MEMORY_BLOCK_CACHE_MAX_SHARDS: usize = 16;
const MEMORY_BLOCK_CACHE_ALIAS_ITEMS_MULTIPLIER: usize = 4;
const MEMORY_BLOCK_CACHE_TARGET_BYTES_PER_SHARD: u64 = 32 * 1024 * 1024;
const MEMORY_BLOCK_CACHE_TARGET_ITEMS_PER_SHARD: usize = 1024;
const MEMORY_BLOCK_CACHE_EXACT_PROMOTION_MAX_BYTES: usize = 64 * 1024;

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
    shards: Arc<[Mutex<MemoryBlockCacheState>]>,
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
    blocks: HashMap<String, CachedBlock>,
    exact_ranges: HashMap<String, HashMap<(u64, usize), String>>,
    range_aliases: HashMap<String, HashMap<(u64, usize), CachedRangeAlias>>,
    range_index: BTreeMap<(String, u64), Vec<CachedRangeEntry>>,
    alias_order: VecDeque<(String, u64, usize)>,
    order: VecDeque<String>,
    bytes: u64,
    stats: BlockCacheStats,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CachedRangeAlias {
    key: String,
    relative: usize,
    len: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CachedRangeEntry {
    key: String,
    len: usize,
}

#[derive(Clone, Debug)]
struct CachedBlock {
    bytes: Arc<[u8]>,
    inserted_at: Instant,
}

#[derive(Clone, Debug)]
struct DiskBlockCacheState {
    options: DiskBlockCacheOptions,
    blocks: HashMap<String, DiskCachedBlock>,
    range_index: BTreeMap<(String, u64), Vec<CachedRangeEntry>>,
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
        let shard_count = memory_block_cache_shard_count(options);
        let shard_options = memory_block_cache_shard_options(options, shard_count);
        let shards = (0..shard_count)
            .map(|_| Mutex::new(MemoryBlockCacheState::new(shard_options)))
            .collect::<Vec<_>>();
        Self {
            shards: Arc::from(shards),
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ObjectError> {
        self.get_block(key)
    }

    pub fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), ObjectError> {
        self.put_block(key, bytes)
    }

    #[cfg(test)]
    pub(crate) fn shard_count(&self) -> usize {
        self.shards.len()
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
                blocks: HashMap::new(),
                range_index: BTreeMap::new(),
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
        let bytes = {
            let mut inner = self
                .shard_for_key(key)
                .lock()
                .map_err(ObjectError::from_poisoned_lock)?;
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
            let bytes = Arc::clone(&block.bytes);
            inner.stats.hits = inner.stats.hits.saturating_add(1);
            inner.stats.hit_bytes = inner.stats.hit_bytes.saturating_add(bytes.len() as u64);
            bytes
        };
        Ok(Some(bytes.to_vec()))
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
        let (bytes, relative, relative_end) = {
            let mut inner = self
                .shard_for_object_key(object_key)
                .lock()
                .map_err(ObjectError::from_poisoned_lock)?;
            if let Some(exact_key) = inner.exact_range_key(object_key, object_offset, len) {
                let exact = inner
                    .blocks
                    .get(exact_key.as_str())
                    .map(|block| (block.inserted_at, Arc::clone(&block.bytes)));
                if let Some((inserted_at, bytes)) = exact {
                    if inner.is_expired(inserted_at) {
                        inner.remove(&exact_key);
                        inner.stats.expired = inner.stats.expired.saturating_add(1);
                    } else if bytes.len() >= len {
                        inner.stats.hits = inner.stats.hits.saturating_add(1);
                        inner.stats.hit_bytes = inner.stats.hit_bytes.saturating_add(len as u64);
                        return Ok(Some(bytes[..len].to_vec()));
                    } else {
                        inner.remove(&exact_key);
                    }
                } else {
                    inner.remove_exact_range_key(object_key, object_offset, len);
                }
            }
            if let Some(alias) = inner.range_alias(object_key, object_offset, len) {
                if let Some((inserted_at, bytes)) = inner
                    .blocks
                    .get(&alias.key)
                    .map(|block| (block.inserted_at, Arc::clone(&block.bytes)))
                {
                    if inner.is_expired(inserted_at) {
                        inner.remove(&alias.key);
                        inner.stats.expired = inner.stats.expired.saturating_add(1);
                    } else {
                        let alias_end = alias
                            .relative
                            .checked_add(alias.len)
                            .ok_or(ObjectError::InvalidRange)?;
                        if alias.len == len && alias_end <= bytes.len() {
                            inner.stats.hits = inner.stats.hits.saturating_add(1);
                            inner.stats.hit_bytes =
                                inner.stats.hit_bytes.saturating_add(len as u64);
                            return Ok(Some(bytes[alias.relative..alias_end].to_vec()));
                        }
                        inner.remove_range_alias(object_key, object_offset, len);
                    }
                } else {
                    inner.remove_range_alias(object_key, object_offset, len);
                }
            }
            let Some((key, relative)) =
                find_covering_block_range_key(&inner.range_index, object_key, object_offset, len)
            else {
                inner.stats.misses = inner.stats.misses.saturating_add(1);
                return Ok(None);
            };
            let Some(block) = inner.blocks.get(&key) else {
                remove_block_range_index(&mut inner.range_index, &key);
                inner.stats.misses = inner.stats.misses.saturating_add(1);
                return Ok(None);
            };
            if inner.is_expired(block.inserted_at) {
                inner.remove(&key);
                inner.stats.misses = inner.stats.misses.saturating_add(1);
                inner.stats.expired = inner.stats.expired.saturating_add(1);
                return Ok(None);
            }
            let relative_end = relative.checked_add(len).ok_or(ObjectError::InvalidRange)?;
            if relative_end > block.bytes.len() {
                inner.remove(&key);
                inner.stats.misses = inner.stats.misses.saturating_add(1);
                return Ok(None);
            }
            let bytes = Arc::clone(&block.bytes);
            if len <= MEMORY_BLOCK_CACHE_EXACT_PROMOTION_MAX_BYTES {
                inner.insert_range_alias(
                    object_key,
                    object_offset,
                    len,
                    CachedRangeAlias { key, relative, len },
                );
            }
            inner.stats.hits = inner.stats.hits.saturating_add(1);
            inner.stats.hit_bytes = inner.stats.hit_bytes.saturating_add(len as u64);
            (bytes, relative, relative_end)
        };
        Ok(Some(bytes[relative..relative_end].to_vec()))
    }

    fn put_block(&self, key: String, bytes: Vec<u8>) -> Result<(), ObjectError> {
        let mut inner = self
            .shard_for_key(&key)
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?;
        let len = bytes.len() as u64;
        let bytes: Arc<[u8]> = bytes.into();
        inner.remove(&key);
        inner.blocks.insert(
            key.clone(),
            CachedBlock {
                bytes,
                inserted_at: Instant::now(),
            },
        );
        insert_block_range_index(&mut inner.range_index, &key);
        inner.insert_exact_range_key(&key);
        inner.order.push_back(key);
        inner.bytes = inner.bytes.saturating_add(len);
        inner.stats.puts = inner.stats.puts.saturating_add(1);
        inner.stats.put_bytes = inner.stats.put_bytes.saturating_add(len);
        inner.evict_over_limit();
        Ok(())
    }

    fn stats(&self) -> Result<BlockCacheStats, ObjectError> {
        let mut stats = BlockCacheStats::default();
        for shard in self.shards.iter() {
            let inner = shard.lock().map_err(ObjectError::from_poisoned_lock)?;
            stats.items = stats.items.saturating_add(inner.blocks.len());
            stats.bytes = stats.bytes.saturating_add(inner.bytes);
            stats.hits = stats.hits.saturating_add(inner.stats.hits);
            stats.hit_bytes = stats.hit_bytes.saturating_add(inner.stats.hit_bytes);
            stats.misses = stats.misses.saturating_add(inner.stats.misses);
            stats.puts = stats.puts.saturating_add(inner.stats.puts);
            stats.put_bytes = stats.put_bytes.saturating_add(inner.stats.put_bytes);
            stats.evictions = stats.evictions.saturating_add(inner.stats.evictions);
            stats.eviction_bytes = stats
                .eviction_bytes
                .saturating_add(inner.stats.eviction_bytes);
            stats.expired = stats.expired.saturating_add(inner.stats.expired);
        }
        Ok(stats)
    }
}

impl MemoryBlockCache {
    fn shard_for_key(&self, key: &str) -> &Mutex<MemoryBlockCacheState> {
        let shard_key = parse_block_range_cache_key_parts(key)
            .map(|(object_key, _, _)| object_key)
            .unwrap_or(key);
        &self.shards[hash_shard_index(shard_key, self.shards.len())]
    }

    fn shard_for_object_key(&self, object_key: &str) -> &Mutex<MemoryBlockCacheState> {
        &self.shards[hash_shard_index(object_key, self.shards.len())]
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
        let exact_key = block_range_cache_key(object_key, object_offset, len);
        if let Some(block) = inner.blocks.get(&exact_key).cloned() {
            if inner.is_expired(block.inserted_at) {
                inner.remove(&exact_key)?;
                inner.stats.expired = inner.stats.expired.saturating_add(1);
            } else {
                let path = inner.file_path(&block.file_name);
                let encoded = match fs::read(&path) {
                    Ok(bytes) => bytes,
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {
                        inner.remove(&exact_key)?;
                        inner.stats.misses = inner.stats.misses.saturating_add(1);
                        return Ok(None);
                    }
                    Err(err) => return Err(ObjectError::from_backend(err)),
                };
                let Some(bytes) = decode_cache_file(&exact_key, &encoded) else {
                    inner.remove(&exact_key)?;
                    inner.stats.misses = inner.stats.misses.saturating_add(1);
                    return Ok(None);
                };
                if bytes.len() < len {
                    inner.remove(&exact_key)?;
                    inner.stats.misses = inner.stats.misses.saturating_add(1);
                    return Ok(None);
                }
                inner.stats.hits = inner.stats.hits.saturating_add(1);
                inner.stats.hit_bytes = inner.stats.hit_bytes.saturating_add(len as u64);
                return Ok(Some(bytes[..len].to_vec()));
            }
        }
        let Some((key, relative)) =
            find_covering_block_range_key(&inner.range_index, object_key, object_offset, len)
        else {
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        };
        let Some(block) = inner.blocks.get(&key).cloned() else {
            remove_block_range_index(&mut inner.range_index, &key);
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        };
        if inner.is_expired(block.inserted_at) {
            inner.remove(&key)?;
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            inner.stats.expired = inner.stats.expired.saturating_add(1);
            return Ok(None);
        }
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
        inner.remove(&key)?;
        inner.order.retain(|cached_key| cached_key != &key);
        let file_name = cache_file_name(&key);
        let path = inner.file_path(&file_name);
        let tmp_path = inner.file_path(&format!("{file_name}.tmp"));
        let encoded = encode_cache_file(&key, &bytes)?;
        fs::write(&tmp_path, encoded).map_err(ObjectError::from_backend)?;
        fs::rename(&tmp_path, &path).map_err(ObjectError::from_backend)?;
        inner.blocks.insert(
            key.clone(),
            DiskCachedBlock {
                file_name,
                bytes: bytes_len,
                inserted_at: Instant::now(),
            },
        );
        insert_block_range_index(&mut inner.range_index, &key);
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
    fn new(options: MemoryBlockCacheOptions) -> Self {
        Self {
            options,
            blocks: HashMap::new(),
            exact_ranges: HashMap::new(),
            range_aliases: HashMap::new(),
            range_index: BTreeMap::new(),
            alias_order: VecDeque::new(),
            order: VecDeque::new(),
            bytes: 0,
            stats: BlockCacheStats::default(),
        }
    }

    fn exact_range_key(&self, object_key: &str, offset: u64, len: usize) -> Option<String> {
        self.exact_ranges
            .get(object_key)?
            .get(&(offset, len))
            .cloned()
    }

    fn insert_exact_range_key(&mut self, key: &str) {
        let Some((object_key, offset, len)) = parse_block_range_cache_key_parts(key) else {
            return;
        };
        self.exact_ranges
            .entry(object_key.to_owned())
            .or_default()
            .insert((offset, len), key.to_owned());
    }

    fn remove_exact_range_key(&mut self, object_key: &str, offset: u64, len: usize) {
        let remove_empty = if let Some(ranges) = self.exact_ranges.get_mut(object_key) {
            ranges.remove(&(offset, len));
            ranges.is_empty()
        } else {
            false
        };
        if remove_empty {
            self.exact_ranges.remove(object_key);
        }
    }

    fn range_alias(&self, object_key: &str, offset: u64, len: usize) -> Option<CachedRangeAlias> {
        self.range_aliases
            .get(object_key)?
            .get(&(offset, len))
            .cloned()
    }

    fn insert_range_alias(
        &mut self,
        object_key: &str,
        offset: u64,
        len: usize,
        alias: CachedRangeAlias,
    ) {
        let inserted = self
            .range_aliases
            .entry(object_key.to_owned())
            .or_default()
            .insert((offset, len), alias)
            .is_none();
        if inserted {
            self.alias_order
                .push_back((object_key.to_owned(), offset, len));
        }
        self.evict_aliases_over_limit();
    }

    fn remove_range_alias(&mut self, object_key: &str, offset: u64, len: usize) {
        let remove_empty = if let Some(aliases) = self.range_aliases.get_mut(object_key) {
            aliases.remove(&(offset, len));
            aliases.is_empty()
        } else {
            false
        };
        if remove_empty {
            self.range_aliases.remove(object_key);
        }
    }

    fn is_expired(&self, inserted_at: Instant) -> bool {
        self.options
            .ttl
            .is_some_and(|ttl| inserted_at.elapsed() >= ttl)
    }

    fn remove(&mut self, key: &str) -> Option<u64> {
        let removed = self.blocks.remove(key);
        remove_block_range_index(&mut self.range_index, key);
        if let Some((object_key, offset, len)) = parse_block_range_cache_key_parts(key) {
            self.remove_exact_range_key(object_key, offset, len);
            self.remove_range_alias(object_key, offset, len);
        }
        self.range_aliases.retain(|_, aliases| {
            aliases.retain(|_, alias| alias.key != key);
            !aliases.is_empty()
        });
        let removed = removed?;
        let len = removed.bytes.len() as u64;
        self.bytes = self.bytes.saturating_sub(len);
        Some(len)
    }

    fn evict_aliases_over_limit(&mut self) {
        let max_aliases = self
            .options
            .max_items
            .saturating_mul(MEMORY_BLOCK_CACHE_ALIAS_ITEMS_MULTIPLIER);
        while self.range_alias_count() > max_aliases {
            let Some((object_key, offset, len)) = self.alias_order.pop_front() else {
                break;
            };
            self.remove_range_alias(&object_key, offset, len);
        }
    }

    fn range_alias_count(&self) -> usize {
        self.range_aliases
            .values()
            .map(HashMap::len)
            .fold(0_usize, usize::saturating_add)
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

fn memory_block_cache_shard_count(options: MemoryBlockCacheOptions) -> usize {
    let by_items = options
        .max_items
        .div_ceil(MEMORY_BLOCK_CACHE_TARGET_ITEMS_PER_SHARD);
    let by_bytes = options
        .max_bytes
        .div_ceil(MEMORY_BLOCK_CACHE_TARGET_BYTES_PER_SHARD)
        .try_into()
        .unwrap_or(usize::MAX);
    by_items
        .min(by_bytes)
        .clamp(1, MEMORY_BLOCK_CACHE_MAX_SHARDS)
}

fn memory_block_cache_shard_options(
    options: MemoryBlockCacheOptions,
    shard_count: usize,
) -> MemoryBlockCacheOptions {
    debug_assert!(shard_count > 0);
    MemoryBlockCacheOptions {
        max_bytes: options.max_bytes.div_ceil(shard_count as u64),
        max_items: options.max_items.div_ceil(shard_count),
        ttl: options.ttl,
    }
}

fn hash_shard_index(key: &str, shard_count: usize) -> usize {
    debug_assert!(shard_count > 0);
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as usize % shard_count
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
            remove_block_range_index(&mut self.range_index, key);
            return Ok(None);
        };
        remove_block_range_index(&mut self.range_index, key);
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

fn parse_block_range_cache_key_parts(key: &str) -> Option<(&str, u64, usize)> {
    let (prefix, len) = key.rsplit_once(':')?;
    let (object_key, offset) = prefix.rsplit_once(':')?;
    Some((object_key, offset.parse().ok()?, len.parse().ok()?))
}

fn insert_block_range_index(index: &mut BTreeMap<(String, u64), Vec<CachedRangeEntry>>, key: &str) {
    let Some((object_key, offset, len)) = parse_block_range_cache_key_parts(key) else {
        return;
    };
    let entries = index.entry((object_key.to_owned(), offset)).or_default();
    if entries.iter().any(|entry| entry.key == key) {
        return;
    }
    entries.push(CachedRangeEntry {
        key: key.to_owned(),
        len,
    });
}

fn remove_block_range_index(index: &mut BTreeMap<(String, u64), Vec<CachedRangeEntry>>, key: &str) {
    let Some((object_key, offset, _)) = parse_block_range_cache_key_parts(key) else {
        return;
    };
    let index_key = (object_key.to_owned(), offset);
    let remove_empty = if let Some(entries) = index.get_mut(&index_key) {
        entries.retain(|entry| entry.key != key);
        entries.is_empty()
    } else {
        false
    };
    if remove_empty {
        index.remove(&index_key);
    }
}

fn find_covering_block_range_key(
    index: &BTreeMap<(String, u64), Vec<CachedRangeEntry>>,
    object_key: &str,
    object_offset: u64,
    len: usize,
) -> Option<(String, usize)> {
    let search = (object_key.to_owned(), object_offset);
    for ((candidate_object_key, cached_offset), entries) in index.range(..=search).rev() {
        if candidate_object_key != object_key {
            break;
        }
        for entry in entries.iter().rev() {
            let Some(relative) =
                covered_range_offset(*cached_offset, entry.len, object_offset, len)
            else {
                continue;
            };
            return Some((entry.key.clone(), relative));
        }
    }
    None
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
