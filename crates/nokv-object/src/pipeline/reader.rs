use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use super::prefetch::ObjectPrefetchRequest;
use crate::cache::BlockCache;
use crate::chunk::{
    BlockReadIntoOutcome, BlockReadOptions, BlockReadOutcome, ChunkStore, ObjectReadBlock,
    ReadCacheFillMode, DEFAULT_BLOCK_SIZE,
};
use crate::store::ObjectError;

const CACHE_WARMUP_SEGMENT_BYTES: usize = DEFAULT_BLOCK_SIZE / 4;
const MAX_SPARSE_FORWARD_GAP_BYTES: u64 = (DEFAULT_BLOCK_SIZE / 4) as u64;

#[derive(Clone, Debug, Default)]
pub struct FileReadPipeline {
    options: FileReadPipelineOptions,
    last_read_end: Option<u64>,
    prefetch_until: u64,
    readahead_bytes: usize,
    sequential_stream_bytes: u64,
    sparse_forward_reads: u64,
    read_window: PipelineReadWindowCache,
    stats: FileReadPipelineStats,
}

#[derive(Clone, Copy, Debug)]
pub struct FileReadRequest<'a> {
    pub file_size: u64,
    pub offset: u64,
    pub output_len: usize,
    pub blocks: &'a [ObjectReadBlock],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileReadPipelineOptions {
    pub max_readahead_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FileReadPipelineStats {
    pub reads: u64,
    pub read_bytes: u64,
    pub sequential_reads: u64,
    pub readahead_hints: u64,
    pub readahead_hint_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileReadOutcome {
    pub blocks: BlockReadOutcome,
    pub readahead: Option<ReadAheadHint>,
    pub cache_warmup: Option<ObjectPrefetchRequest>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileReadIntoOutcome {
    pub blocks: BlockReadIntoOutcome,
    pub readahead: Option<ReadAheadHint>,
    pub cache_warmup: Option<ObjectPrefetchRequest>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadAheadHint {
    pub offset: u64,
    pub len: usize,
}

#[derive(Clone, Debug)]
struct PipelineReadWindowCache {
    inner: Arc<Mutex<PipelineReadWindowState>>,
}

#[derive(Clone, Debug, Default)]
struct PipelineReadWindowState {
    entries: VecDeque<PipelineReadWindowEntry>,
    bytes: u64,
    stats: crate::cache::BlockCacheStats,
}

#[derive(Clone, Debug)]
struct PipelineReadWindowEntry {
    raw_key: String,
    object_key: String,
    object_offset: u64,
    bytes: Vec<u8>,
}

impl Default for FileReadPipelineOptions {
    fn default() -> Self {
        Self {
            max_readahead_bytes: DEFAULT_BLOCK_SIZE,
        }
    }
}

impl FileReadPipeline {
    pub fn new(options: FileReadPipelineOptions) -> Self {
        Self {
            options,
            last_read_end: None,
            prefetch_until: 0,
            readahead_bytes: 0,
            sequential_stream_bytes: 0,
            sparse_forward_reads: 0,
            read_window: PipelineReadWindowCache::default(),
            stats: FileReadPipelineStats::default(),
        }
    }

    pub fn read_blocks_with_options<S, C>(
        &mut self,
        store: &S,
        cache: Option<&C>,
        request: FileReadRequest<'_>,
        options: BlockReadOptions,
    ) -> Result<FileReadOutcome, ObjectError>
    where
        S: ChunkStore,
        C: BlockCache + ?Sized,
    {
        let previous_read_end = self.last_read_end;
        let starts_stream = self.last_read_end.is_none() && request.offset == 0;
        let continued_stream = self.last_read_end == Some(request.offset);
        let sequential = continued_stream || starts_stream;
        let sparse_forward = is_sparse_forward_read(previous_read_end, request);
        let sparse_forward_warmup = sparse_forward && self.sparse_forward_reads > 0;
        let read_options = if continued_stream
            && (cache.is_none() || should_forward_fill_continued_read(request.blocks))
        {
            options.with_cache_fill(ReadCacheFillMode::ForwardAligned {
                block_size: DEFAULT_BLOCK_SIZE,
            })
        } else {
            options
        };
        let cache_fill = read_options.cache_fill;
        let use_read_window =
            cache.is_none() && !matches!(read_options.cache_fill, ReadCacheFillMode::Exact);
        let read = if use_read_window {
            store.read_blocks_with_options(
                Some(&self.read_window),
                request.output_len,
                request.blocks,
                read_options,
            )?
        } else {
            store.read_blocks_with_options(
                cache,
                request.output_len,
                request.blocks,
                read_options,
            )?
        };
        let cache_warmup = cache_warmup_request(
            cache.is_some() && (continued_stream || sparse_forward_warmup),
            request,
            cache_fill,
            read.object_gets,
        );
        let read_end = request
            .offset
            .checked_add(u64::try_from(request.output_len).map_err(|_| ObjectError::InvalidRange)?)
            .ok_or(ObjectError::InvalidRange)?;
        self.last_read_end = Some(read_end);
        self.stats.reads = self.stats.reads.saturating_add(1);
        self.stats.read_bytes = self
            .stats
            .read_bytes
            .saturating_add(request.output_len as u64);
        let mut readahead = None;
        if sequential && read_end < request.file_size && self.options.max_readahead_bytes > 0 {
            self.stats.sequential_reads = self.stats.sequential_reads.saturating_add(1);
            self.advance_readahead_window(request.offset, request.output_len);
            if read_end >= self.prefetch_until {
                let len = self
                    .readahead_bytes
                    .min(usize::try_from(request.file_size - read_end).unwrap_or(usize::MAX));
                if len > 0 {
                    self.prefetch_until =
                        read_end.saturating_add(len as u64).min(request.file_size);
                    self.stats.readahead_hints = self.stats.readahead_hints.saturating_add(1);
                    self.stats.readahead_hint_bytes =
                        self.stats.readahead_hint_bytes.saturating_add(len as u64);
                    readahead = Some(ReadAheadHint {
                        offset: read_end,
                        len,
                    });
                }
            }
        } else if !sequential {
            self.prefetch_until = read_end;
            self.readahead_bytes = 0;
            self.sequential_stream_bytes = request.output_len as u64;
        }
        if sparse_forward {
            self.sparse_forward_reads = self.sparse_forward_reads.saturating_add(1);
        } else {
            self.sparse_forward_reads = 0;
        }
        Ok(FileReadOutcome {
            blocks: read,
            readahead,
            cache_warmup,
        })
    }

    pub fn needs_stateful_pipeline(&self, offset: u64, output_len: usize) -> bool {
        if output_len == 0 {
            return false;
        }
        let starts_stream = self.last_read_end.is_none() && offset == 0;
        let continued_stream = self.last_read_end == Some(offset);
        starts_stream || continued_stream || self.is_sparse_forward_candidate(offset, output_len)
    }

    pub fn observe_unpipelined_read(
        &mut self,
        offset: u64,
        output_len: usize,
    ) -> Result<(), ObjectError> {
        let read_end = offset
            .checked_add(u64::try_from(output_len).map_err(|_| ObjectError::InvalidRange)?)
            .ok_or(ObjectError::InvalidRange)?;
        self.last_read_end = Some(read_end);
        self.prefetch_until = read_end;
        self.readahead_bytes = 0;
        self.sequential_stream_bytes = output_len as u64;
        self.sparse_forward_reads = 0;
        Ok(())
    }

    pub fn read_blocks_into_with_options<S, C>(
        &mut self,
        store: &S,
        cache: Option<&C>,
        output: &mut [u8],
        request: FileReadRequest<'_>,
        options: BlockReadOptions,
    ) -> Result<FileReadIntoOutcome, ObjectError>
    where
        S: ChunkStore,
        C: BlockCache + ?Sized,
    {
        if output.len() != request.output_len {
            return Err(ObjectError::InvalidRange);
        }
        let previous_read_end = self.last_read_end;
        let starts_stream = self.last_read_end.is_none() && request.offset == 0;
        let continued_stream = self.last_read_end == Some(request.offset);
        let sequential = continued_stream || starts_stream;
        let sparse_forward = is_sparse_forward_read(previous_read_end, request);
        let sparse_forward_warmup = sparse_forward && self.sparse_forward_reads > 0;
        let read_options = if continued_stream
            && (cache.is_none() || should_forward_fill_continued_read(request.blocks))
        {
            options.with_cache_fill(ReadCacheFillMode::ForwardAligned {
                block_size: DEFAULT_BLOCK_SIZE,
            })
        } else {
            options
        };
        let cache_fill = read_options.cache_fill;
        let use_read_window =
            cache.is_none() && !matches!(read_options.cache_fill, ReadCacheFillMode::Exact);
        let read = if use_read_window {
            store.read_blocks_into_with_options(
                Some(&self.read_window),
                output,
                request.blocks,
                read_options,
            )?
        } else {
            store.read_blocks_into_with_options(cache, output, request.blocks, read_options)?
        };
        let cache_warmup = cache_warmup_request(
            cache.is_some() && (continued_stream || sparse_forward_warmup),
            request,
            cache_fill,
            read.object_gets,
        );
        let read_end = request
            .offset
            .checked_add(u64::try_from(request.output_len).map_err(|_| ObjectError::InvalidRange)?)
            .ok_or(ObjectError::InvalidRange)?;
        self.last_read_end = Some(read_end);
        self.stats.reads = self.stats.reads.saturating_add(1);
        self.stats.read_bytes = self
            .stats
            .read_bytes
            .saturating_add(request.output_len as u64);
        let mut readahead = None;
        if sequential && read_end < request.file_size && self.options.max_readahead_bytes > 0 {
            self.stats.sequential_reads = self.stats.sequential_reads.saturating_add(1);
            self.advance_readahead_window(request.offset, request.output_len);
            if read_end >= self.prefetch_until {
                let len = self
                    .readahead_bytes
                    .min(usize::try_from(request.file_size - read_end).unwrap_or(usize::MAX));
                if len > 0 {
                    self.prefetch_until =
                        read_end.saturating_add(len as u64).min(request.file_size);
                    self.stats.readahead_hints = self.stats.readahead_hints.saturating_add(1);
                    self.stats.readahead_hint_bytes =
                        self.stats.readahead_hint_bytes.saturating_add(len as u64);
                    readahead = Some(ReadAheadHint {
                        offset: read_end,
                        len,
                    });
                }
            }
        } else if !sequential {
            self.prefetch_until = read_end;
            self.readahead_bytes = 0;
            self.sequential_stream_bytes = request.output_len as u64;
        }
        if sparse_forward {
            self.sparse_forward_reads = self.sparse_forward_reads.saturating_add(1);
        } else {
            self.sparse_forward_reads = 0;
        }
        Ok(FileReadIntoOutcome {
            blocks: read,
            readahead,
            cache_warmup,
        })
    }

    fn advance_readahead_window(&mut self, offset: u64, output_len: usize) {
        self.sequential_stream_bytes = self
            .sequential_stream_bytes
            .saturating_add(output_len as u64);
        if self.readahead_bytes == 0 {
            if self.sequential_stream_bytes > output_len as u64
                || initial_read_is_large_enough_for_readahead(offset, output_len)
            {
                self.readahead_bytes = self.initial_readahead_bytes();
            }
            return;
        }
        if self.readahead_bytes < self.options.max_readahead_bytes
            && self.sequential_stream_bytes >= self.readahead_bytes as u64
        {
            self.readahead_bytes = self
                .readahead_bytes
                .saturating_mul(2)
                .min(self.options.max_readahead_bytes);
        }
    }

    fn initial_readahead_bytes(&self) -> usize {
        self.options.max_readahead_bytes.min(DEFAULT_BLOCK_SIZE)
    }

    pub fn stats(&self) -> FileReadPipelineStats {
        self.stats
    }

    fn is_sparse_forward_candidate(&self, offset: u64, output_len: usize) -> bool {
        let Some(previous_read_end) = self.last_read_end else {
            return false;
        };
        if offset <= previous_read_end {
            return false;
        }
        if output_len == 0 || output_len > DEFAULT_BLOCK_SIZE / 4 {
            return false;
        }
        offset - previous_read_end <= MAX_SPARSE_FORWARD_GAP_BYTES
    }
}

fn should_forward_fill_continued_read(blocks: &[ObjectReadBlock]) -> bool {
    blocks
        .iter()
        .any(|block| !is_small_inner_block_read(block.object_offset, block.len))
}

fn initial_read_is_large_enough_for_readahead(offset: u64, output_len: usize) -> bool {
    offset == 0 && output_len >= DEFAULT_BLOCK_SIZE / 2
}

fn is_sparse_forward_read(previous_read_end: Option<u64>, request: FileReadRequest<'_>) -> bool {
    let Some(previous_read_end) = previous_read_end else {
        return false;
    };
    if request.offset <= previous_read_end {
        return false;
    }
    if request.output_len == 0 || request.output_len > DEFAULT_BLOCK_SIZE / 4 {
        return false;
    }
    let gap = request.offset - previous_read_end;
    if gap > MAX_SPARSE_FORWARD_GAP_BYTES {
        return false;
    }
    request.blocks.iter().all(|block| {
        block.output_offset == 0
            && block.len == request.output_len
            && is_small_block_read(block.object_offset, block.len)
    })
}

fn cache_warmup_request(
    cache_present: bool,
    request: FileReadRequest<'_>,
    cache_fill: ReadCacheFillMode,
    object_gets: usize,
) -> Option<ObjectPrefetchRequest> {
    if !cache_present || !matches!(cache_fill, ReadCacheFillMode::Exact) || object_gets == 0 {
        return None;
    }
    let mut blocks = Vec::new();
    let mut output_len = 0_usize;
    for block in request.blocks {
        let Some(mut warmup) = cache_warmup_block(block) else {
            continue;
        };
        if blocks.iter().any(|existing: &ObjectReadBlock| {
            existing.object_key == warmup.object_key
                && existing.digest_uri == warmup.digest_uri
                && existing.object_offset == warmup.object_offset
                && existing.len == warmup.len
        }) {
            continue;
        }
        warmup.output_offset = output_len;
        output_len = output_len.checked_add(warmup.len)?;
        blocks.push(warmup);
    }
    (!blocks.is_empty()).then(|| ObjectPrefetchRequest::exact(output_len, blocks))
}

fn cache_warmup_block(block: &ObjectReadBlock) -> Option<ObjectReadBlock> {
    if !is_small_block_read(block.object_offset, block.len) {
        return None;
    }
    if block.object_len == 0 || block.object_len > DEFAULT_BLOCK_SIZE as u64 {
        return None;
    }
    let read_end = block
        .object_offset
        .checked_add(u64::try_from(block.len).ok()?)?;
    if read_end > block.object_len {
        return None;
    }
    let segment_size = CACHE_WARMUP_SEGMENT_BYTES.max(block.len);
    let segment_size = u64::try_from(segment_size).ok()?;
    let warmup_offset = block.object_offset / segment_size * segment_size;
    let warmup_end = warmup_offset
        .checked_add(segment_size)?
        .min(block.object_len);
    let warmup_len = usize::try_from(warmup_end.checked_sub(warmup_offset)?).ok()?;
    if warmup_len == 0 {
        return None;
    }
    Some(ObjectReadBlock {
        object_key: block.object_key.clone(),
        digest_uri: block.digest_uri.clone(),
        object_offset: warmup_offset,
        object_len: block.object_len,
        len: warmup_len,
        output_offset: 0,
    })
}

fn is_small_inner_block_read(object_offset: u64, len: usize) -> bool {
    if object_offset.is_multiple_of(DEFAULT_BLOCK_SIZE as u64) {
        return false;
    }
    is_small_block_read(object_offset, len)
}

fn is_small_block_read(object_offset: u64, len: usize) -> bool {
    if len > DEFAULT_BLOCK_SIZE / 4 {
        return false;
    }
    let Ok(len) = u64::try_from(len) else {
        return false;
    };
    let block_size = DEFAULT_BLOCK_SIZE as u64;
    let Some(read_end) = object_offset.checked_add(len) else {
        return false;
    };
    let Some(block_end) = object_offset
        .checked_div(block_size)
        .and_then(|index| index.checked_add(1))
        .and_then(|index| index.checked_mul(block_size))
    else {
        return false;
    };
    read_end <= block_end
}

impl Default for PipelineReadWindowCache {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PipelineReadWindowState::default())),
        }
    }
}

impl BlockCache for PipelineReadWindowCache {
    fn get_block(&self, key: &str) -> Result<Option<Vec<u8>>, ObjectError> {
        let mut inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        let Some(entry) = inner.entries.iter().find(|entry| entry.raw_key == key) else {
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        };
        let bytes = entry.bytes.clone();
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
        let mut hit = None;
        for entry in &inner.entries {
            if entry.object_key != object_key {
                continue;
            }
            let Some(relative) = window_covered_range_offset(
                entry.object_offset,
                entry.bytes.len(),
                object_offset,
                len,
            ) else {
                continue;
            };
            let relative_end = relative.checked_add(len).ok_or(ObjectError::InvalidRange)?;
            if relative_end <= entry.bytes.len() {
                hit = Some(entry.bytes[relative..relative_end].to_vec());
                break;
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
        let Some((object_key, object_offset, _len)) = parse_window_cache_key(&key) else {
            return Ok(());
        };
        let mut inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        if let Some(index) = inner.entries.iter().position(|entry| entry.raw_key == key) {
            let old = inner.entries.remove(index).expect("entry index exists");
            inner.bytes = inner.bytes.saturating_sub(old.bytes.len() as u64);
        }
        inner.bytes = inner.bytes.saturating_add(bytes.len() as u64);
        inner.stats.puts = inner.stats.puts.saturating_add(1);
        inner.stats.put_bytes = inner.stats.put_bytes.saturating_add(bytes.len() as u64);
        inner.entries.push_back(PipelineReadWindowEntry {
            raw_key: key,
            object_key,
            object_offset,
            bytes,
        });
        inner.evict_over_limit();
        Ok(())
    }

    fn stats(&self) -> Result<crate::cache::BlockCacheStats, ObjectError> {
        let inner = self.inner.lock().map_err(ObjectError::from_poisoned_lock)?;
        let mut stats = inner.stats;
        stats.items = inner.entries.len();
        stats.bytes = inner.bytes;
        Ok(stats)
    }
}

impl PipelineReadWindowState {
    fn evict_over_limit(&mut self) {
        const MAX_ENTRIES: usize = 4;
        const MAX_BYTES: u64 = (DEFAULT_BLOCK_SIZE as u64) * 4;

        while self.entries.len() > MAX_ENTRIES || self.bytes > MAX_BYTES {
            let Some(entry) = self.entries.pop_front() else {
                break;
            };
            let len = entry.bytes.len() as u64;
            self.bytes = self.bytes.saturating_sub(len);
            self.stats.evictions = self.stats.evictions.saturating_add(1);
            self.stats.eviction_bytes = self.stats.eviction_bytes.saturating_add(len);
        }
    }
}

fn parse_window_cache_key(key: &str) -> Option<(String, u64, usize)> {
    let (prefix, len) = key.rsplit_once(':')?;
    let (object_key, offset) = prefix.rsplit_once(':')?;
    Some((
        object_key.to_owned(),
        offset.parse().ok()?,
        len.parse().ok()?,
    ))
}

fn window_covered_range_offset(
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
