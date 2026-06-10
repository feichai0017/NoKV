use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Read};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::cache::{block_range_cache_key, BlockCache};
use crate::store::{ObjectBytes, ObjectError, ObjectKey, ObjectStore};
use xxhash_rust::xxh3::xxh3_64;

use super::types::{
    ChunkWriteOptions, ChunkWriteRange, ChunkedWrite, ObjectCleanupOutcome, StagedObject,
    StagedObjectSet, StoredBlock, StoredChunk,
};

pub fn put_chunked_object<O: ObjectStore>(
    store: &O,
    bytes: &[u8],
    options: ChunkWriteOptions,
) -> Result<ChunkedWrite, ObjectError> {
    options.validate()?;
    let mut chunks = Vec::new();
    let mut object_puts = 0_usize;
    let mut object_put_bytes = 0_u64;
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
            let info =
                store
                    .put(&key, block.to_vec())
                    .map_err(|err| ObjectError::StagedWriteFailed {
                        source: err.to_string(),
                        staged: StagedObjectSet::new(staged.clone()),
                    })?;
            object_puts += 1;
            object_put_bytes = object_put_bytes.saturating_add(block.len() as u64);
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
        object_put_bytes,
    })
}

pub fn put_chunked_reader<O, R>(
    store: &O,
    mut reader: R,
    options: ChunkWriteOptions,
) -> Result<ChunkedWrite, ObjectError>
where
    O: ObjectStore,
    R: Read,
{
    options.validate()?;
    let mut chunks = BTreeMap::<u64, StoredChunk>::new();
    let mut object_puts = 0_usize;
    let mut object_put_bytes = 0_u64;
    let mut staged = Vec::new();
    let mut offset = 0_u64;
    let mut buffer = vec![0_u8; options.block_size];
    loop {
        let chunk_index = offset / options.chunk_size;
        let chunk_start = chunk_index.saturating_mul(options.chunk_size);
        let next_chunk = chunk_start
            .checked_add(options.chunk_size)
            .ok_or(ObjectError::InvalidRange)?;
        let remaining_in_chunk =
            usize::try_from(next_chunk - offset).map_err(|_| ObjectError::InvalidRange)?;
        let target_len = options.block_size.min(remaining_in_chunk);
        let read_len = read_chunk_block(&mut reader, &mut buffer[..target_len]).map_err(|err| {
            ObjectError::StagedWriteFailed {
                source: format!("object reader error: {err}"),
                staged: StagedObjectSet::new(staged.clone()),
            }
        })?;
        if read_len == 0 {
            break;
        }
        let block_index = (offset - chunk_start) / (options.block_size as u64);
        let object_key = block_object_key(&options, chunk_index, block_index);
        let key = ObjectKey::new(object_key.clone())?;
        let block = &buffer[..read_len];
        let info =
            store
                .put(&key, block.to_vec())
                .map_err(|err| ObjectError::StagedWriteFailed {
                    source: err.to_string(),
                    staged: StagedObjectSet::new(staged.clone()),
                })?;
        object_puts += 1;
        object_put_bytes = object_put_bytes.saturating_add(read_len as u64);
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
                logical_offset: offset,
                object_offset: 0,
                len: read_len as u64,
                digest_uri: block_digest_uri(block),
            });
        offset = offset
            .checked_add(read_len as u64)
            .ok_or(ObjectError::InvalidRange)?;
    }
    let mut chunks = chunks.into_values().collect::<Vec<_>>();
    for chunk in &mut chunks {
        let chunk_end = offset.min(chunk.logical_offset.saturating_add(options.chunk_size));
        chunk.len = chunk_end.saturating_sub(chunk.logical_offset);
    }
    Ok(ChunkedWrite {
        manifest_id: options.manifest_id,
        size: offset,
        chunk_size: options.chunk_size,
        block_size: options.block_size as u64,
        chunks,
        object_puts,
        object_put_bytes,
    })
}

pub fn put_chunked_ranges_parallel<O>(
    store: &O,
    ranges: Vec<ChunkWriteRange>,
    options: ChunkWriteOptions,
    block_index_base: u64,
    workers: usize,
    cache: Option<&(dyn BlockCache + Sync)>,
) -> Result<ChunkedWrite, ObjectError>
where
    O: ObjectStore + Sync,
{
    put_chunked_ranges_parallel_with_timings(
        store,
        ranges,
        options,
        block_index_base,
        workers,
        cache,
    )
    .map(|(written, _timings)| written)
}

pub(crate) fn put_chunked_ranges_parallel_with_timings<O>(
    store: &O,
    ranges: Vec<ChunkWriteRange>,
    options: ChunkWriteOptions,
    block_index_base: u64,
    workers: usize,
    cache: Option<&(dyn BlockCache + Sync)>,
) -> Result<(ChunkedWrite, ChunkWriteTimings), ObjectError>
where
    O: ObjectStore + Sync,
{
    options.validate()?;
    let plan = plan_chunked_range_blocks(ranges, &options, block_index_base)?;
    let uploaded = upload_planned_blocks_parallel(store, plan.blocks, workers.max(1), cache)?;
    let timings = uploaded.timings;
    finish_chunked_range_write(options, plan.max_end, uploaded.blocks)
        .map(|written| (written, timings))
}

pub fn put_chunked_ranges_with_block_index_base<O: ObjectStore>(
    store: &O,
    ranges: Vec<ChunkWriteRange>,
    options: ChunkWriteOptions,
    block_index_base: u64,
    cache: Option<&(dyn BlockCache + Sync)>,
) -> Result<ChunkedWrite, ObjectError> {
    options.validate()?;
    let plan = plan_chunked_range_blocks(ranges, &options, block_index_base)?;
    let uploaded = upload_planned_blocks_sequential(store, plan.blocks, cache)?;
    finish_chunked_range_write(options, plan.max_end, uploaded.blocks)
}

pub fn chunk_write_ranges_block_count(
    ranges: &[ChunkWriteRange],
    options: &ChunkWriteOptions,
) -> Result<u64, ObjectError> {
    options.validate()?;
    ranges.iter().try_fold(0_u64, |count, range| {
        staged_range_block_count(range.logical_offset, range.bytes.len(), options)
            .map(|next| count.saturating_add(next))
    })
}

fn plan_chunked_range_blocks(
    ranges: Vec<ChunkWriteRange>,
    options: &ChunkWriteOptions,
    block_index_base: u64,
) -> Result<ChunkedRangePlan, ObjectError> {
    let mut blocks = Vec::new();
    let mut max_end = 0_u64;
    let mut block_indexes = BTreeMap::<u64, u64>::new();
    for range in ranges {
        if range.bytes.is_empty() {
            continue;
        }
        let mut range_offset = 0_usize;
        let range_len = range.bytes.len();
        let mut range_blocks = Vec::new();
        while range_offset < range_len {
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
                .min(range_len - range_offset);
            let block_index = block_indexes.entry(chunk_index).or_insert(block_index_base);
            let block_index_value = *block_index;
            let object_key = block_object_key(options, chunk_index, block_index_value);
            *block_index = block_index_value.saturating_add(1);
            ObjectKey::new(object_key.clone())?;
            range_blocks.push(PlannedBlockMeta {
                order: blocks.len(),
                chunk_index,
                chunk_start,
                object_key,
                logical_offset,
                len: write_len,
            });
            let block_end = logical_offset
                .checked_add(write_len as u64)
                .ok_or(ObjectError::InvalidRange)?;
            max_end = max_end.max(block_end);
            range_offset = range_offset
                .checked_add(write_len)
                .ok_or(ObjectError::InvalidRange)?;
        }
        let range_logical_offset = range.logical_offset;
        let (range_bytes, range_bytes_offset, range_len) = range.bytes.into_shared_vec_window()?;
        for block in range_blocks {
            let relative_offset = usize::try_from(block.logical_offset - range_logical_offset)
                .map_err(|_| ObjectError::InvalidRange)?;
            let relative_end = relative_offset
                .checked_add(block.len)
                .ok_or(ObjectError::InvalidRange)?;
            if relative_end > range_len {
                return Err(ObjectError::InvalidRange);
            }
            let block_bytes_offset = range_bytes_offset
                .checked_add(relative_offset)
                .ok_or(ObjectError::InvalidRange)?;
            let block_bytes = ObjectBytes::shared_vec_slice(
                Arc::clone(&range_bytes),
                block_bytes_offset,
                block.len,
            )?;
            blocks.push(block.with_bytes(block_bytes));
        }
    }
    Ok(ChunkedRangePlan { blocks, max_end })
}

fn staged_range_block_count(
    offset: u64,
    len: usize,
    options: &ChunkWriteOptions,
) -> Result<u64, ObjectError> {
    if len == 0 {
        return Ok(0);
    }
    let mut count = 0_u64;
    let mut range_offset = 0_usize;
    while range_offset < len {
        let logical_offset = offset
            .checked_add(u64::try_from(range_offset).map_err(|_| ObjectError::InvalidRange)?)
            .ok_or(ObjectError::InvalidRange)?;
        let chunk_index = logical_offset / options.chunk_size;
        let chunk_start = chunk_index.saturating_mul(options.chunk_size);
        let next_chunk = chunk_start
            .checked_add(options.chunk_size)
            .ok_or(ObjectError::InvalidRange)?;
        let remaining_in_chunk =
            usize::try_from(next_chunk - logical_offset).map_err(|_| ObjectError::InvalidRange)?;
        let write_len = options
            .block_size
            .min(remaining_in_chunk)
            .min(len - range_offset);
        if write_len == 0 {
            return Err(ObjectError::InvalidRange);
        }
        count = count.saturating_add(1);
        range_offset = range_offset
            .checked_add(write_len)
            .ok_or(ObjectError::InvalidRange)?;
    }
    Ok(count)
}

fn upload_planned_blocks_sequential<O: ObjectStore>(
    store: &O,
    planned: Vec<PlannedBlock>,
    cache: Option<&(dyn BlockCache + Sync)>,
) -> Result<TimedUploadedBlocks, ObjectError> {
    let mut staged = Vec::new();
    let mut uploaded = Vec::with_capacity(planned.len());
    let mut timings = ChunkWriteTimings::default();
    for block in planned {
        let uploaded_block = upload_planned_block(store, block, cache).map_err(|err| {
            ObjectError::StagedWriteFailed {
                source: err.to_string(),
                staged: StagedObjectSet::new(staged.clone()),
            }
        })?;
        timings.add(uploaded_block.timings);
        staged.push(uploaded_block.block.staged.clone());
        uploaded.push(uploaded_block.block);
    }
    Ok(TimedUploadedBlocks {
        blocks: uploaded,
        timings,
    })
}

fn upload_planned_blocks_parallel<O>(
    store: &O,
    planned: Vec<PlannedBlock>,
    workers: usize,
    cache: Option<&(dyn BlockCache + Sync)>,
) -> Result<TimedUploadedBlocks, ObjectError>
where
    O: ObjectStore + Sync,
{
    if workers <= 1 || planned.len() <= 1 {
        return upload_planned_blocks_sequential(store, planned, cache);
    }
    let worker_count = workers.min(planned.len());
    let queue = Mutex::new(VecDeque::from(planned));
    let uploaded = Mutex::new(Vec::<UploadedBlock>::new());
    let timings = Mutex::new(ChunkWriteTimings::default());
    let error = Mutex::new(None::<String>);
    thread::scope(|scope| {
        for _ in 0..worker_count {
            scope.spawn(|| loop {
                if error.lock().map(|guard| guard.is_some()).unwrap_or(true) {
                    break;
                }
                let next = match queue.lock() {
                    Ok(mut queue) => queue.pop_front(),
                    Err(err) => {
                        if let Ok(mut error) = error.lock() {
                            *error = Some(err.to_string());
                        }
                        break;
                    }
                };
                let Some(block) = next else {
                    break;
                };
                match upload_planned_block(store, block, cache) {
                    Ok(block) => {
                        if let Ok(mut uploaded) = uploaded.lock() {
                            uploaded.push(block.block);
                        }
                        if let Ok(mut timings) = timings.lock() {
                            timings.add(block.timings);
                        }
                    }
                    Err(err) => {
                        if let Ok(mut error) = error.lock() {
                            *error = Some(err.to_string());
                        }
                        break;
                    }
                }
            });
        }
    });
    let timings = timings
        .into_inner()
        .map_err(ObjectError::from_poisoned_lock)?;
    let mut uploaded = uploaded
        .into_inner()
        .map_err(ObjectError::from_poisoned_lock)?;
    if let Some(source) = error
        .into_inner()
        .map_err(ObjectError::from_poisoned_lock)?
    {
        let staged = uploaded.iter().map(|block| block.staged.clone()).collect();
        return Err(ObjectError::StagedWriteFailed {
            source,
            staged: StagedObjectSet::new(staged),
        });
    }
    uploaded.sort_by_key(|block| block.order);
    Ok(TimedUploadedBlocks {
        blocks: uploaded,
        timings,
    })
}

fn upload_planned_block<O: ObjectStore>(
    store: &O,
    block: PlannedBlock,
    cache: Option<&(dyn BlockCache + Sync)>,
) -> Result<TimedUploadedBlock, ObjectError> {
    let key = ObjectKey::new(block.object_key.clone())?;
    let len = block.bytes.len() as u64;
    let digest_start = Instant::now();
    let digest_uri = block_digest_uri(block.bytes.as_slice());
    let digest_ns = duration_ns(digest_start.elapsed());
    let cache_bytes = cache.map(|_| block.bytes.clone());
    let put_start = Instant::now();
    let info = store.put(&key, block.bytes)?;
    let store_put_ns = duration_ns(put_start.elapsed());
    let mut cache_put_ns = 0_u64;
    if let Some(cache) = cache {
        // Write-through: populate the local block cache with the just-uploaded
        // block so a read-after-write hits locally instead of re-fetching it
        // from the object store. Each block is its own object (offset 0), so a
        // later `get_block_range(object_key, off, len)` is covered by this
        // whole-block entry. Best-effort: a cache failure must not fail the
        // durable write that already landed in the object store.
        if let Some(bytes) = cache_bytes {
            let cache_start = Instant::now();
            let _ = cache.put_block(
                block_range_cache_key(&block.object_key, 0, bytes.len()),
                bytes.into_vec(),
            );
            cache_put_ns = duration_ns(cache_start.elapsed());
        }
    }
    let stored = StoredBlock {
        object_key: block.object_key,
        logical_offset: block.logical_offset,
        object_offset: 0,
        len,
        digest_uri,
    };
    Ok(TimedUploadedBlock {
        block: UploadedBlock {
            order: block.order,
            chunk_index: block.chunk_index,
            chunk_start: block.chunk_start,
            block: stored,
            staged: StagedObject {
                key: info.key,
                size: info.size,
            },
        },
        timings: ChunkWriteTimings {
            digest_ns,
            store_put_ns,
            cache_put_ns,
        },
    })
}

fn finish_chunked_range_write(
    options: ChunkWriteOptions,
    max_end: u64,
    uploaded: Vec<UploadedBlock>,
) -> Result<ChunkedWrite, ObjectError> {
    let mut chunks = BTreeMap::<u64, StoredChunk>::new();
    let mut object_put_bytes = 0_u64;
    for uploaded in uploaded {
        object_put_bytes = object_put_bytes.saturating_add(uploaded.block.len);
        chunks
            .entry(uploaded.chunk_index)
            .or_insert_with(|| StoredChunk {
                chunk_index: uploaded.chunk_index,
                logical_offset: uploaded.chunk_start,
                len: 0,
                blocks: Vec::new(),
            })
            .blocks
            .push(uploaded.block);
    }
    let object_puts = chunks
        .values()
        .map(|chunk| chunk.blocks.len())
        .sum::<usize>();
    let mut chunks = chunks.into_values().collect::<Vec<_>>();
    for chunk in &mut chunks {
        chunk.blocks.sort_by_key(|block| block.logical_offset);
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
        object_put_bytes,
    })
}

#[derive(Clone, Debug)]
struct ChunkedRangePlan {
    blocks: Vec<PlannedBlock>,
    max_end: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ChunkWriteTimings {
    pub digest_ns: u64,
    pub store_put_ns: u64,
    pub cache_put_ns: u64,
}

impl ChunkWriteTimings {
    fn add(&mut self, next: Self) {
        self.digest_ns = self.digest_ns.saturating_add(next.digest_ns);
        self.store_put_ns = self.store_put_ns.saturating_add(next.store_put_ns);
        self.cache_put_ns = self.cache_put_ns.saturating_add(next.cache_put_ns);
    }
}

#[derive(Clone, Debug)]
struct TimedUploadedBlocks {
    blocks: Vec<UploadedBlock>,
    timings: ChunkWriteTimings,
}

#[derive(Clone, Debug)]
struct TimedUploadedBlock {
    block: UploadedBlock,
    timings: ChunkWriteTimings,
}

#[derive(Clone, Debug)]
struct PlannedBlock {
    order: usize,
    chunk_index: u64,
    chunk_start: u64,
    object_key: String,
    logical_offset: u64,
    bytes: ObjectBytes,
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Clone, Debug)]
struct PlannedBlockMeta {
    order: usize,
    chunk_index: u64,
    chunk_start: u64,
    object_key: String,
    logical_offset: u64,
    len: usize,
}

impl PlannedBlockMeta {
    fn with_bytes(self, bytes: ObjectBytes) -> PlannedBlock {
        debug_assert_eq!(bytes.len(), self.len);
        PlannedBlock {
            order: self.order,
            chunk_index: self.chunk_index,
            chunk_start: self.chunk_start,
            object_key: self.object_key,
            logical_offset: self.logical_offset,
            bytes,
        }
    }
}

#[derive(Clone, Debug)]
struct UploadedBlock {
    order: usize,
    chunk_index: u64,
    chunk_start: u64,
    block: StoredBlock,
    staged: StagedObject,
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

fn block_object_key(options: &ChunkWriteOptions, chunk_index: u64, block_index: u64) -> String {
    format!(
        "blocks/{}/{}/{}/{}/{}",
        options.mount, options.inode, options.generation, chunk_index, block_index
    )
}

fn block_digest_uri(bytes: &[u8]) -> String {
    format!("xxh3-64:{:016x}", xxh3_64(bytes))
}

fn read_chunk_block<R: Read>(reader: &mut R, buffer: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0_usize;
    while filled < buffer.len() {
        let read = reader.read(&mut buffer[filled..])?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}
