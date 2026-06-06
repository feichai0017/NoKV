use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Read};
use std::sync::Mutex;
use std::thread;

use crate::cache::BlockCache;
use crate::digest::sha256_uri;
use crate::store::{validate_key, ObjectError, ObjectKey, ObjectRange, ObjectStore};
use nokv_types::{BlockDescriptor, ChunkManifest, SliceManifest};
use sha2::{Digest, Sha256};

pub const DEFAULT_CHUNK_SIZE: u64 = 64 * 1024 * 1024;
pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024 * 1024;

pub trait ChunkStore {
    fn write_bytes(
        &self,
        bytes: &[u8],
        options: ChunkWriteOptions,
    ) -> Result<ChunkedWrite, ObjectError>;

    fn write_reader<R>(
        &self,
        reader: R,
        options: ChunkWriteOptions,
    ) -> Result<ChunkedWrite, ObjectError>
    where
        R: Read;

    fn write_ranges(
        &self,
        ranges: &[ChunkWriteRange],
        options: ChunkWriteOptions,
    ) -> Result<ChunkedWrite, ObjectError>;

    fn write_ranges_with_block_index_base(
        &self,
        ranges: &[ChunkWriteRange],
        options: ChunkWriteOptions,
        block_index_base: u64,
    ) -> Result<ChunkedWrite, ObjectError>;

    fn read_blocks<C>(
        &self,
        cache: Option<&C>,
        output_len: usize,
        blocks: &[ObjectReadBlock],
    ) -> Result<BlockReadOutcome, ObjectError>
    where
        C: BlockCache + ?Sized;

    fn delete_staged(&self, staged: &StagedObjectSet) -> Result<ObjectCleanupOutcome, ObjectError>;
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
    pub object_put_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredSlice {
    pub slice_id: u64,
    pub logical_offset: u64,
    pub len: u64,
    pub chunks: Vec<StoredChunk>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SliceReadPlan {
    pub output_len: usize,
    pub blocks: Vec<ObjectReadBlock>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirtyChunkExtent {
    pub chunks: Vec<StoredChunk>,
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
    pub object_get_bytes: u64,
    pub coalesced_gets: usize,
    pub coalesced_get_bytes: u64,
    pub cache_hits: usize,
    pub cache_hit_bytes: u64,
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
    pub fn into_slice(self, slice_id: u64) -> StoredSlice {
        let len = self.size;
        self.into_slice_at(slice_id, 0, len)
    }

    pub fn into_slice_at(self, slice_id: u64, logical_offset: u64, len: u64) -> StoredSlice {
        StoredSlice {
            slice_id,
            logical_offset,
            len,
            chunks: self.chunks,
        }
    }

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

    pub fn chunk_manifests(&self) -> Vec<ChunkManifest> {
        chunk_manifests_from_stored_chunks(&self.chunks)
    }
}

pub fn chunk_manifest_from_stored_chunk(chunk: &StoredChunk) -> ChunkManifest {
    ChunkManifest {
        chunk_index: chunk.chunk_index,
        logical_offset: chunk.logical_offset,
        len: chunk.len,
        slices: vec![SliceManifest {
            slice_id: 1,
            logical_offset: chunk.logical_offset,
            len: chunk.len,
            blocks: chunk
                .blocks
                .iter()
                .map(|block| BlockDescriptor {
                    object_key: block.object_key.clone(),
                    logical_offset: block.logical_offset,
                    object_offset: block.object_offset,
                    len: block.len,
                    digest_uri: block.digest_uri.clone(),
                })
                .collect(),
        }],
    }
}

pub fn chunk_manifests_from_stored_chunks(chunks: &[StoredChunk]) -> Vec<ChunkManifest> {
    chunks
        .iter()
        .map(chunk_manifest_from_stored_chunk)
        .collect()
}

pub fn manifest_digest_uri(size: u64, generation: u64, chunks: &[StoredChunk]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(size.to_be_bytes());
    hasher.update(generation.to_be_bytes());
    for chunk in chunks {
        hasher.update(chunk.chunk_index.to_be_bytes());
        hasher.update(chunk.logical_offset.to_be_bytes());
        hasher.update(chunk.len.to_be_bytes());
        for block in &chunk.blocks {
            hasher.update(block.object_key.as_bytes());
            hasher.update([0]);
            hasher.update(block.logical_offset.to_be_bytes());
            hasher.update(block.object_offset.to_be_bytes());
            hasher.update(block.len.to_be_bytes());
            hasher.update(block.digest_uri.as_bytes());
            hasher.update([0]);
        }
    }
    let digest = hasher.finalize();
    format!("manifest-sha256:{digest:x}")
}

pub fn plan_slice_reads(
    slices: &[StoredSlice],
    file_offset: u64,
    len: usize,
) -> Result<SliceReadPlan, ObjectError> {
    if len == 0 {
        return Ok(SliceReadPlan {
            output_len: 0,
            blocks: Vec::new(),
        });
    }
    let request_end = file_offset
        .checked_add(u64::try_from(len).map_err(|_| ObjectError::InvalidRange)?)
        .ok_or(ObjectError::InvalidRange)?;
    let request = ReadInterval {
        start: file_offset,
        end: request_end,
    };
    let mut remaining = vec![request];
    let mut blocks = Vec::new();

    for slice in slices.iter().rev() {
        if remaining.is_empty() || slice.len == 0 {
            break;
        }
        let slice_end = slice
            .logical_offset
            .checked_add(slice.len)
            .ok_or(ObjectError::InvalidRange)?;
        let slice_interval = ReadInterval {
            start: slice.logical_offset,
            end: slice_end,
        };
        if slice_interval.intersect(request).is_none() {
            continue;
        }
        for chunk in &slice.chunks {
            if remaining.is_empty() {
                break;
            }
            let chunk_end = chunk
                .logical_offset
                .checked_add(chunk.len)
                .ok_or(ObjectError::InvalidRange)?;
            let chunk_interval = ReadInterval {
                start: chunk.logical_offset,
                end: chunk_end,
            };
            let Some(chunk_scope) = chunk_interval
                .intersect(slice_interval)
                .and_then(|scope| scope.intersect(request))
            else {
                continue;
            };
            for block in &chunk.blocks {
                if block.len == 0 {
                    return Err(ObjectError::InvalidRange);
                }
                let block_end = block
                    .logical_offset
                    .checked_add(block.len)
                    .ok_or(ObjectError::InvalidRange)?;
                let block_interval = ReadInterval {
                    start: block.logical_offset,
                    end: block_end,
                };
                let Some(block_scope) = block_interval.intersect(chunk_scope) else {
                    continue;
                };
                let mut covered = Vec::new();
                for interval in &remaining {
                    let Some(segment) = interval.intersect(block_scope) else {
                        continue;
                    };
                    let block_skip = segment.start.saturating_sub(block.logical_offset);
                    let object_offset = block
                        .object_offset
                        .checked_add(block_skip)
                        .ok_or(ObjectError::InvalidRange)?;
                    blocks.push(ObjectReadBlock {
                        object_key: block.object_key.clone(),
                        object_offset,
                        len: usize::try_from(segment.end - segment.start)
                            .map_err(|_| ObjectError::InvalidRange)?,
                        output_offset: usize::try_from(segment.start - file_offset)
                            .map_err(|_| ObjectError::InvalidRange)?,
                    });
                    covered.push(segment);
                }
                for segment in covered {
                    subtract_interval(&mut remaining, segment);
                }
            }
        }
    }
    blocks.sort_by_key(|block| block.output_offset);
    Ok(SliceReadPlan {
        output_len: len,
        blocks,
    })
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

impl<T> ChunkStore for T
where
    T: ObjectStore,
{
    fn write_bytes(
        &self,
        bytes: &[u8],
        options: ChunkWriteOptions,
    ) -> Result<ChunkedWrite, ObjectError> {
        put_chunked_object(self, bytes, options)
    }

    fn write_reader<R>(
        &self,
        reader: R,
        options: ChunkWriteOptions,
    ) -> Result<ChunkedWrite, ObjectError>
    where
        R: Read,
    {
        put_chunked_reader(self, reader, options)
    }

    fn write_ranges(
        &self,
        ranges: &[ChunkWriteRange],
        options: ChunkWriteOptions,
    ) -> Result<ChunkedWrite, ObjectError> {
        put_chunked_ranges(self, ranges, options)
    }

    fn write_ranges_with_block_index_base(
        &self,
        ranges: &[ChunkWriteRange],
        options: ChunkWriteOptions,
        block_index_base: u64,
    ) -> Result<ChunkedWrite, ObjectError> {
        put_chunked_ranges_with_block_index_base(self, ranges, options, block_index_base)
    }

    fn read_blocks<C>(
        &self,
        cache: Option<&C>,
        output_len: usize,
        blocks: &[ObjectReadBlock],
    ) -> Result<BlockReadOutcome, ObjectError>
    where
        C: BlockCache + ?Sized,
    {
        read_object_blocks_with_cache(self, cache, output_len, blocks)
    }

    fn delete_staged(&self, staged: &StagedObjectSet) -> Result<ObjectCleanupOutcome, ObjectError> {
        delete_staged_objects(self, staged)
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
            let info = store
                .put(&key, block)
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
        let info = store
            .put(&key, block)
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

pub fn put_chunked_ranges<O: ObjectStore>(
    store: &O,
    ranges: &[ChunkWriteRange],
    options: ChunkWriteOptions,
) -> Result<ChunkedWrite, ObjectError> {
    put_chunked_ranges_with_block_index_base(store, ranges, options, 0)
}

pub fn put_chunked_ranges_parallel<O>(
    store: &O,
    ranges: &[ChunkWriteRange],
    options: ChunkWriteOptions,
    block_index_base: u64,
    workers: usize,
) -> Result<ChunkedWrite, ObjectError>
where
    O: ObjectStore + Sync,
{
    options.validate()?;
    let plan = plan_chunked_range_blocks(ranges, &options, block_index_base)?;
    let uploaded = upload_planned_blocks_parallel(store, plan.blocks, workers.max(1))?;
    finish_chunked_range_write(options, plan.max_end, uploaded)
}

pub fn put_chunked_ranges_with_block_index_base<O: ObjectStore>(
    store: &O,
    ranges: &[ChunkWriteRange],
    options: ChunkWriteOptions,
    block_index_base: u64,
) -> Result<ChunkedWrite, ObjectError> {
    options.validate()?;
    let plan = plan_chunked_range_blocks(ranges, &options, block_index_base)?;
    let uploaded = upload_planned_blocks_sequential(store, plan.blocks)?;
    finish_chunked_range_write(options, plan.max_end, uploaded)
}

fn plan_chunked_range_blocks(
    ranges: &[ChunkWriteRange],
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
            let object_key = block_object_key(options, chunk_index, block_index_value);
            *block_index = block_index_value.saturating_add(1);
            ObjectKey::new(object_key.clone())?;
            let bytes = range.bytes[range_offset..range_offset + write_len].to_vec();
            blocks.push(PlannedBlock {
                order: blocks.len(),
                chunk_index,
                chunk_start,
                object_key,
                logical_offset,
                bytes,
            });
            let block_end = logical_offset
                .checked_add(write_len as u64)
                .ok_or(ObjectError::InvalidRange)?;
            max_end = max_end.max(block_end);
            range_offset += write_len;
        }
    }
    Ok(ChunkedRangePlan { blocks, max_end })
}

fn upload_planned_blocks_sequential<O: ObjectStore>(
    store: &O,
    planned: Vec<PlannedBlock>,
) -> Result<Vec<UploadedBlock>, ObjectError> {
    let mut staged = Vec::new();
    let mut uploaded = Vec::with_capacity(planned.len());
    for block in planned {
        let uploaded_block =
            upload_planned_block(store, block).map_err(|err| ObjectError::StagedWriteFailed {
                source: err.to_string(),
                staged: StagedObjectSet::new(staged.clone()),
            })?;
        staged.push(uploaded_block.staged.clone());
        uploaded.push(uploaded_block);
    }
    Ok(uploaded)
}

fn upload_planned_blocks_parallel<O>(
    store: &O,
    planned: Vec<PlannedBlock>,
    workers: usize,
) -> Result<Vec<UploadedBlock>, ObjectError>
where
    O: ObjectStore + Sync,
{
    if workers <= 1 || planned.len() <= 1 {
        return upload_planned_blocks_sequential(store, planned);
    }
    let worker_count = workers.min(planned.len());
    let queue = Mutex::new(VecDeque::from(planned));
    let uploaded = Mutex::new(Vec::<UploadedBlock>::new());
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
                match upload_planned_block(store, block) {
                    Ok(block) => {
                        if let Ok(mut uploaded) = uploaded.lock() {
                            uploaded.push(block);
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
    Ok(uploaded)
}

fn upload_planned_block<O: ObjectStore>(
    store: &O,
    block: PlannedBlock,
) -> Result<UploadedBlock, ObjectError> {
    let key = ObjectKey::new(block.object_key.clone())?;
    let info = store.put(&key, &block.bytes)?;
    let len = block.bytes.len() as u64;
    let stored = StoredBlock {
        object_key: block.object_key,
        logical_offset: block.logical_offset,
        object_offset: 0,
        len,
        digest_uri: block_digest_uri(&block.bytes),
    };
    Ok(UploadedBlock {
        order: block.order,
        chunk_index: block.chunk_index,
        chunk_start: block.chunk_start,
        block: stored,
        staged: StagedObject {
            key: info.key,
            size: info.size,
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

#[derive(Clone, Debug)]
struct PlannedBlock {
    order: usize,
    chunk_index: u64,
    chunk_start: u64,
    object_key: String,
    logical_offset: u64,
    bytes: Vec<u8>,
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

pub fn read_object_blocks_with_cache<O, C>(
    store: &O,
    cache: Option<&C>,
    output_len: usize,
    blocks: &[ObjectReadBlock],
) -> Result<BlockReadOutcome, ObjectError>
where
    O: ObjectStore,
    C: BlockCache + ?Sized,
{
    #[derive(Clone)]
    struct PendingRead {
        key: ObjectKey,
        cache_key: String,
        object_offset: u64,
        len: usize,
        output_offset: usize,
    }

    let mut out = vec![0_u8; output_len];
    let mut object_gets = 0_usize;
    let mut object_get_bytes = 0_u64;
    let mut coalesced_gets = 0_usize;
    let mut coalesced_get_bytes = 0_u64;
    let mut cache_hits = 0_usize;
    let mut cache_hit_bytes = 0_u64;
    let mut pending = Vec::new();
    for block in blocks {
        let key = ObjectKey::new(block.object_key.clone())?;
        let cache_key = format!("{}:{}:{}", key.as_str(), block.object_offset, block.len);
        if block.len == 0 {
            return Err(ObjectError::InvalidRange);
        }
        let end = block
            .output_offset
            .checked_add(block.len)
            .ok_or(ObjectError::InvalidRange)?;
        if end > out.len() {
            return Err(ObjectError::InvalidRange);
        }
        if let Some(cache) = cache {
            if let Some(cached) = cache.get_block(&cache_key)? {
                if cached.len() != block.len {
                    return Err(ObjectError::InvalidRange);
                }
                cache_hits += 1;
                cache_hit_bytes = cache_hit_bytes.saturating_add(cached.len() as u64);
                out[block.output_offset..end].copy_from_slice(&cached);
                continue;
            }
        }
        pending.push(PendingRead {
            key,
            cache_key,
            object_offset: block.object_offset,
            len: block.len,
            output_offset: block.output_offset,
        });
    }
    pending.sort_by(|left, right| {
        left.key
            .as_str()
            .cmp(right.key.as_str())
            .then_with(|| left.object_offset.cmp(&right.object_offset))
            .then_with(|| left.output_offset.cmp(&right.output_offset))
    });

    let mut index = 0_usize;
    while index < pending.len() {
        let start = index;
        let mut end = index + 1;
        let mut fetch_end = pending[index]
            .object_offset
            .checked_add(pending[index].len as u64)
            .ok_or(ObjectError::InvalidRange)?;
        while end < pending.len()
            && pending[end].key == pending[start].key
            && pending[end].object_offset == fetch_end
        {
            fetch_end = pending[end]
                .object_offset
                .checked_add(pending[end].len as u64)
                .ok_or(ObjectError::InvalidRange)?;
            end += 1;
        }
        let fetch_offset = pending[start].object_offset;
        let fetch_len =
            usize::try_from(fetch_end - fetch_offset).map_err(|_| ObjectError::InvalidRange)?;
        let fetched = store.get(
            &pending[start].key,
            Some(ObjectRange::new(fetch_offset, fetch_len)?),
        )?;
        object_gets += 1;
        object_get_bytes = object_get_bytes.saturating_add(fetched.len() as u64);
        if end - start > 1 {
            coalesced_gets += 1;
            coalesced_get_bytes = coalesced_get_bytes.saturating_add(fetched.len() as u64);
        }
        for request in &pending[start..end] {
            let relative = usize::try_from(request.object_offset - fetch_offset)
                .map_err(|_| ObjectError::InvalidRange)?;
            let relative_end = relative
                .checked_add(request.len)
                .ok_or(ObjectError::InvalidRange)?;
            if relative_end > fetched.len() {
                return Err(ObjectError::InvalidRange);
            }
            let bytes = &fetched[relative..relative_end];
            let output_end = request
                .output_offset
                .checked_add(bytes.len())
                .ok_or(ObjectError::InvalidRange)?;
            out[request.output_offset..output_end].copy_from_slice(bytes);
            if let Some(cache) = cache {
                cache.put_block(request.cache_key.clone(), bytes.to_vec())?;
            }
        }
        index = end;
    }
    Ok(BlockReadOutcome {
        bytes: out,
        object_gets,
        object_get_bytes,
        coalesced_gets,
        coalesced_get_bytes,
        cache_hits,
        cache_hit_bytes,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReadInterval {
    start: u64,
    end: u64,
}

impl ReadInterval {
    fn intersect(self, other: ReadInterval) -> Option<ReadInterval> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        (start < end).then_some(ReadInterval { start, end })
    }
}

fn subtract_interval(remaining: &mut Vec<ReadInterval>, covered: ReadInterval) {
    let mut next = Vec::with_capacity(remaining.len().saturating_add(1));
    for interval in remaining.drain(..) {
        let Some(overlap) = interval.intersect(covered) else {
            next.push(interval);
            continue;
        };
        if interval.start < overlap.start {
            next.push(ReadInterval {
                start: interval.start,
                end: overlap.start,
            });
        }
        if overlap.end < interval.end {
            next.push(ReadInterval {
                start: overlap.end,
                end: interval.end,
            });
        }
    }
    *remaining = next;
}

fn block_object_key(options: &ChunkWriteOptions, chunk_index: u64, block_index: u64) -> String {
    format!(
        "blocks/{}/{}/{}/{}/{}",
        options.mount, options.inode, options.generation, chunk_index, block_index
    )
}

fn block_digest_uri(bytes: &[u8]) -> String {
    sha256_uri(bytes)
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
