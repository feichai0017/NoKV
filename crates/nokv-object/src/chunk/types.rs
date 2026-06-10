use std::io::Read;

use crate::cache::BlockCache;
use crate::store::{validate_key, ObjectBytes, ObjectError, ObjectKey, ObjectStore};
use nokv_types::ChunkManifest;

use super::manifest::chunk_manifests_from_stored_chunks;
use super::read::{
    read_object_blocks_with_cache_options, BlockReadOptions, BlockReadOutcome, ObjectReadBlock,
};
use super::write::{
    delete_staged_objects, put_chunked_object, put_chunked_ranges_with_block_index_base,
    put_chunked_reader,
};

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

    fn write_ranges_with_block_index_base(
        &self,
        ranges: Vec<ChunkWriteRange>,
        options: ChunkWriteOptions,
        block_index_base: u64,
    ) -> Result<ChunkedWrite, ObjectError>;

    fn read_blocks_with_options<C>(
        &self,
        cache: Option<&C>,
        output_len: usize,
        blocks: &[ObjectReadBlock],
        options: BlockReadOptions,
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
    pub bytes: ObjectBytes,
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

    fn write_ranges_with_block_index_base(
        &self,
        ranges: Vec<ChunkWriteRange>,
        options: ChunkWriteOptions,
        block_index_base: u64,
    ) -> Result<ChunkedWrite, ObjectError> {
        put_chunked_ranges_with_block_index_base(self, ranges, options, block_index_base, None)
    }

    fn read_blocks_with_options<C>(
        &self,
        cache: Option<&C>,
        output_len: usize,
        blocks: &[ObjectReadBlock],
        options: BlockReadOptions,
    ) -> Result<BlockReadOutcome, ObjectError>
    where
        C: BlockCache + ?Sized,
    {
        read_object_blocks_with_cache_options(self, cache, output_len, blocks, options)
    }

    fn delete_staged(&self, staged: &StagedObjectSet) -> Result<ObjectCleanupOutcome, ObjectError> {
        delete_staged_objects(self, staged)
    }
}
