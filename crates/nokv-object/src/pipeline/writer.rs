use crate::chunk::{
    chunk_write_ranges_block_count, ChunkWriteOptions, ChunkWriteRange, ChunkedWrite,
    DirtyChunkExtent, StagedObjectSet, StoredChunk,
};
use crate::store::{ObjectBytes, ObjectError};

use super::slice_writer::ObjectSliceWriter;

#[derive(Clone, Debug)]
pub struct FileWritePipeline {
    options: ChunkWriteOptions,
    active_slice: ObjectSliceWriter,
    staged_chunks: Vec<StoredChunk>,
    staged: StagedObjectSet,
    dirty_extents: Vec<DirtyChunkExtent>,
    next_block_index: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileWriteUpload {
    pub block_index_base: u64,
    pub block_count: u64,
    pub ranges: Vec<ChunkWriteRange>,
}

impl FileWritePipeline {
    pub fn new(options: ChunkWriteOptions) -> Result<Self, ObjectError> {
        options.validate()?;
        let active_slice = ObjectSliceWriter::new(options.block_size)?;
        Ok(Self {
            options,
            active_slice,
            staged_chunks: Vec::new(),
            staged: StagedObjectSet::default(),
            dirty_extents: Vec::new(),
            next_block_index: 0,
        })
    }

    pub fn options(&self) -> &ChunkWriteOptions {
        &self.options
    }

    pub fn reserve_blocks(&mut self, block_count: u64) -> u64 {
        let base = self.next_block_index;
        self.next_block_index = self.next_block_index.saturating_add(block_count);
        base
    }

    pub fn write_at(
        &mut self,
        logical_offset: u64,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<(), ObjectError> {
        self.active_slice.write_at(logical_offset, bytes)
    }

    pub fn flush_to(&mut self, offset: u64) -> Result<Option<FileWriteUpload>, ObjectError> {
        let ranges = self.active_slice.flush_to(offset)?;
        self.reserve_upload(ranges)
    }

    pub fn finish_upload(&mut self) -> Result<Option<FileWriteUpload>, ObjectError> {
        let ranges = self.active_slice.finish()?;
        self.active_slice = ObjectSliceWriter::new(self.options.block_size)?;
        self.reserve_upload(ranges)
    }

    pub fn prepare_upload(
        &mut self,
        ranges: Vec<ChunkWriteRange>,
    ) -> Result<Option<FileWriteUpload>, ObjectError> {
        self.reserve_upload(ranges)
    }

    pub fn record_write(&mut self, written: ChunkedWrite) -> Result<(), ObjectError> {
        let staged = written.staged_objects()?;
        let staged_objects = self
            .staged
            .objects()
            .iter()
            .cloned()
            .chain(staged.objects().iter().cloned())
            .collect();
        self.staged = StagedObjectSet::new(staged_objects);
        self.staged_chunks.extend(written.chunks.clone());
        self.dirty_extents.push(DirtyChunkExtent {
            chunks: written.chunks,
        });
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.staged.is_empty()
    }

    pub fn staged_chunks(&self) -> &[StoredChunk] {
        &self.staged_chunks
    }

    pub fn staged_objects(&self) -> &StagedObjectSet {
        &self.staged
    }

    pub fn dirty_extents(&self) -> &[DirtyChunkExtent] {
        &self.dirty_extents
    }

    fn reserve_upload(
        &mut self,
        ranges: Vec<ChunkWriteRange>,
    ) -> Result<Option<FileWriteUpload>, ObjectError> {
        let block_count = chunk_write_ranges_block_count(&ranges, &self.options)?;
        if block_count == 0 {
            return Ok(None);
        }
        let block_index_base = self.reserve_blocks(block_count);
        Ok(Some(FileWriteUpload {
            block_index_base,
            block_count,
            ranges,
        }))
    }
}
