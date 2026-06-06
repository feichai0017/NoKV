//! Object storage boundary for NoKV file bodies.
//!
//! This crate owns body-object keys, an S3-compatible object backend, and an
//! in-memory object store for package tests. It does not own namespace metadata,
//! Holt state, Raft replication, FUSE, or wire types.

mod cache;
mod chunk;
mod digest;
mod pipeline;
mod store;

pub use cache::{
    BlockCache, BlockCachePolicy, BlockCacheStats, DiskBlockCache, DiskBlockCacheOptions,
    MemoryBlockCache, MemoryBlockCacheOptions, ObjectBlockCache, WritebackCache,
    WritebackCacheOptions, WritebackCacheStats, WritebackTicket,
};
pub use chunk::{
    chunk_manifest_from_stored_chunk, chunk_manifests_from_stored_chunks, delete_staged_objects,
    manifest_digest_uri, plan_slice_reads, put_chunked_object, put_chunked_ranges,
    put_chunked_ranges_parallel, put_chunked_ranges_with_block_index_base, put_chunked_reader,
    read_object_blocks_with_cache, BlockReadOutcome, ChunkStore, ChunkWriteOptions,
    ChunkWriteRange, ChunkedWrite, DirtyChunkExtent, ObjectCleanupOutcome, ObjectReadBlock,
    SliceReadPlan, StagedObject, StagedObjectSet, StoredBlock, StoredChunk, StoredSlice,
    DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
};
pub use pipeline::{
    FileReadOutcome, FileReadPipeline, FileReadPipelineOptions, FileReadPipelineStats,
    FileWritePipeline, ObjectPrefetchOptions, ObjectPrefetchRequest, ObjectPrefetchStats,
    ObjectPrefetcher, ObjectWritebackOptions, ObjectWritebackRequest, ObjectWritebackStats,
    ObjectWritebackUploader, PendingChunkedWrite, ReadAheadHint, WritebackUploadRange,
};
pub use store::{
    MemoryObjectStore, ObjectCapabilities, ObjectError, ObjectInfo, ObjectKey, ObjectRange,
    ObjectStore, ObjectStoreConfig, S3ObjectStore, S3ObjectStoreOptions,
    DEFAULT_S3_MULTIPART_CHUNK_SIZE, DEFAULT_S3_MULTIPART_CONCURRENCY,
};

#[cfg(test)]
mod tests;
