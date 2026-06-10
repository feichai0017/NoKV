//! Object storage boundary for NoKV file bodies.
//!
//! This crate owns body-object keys, an S3-compatible object backend, and an
//! in-memory object store for package tests. It does not own namespace metadata,
//! Holt state, Raft replication, FUSE, or wire types.

mod cache;
mod chunk;
mod digest;
mod fabric;
mod pipeline;
mod store;

pub use cache::{
    BlockCache, BlockCachePolicy, BlockCacheStats, DiskBlockCache, DiskBlockCacheOptions,
    MemoryBlockCache, MemoryBlockCacheOptions, ObjectBlockCache, WritebackCache,
    WritebackCacheOptions, WritebackCacheStats, WritebackTicket,
};
pub use chunk::{
    chunk_manifest_from_stored_chunk, chunk_manifests_from_stored_chunks,
    chunk_write_ranges_block_count, delete_staged_objects, manifest_digest_uri,
    plan_chunk_manifest_reads, plan_slice_reads, put_chunked_object, put_chunked_ranges_parallel,
    put_chunked_ranges_with_block_index_base, put_chunked_reader,
    read_object_blocks_with_cache_options, BlockReadOptions, BlockReadOutcome, ChunkStore,
    ChunkWriteOptions, ChunkWriteRange, ChunkedWrite, DirtyChunkExtent, ObjectCleanupOutcome,
    ObjectReadBlock, ObjectReadCoordinator, ReadCacheFillMode, SliceReadPlan, StagedObject,
    StagedObjectSet, StoredBlock, StoredChunk, StoredSlice, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
};
pub use fabric::{
    resolve_block_placements, BlockPlacement, DataFabricReadStats, DataTransport, HotFillMode,
    LayoutReadExecutor, LayoutReadOutcome, LocalObjectStore, LocalObjectStoreOptions,
    LocalObjectStoreStats, TieredObjectStore, TieredObjectStoreOptions, TieredObjectStoreStats,
    TieredPutPolicy,
};
pub use pipeline::{
    FileReadOutcome, FileReadPipeline, FileReadPipelineOptions, FileReadPipelineStats,
    FileReadRequest, FileWritePipeline, FileWriteUpload, ObjectPrefetchOptions,
    ObjectPrefetchRequest, ObjectPrefetchStats, ObjectPrefetcher, ObjectReadPlan,
    ObjectReadPlanCache, ObjectReadPlanKey, ObjectSliceWriter, ObjectWritebackOptions,
    ObjectWritebackRequest, ObjectWritebackStats, ObjectWritebackUploader, PendingChunkedWrite,
    ReadAheadHint, WritebackUploadRange,
};
pub use store::{
    ConfiguredObjectStore, MemoryObjectStore, ObjectBytes, ObjectCapabilities, ObjectError,
    ObjectGetRequest, ObjectInfo, ObjectKey, ObjectRange, ObjectStore, ObjectStoreConfig,
    S3ObjectStore, S3ObjectStoreOptions, DEFAULT_S3_MULTIPART_CHUNK_SIZE,
    DEFAULT_S3_MULTIPART_CONCURRENCY,
};

#[cfg(test)]
mod tests;
