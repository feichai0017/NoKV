use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use std::{
    collections::{HashMap, VecDeque},
    io,
};

use nokv_meta::{
    DentryWithAttr, ObjectTransferStats, PublishArtifactStagedSession, RenameReplaceResult,
    UpdateAttr, XattrSetMode,
};
use nokv_object::{
    ChunkStore, ChunkWriteOptions, ChunkedWrite, FileReadPipeline, ObjectBlockCache, ObjectError,
    ObjectPrefetchOptions, ObjectPrefetchRequest, ObjectPrefetcher, ObjectReadBlock, ObjectStore,
    StagedObjectSet, StoredChunk, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
};
use nokv_protocol::{
    decode_envelope, decode_name_cursor, decode_xattr_name, encode_advisory_lock_kind,
    encode_file_type, encode_name_cursor, encode_request, encode_xattr_name, MetadataProtocolError,
    MetadataRpcEnvelope, MetadataRpcRequest, MetadataRpcResult, WireAdvisoryLock,
    WireBodyDescriptor, WireBodyReadPlan, WireChunkManifest, WireDentryWithAttr, WireMetadataError,
    WireMetadataPosition, WireObjectReadBlock, WirePathMetadata, WirePreparedArtifact,
    WireSliceManifest, WireStagedObject, WireStagedObjectSet, WireUpdateAttr, WireXattrSetMode,
};
use nokv_types::{
    parse_absolute_path, AdvisoryLock, AdvisoryLockRequest, BlockDescriptor, BodyDescriptor,
    ChunkManifest, DentryName, FileType, InodeAttr, InodeId, PathMetadata, SliceManifest,
    SnapshotPin, SpecialNodeSpec,
};

use crate::{ArtifactMetadata, ClientError, NamespaceRead};

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);
const READ_NOT_FRESH_RETRY_INTERVAL: Duration = Duration::from_millis(25);
const MAX_RPC_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_BATCH_RPC_REQUESTS: usize = 128;
const FRAMED_RPC_MAGIC: &[u8; 8] = b"NKVRPC3\n";
const FRAME_HEADER_BYTES: usize = 16;
const MAX_READ_PIPELINES: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataClientOptions {
    pub address: SocketAddr,
    pub read_endpoints: Vec<SocketAddr>,
    pub timeout: Duration,
}

pub struct MetadataClient {
    options: MetadataClientOptions,
    next_request_id: AtomicU64,
    connections: Mutex<HashMap<SocketAddr, Arc<PipelinedConnection>>>,
    observed_position: Mutex<Option<WireMetadataPosition>>,
}

struct PipelinedConnection {
    writer: Mutex<TcpStream>,
    pending: Arc<Mutex<HashMap<u64, mpsc::Sender<PendingFrame>>>>,
}

enum PendingFrame {
    Payload(Vec<u8>),
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientBodyReadPlan {
    pub output_len: usize,
    pub blocks: Vec<ObjectReadBlock>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientPreparedArtifact {
    pub mount: u64,
    pub parent: InodeId,
    pub name: DentryName,
    pub path: Option<String>,
    pub inode: InodeId,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
    pub replace: bool,
    pub dentry_version: Option<u64>,
    pub old_generation: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientReadDirPlusPage {
    pub entries: Vec<DentryWithAttr>,
    pub next_cursor: Option<DentryName>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClientMetadataPosition {
    pub term: u64,
    pub index: u64,
}

const DEFAULT_LIST_PAGE_SIZE: usize = 1024;

pub struct NoKvFsClient<O> {
    metadata: MetadataClient,
    objects: Arc<O>,
    block_cache: ObjectBlockCache,
    prefetcher: ObjectPrefetcher<Arc<O>>,
    read_pipelines: Mutex<ReadPipelineCache>,
    block_cache_enabled: bool,
    object_puts: AtomicU64,
    object_put_bytes: AtomicU64,
    object_gets: AtomicU64,
    object_get_bytes: AtomicU64,
    coalesced_gets: AtomicU64,
    coalesced_get_bytes: AtomicU64,
    cache_hits: AtomicU64,
    cache_hit_bytes: AtomicU64,
    manifest_chunks: AtomicU64,
    manifest_blocks: AtomicU64,
}

#[derive(Debug)]
struct ReadPipelineCache {
    capacity: usize,
    pipelines: HashMap<String, FileReadPipeline>,
    order: VecDeque<String>,
}

impl MetadataClientOptions {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            read_endpoints: Vec::new(),
            timeout: DEFAULT_RPC_TIMEOUT,
        }
    }

    pub fn with_read_endpoints(mut self, endpoints: Vec<SocketAddr>) -> Self {
        self.read_endpoints = endpoints;
        self
    }
}

impl<O> NoKvFsClient<O>
where
    O: ObjectStore + Send + Sync + 'static,
{
    pub fn new(metadata: MetadataClient, objects: O) -> Self {
        Self::with_block_cache(metadata, objects, ObjectBlockCache::default())
    }

    pub fn with_block_cache(
        metadata: MetadataClient,
        objects: O,
        block_cache: ObjectBlockCache,
    ) -> Self {
        let objects = Arc::new(objects);
        let prefetcher = ObjectPrefetcher::new(
            Arc::clone(&objects),
            block_cache.clone(),
            ObjectPrefetchOptions::default(),
        );
        Self {
            metadata,
            objects,
            block_cache,
            prefetcher,
            read_pipelines: Mutex::new(ReadPipelineCache::new(MAX_READ_PIPELINES)),
            block_cache_enabled: true,
            object_puts: AtomicU64::new(0),
            object_put_bytes: AtomicU64::new(0),
            object_gets: AtomicU64::new(0),
            object_get_bytes: AtomicU64::new(0),
            coalesced_gets: AtomicU64::new(0),
            coalesced_get_bytes: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_hit_bytes: AtomicU64::new(0),
            manifest_chunks: AtomicU64::new(0),
            manifest_blocks: AtomicU64::new(0),
        }
    }

    pub fn connect(address: SocketAddr, objects: O) -> Self {
        Self::new(MetadataClient::connect(address), objects)
    }

    pub fn metadata(&self) -> &MetadataClient {
        &self.metadata
    }

    pub fn set_block_cache_enabled(&mut self, enabled: bool) {
        self.block_cache_enabled = enabled;
    }

    pub fn block_cache_enabled(&self) -> bool {
        self.block_cache_enabled
    }

    pub fn object_stats(&self) -> ObjectTransferStats {
        let prefetch = self.prefetcher.stats().unwrap_or_default();
        ObjectTransferStats {
            object_puts: self.object_puts.load(Ordering::Relaxed),
            object_put_bytes: self.object_put_bytes.load(Ordering::Relaxed),
            object_gets: self
                .object_gets
                .load(Ordering::Relaxed)
                .saturating_add(prefetch.object_gets),
            object_get_bytes: self
                .object_get_bytes
                .load(Ordering::Relaxed)
                .saturating_add(prefetch.object_get_bytes),
            coalesced_gets: self.coalesced_gets.load(Ordering::Relaxed),
            coalesced_get_bytes: self.coalesced_get_bytes.load(Ordering::Relaxed),
            cache_hits: self
                .cache_hits
                .load(Ordering::Relaxed)
                .saturating_add(prefetch.cache_hits),
            cache_hit_bytes: self
                .cache_hit_bytes
                .load(Ordering::Relaxed)
                .saturating_add(prefetch.cache_hit_bytes),
            prefetch_enqueued: prefetch.enqueued,
            prefetch_dropped: prefetch.dropped,
            prefetch_completed: prefetch.completed,
            prefetch_failed: prefetch.failed,
            prefetch_object_gets: prefetch.object_gets,
            prefetch_object_get_bytes: prefetch.object_get_bytes,
            prefetch_cache_hits: prefetch.cache_hits,
            prefetch_cache_hit_bytes: prefetch.cache_hit_bytes,
            manifest_chunks: self.manifest_chunks.load(Ordering::Relaxed),
            manifest_blocks: self.manifest_blocks.load(Ordering::Relaxed),
        }
    }

    pub fn cat(&self, path: &str) -> Result<Vec<u8>, ClientError> {
        let entry = self
            .metadata
            .lookup(path)?
            .ok_or_else(|| ClientError::NotFound(path.to_owned()))?;
        self.read_entry(path, &entry, 0, file_len(entry.attr.size)?)
    }

    pub fn cat_snapshot(&self, snapshot_id: u64, path: &str) -> Result<Vec<u8>, ClientError> {
        self.metadata.read_artifact_at_snapshot(snapshot_id, path)
    }

    pub fn read_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        self.metadata
            .read_file_path_at_snapshot(snapshot_id, path, offset, len)
    }

    pub fn read(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, ClientError> {
        let entry = self
            .metadata
            .lookup(path)?
            .ok_or_else(|| ClientError::NotFound(path.to_owned()))?;
        self.read_entry(path, &entry, offset, len)
    }

    pub fn read_path(
        &self,
        path: &str,
        offset: u64,
        len: usize,
        expected_generation: Option<u64>,
    ) -> Result<NamespaceRead, ClientError> {
        let (metadata, plan) =
            self.metadata
                .read_path_plan(path, offset, len, expected_generation)?;
        let generation = metadata
            .body
            .as_ref()
            .map(|body| body.generation)
            .unwrap_or(metadata.attr.generation);
        let bytes = self.read_planned_object_blocks(
            &read_pipeline_key(path, generation),
            metadata.attr.inode,
            generation,
            metadata.attr.size,
            offset,
            &plan,
        )?;
        Ok(NamespaceRead { metadata, bytes })
    }

    fn read_entry(
        &self,
        path: &str,
        entry: &DentryWithAttr,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        if entry.attr.file_type != FileType::File {
            return Err(ClientError::Metadata(nokv_meta::MetadError::NotFile));
        }
        if len == 0 || offset >= entry.attr.size {
            return Ok(Vec::new());
        }
        let body = entry.body.as_ref().ok_or_else(|| {
            ClientError::Protocol(format!("file {path} is missing body descriptor"))
        })?;
        let len = bounded_read_len(entry.attr.size - offset, len)?;
        let plan = self
            .metadata
            .read_body_plan(entry.attr.inode, body.generation, offset, len)?;
        self.read_planned_object_blocks(
            &read_pipeline_key(path, body.generation),
            entry.attr.inode,
            body.generation,
            entry.attr.size,
            offset,
            &plan,
        )
    }

    fn read_planned_object_blocks(
        &self,
        pipeline_key: &str,
        inode: InodeId,
        generation: u64,
        file_size: u64,
        offset: u64,
        plan: &ClientBodyReadPlan,
    ) -> Result<Vec<u8>, ClientError> {
        if plan.output_len == 0 {
            return Ok(Vec::new());
        }
        let cache = if self.block_cache_enabled {
            Some(&self.block_cache)
        } else {
            None
        };
        let mut pipeline = {
            let mut pipelines = self.read_pipelines.lock().map_err(|err| {
                ClientError::Protocol(format!("read pipeline lock poisoned: {err}"))
            })?;
            pipelines.take(pipeline_key)
        };
        let outcome = pipeline
            .read_blocks(
                &self.objects,
                cache,
                file_size,
                offset,
                plan.output_len,
                &plan.blocks,
            )
            .map_err(ClientError::Object)?;
        {
            let mut pipelines = self.read_pipelines.lock().map_err(|err| {
                ClientError::Protocol(format!("read pipeline lock poisoned: {err}"))
            })?;
            pipelines.insert(pipeline_key.to_owned(), pipeline);
        }
        if let Some(hint) = outcome.readahead {
            self.prefetch_read_blocks(inode, generation, hint.offset, hint.len);
        }
        let blocks = outcome.blocks;
        self.object_gets
            .fetch_add(blocks.object_gets as u64, Ordering::Relaxed);
        self.object_get_bytes
            .fetch_add(blocks.object_get_bytes, Ordering::Relaxed);
        self.coalesced_gets
            .fetch_add(blocks.coalesced_gets as u64, Ordering::Relaxed);
        self.coalesced_get_bytes
            .fetch_add(blocks.coalesced_get_bytes, Ordering::Relaxed);
        self.cache_hits
            .fetch_add(blocks.cache_hits as u64, Ordering::Relaxed);
        self.cache_hit_bytes
            .fetch_add(blocks.cache_hit_bytes, Ordering::Relaxed);
        Ok(blocks.bytes)
    }

    fn prefetch_read_blocks(&self, inode: InodeId, generation: u64, offset: u64, len: usize) {
        if !self.block_cache_enabled || len == 0 {
            return;
        }
        let Ok(plan) = self.metadata.read_body_plan(inode, generation, offset, len) else {
            return;
        };
        let _ = self
            .prefetcher
            .submit(ObjectPrefetchRequest::new(plan.output_len, plan.blocks));
    }

    pub fn put_artifact(
        &self,
        path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, ClientError> {
        let prepared = self.metadata.prepare_artifact_path(path, false)?;
        let mode = metadata.mode;
        let uid = metadata.uid;
        let gid = metadata.gid;
        let (body, chunks, staged) = self.stage_artifact_body(&prepared, &bytes, metadata)?;
        match self
            .metadata
            .publish_prepared_artifact(prepared, body, chunks, mode, uid, gid)
        {
            Ok(result) => Ok(result.entry),
            Err(err) => {
                self.objects
                    .delete_staged(&staged)
                    .map_err(ClientError::Object)?;
                Err(err)
            }
        }
    }

    pub fn put_artifact_from_reader<R: Read>(
        &self,
        path: &str,
        reader: R,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, ClientError> {
        let prepared = self.metadata.prepare_artifact_path(path, false)?;
        let mode = metadata.mode;
        let uid = metadata.uid;
        let gid = metadata.gid;
        let (body, chunks, staged) = self.stage_artifact_reader(&prepared, reader, metadata)?;
        match self
            .metadata
            .publish_prepared_artifact(prepared, body, chunks, mode, uid, gid)
        {
            Ok(result) => Ok(result.entry),
            Err(err) => {
                self.objects
                    .delete_staged(&staged)
                    .map_err(ClientError::Object)?;
                Err(err)
            }
        }
    }

    pub fn put_artifact_replace(
        &self,
        path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<RenameReplaceResult, ClientError> {
        let prepared = self.metadata.prepare_artifact_path(path, true)?;
        let mode = metadata.mode;
        let uid = metadata.uid;
        let gid = metadata.gid;
        let (body, chunks, staged) = self.stage_artifact_body(&prepared, &bytes, metadata)?;
        match self
            .metadata
            .publish_prepared_artifact(prepared, body, chunks, mode, uid, gid)
        {
            Ok(result) => Ok(result),
            Err(err) => {
                self.objects
                    .delete_staged(&staged)
                    .map_err(ClientError::Object)?;
                Err(err)
            }
        }
    }

    pub fn put_artifact_replace_from_reader<R: Read>(
        &self,
        path: &str,
        reader: R,
        metadata: ArtifactMetadata,
    ) -> Result<RenameReplaceResult, ClientError> {
        let prepared = self.metadata.prepare_artifact_path(path, true)?;
        let mode = metadata.mode;
        let uid = metadata.uid;
        let gid = metadata.gid;
        let (body, chunks, staged) = self.stage_artifact_reader(&prepared, reader, metadata)?;
        match self
            .metadata
            .publish_prepared_artifact(prepared, body, chunks, mode, uid, gid)
        {
            Ok(result) => Ok(result),
            Err(err) => {
                self.objects
                    .delete_staged(&staged)
                    .map_err(ClientError::Object)?;
                Err(err)
            }
        }
    }

    fn stage_artifact_body(
        &self,
        prepared: &ClientPreparedArtifact,
        bytes: &[u8],
        metadata: ArtifactMetadata,
    ) -> Result<(BodyDescriptor, Vec<ChunkManifest>, StagedObjectSet), ClientError> {
        let written = match self.objects.write_bytes(
            bytes,
            ChunkWriteOptions {
                manifest_id: metadata.manifest_id.clone(),
                mount: prepared.mount,
                inode: prepared.inode.get(),
                generation: prepared.generation,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
        ) {
            Ok(written) => written,
            Err(err) => {
                cleanup_staged_write_error(&self.objects, &err)?;
                return Err(ClientError::Object(err));
            }
        };
        self.finish_staged_artifact(prepared, metadata, written)
    }

    fn stage_artifact_reader<R: Read>(
        &self,
        prepared: &ClientPreparedArtifact,
        reader: R,
        metadata: ArtifactMetadata,
    ) -> Result<(BodyDescriptor, Vec<ChunkManifest>, StagedObjectSet), ClientError> {
        let written = match self.objects.write_reader(
            reader,
            ChunkWriteOptions {
                manifest_id: metadata.manifest_id.clone(),
                mount: prepared.mount,
                inode: prepared.inode.get(),
                generation: prepared.generation,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
        ) {
            Ok(written) => written,
            Err(err) => {
                cleanup_staged_write_error(&self.objects, &err)?;
                return Err(ClientError::Object(err));
            }
        };
        self.finish_staged_artifact(prepared, metadata, written)
    }

    fn finish_staged_artifact(
        &self,
        prepared: &ClientPreparedArtifact,
        metadata: ArtifactMetadata,
        written: ChunkedWrite,
    ) -> Result<(BodyDescriptor, Vec<ChunkManifest>, StagedObjectSet), ClientError> {
        self.object_puts
            .fetch_add(written.object_puts as u64, Ordering::Relaxed);
        self.object_put_bytes
            .fetch_add(written.object_put_bytes, Ordering::Relaxed);
        self.manifest_chunks
            .fetch_add(written.chunks.len() as u64, Ordering::Relaxed);
        self.manifest_blocks.fetch_add(
            written
                .chunks
                .iter()
                .map(|chunk| chunk.blocks.len() as u64)
                .sum::<u64>(),
            Ordering::Relaxed,
        );
        let staged = written.staged_objects()?;
        let chunks = written
            .chunks
            .iter()
            .map(|chunk| ChunkManifest {
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
            })
            .collect();
        Ok((
            BodyDescriptor {
                producer: metadata.producer,
                digest_uri: metadata.digest_uri,
                size: written.size,
                content_type: metadata.content_type,
                manifest_id: written.manifest_id,
                generation: prepared.generation,
                chunk_size: written.chunk_size,
                block_size: written.block_size,
            },
            chunks,
            staged,
        ))
    }
}

fn cleanup_staged_write_error<O: ObjectStore>(
    objects: &O,
    err: &ObjectError,
) -> Result<(), ClientError> {
    if let ObjectError::StagedWriteFailed { staged, .. } = err {
        objects.delete_staged(staged).map_err(ClientError::Object)?;
    }
    Ok(())
}

impl ReadPipelineCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            pipelines: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn take(&mut self, key: &str) -> FileReadPipeline {
        self.order.retain(|existing| existing != key);
        self.pipelines.remove(key).unwrap_or_default()
    }

    fn insert(&mut self, key: String, pipeline: FileReadPipeline) {
        self.order.retain(|existing| existing != &key);
        self.order.push_back(key.clone());
        self.pipelines.insert(key, pipeline);
        while self.pipelines.len() > self.capacity {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.pipelines.remove(&oldest);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.pipelines.len()
    }

    #[cfg(test)]
    fn contains(&self, key: &str) -> bool {
        self.pipelines.contains_key(key)
    }
}

impl MetadataClient {
    pub fn new(options: MetadataClientOptions) -> Self {
        Self {
            options,
            next_request_id: AtomicU64::new(1),
            connections: Mutex::new(HashMap::new()),
            observed_position: Mutex::new(None),
        }
    }

    pub fn connect(address: SocketAddr) -> Self {
        Self::new(MetadataClientOptions::new(address))
    }

    pub fn observed_metadata_position(&self) -> Option<ClientMetadataPosition> {
        self.observed_position
            .lock()
            .expect("metadata observed position")
            .map(wire_metadata_position)
    }

    pub fn observe_metadata_position(&self, position: ClientMetadataPosition) {
        self.record_observed_position(metadata_position_to_wire(position));
    }

    pub fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::BootstrapRoot { mode, uid, gid })? {
            MetadataRpcResult::InodeAttr { .. } => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn add_metadata_raft_learner(
        &self,
        node: u64,
        address: SocketAddr,
        blocking: bool,
    ) -> Result<ClientMetadataPosition, ClientError> {
        match self.call(MetadataRpcRequest::MetadataRaftAddLearner {
            node,
            address: address.to_string(),
            blocking,
        })? {
            MetadataRpcResult::MetadataPosition { position } => {
                Ok(wire_metadata_position(position))
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn get_attr(&self, inode: InodeId) -> Result<Option<InodeAttr>, ClientError> {
        match self.call(MetadataRpcRequest::GetAttr { inode: inode.get() })? {
            MetadataRpcResult::InodeAttr { attr } => attr
                .map(|attr| attr.into_inode_attr().map_err(protocol_error))
                .transpose(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn get_attr_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> Result<Option<InodeAttr>, ClientError> {
        match self.call(MetadataRpcRequest::GetAttrAtSnapshot {
            snapshot_id,
            inode: inode.get(),
        })? {
            MetadataRpcResult::InodeAttr { attr } => attr
                .map(|attr| attr.into_inode_attr().map_err(protocol_error))
                .transpose(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn lookup_plus(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<Option<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::LookupPlus {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry } => {
                entry.map(|entry| wire_dentry(*entry)).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn lookup_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
        name: DentryName,
    ) -> Result<Option<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::LookupPlusAtSnapshot {
            snapshot_id,
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry } => {
                entry.map(|entry| wire_dentry(*entry)).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_dir_plus_page(
        &self,
        parent: InodeId,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ClientReadDirPlusPage, ClientError> {
        match self.call(MetadataRpcRequest::ReadDirPlusPage {
            parent: parent.get(),
            after_name_hex: after.map(encode_name_cursor),
            limit,
        })? {
            MetadataRpcResult::DentriesPage {
                entries,
                next_name_hex,
            } => Ok(ClientReadDirPlusPage {
                entries: entries
                    .into_iter()
                    .map(wire_dentry)
                    .collect::<Result<Vec<_>, _>>()?,
                next_cursor: next_name_hex
                    .as_deref()
                    .map(decode_name_cursor)
                    .transpose()
                    .map_err(|err| ClientError::Protocol(err.to_string()))?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_dir_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
    ) -> Result<Vec<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::ReadDirPlusAtSnapshot {
            snapshot_id,
            parent: parent.get(),
        })? {
            MetadataRpcResult::Dentries { entries } => {
                entries.into_iter().map(wire_dentry).collect()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateDir {
            parent: parent.get(),
            name: rpc_name(&name)?,
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_file_in_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateFile {
            parent: parent.get(),
            name: rpc_name(&name)?,
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_symlink(
        &self,
        parent: InodeId,
        name: DentryName,
        target: Vec<u8>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateSymlink {
            parent: parent.get(),
            name: rpc_name(&name)?,
            target,
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_special_node(
        &self,
        parent: InodeId,
        name: DentryName,
        spec: SpecialNodeSpec,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateSpecialNode {
            parent: parent.get(),
            name: rpc_name(&name)?,
            file_type: encode_file_type(spec.file_type).to_owned(),
            mode: spec.mode,
            rdev: spec.rdev,
            uid: spec.uid,
            gid: spec.gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn update_attrs(
        &self,
        parent: InodeId,
        name: DentryName,
        changes: UpdateAttr,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::UpdateAttrs {
            parent: parent.get(),
            name: rpc_name(&name)?,
            changes: update_attr_to_wire(changes),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn update_root_attrs(&self, changes: UpdateAttr) -> Result<InodeAttr, ClientError> {
        match self.call(MetadataRpcRequest::UpdateRootAttrs {
            changes: update_attr_to_wire(changes),
        })? {
            MetadataRpcResult::InodeAttr { attr: Some(attr) } => {
                attr.into_inode_attr().map_err(protocol_error)
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn set_xattr(
        &self,
        inode: InodeId,
        name: &[u8],
        value: Vec<u8>,
        mode: XattrSetMode,
    ) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::SetXattr {
            inode: inode.get(),
            name_hex: encode_xattr_name(name),
            value,
            mode: xattr_set_mode_to_wire(mode),
        })? {
            MetadataRpcResult::Unit => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn get_xattr(&self, inode: InodeId, name: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        match self.call(MetadataRpcRequest::GetXattr {
            inode: inode.get(),
            name_hex: encode_xattr_name(name),
        })? {
            MetadataRpcResult::XattrValue { value } => Ok(value),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn list_xattr(&self, inode: InodeId) -> Result<Vec<Vec<u8>>, ClientError> {
        match self.call(MetadataRpcRequest::ListXattr { inode: inode.get() })? {
            MetadataRpcResult::XattrNames { names_hex } => names_hex
                .iter()
                .map(|name| decode_xattr_name(name).map_err(protocol_error))
                .collect(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove_xattr(&self, inode: InodeId, name: &[u8]) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::RemoveXattr {
            inode: inode.get(),
            name_hex: encode_xattr_name(name),
        })? {
            MetadataRpcResult::Unit => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn get_advisory_lock(
        &self,
        request: AdvisoryLockRequest,
    ) -> Result<Option<AdvisoryLock>, ClientError> {
        match self.call(MetadataRpcRequest::GetAdvisoryLock {
            inode: request.inode.get(),
            owner: request.owner,
            start: request.start,
            end: request.end,
            kind: encode_advisory_lock_kind(request.kind).to_owned(),
            pid: request.pid,
        })? {
            MetadataRpcResult::AdvisoryLock { lock } => lock.map(wire_advisory_lock).transpose(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn set_advisory_lock(&self, request: AdvisoryLockRequest) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::SetAdvisoryLock {
            inode: request.inode.get(),
            owner: request.owner,
            start: request.start,
            end: request.end,
            kind: encode_advisory_lock_kind(request.kind).to_owned(),
            pid: request.pid,
            wait: request.wait,
        })? {
            MetadataRpcResult::Unit => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn mkdir(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateDirPath {
            path: path.to_owned(),
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn mkdirs(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut entries = Vec::with_capacity(paths.len());
        for chunk in paths.chunks(MAX_BATCH_RPC_REQUESTS) {
            let requests = chunk
                .iter()
                .map(|path| MetadataRpcRequest::CreateDirPath {
                    path: path.clone(),
                    mode,
                    uid,
                    gid,
                })
                .collect();
            let results: Vec<Result<MetadataRpcResult, ClientError>> =
                match self.call(MetadataRpcRequest::Batch { requests })? {
                    MetadataRpcResult::Batch { results } => {
                        results.into_iter().map(envelope_result).collect()
                    }
                    other => return Err(unexpected_result(other)),
                };
            for result in results {
                entries.push(result.and_then(dentry_result));
            }
        }
        Ok(entries)
    }

    pub fn create_file(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateFilePath {
            path: path.to_owned(),
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_files(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut entries = Vec::with_capacity(paths.len());
        for chunk in paths.chunks(MAX_BATCH_RPC_REQUESTS) {
            let request = create_files_request(chunk, mode, uid, gid)?;
            let results: Vec<Result<MetadataRpcResult, ClientError>> = match self.call(request)? {
                MetadataRpcResult::Batch { results } => {
                    results.into_iter().map(envelope_result).collect()
                }
                other => return Err(unexpected_result(other)),
            };
            for result in results {
                entries.push(result.and_then(dentry_result));
            }
        }
        Ok(entries)
    }

    pub fn lookup(&self, path: &str) -> Result<Option<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::LookupPath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry } => {
                entry.map(|entry| wire_dentry(*entry)).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn stat_path(&self, path: &str) -> Result<Option<PathMetadata>, ClientError> {
        match self.call(MetadataRpcRequest::StatPath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::PathMetadata { metadata } => {
                metadata.map(wire_path_metadata).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn list(&self, path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        let mut entries = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.list_page(path, cursor.as_ref(), DEFAULT_LIST_PAGE_SIZE)?;
            let page_empty = page.entries.is_empty();
            entries.extend(page.entries);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if page_empty || cursor.as_ref() == Some(&next_cursor) {
                return Err(ClientError::Protocol(
                    "metadata list page cursor did not advance".to_owned(),
                ));
            }
            cursor = Some(next_cursor);
        }
        Ok(entries)
    }

    pub fn list_indexed(&self, path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        let mut entries = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.list_indexed_page(path, cursor.as_ref(), DEFAULT_LIST_PAGE_SIZE)?;
            let page_empty = page.entries.is_empty();
            entries.extend(page.entries);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if page_empty || cursor.as_ref() == Some(&next_cursor) {
                return Err(ClientError::Protocol(
                    "indexed metadata list page cursor did not advance".to_owned(),
                ));
            }
            cursor = Some(next_cursor);
        }
        Ok(entries)
    }

    pub fn list_page(
        &self,
        path: &str,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ClientReadDirPlusPage, ClientError> {
        match self.call(MetadataRpcRequest::ReadDirPlusPathPage {
            path: path.to_owned(),
            after_name_hex: after.map(encode_name_cursor),
            limit,
        })? {
            MetadataRpcResult::DentriesPage {
                entries,
                next_name_hex,
            } => Ok(ClientReadDirPlusPage {
                entries: entries
                    .into_iter()
                    .map(wire_dentry)
                    .collect::<Result<Vec<_>, _>>()?,
                next_cursor: next_name_hex
                    .as_deref()
                    .map(decode_name_cursor)
                    .transpose()
                    .map_err(|err| ClientError::Protocol(err.to_string()))?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn list_indexed_page(
        &self,
        path: &str,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ClientReadDirPlusPage, ClientError> {
        match self.call(MetadataRpcRequest::ReadIndexedPathPage {
            path: path.to_owned(),
            after_name_hex: after.map(encode_name_cursor),
            limit,
        })? {
            MetadataRpcResult::DentriesPage {
                entries,
                next_name_hex,
            } => Ok(ClientReadDirPlusPage {
                entries: entries
                    .into_iter()
                    .map(wire_dentry)
                    .collect::<Result<Vec<_>, _>>()?,
                next_cursor: next_name_hex
                    .as_deref()
                    .map(decode_name_cursor)
                    .transpose()
                    .map_err(|err| ClientError::Protocol(err.to_string()))?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn stat_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Option<PathMetadata>, ClientError> {
        match self.call(MetadataRpcRequest::StatPathAtSnapshot {
            snapshot_id,
            path: path.to_owned(),
        })? {
            MetadataRpcResult::PathMetadata { metadata } => {
                metadata.map(wire_path_metadata).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn list_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Vec<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::ReadDirPlusPathAtSnapshot {
            snapshot_id,
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentries { entries } => {
                entries.into_iter().map(wire_dentry).collect()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove(&self, path: &str) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RemoveFilePath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove_file(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RemoveFile {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove_many(
        &self,
        paths: &[String],
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut entries = Vec::with_capacity(paths.len());
        for chunk in paths.chunks(MAX_BATCH_RPC_REQUESTS) {
            let requests = chunk
                .iter()
                .map(|path| MetadataRpcRequest::RemoveFilePath { path: path.clone() })
                .collect();
            let results: Vec<Result<MetadataRpcResult, ClientError>> =
                match self.call(MetadataRpcRequest::Batch { requests })? {
                    MetadataRpcResult::Batch { results } => {
                        results.into_iter().map(envelope_result).collect()
                    }
                    other => return Err(unexpected_result(other)),
                };
            for result in results {
                entries.push(result.and_then(dentry_result));
            }
        }
        Ok(entries)
    }

    pub fn rmdir(&self, path: &str) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RemoveEmptyDirPath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove_empty_dir(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RemoveEmptyDir {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn link(
        &self,
        inode: InodeId,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::Link {
            inode: inode.get(),
            new_parent: new_parent.get(),
            new_name: rpc_name(&new_name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rmdir_many(
        &self,
        paths: &[String],
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut entries = Vec::with_capacity(paths.len());
        for chunk in paths.chunks(MAX_BATCH_RPC_REQUESTS) {
            let requests = chunk
                .iter()
                .map(|path| MetadataRpcRequest::RemoveEmptyDirPath { path: path.clone() })
                .collect();
            let results: Vec<Result<MetadataRpcResult, ClientError>> =
                match self.call(MetadataRpcRequest::Batch { requests })? {
                    MetadataRpcResult::Batch { results } => {
                        results.into_iter().map(envelope_result).collect()
                    }
                    other => return Err(unexpected_result(other)),
                };
            for result in results {
                entries.push(result.and_then(dentry_result));
            }
        }
        Ok(entries)
    }

    pub fn rename(&self, source: &str, destination: &str) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RenamePath {
            source: source.to_owned(),
            destination: destination.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rename_in_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::Rename {
            parent: parent.get(),
            name: rpc_name(&name)?,
            new_parent: new_parent.get(),
            new_name: rpc_name(&new_name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rename_replace(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<RenameReplaceResult, ClientError> {
        match self.call(MetadataRpcRequest::RenameReplacePath {
            source: source.to_owned(),
            destination: destination.to_owned(),
        })? {
            MetadataRpcResult::RenameReplace { entry, replaced } => Ok(RenameReplaceResult {
                entry: wire_dentry(*entry)?,
                replaced: replaced.map(|entry| wire_dentry(*entry)).transpose()?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rename_replace_in_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<RenameReplaceResult, ClientError> {
        match self.call(MetadataRpcRequest::RenameReplace {
            parent: parent.get(),
            name: rpc_name(&name)?,
            new_parent: new_parent.get(),
            new_name: rpc_name(&new_name)?,
        })? {
            MetadataRpcResult::RenameReplace { entry, replaced } => Ok(RenameReplaceResult {
                entry: wire_dentry(*entry)?,
                replaced: replaced.map(|entry| wire_dentry(*entry)).transpose()?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn snapshot(&self, path: &str) -> Result<SnapshotPin, ClientError> {
        match self.call(MetadataRpcRequest::SnapshotSubtreePath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Snapshot { snapshot } => wire_snapshot(snapshot),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn snapshot_subtree(&self, root: InodeId) -> Result<SnapshotPin, ClientError> {
        match self.call(MetadataRpcRequest::SnapshotSubtree { root: root.get() })? {
            MetadataRpcResult::Snapshot { snapshot } => wire_snapshot(snapshot),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn snapshot_pin(&self, snapshot_id: u64) -> Result<Option<SnapshotPin>, ClientError> {
        match self.call(MetadataRpcRequest::SnapshotPin { snapshot_id })? {
            MetadataRpcResult::SnapshotPin { snapshot } => snapshot.map(wire_snapshot).transpose(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn retire_snapshot(&self, snapshot_id: u64) -> Result<bool, ClientError> {
        match self.call(MetadataRpcRequest::RetireSnapshot { snapshot_id })? {
            MetadataRpcResult::RetiredSnapshot { retired } => Ok(retired),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_body_plan(
        &self,
        inode: InodeId,
        generation: u64,
        offset: u64,
        len: usize,
    ) -> Result<ClientBodyReadPlan, ClientError> {
        let len = u64::try_from(len)
            .map_err(|_| ClientError::Protocol("body read length exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::ReadBodyPlan {
            inode: inode.get(),
            generation,
            offset,
            len,
        })? {
            MetadataRpcResult::BodyReadPlan { plan } => wire_body_read_plan(plan),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_path_plan(
        &self,
        path: &str,
        offset: u64,
        len: usize,
        expected_generation: Option<u64>,
    ) -> Result<(PathMetadata, ClientBodyReadPlan), ClientError> {
        let len = u64::try_from(len)
            .map_err(|_| ClientError::Protocol("path read length exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::ReadPathPlan {
            path: path.to_owned(),
            offset,
            len,
            expected_generation,
        })? {
            MetadataRpcResult::PathReadPlan { metadata, plan } => {
                Ok((wire_path_metadata(metadata)?, wire_body_read_plan(plan)?))
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_artifact_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Vec<u8>, ClientError> {
        match self.call(MetadataRpcRequest::ReadArtifactPathAtSnapshot {
            snapshot_id,
            path: path.to_owned(),
        })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_file_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        let len = u64::try_from(len)
            .map_err(|_| ClientError::Protocol("snapshot read length exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::ReadFilePathAtSnapshot {
            snapshot_id,
            path: path.to_owned(),
            offset,
            len,
        })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_file_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        let len = u64::try_from(len)
            .map_err(|_| ClientError::Protocol("snapshot read length exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::ReadFileAtSnapshot {
            snapshot_id,
            inode: inode.get(),
            offset,
            len,
        })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_symlink(&self, inode: InodeId) -> Result<Vec<u8>, ClientError> {
        match self.call(MetadataRpcRequest::ReadSymlink { inode: inode.get() })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_symlink_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> Result<Vec<u8>, ClientError> {
        match self.call(MetadataRpcRequest::ReadSymlinkAtSnapshot {
            snapshot_id,
            inode: inode.get(),
        })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn prepare_artifact(
        &self,
        parent: InodeId,
        name: DentryName,
        replace: bool,
    ) -> Result<ClientPreparedArtifact, ClientError> {
        match self.call(MetadataRpcRequest::PrepareArtifact {
            parent: parent.get(),
            name: rpc_name(&name)?,
            replace,
        })? {
            MetadataRpcResult::PreparedArtifact { prepared } => wire_prepared_artifact(prepared),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn prepare_artifact_path(
        &self,
        path: &str,
        replace: bool,
    ) -> Result<ClientPreparedArtifact, ClientError> {
        match self.call(MetadataRpcRequest::PrepareArtifactPath {
            path: path.to_owned(),
            replace,
        })? {
            MetadataRpcResult::PreparedArtifact { prepared } => wire_prepared_artifact(prepared),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn publish_prepared_artifact(
        &self,
        prepared: ClientPreparedArtifact,
        body: BodyDescriptor,
        chunks: Vec<ChunkManifest>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<RenameReplaceResult, ClientError> {
        match self.call(MetadataRpcRequest::PublishPreparedArtifact {
            prepared: prepared_artifact_to_wire(&prepared)?,
            body: Box::new(body_to_wire(&body)),
            chunks: chunks.iter().map(chunk_to_wire).collect(),
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::RenameReplace { entry, replaced } => Ok(RenameReplaceResult {
                entry: wire_dentry(*entry)?,
                replaced: replaced.map(|entry| wire_dentry(*entry)).transpose()?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn publish_prepared_artifact_staged_session(
        &self,
        prepared: ClientPreparedArtifact,
        request: PublishArtifactStagedSession,
    ) -> Result<RenameReplaceResult, ClientError> {
        match self.call(MetadataRpcRequest::PublishPreparedArtifactStagedSession {
            prepared: prepared_artifact_to_wire(&prepared)?,
            producer: request.producer,
            digest_uri: request.digest_uri,
            content_type: request.content_type,
            manifest_id: request.manifest_id,
            size: request.size,
            chunks: request.chunks.iter().map(stored_chunk_to_wire).collect(),
            staged: staged_object_set_to_wire(&request.staged),
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
        })? {
            MetadataRpcResult::RenameReplace { entry, replaced } => Ok(RenameReplaceResult {
                entry: wire_dentry(*entry)?,
                replaced: replaced.map(|entry| wire_dentry(*entry)).transpose()?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    fn call(&self, request: MetadataRpcRequest) -> Result<MetadataRpcResult, ClientError> {
        let mut endpoints = self.request_endpoints(&request);
        if endpoints.is_empty() {
            return Err(ClientError::Protocol(
                "metadata request had no target endpoint".to_owned(),
            ));
        }
        let started = Instant::now();
        let mut fallback_error = None;

        loop {
            let mut saw_stale_read = false;
            let mut saw_forward_to_leader = false;
            let mut index = 0;
            while index < endpoints.len() {
                let address = endpoints[index];
                index += 1;
                let request = self.request_with_observed_position(request.clone());
                match self.call_at(address, &request) {
                    Ok(result) => return Ok(result),
                    Err(err @ ClientError::ReadNotFresh { .. }) => {
                        saw_stale_read = true;
                        fallback_error = Some(err);
                    }
                    Err(
                        err @ ClientError::ForwardToLeader {
                            address: Some(leader),
                            ..
                        },
                    ) => {
                        saw_forward_to_leader = true;
                        fallback_error = Some(err);
                        if !endpoints.contains(&leader) {
                            endpoints.insert(index, leader);
                        }
                    }
                    Err(err @ ClientError::ForwardToLeader { address: None, .. }) => {
                        saw_forward_to_leader = true;
                        fallback_error = Some(err);
                    }
                    Err(err @ ClientError::Io(_)) => {
                        self.drop_connection(address);
                        fallback_error = Some(err);
                    }
                    Err(err) => return Err(err),
                }
            }

            if !saw_stale_read && !saw_forward_to_leader {
                break;
            }
            let Some(delay) = read_not_fresh_retry_delay(started, self.options.timeout) else {
                break;
            };
            thread::sleep(delay);
        }

        Err(fallback_error.unwrap_or_else(|| {
            ClientError::Protocol("metadata request had no target endpoint".to_owned())
        }))
    }

    fn call_at(
        &self,
        address: SocketAddr,
        request: &MetadataRpcRequest,
    ) -> Result<MetadataRpcResult, ClientError> {
        let body = encode_request(request).map_err(|err| ClientError::Protocol(err.to_string()))?;
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let connection = self.connection(address)?;
        let body = connection.call(request_id, &body, self.options.timeout)?;
        let envelope =
            decode_envelope(&body).map_err(|err| ClientError::Protocol(err.to_string()))?;
        if envelope.ok {
            if let Some(position) = envelope.metadata_position {
                self.record_observed_position(position);
            }
        }
        envelope_result(envelope)
    }

    fn request_endpoints(&self, request: &MetadataRpcRequest) -> Vec<SocketAddr> {
        if !request_uses_read_endpoints(request) {
            return vec![self.options.address];
        }
        let mut endpoints = Vec::with_capacity(self.options.read_endpoints.len() + 1);
        for endpoint in &self.options.read_endpoints {
            if !endpoints.contains(endpoint) {
                endpoints.push(*endpoint);
            }
        }
        if !endpoints.contains(&self.options.address) {
            endpoints.push(self.options.address);
        }
        endpoints
    }

    fn request_with_observed_position(&self, request: MetadataRpcRequest) -> MetadataRpcRequest {
        if !request_requires_observed_position(&request) {
            return request;
        }
        let Some(position) = *self
            .observed_position
            .lock()
            .expect("metadata observed position")
        else {
            return request;
        };
        MetadataRpcRequest::RequireApplied {
            position,
            request: Box::new(request),
        }
    }

    fn record_observed_position(&self, position: WireMetadataPosition) {
        let mut observed = self
            .observed_position
            .lock()
            .expect("metadata observed position");
        if observed.map(|existing| position > existing).unwrap_or(true) {
            *observed = Some(position);
        }
    }

    fn connection(&self, address: SocketAddr) -> Result<Arc<PipelinedConnection>, ClientError> {
        let mut guard = self.connections.lock().expect("metadata rpc connections");
        if let Some(connection) = guard.get(&address) {
            return Ok(Arc::clone(connection));
        }
        let connection = Arc::new(PipelinedConnection::connect(address)?);
        guard.insert(address, Arc::clone(&connection));
        Ok(connection)
    }

    fn drop_connection(&self, address: SocketAddr) {
        self.connections
            .lock()
            .expect("metadata rpc connections")
            .remove(&address);
    }
}

fn request_uses_read_endpoints(request: &MetadataRpcRequest) -> bool {
    match request {
        MetadataRpcRequest::Batch { requests } => {
            !requests.is_empty() && requests.iter().all(request_uses_read_endpoints)
        }
        MetadataRpcRequest::RequireApplied { request, .. } => request_uses_read_endpoints(request),
        MetadataRpcRequest::GetAttr { .. }
        | MetadataRpcRequest::GetAttrAtSnapshot { .. }
        | MetadataRpcRequest::LookupPlus { .. }
        | MetadataRpcRequest::LookupPlusAtSnapshot { .. }
        | MetadataRpcRequest::LookupPath { .. }
        | MetadataRpcRequest::StatPath { .. }
        | MetadataRpcRequest::ReadDirPlus { .. }
        | MetadataRpcRequest::ReadDirPlusPage { .. }
        | MetadataRpcRequest::ReadDirPlusAtSnapshot { .. }
        | MetadataRpcRequest::ReadDirPlusPath { .. }
        | MetadataRpcRequest::ReadDirPlusPathPage { .. }
        | MetadataRpcRequest::ReadIndexedPathPage { .. }
        | MetadataRpcRequest::ReadFileAtSnapshot { .. }
        | MetadataRpcRequest::ReadFilePathAtSnapshot { .. }
        | MetadataRpcRequest::ReadSymlink { .. }
        | MetadataRpcRequest::ReadSymlinkAtSnapshot { .. }
        | MetadataRpcRequest::ReadBodyPlan { .. }
        | MetadataRpcRequest::ReadPathPlan { .. }
        | MetadataRpcRequest::SnapshotPin { .. } => true,
        _ => false,
    }
}

fn create_files_request(
    paths: &[String],
    mode: u32,
    uid: u32,
    gid: u32,
) -> Result<MetadataRpcRequest, ClientError> {
    let mut parent_path = None;
    let mut names = Vec::with_capacity(paths.len());
    for path in paths {
        let (parent, name) = rpc_parent_and_name(path)?;
        if parent_path
            .as_deref()
            .is_some_and(|existing| existing != parent)
        {
            let requests = paths
                .iter()
                .map(|path| MetadataRpcRequest::CreateFilePath {
                    path: path.clone(),
                    mode,
                    uid,
                    gid,
                })
                .collect();
            return Ok(MetadataRpcRequest::Batch { requests });
        }
        parent_path = Some(parent);
        names.push(name);
    }
    Ok(MetadataRpcRequest::CreateFilesInDirPath {
        parent_path: parent_path.unwrap_or_else(|| "/".to_owned()),
        names,
        mode,
        uid,
        gid,
    })
}

fn request_requires_observed_position(request: &MetadataRpcRequest) -> bool {
    match request {
        MetadataRpcRequest::Batch { requests } => {
            requests.iter().any(request_requires_observed_position)
        }
        MetadataRpcRequest::RequireApplied { .. }
        | MetadataRpcRequest::BootstrapRoot { .. }
        | MetadataRpcRequest::MetadataRaftAddLearner { .. }
        | MetadataRpcRequest::MetadataRaftVote { .. }
        | MetadataRpcRequest::MetadataRaftAppendEntries { .. }
        | MetadataRpcRequest::MetadataRaftInstallSnapshot { .. } => false,
        _ => true,
    }
}

fn read_not_fresh_retry_delay(started: Instant, timeout: Duration) -> Option<Duration> {
    let elapsed = started.elapsed();
    if elapsed >= timeout {
        return None;
    }
    Some((timeout - elapsed).min(READ_NOT_FRESH_RETRY_INTERVAL))
}

fn rpc_parent_and_name(path: &str) -> Result<(String, String), ClientError> {
    let mut components = parse_absolute_path(path)?;
    let name = components.pop().ok_or(ClientError::RootHasNoParent)?;
    let name = rpc_name(&name)?;
    let mut parent = String::from("/");
    for (index, component) in components.iter().enumerate() {
        if index > 0 {
            parent.push('/');
        }
        parent.push_str(&rpc_name(component)?);
    }
    Ok((parent, name))
}

impl PipelinedConnection {
    fn connect(address: SocketAddr) -> Result<Self, ClientError> {
        let mut stream =
            TcpStream::connect(address).map_err(|err| ClientError::Io(err.to_string()))?;
        stream
            .set_nodelay(true)
            .map_err(|err| ClientError::Io(err.to_string()))?;
        stream
            .write_all(FRAMED_RPC_MAGIC)
            .map_err(|err| ClientError::Io(err.to_string()))?;
        let reader = stream
            .try_clone()
            .map_err(|err| ClientError::Io(err.to_string()))?;
        let connection = Self {
            writer: Mutex::new(stream),
            pending: Arc::new(Mutex::new(HashMap::new())),
        };
        connection.spawn_reader(reader);
        Ok(connection)
    }

    fn spawn_reader(&self, mut reader: TcpStream) {
        let pending = Arc::clone(&self.pending);
        thread::spawn(move || loop {
            match read_frame(&mut reader) {
                Ok((request_id, _flags, payload)) => {
                    let waiter = pending
                        .lock()
                        .expect("metadata rpc pending")
                        .remove(&request_id);
                    if let Some(waiter) = waiter {
                        let _ = waiter.send(PendingFrame::Payload(payload));
                    }
                }
                Err(err) => {
                    let mut pending = pending.lock().expect("metadata rpc pending");
                    let waiters = pending
                        .drain()
                        .map(|(_, waiter)| waiter)
                        .collect::<Vec<_>>();
                    drop(pending);
                    for waiter in waiters {
                        let _ = waiter.send(PendingFrame::Failed(err.to_string()));
                    }
                    break;
                }
            }
        });
    }

    fn call(
        &self,
        request_id: u64,
        body: &[u8],
        timeout: Duration,
    ) -> Result<Vec<u8>, ClientError> {
        let (tx, rx) = mpsc::channel();
        self.pending
            .lock()
            .expect("metadata rpc pending")
            .insert(request_id, tx);
        let write_result = {
            let mut writer = self.writer.lock().expect("metadata rpc writer");
            write_frame(&mut writer, request_id, 0, body)
        };
        if let Err(err) = write_result {
            self.pending
                .lock()
                .expect("metadata rpc pending")
                .remove(&request_id);
            return Err(err);
        }
        match rx.recv_timeout(timeout) {
            Ok(PendingFrame::Payload(payload)) => Ok(payload),
            Ok(PendingFrame::Failed(err)) => Err(ClientError::Io(err)),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.pending
                    .lock()
                    .expect("metadata rpc pending")
                    .remove(&request_id);
                Err(ClientError::Io(
                    "metadata rpc response timed out".to_owned(),
                ))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(ClientError::Io("metadata rpc connection closed".to_owned()))
            }
        }
    }
}

fn write_frame(
    stream: &mut TcpStream,
    request_id: u64,
    flags: u32,
    body: &[u8],
) -> Result<(), ClientError> {
    let len = u32::try_from(body.len())
        .map_err(|_| ClientError::Protocol("metadata rpc request exceeds u32".to_owned()))?;
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    header[0..8].copy_from_slice(&request_id.to_be_bytes());
    header[8..12].copy_from_slice(&flags.to_be_bytes());
    header[12..16].copy_from_slice(&len.to_be_bytes());
    stream
        .write_all(&header)
        .and_then(|_| stream.write_all(body))
        .map_err(|err| ClientError::Io(err.to_string()))
}

fn read_frame(stream: &mut TcpStream) -> Result<(u64, u32, Vec<u8>), ClientError> {
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    stream.read_exact(&mut header).map_err(rpc_read_error)?;
    let request_id = u64::from_be_bytes(header[0..8].try_into().expect("request id header"));
    let flags = u32::from_be_bytes(header[8..12].try_into().expect("flags header"));
    let len = u32::from_be_bytes(header[12..16].try_into().expect("length header")) as usize;
    if len > MAX_RPC_RESPONSE_BYTES {
        return Err(ClientError::Protocol(
            "metadata rpc response exceeds size limit".to_owned(),
        ));
    }
    let mut body = vec![0_u8; len];
    stream
        .read_exact(&mut body)
        .map_err(|err| ClientError::Io(err.to_string()))?;
    Ok((request_id, flags, body))
}

fn rpc_read_error(err: io::Error) -> ClientError {
    ClientError::Io(err.to_string())
}

fn rpc_name(name: &DentryName) -> Result<String, ClientError> {
    String::from_utf8(name.as_bytes().to_vec())
        .map_err(|_| ClientError::InvalidName("metadata rpc requires utf-8 names".to_owned()))
}

fn update_attr_to_wire(changes: UpdateAttr) -> WireUpdateAttr {
    WireUpdateAttr {
        mode: changes.mode,
        uid: changes.uid,
        gid: changes.gid,
        size: changes.size,
        mtime_ms: changes.mtime_ms,
        ctime_ms: changes.ctime_ms,
    }
}

fn xattr_set_mode_to_wire(mode: XattrSetMode) -> WireXattrSetMode {
    match mode {
        XattrSetMode::Any => WireXattrSetMode::Any,
        XattrSetMode::Create => WireXattrSetMode::Create,
        XattrSetMode::Replace => WireXattrSetMode::Replace,
    }
}

fn wire_dentry(entry: WireDentryWithAttr) -> Result<DentryWithAttr, ClientError> {
    Ok(DentryWithAttr {
        dentry: entry.dentry.into_dentry_record().map_err(protocol_error)?,
        attr: entry.attr.into_inode_attr().map_err(protocol_error)?,
        body: entry.body.map(|body| body.into_body_descriptor()),
    })
}

fn wire_path_metadata(metadata: WirePathMetadata) -> Result<PathMetadata, ClientError> {
    metadata.into_path_metadata().map_err(protocol_error)
}

fn wire_prepared_artifact(
    prepared: WirePreparedArtifact,
) -> Result<ClientPreparedArtifact, ClientError> {
    Ok(ClientPreparedArtifact {
        mount: prepared.mount,
        parent: inode_id(prepared.parent)?,
        name: DentryName::new(prepared.name.into_bytes())
            .map_err(|err| ClientError::InvalidName(err.to_string()))?,
        path: prepared.path,
        inode: inode_id(prepared.inode)?,
        generation: prepared.generation,
        mtime_ms: prepared.mtime_ms,
        ctime_ms: prepared.ctime_ms,
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
    })
}

fn prepared_artifact_to_wire(
    prepared: &ClientPreparedArtifact,
) -> Result<WirePreparedArtifact, ClientError> {
    Ok(WirePreparedArtifact {
        mount: prepared.mount,
        parent: prepared.parent.get(),
        name: rpc_name(&prepared.name)?,
        path: prepared.path.clone(),
        inode: prepared.inode.get(),
        generation: prepared.generation,
        mtime_ms: prepared.mtime_ms,
        ctime_ms: prepared.ctime_ms,
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
    })
}

fn body_to_wire(body: &BodyDescriptor) -> WireBodyDescriptor {
    WireBodyDescriptor::from_body_descriptor(body)
}

fn chunk_to_wire(chunk: &ChunkManifest) -> WireChunkManifest {
    WireChunkManifest::from_chunk_manifest(chunk)
}

fn stored_chunk_to_wire(chunk: &StoredChunk) -> WireChunkManifest {
    WireChunkManifest {
        chunk_index: chunk.chunk_index,
        logical_offset: chunk.logical_offset,
        len: chunk.len,
        slices: vec![WireSliceManifest {
            slice_id: 1,
            logical_offset: chunk.logical_offset,
            len: chunk.len,
            blocks: chunk
                .blocks
                .iter()
                .map(|block| nokv_protocol::WireBlockDescriptor {
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

fn staged_object_set_to_wire(staged: &StagedObjectSet) -> WireStagedObjectSet {
    WireStagedObjectSet {
        objects: staged
            .objects()
            .iter()
            .map(|object| WireStagedObject {
                key: object.key.as_str().to_owned(),
                size: object.size,
            })
            .collect(),
    }
}

fn wire_body_read_plan(plan: WireBodyReadPlan) -> Result<ClientBodyReadPlan, ClientError> {
    Ok(ClientBodyReadPlan {
        output_len: usize::try_from(plan.output_len).map_err(|_| {
            ClientError::Protocol("body read plan output length exceeds platform limit".to_owned())
        })?,
        blocks: plan
            .blocks
            .into_iter()
            .map(wire_object_read_block)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn wire_object_read_block(block: WireObjectReadBlock) -> Result<ObjectReadBlock, ClientError> {
    Ok(ObjectReadBlock {
        object_key: block.object_key,
        object_offset: block.object_offset,
        len: usize::try_from(block.len).map_err(|_| {
            ClientError::Protocol("body read block length exceeds platform limit".to_owned())
        })?,
        output_offset: usize::try_from(block.output_offset).map_err(|_| {
            ClientError::Protocol("body read block offset exceeds platform limit".to_owned())
        })?,
    })
}

fn wire_snapshot(snapshot: nokv_protocol::WireSnapshotPin) -> Result<SnapshotPin, ClientError> {
    snapshot.into_snapshot_pin().map_err(protocol_error)
}

fn wire_metadata_position(position: WireMetadataPosition) -> ClientMetadataPosition {
    ClientMetadataPosition {
        term: position.term,
        index: position.index,
    }
}

fn wire_advisory_lock(lock: WireAdvisoryLock) -> Result<AdvisoryLock, ClientError> {
    lock.into_advisory_lock().map_err(protocol_error)
}

fn metadata_position_to_wire(position: ClientMetadataPosition) -> WireMetadataPosition {
    WireMetadataPosition {
        term: position.term,
        index: position.index,
    }
}

fn file_len(size: u64) -> Result<usize, ClientError> {
    usize::try_from(size)
        .map_err(|_| ClientError::Protocol("file size exceeds platform limit".to_owned()))
}

fn read_pipeline_key(path: &str, generation: u64) -> String {
    format!("{path}#{generation}")
}

fn bounded_read_len(available: u64, requested: usize) -> Result<usize, ClientError> {
    let requested_u64 = u64::try_from(requested).unwrap_or(u64::MAX);
    if requested_u64 <= available {
        return Ok(requested);
    }
    file_len(available)
}

fn inode_id(raw: u64) -> Result<InodeId, ClientError> {
    InodeId::new(raw).map_err(|err| ClientError::Protocol(err.to_string()))
}

fn protocol_error(err: MetadataProtocolError) -> ClientError {
    ClientError::Protocol(err.to_string())
}

fn dentry_result(result: MetadataRpcResult) -> Result<DentryWithAttr, ClientError> {
    match result {
        MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
        other => Err(unexpected_result(other)),
    }
}

fn envelope_result(envelope: MetadataRpcEnvelope) -> Result<MetadataRpcResult, ClientError> {
    if !envelope.ok {
        let message = envelope
            .error
            .unwrap_or_else(|| "unknown metadata service error".to_owned());
        let Some(error) = envelope.error_kind else {
            return Err(ClientError::Protocol(format!(
                "metadata rpc error is missing typed error_kind: {message}"
            )));
        };
        return Err(client_error_from_wire_error(error));
    }
    envelope
        .result
        .ok_or_else(|| ClientError::Protocol("metadata rpc response missing result".to_owned()))
}

fn client_error_from_wire_error(error: WireMetadataError) -> ClientError {
    match error {
        WireMetadataError::NotFound => ClientError::Metadata(nokv_meta::MetadError::NotFound),
        WireMetadataError::NotFile => ClientError::Metadata(nokv_meta::MetadError::NotFile),
        WireMetadataError::NotDirectory => {
            ClientError::Metadata(nokv_meta::MetadError::NotDirectory)
        }
        WireMetadataError::MissingBodyDescriptor => {
            ClientError::Metadata(nokv_meta::MetadError::MissingBodyDescriptor)
        }
        WireMetadataError::PredicateFailed => ClientError::Metadata(
            nokv_meta::MetadError::Metadata(nokv_meta::MetadataError::PredicateFailed),
        ),
        WireMetadataError::ReadNotFresh { required, applied } => ClientError::ReadNotFresh {
            required_term: required.term,
            required_index: required.index,
            applied_term: applied.map(|position| position.term),
            applied_index: applied.map(|position| position.index),
        },
        WireMetadataError::ForwardToLeader { leader_id, address } => {
            let address = match address {
                Some(address) => match address.parse::<SocketAddr>() {
                    Ok(address) => Some(address),
                    Err(err) => {
                        return ClientError::Protocol(format!(
                            "metadata leader address {address:?} is invalid: {err}"
                        ));
                    }
                },
                None => None,
            };
            ClientError::ForwardToLeader { leader_id, address }
        }
        WireMetadataError::StaleBodyGeneration { expected, current } => {
            ClientError::Metadata(nokv_meta::MetadError::StaleBodyGeneration { expected, current })
        }
        WireMetadataError::LockConflict { lock } => match wire_advisory_lock(lock) {
            Ok(lock) => ClientError::LockConflict(lock),
            Err(err) => err,
        },
        WireMetadataError::InvalidPath { message } => {
            ClientError::Metadata(nokv_meta::MetadError::InvalidPath(message))
        }
        WireMetadataError::Metadata { message } => ClientError::Metadata(
            nokv_meta::MetadError::Metadata(nokv_meta::MetadataError::Backend(message)),
        ),
        WireMetadataError::Object { message } => {
            ClientError::Object(nokv_object::ObjectError::Backend(message))
        }
        WireMetadataError::Io { message } => ClientError::Io(message),
        WireMetadataError::Protocol { message } => ClientError::Protocol(message),
    }
}

fn unexpected_result(result: MetadataRpcResult) -> ClientError {
    ClientError::Protocol(format!("unexpected metadata rpc result {result:?}"))
}

#[cfg(test)]
mod tests;
