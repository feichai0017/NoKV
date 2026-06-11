use std::io::Read;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nokv_meta::{
    DentryWithAttr, NamespaceAggregateRequest, NamespaceAggregateResult, NamespaceCard,
    NamespaceFindRequest, NamespaceFindResult, NamespaceGrepRequest, NamespaceGrepResult,
    NamespaceListOptions, NamespaceListPage, NamespaceReadOptions, NamespaceReadPage,
    ObjectTransferStats, RenameReplaceResult,
};
use nokv_object::{
    BlockReadOptions, ChunkStore, ChunkWriteOptions, ChunkedWrite, DataFabricReadStats,
    LayoutReadExecutor, ObjectBlockCache, ObjectError, ObjectPrefetchOptions,
    ObjectPrefetchRequest, ObjectPrefetcher, ObjectReadPlan, ObjectReadPlanCache,
    ObjectReadPlanKey, ObjectStore, StagedObjectSet, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
};
use nokv_types::{BodyDescriptor, ChunkManifest, FileType, InodeId};

use crate::read_cache::ReadPipelineCache;
use crate::service::{ClientPreparedArtifact, MetadataClient};
use crate::{ArtifactMetadata, ClientError, NamespaceRead};

const MAX_READ_PIPELINES: usize = 1024;
const MAX_READ_PLAN_CACHE_ENTRIES: usize = 4096;

pub struct NoKvFsClient<O> {
    metadata: MetadataClient,
    objects: Arc<O>,
    block_cache: ObjectBlockCache,
    prefetcher: ObjectPrefetcher<Arc<O>>,
    read_pipelines: Mutex<ReadPipelineCache>,
    read_plans: Mutex<ObjectReadPlanCache>,
    block_cache_enabled: bool,
    object_puts: AtomicU64,
    object_put_bytes: AtomicU64,
    object_gets: AtomicU64,
    object_get_bytes: AtomicU64,
    coalesced_gets: AtomicU64,
    coalesced_get_bytes: AtomicU64,
    cache_hits: AtomicU64,
    cache_hit_bytes: AtomicU64,
    read_plan_cache_hits: AtomicU64,
    read_plan_cache_misses: AtomicU64,
    manifest_chunks: AtomicU64,
    manifest_blocks: AtomicU64,
    data_fabric_stats: Mutex<DataFabricReadStats>,
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
            read_plans: Mutex::new(ObjectReadPlanCache::new(MAX_READ_PLAN_CACHE_ENTRIES)),
            block_cache_enabled: true,
            object_puts: AtomicU64::new(0),
            object_put_bytes: AtomicU64::new(0),
            object_gets: AtomicU64::new(0),
            object_get_bytes: AtomicU64::new(0),
            coalesced_gets: AtomicU64::new(0),
            coalesced_get_bytes: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_hit_bytes: AtomicU64::new(0),
            read_plan_cache_hits: AtomicU64::new(0),
            read_plan_cache_misses: AtomicU64::new(0),
            manifest_chunks: AtomicU64::new(0),
            manifest_blocks: AtomicU64::new(0),
            data_fabric_stats: Mutex::new(DataFabricReadStats::default()),
        }
    }

    pub fn connect(address: SocketAddr, objects: O) -> Self {
        Self::new(MetadataClient::connect(address), objects)
    }

    pub fn metadata(&self) -> &MetadataClient {
        &self.metadata
    }

    pub fn stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, ClientError> {
        self.metadata.stat_card(path)
    }

    pub fn namespace_list_page(
        &self,
        path: &str,
        options: NamespaceListOptions,
    ) -> Result<NamespaceListPage, ClientError> {
        self.metadata.namespace_list_page(path, options)
    }

    pub fn find_paths(
        &self,
        request: NamespaceFindRequest,
    ) -> Result<NamespaceFindResult, ClientError> {
        self.metadata.find_paths(request)
    }

    pub fn aggregate_paths(
        &self,
        request: NamespaceAggregateRequest,
    ) -> Result<NamespaceAggregateResult, ClientError> {
        self.metadata.aggregate_paths(request)
    }

    pub fn grep_paths(
        &self,
        request: NamespaceGrepRequest,
    ) -> Result<NamespaceGrepResult, ClientError> {
        self.metadata.grep_paths(request)
    }

    pub fn read_page(
        &self,
        path: &str,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, ClientError> {
        self.metadata.read_page(path, options)
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
            read_plan_cache_hits: self.read_plan_cache_hits.load(Ordering::Relaxed),
            read_plan_cache_misses: self.read_plan_cache_misses.load(Ordering::Relaxed),
            object_writeback_enqueued: 0,
            object_writeback_inline: 0,
            object_writeback_completed: 0,
            object_writeback_failed: 0,
            object_writeback_staged_bytes: 0,
            object_writeback_uploaded_bytes: 0,
            object_writeback_queue_wait_ns: 0,
            object_writeback_queue_max_wait_ns: 0,
            object_writeback_upload_ns: 0,
            object_writeback_upload_max_ns: 0,
            object_writeback_collect_ns: 0,
            object_writeback_digest_ns: 0,
            object_writeback_store_put_ns: 0,
            object_writeback_cache_put_ns: 0,
            manifest_chunks: self.manifest_chunks.load(Ordering::Relaxed),
            manifest_blocks: self.manifest_blocks.load(Ordering::Relaxed),
        }
    }

    pub fn data_fabric_stats(&self) -> Result<DataFabricReadStats, ClientError> {
        self.data_fabric_stats
            .lock()
            .map(|stats| *stats)
            .map_err(|err| ClientError::Protocol(format!("data fabric stats lock poisoned: {err}")))
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
        let open = self
            .metadata
            .open_path_read_plan(path, offset, len, expected_generation)?;
        let generation = open.lease.generation;
        let bytes = self.read_planned_object_blocks(
            &read_pipeline_key(path, generation),
            open.metadata.attr.inode,
            generation,
            open.metadata.attr.size,
            offset,
            &open.plan,
        )?;
        Ok(NamespaceRead {
            metadata: open.metadata,
            bytes,
        })
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
        let plan = self.cached_read_body_plan(entry.attr.inode, body.generation, offset, len)?;
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
        plan: &ObjectReadPlan,
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
        let executor = LayoutReadExecutor::new(self.objects.as_ref());
        let read_options = if self.block_cache_enabled {
            BlockReadOptions::default().with_read_coordinator(self.prefetcher.read_coordinator())
        } else {
            BlockReadOptions::default()
        };
        let outcome = executor
            .read_plan_with_options(&mut pipeline, cache, file_size, offset, plan, read_options)
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
        if self.block_cache_enabled {
            if let Some(request) = outcome.cache_warmup {
                let _ = self.prefetcher.submit(request);
            }
        }
        let stats = outcome.stats;
        self.record_object_read_stats(stats)?;
        Ok(outcome.bytes)
    }

    fn record_object_read_stats(&self, stats: DataFabricReadStats) -> Result<(), ClientError> {
        self.record_data_fabric_stats(stats)?;
        self.object_gets
            .fetch_add(stats.object_gets, Ordering::Relaxed);
        self.object_get_bytes
            .fetch_add(stats.object_get_bytes, Ordering::Relaxed);
        self.coalesced_gets
            .fetch_add(stats.coalesced_ranges, Ordering::Relaxed);
        self.coalesced_get_bytes
            .fetch_add(stats.coalesced_range_bytes, Ordering::Relaxed);
        self.cache_hits
            .fetch_add(stats.cache_hits, Ordering::Relaxed);
        self.cache_hit_bytes
            .fetch_add(stats.cache_hit_bytes, Ordering::Relaxed);
        Ok(())
    }

    fn record_data_fabric_stats(&self, update: DataFabricReadStats) -> Result<(), ClientError> {
        let mut stats = self.data_fabric_stats.lock().map_err(|err| {
            ClientError::Protocol(format!("data fabric stats lock poisoned: {err}"))
        })?;
        stats.saturating_add_assign(update);
        Ok(())
    }

    fn prefetch_read_blocks(&self, inode: InodeId, generation: u64, offset: u64, len: usize) {
        if !self.block_cache_enabled || len == 0 {
            return;
        }
        let Ok(plan) = self.metadata.read_body_plan(inode, generation, offset, len) else {
            return;
        };
        let key = ObjectReadPlanKey::new(inode.get(), generation, offset, len);
        let _ = self.cache_read_body_plan(key, plan.clone());
        let _ = self
            .prefetcher
            .submit(ObjectPrefetchRequest::new(plan.output_len, plan.blocks));
    }

    fn cached_read_body_plan(
        &self,
        inode: InodeId,
        generation: u64,
        offset: u64,
        len: usize,
    ) -> Result<ObjectReadPlan, ClientError> {
        let key = ObjectReadPlanKey::new(inode.get(), generation, offset, len);
        if let Some(plan) = self.cached_read_body_plan_for_key(&key)? {
            self.read_plan_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(plan);
        }
        self.read_plan_cache_misses.fetch_add(1, Ordering::Relaxed);
        let plan = self
            .metadata
            .read_body_plan(inode, generation, offset, len)?;
        self.cache_read_body_plan(key, plan.clone())?;
        Ok(plan)
    }

    fn cached_read_body_plan_for_key(
        &self,
        key: &ObjectReadPlanKey,
    ) -> Result<Option<ObjectReadPlan>, ClientError> {
        self.read_plans
            .lock()
            .map_err(|err| ClientError::Protocol(format!("read plan cache lock poisoned: {err}")))
            .map(|mut plans| plans.get(key))
    }

    fn cache_read_body_plan(
        &self,
        key: ObjectReadPlanKey,
        plan: ObjectReadPlan,
    ) -> Result<(), ClientError> {
        self.read_plans
            .lock()
            .map_err(|err| ClientError::Protocol(format!("read plan cache lock poisoned: {err}")))?
            .insert(key, plan);
        Ok(())
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
        let chunks = written.chunk_manifests();
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

fn file_len(size: u64) -> Result<usize, ClientError> {
    usize::try_from(size)
        .map_err(|_| ClientError::Protocol("file is too large for this client".to_owned()))
}

fn read_pipeline_key(path: &str, generation: u64) -> String {
    format!("{path}#{generation}")
}

fn bounded_read_len(available: u64, requested: usize) -> Result<usize, ClientError> {
    let requested = u64::try_from(requested)
        .map_err(|_| ClientError::Protocol("read length exceeds u64".to_owned()))?;
    let len = available.min(requested);
    usize::try_from(len).map_err(|_| ClientError::Protocol("read length exceeds usize".to_owned()))
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::net::{SocketAddr, TcpListener};
    use std::thread;

    use nokv_object::MemoryObjectStore;
    use nokv_protocol::{decode_request, encode_envelope, MetadataRpcEnvelope, MetadataRpcRequest};

    use crate::framed::{read_frame, write_frame, FRAMED_RPC_MAGIC};

    use super::*;

    fn serve_body_read_plans(bodies: Vec<Vec<u8>>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
            stream.read_exact(&mut magic).unwrap();
            assert_eq!(&magic, FRAMED_RPC_MAGIC);
            for body in bodies {
                let (request_id, flags, request) = read_frame(&mut stream).unwrap();
                let request = decode_request(&request).expect("framed request is metadata rpc");
                assert!(matches!(request, MetadataRpcRequest::ReadBodyPlan { .. }));
                write_frame(&mut stream, request_id, flags, &body).unwrap();
            }
        });
        addr
    }

    fn response_body(json: &str) -> Vec<u8> {
        let envelope: MetadataRpcEnvelope = serde_json::from_str(json).unwrap();
        encode_envelope(&envelope).unwrap()
    }

    #[test]
    fn file_client_caches_body_read_plan_after_miss() {
        let read_plan = response_body(
            r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":0,"object_len":6,"len":6,"output_offset":0}]}}}"#,
        );
        let addr = serve_body_read_plans(vec![read_plan.clone(), read_plan]);
        let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
        let inode = InodeId::new(42).unwrap();

        let first = client.cached_read_body_plan(inode, 7, 0, 6).unwrap();
        let second = client.cached_read_body_plan(inode, 7, 0, 6).unwrap();

        assert_eq!(first.output_len, 6);
        assert_eq!(first, second);
        let stats = client.object_stats();
        assert_eq!(stats.read_plan_cache_misses, 1);
        assert_eq!(stats.read_plan_cache_hits, 1);
    }
}
