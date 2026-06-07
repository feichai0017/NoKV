use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nokv_client::{ClientError, ClientPreparedArtifact, MetadataClient};
use nokv_meta::{
    DentryWithAttr, MetadError, PublishArtifactRange, PublishArtifactStagedSession,
    ReadDirPlusPage, RenameReplaceResult, UpdateAttr, XattrSetMode,
};
use nokv_object::{
    chunk_manifests_from_stored_chunks, plan_chunk_manifest_reads, put_chunked_ranges_parallel,
    BlockCache, ChunkStore, ChunkWriteOptions, ChunkWriteRange, ChunkedWrite, FileReadPipeline,
    FileWritePipeline, ObjectBlockCache, ObjectError, ObjectPrefetchOptions, ObjectPrefetchRequest,
    ObjectPrefetcher, ObjectReadBlock, ObjectReadPlan, ObjectReadPlanCache, ObjectReadPlanKey,
    ObjectStore, ObjectWritebackOptions, ObjectWritebackRequest, ObjectWritebackUploader,
    PendingChunkedWrite, StoredChunk, WritebackCache, WritebackCacheOptions, WritebackUploadRange,
    DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
};
use nokv_types::{
    AdvisoryLock, AdvisoryLockRequest, DentryName, InodeAttr, InodeId, SpecialNodeSpec,
    WatchCursor, WatchRecord,
};

use crate::filesystem::FuseObjectPipelineStats;
use crate::filesystem::FuseOptions;

pub(crate) type FuseBackendResult<T> = Result<T, FuseBackendError>;

#[derive(Debug)]
pub(crate) enum FuseBackendError {
    Metadata(MetadError),
    Client(ClientError),
    Object(ObjectError),
}

pub(crate) trait FuseBackend: Send + Sync + 'static {
    type Prepared: Clone + Send + Sync + 'static;

    fn prepared_generation(&self, prepared: &Self::Prepared) -> u64;
    /// Whether `prepared` is an artifact-*replace* (vs a fresh create). Only
    /// replaces carry a dentry-version CAS guard that can be invalidated by an
    /// intervening attribute update on the same file.
    fn prepared_is_replace(&self, prepared: &Self::Prepared) -> bool;
    /// Rebind the dentry-version CAS guard carried by a replace `prepared` to
    /// `version`, leaving its pinned generation (and therefore its already
    /// staged object keys) untouched. Used to re-sync the guard to the live
    /// dentry version just before publishing.
    fn rebind_prepared_dentry_version(&self, prepared: &mut Self::Prepared, version: u64);
    fn watch_subtree(&self, scope: InodeId) -> FuseBackendResult<Option<WatchCursor>>;
    fn replay_watch(
        &self,
        scope: InodeId,
        cursor: WatchCursor,
        limit: usize,
    ) -> FuseBackendResult<Vec<WatchRecord>>;
    fn get_attr(&self, inode: InodeId) -> FuseBackendResult<Option<InodeAttr>>;
    fn get_attr_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> FuseBackendResult<Option<InodeAttr>>;
    fn lookup_plus(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<Option<DentryWithAttr>>;
    /// Current record version of the `(parent, name)` dentry. Used to refresh a
    /// write handle's prepared-replace CAS guard just before publishing, so an
    /// intervening attribute update (which advances the dentry version) does not
    /// strand the in-flight handle with a stale version.
    fn current_dentry_version(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<Option<u64>>;
    fn lookup_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<Option<DentryWithAttr>>;
    fn read_dir_plus_page(
        &self,
        inode: InodeId,
        after: Option<&DentryName>,
        limit: usize,
    ) -> FuseBackendResult<ReadDirPlusPage>;
    fn read_dir_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> FuseBackendResult<Vec<DentryWithAttr>>;
    fn rename(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> FuseBackendResult<DentryWithAttr>;
    fn rename_replace(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> FuseBackendResult<RenameReplaceResult>;
    fn read_file(&self, inode: InodeId, offset: u64, len: usize) -> FuseBackendResult<Vec<u8>>;
    fn read_file_with_pipeline(
        &self,
        inode: InodeId,
        offset: u64,
        len: usize,
        pipeline: &mut FileReadPipeline,
    ) -> FuseBackendResult<Vec<u8>> {
        let _ = pipeline;
        self.read_file(inode, offset, len)
    }
    fn read_file_with_known_attr_pipeline(
        &self,
        attr: &InodeAttr,
        offset: u64,
        len: usize,
        pipeline: &mut FileReadPipeline,
    ) -> FuseBackendResult<Vec<u8>> {
        self.read_file_with_pipeline(attr.inode, offset, len, pipeline)
    }
    fn read_file_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> FuseBackendResult<Vec<u8>>;
    fn read_symlink(&self, inode: InodeId) -> FuseBackendResult<Vec<u8>>;
    fn read_symlink_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> FuseBackendResult<Vec<u8>>;
    fn update_root_attrs(&self, changes: UpdateAttr) -> FuseBackendResult<InodeAttr>;
    fn update_attrs(
        &self,
        parent: InodeId,
        name: &DentryName,
        changes: UpdateAttr,
    ) -> FuseBackendResult<DentryWithAttr>;
    fn set_xattr(
        &self,
        inode: InodeId,
        name: &[u8],
        value: Vec<u8>,
        mode: XattrSetMode,
    ) -> FuseBackendResult<()>;
    fn get_xattr(&self, inode: InodeId, name: &[u8]) -> FuseBackendResult<Option<Vec<u8>>>;
    fn list_xattr(&self, inode: InodeId) -> FuseBackendResult<Vec<Vec<u8>>>;
    fn remove_xattr(&self, inode: InodeId, name: &[u8]) -> FuseBackendResult<()>;
    fn get_advisory_lock(
        &self,
        request: AdvisoryLockRequest,
    ) -> FuseBackendResult<Option<AdvisoryLock>>;
    fn set_advisory_lock(&self, request: AdvisoryLockRequest) -> FuseBackendResult<()>;
    fn create_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<DentryWithAttr>;
    fn create_file(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<DentryWithAttr>;

    fn create_file_prepared(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<(DentryWithAttr, Self::Prepared)>;
    fn create_symlink(
        &self,
        parent: InodeId,
        name: DentryName,
        target: Vec<u8>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<DentryWithAttr>;
    fn create_special_node(
        &self,
        parent: InodeId,
        name: DentryName,
        spec: SpecialNodeSpec,
    ) -> FuseBackendResult<DentryWithAttr>;
    fn link(
        &self,
        inode: InodeId,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> FuseBackendResult<DentryWithAttr>;
    fn remove_file(&self, parent: InodeId, name: &DentryName) -> FuseBackendResult<DentryWithAttr>;
    fn remove_empty_dir(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<DentryWithAttr>;
    fn prepare_artifact_replace(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> FuseBackendResult<Self::Prepared>;
    fn new_write_pipeline(
        &self,
        prepared: &Self::Prepared,
        manifest_id: &str,
    ) -> FuseBackendResult<FileWritePipeline>;
    fn stage_prepared_artifact_ranges(
        &self,
        prepared: &Self::Prepared,
        manifest_id: &str,
        ranges: &[PublishArtifactRange],
        block_index_base: u64,
    ) -> FuseBackendResult<ChunkedWrite>;
    fn stage_prepared_artifact_ranges_async(
        &self,
        prepared: &Self::Prepared,
        manifest_id: &str,
        ranges: &[PublishArtifactRange],
        block_index_base: u64,
    ) -> FuseBackendResult<PendingChunkedWrite> {
        self.stage_prepared_artifact_ranges(prepared, manifest_id, ranges, block_index_base)
            .map(|written| PendingChunkedWrite::ready(Ok(written)))
    }
    fn cleanup_staged_objects(
        &self,
        staged: &nokv_object::StagedObjectSet,
    ) -> FuseBackendResult<()>;
    fn read_session_object_blocks(
        &self,
        output_len: usize,
        blocks: &[ObjectReadBlock],
    ) -> FuseBackendResult<Vec<u8>>;
    fn publish_prepared_artifact_staged_session(
        &self,
        prepared: Self::Prepared,
        request: PublishArtifactStagedSession,
    ) -> FuseBackendResult<RenameReplaceResult>;
    fn object_pipeline_stats(&self) -> FuseBackendResult<FuseObjectPipelineStats> {
        Ok(FuseObjectPipelineStats::default())
    }
}

pub(crate) struct ClientFuseBackend<O> {
    metadata: MetadataClient,
    objects: Arc<O>,
    block_cache: Option<ObjectBlockCache>,
    read_plan_cache: Mutex<ObjectReadPlanCache>,
    read_plan_cache_hits: AtomicU64,
    read_plan_cache_misses: AtomicU64,
    prefetcher: Option<ObjectPrefetcher<Arc<O>>>,
    writeback_cache: Option<WritebackCache>,
    writeback_uploader: Option<ObjectWritebackUploader<Arc<O>>>,
    upload_workers: usize,
}

impl<O> ClientFuseBackend<O>
where
    O: ObjectStore + Send + Sync + 'static,
{
    pub(crate) fn new(
        metadata: MetadataClient,
        objects: O,
        options: &FuseOptions,
    ) -> FuseBackendResult<Self> {
        let objects = Arc::new(objects);
        let block_cache = options.block_cache.clone().open()?;
        let prefetcher = if options.prefetch.enabled {
            block_cache.as_ref().map(|cache| {
                ObjectPrefetcher::new(
                    Arc::clone(&objects),
                    cache.clone(),
                    ObjectPrefetchOptions::from(options.prefetch.clone()),
                )
            })
        } else {
            None
        };
        let writeback = &options.writeback;
        let writeback_cache = if writeback.enabled {
            Some(WritebackCache::new(WritebackCacheOptions {
                root: writeback.root.clone(),
                max_bytes: writeback.max_bytes,
                max_items: writeback.max_items,
            })?)
        } else {
            None
        };
        let writeback_uploader = writeback_cache.as_ref().map(|cache| {
            ObjectWritebackUploader::new(
                Arc::clone(&objects),
                cache.clone(),
                block_cache.clone(),
                ObjectWritebackOptions {
                    queue_capacity: writeback.queue_capacity.max(1),
                    workers: writeback.workers.max(1),
                    upload_workers_per_request: writeback.upload_workers_per_request.max(1),
                },
            )
        });
        Ok(Self {
            metadata,
            objects,
            block_cache,
            read_plan_cache: Mutex::new(ObjectReadPlanCache::new(4096)),
            read_plan_cache_hits: AtomicU64::new(0),
            read_plan_cache_misses: AtomicU64::new(0),
            prefetcher,
            writeback_cache,
            writeback_uploader,
            upload_workers: writeback.upload_workers_per_request.max(1),
        })
    }

    fn prefetch_read_blocks(&self, inode: InodeId, generation: u64, offset: u64, len: usize) {
        if len == 0 {
            return;
        }
        let Some(prefetcher) = &self.prefetcher else {
            return;
        };
        let Ok(plan) = self.metadata.read_body_plan(inode, generation, offset, len) else {
            return;
        };
        let key = ObjectReadPlanKey::new(inode.get(), generation, offset, len);
        let _ = self.cache_read_body_plan(key, plan.clone());
        let _ = prefetcher.submit(ObjectPrefetchRequest::new(plan.output_len, plan.blocks));
    }

    fn collect_object_pipeline_stats(&self) -> FuseBackendResult<FuseObjectPipelineStats> {
        Ok(FuseObjectPipelineStats {
            block_cache: self
                .block_cache
                .as_ref()
                .map(|cache| cache.stats())
                .transpose()?,
            prefetch: self
                .prefetcher
                .as_ref()
                .map(|prefetcher| prefetcher.stats())
                .transpose()?,
            writeback_cache: self
                .writeback_cache
                .as_ref()
                .map(|cache| cache.stats())
                .transpose()?,
            writeback: self
                .writeback_uploader
                .as_ref()
                .map(|uploader| uploader.stats())
                .transpose()?,
            read_plan_cache_hits: self.read_plan_cache_hits.load(Ordering::Relaxed),
            read_plan_cache_misses: self.read_plan_cache_misses.load(Ordering::Relaxed),
        })
    }

    fn cached_read_body_plan(
        &self,
        inode: InodeId,
        generation: u64,
        offset: u64,
        len: usize,
    ) -> FuseBackendResult<ObjectReadPlan> {
        let key = ObjectReadPlanKey::new(inode.get(), generation, offset, len);
        let cached = self
            .read_plan_cache
            .lock()
            .map_err(|err| ObjectError::Backend(format!("read plan cache lock poisoned: {err}")))?
            .get(&key);
        if let Some(plan) = cached {
            self.read_plan_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(plan);
        }
        self.read_plan_cache_misses.fetch_add(1, Ordering::Relaxed);
        self.metadata
            .read_body_plan(inode, generation, offset, len)
            .map_err(Into::into)
    }

    fn cache_read_body_plan(
        &self,
        key: ObjectReadPlanKey,
        plan: ObjectReadPlan,
    ) -> FuseBackendResult<()> {
        self.read_plan_cache
            .lock()
            .map_err(|err| ObjectError::Backend(format!("read plan cache lock poisoned: {err}")))?
            .insert(key, plan);
        Ok(())
    }

    fn cache_new_file_staged_read_plan(
        &self,
        inode: InodeId,
        generation: u64,
        size: u64,
        chunks: &[StoredChunk],
    ) -> FuseBackendResult<()> {
        if size == 0 || chunks.is_empty() {
            return Ok(());
        }
        let len = usize::try_from(size).map_err(|_| ObjectError::InvalidRange)?;
        let manifests = chunk_manifests_from_stored_chunks(chunks);
        let plan = plan_chunk_manifest_reads(&manifests, 0, len)?;
        self.cache_read_body_plan(
            ObjectReadPlanKey::new(inode.get(), generation, 0, len),
            ObjectReadPlan::new(plan.output_len, plan.blocks),
        )
    }

    fn stage_prepared_artifact_ranges_direct_pending(
        &self,
        prepared: &ClientPreparedArtifact,
        manifest_id: &str,
        ranges: &[PublishArtifactRange],
        block_index_base: u64,
    ) -> FuseBackendResult<PendingChunkedWrite> {
        <Self as FuseBackend>::stage_prepared_artifact_ranges(
            self,
            prepared,
            manifest_id,
            ranges,
            block_index_base,
        )
        .map(|written| PendingChunkedWrite::ready(Ok(written)))
    }
}

impl From<MetadError> for FuseBackendError {
    fn from(err: MetadError) -> Self {
        Self::Metadata(err)
    }
}

impl From<ClientError> for FuseBackendError {
    fn from(err: ClientError) -> Self {
        Self::Client(err)
    }
}

impl From<ObjectError> for FuseBackendError {
    fn from(err: ObjectError) -> Self {
        Self::Object(err)
    }
}

impl fmt::Display for FuseBackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metadata(err) => write!(f, "{err}"),
            Self::Client(err) => write!(f, "{err}"),
            Self::Object(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for FuseBackendError {}

impl<O> FuseBackend for ClientFuseBackend<O>
where
    O: ObjectStore + Send + Sync + 'static,
{
    type Prepared = ClientPreparedArtifact;

    fn prepared_generation(&self, prepared: &Self::Prepared) -> u64 {
        prepared.generation
    }

    fn prepared_is_replace(&self, prepared: &Self::Prepared) -> bool {
        prepared.replace
    }

    fn rebind_prepared_dentry_version(&self, prepared: &mut Self::Prepared, version: u64) {
        prepared.dentry_version = Some(version);
    }

    fn watch_subtree(&self, _scope: InodeId) -> FuseBackendResult<Option<WatchCursor>> {
        Ok(None)
    }

    fn replay_watch(
        &self,
        _scope: InodeId,
        _cursor: WatchCursor,
        _limit: usize,
    ) -> FuseBackendResult<Vec<WatchRecord>> {
        Ok(Vec::new())
    }

    fn get_attr(&self, inode: InodeId) -> FuseBackendResult<Option<InodeAttr>> {
        self.metadata.get_attr(inode).map_err(Into::into)
    }

    fn get_attr_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> FuseBackendResult<Option<InodeAttr>> {
        self.metadata
            .get_attr_at_snapshot(snapshot_id, inode)
            .map_err(Into::into)
    }

    fn lookup_plus(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<Option<DentryWithAttr>> {
        self.metadata
            .lookup_plus(parent, name.clone())
            .map_err(Into::into)
    }

    fn current_dentry_version(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<Option<u64>> {
        self.metadata
            .current_dentry_version(parent, name.clone())
            .map_err(Into::into)
    }

    fn lookup_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<Option<DentryWithAttr>> {
        self.metadata
            .lookup_plus_at_snapshot(snapshot_id, parent, name.clone())
            .map_err(Into::into)
    }

    fn read_dir_plus_page(
        &self,
        inode: InodeId,
        after: Option<&DentryName>,
        limit: usize,
    ) -> FuseBackendResult<ReadDirPlusPage> {
        let page = self
            .metadata
            .read_dir_plus_page(inode, after, limit)
            .map_err(FuseBackendError::from)?;
        Ok(ReadDirPlusPage {
            entries: page.entries,
            next_cursor: page.next_cursor,
        })
    }

    fn read_dir_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> FuseBackendResult<Vec<DentryWithAttr>> {
        self.metadata
            .read_dir_plus_at_snapshot(snapshot_id, inode)
            .map_err(Into::into)
    }

    fn rename(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .rename_in_dir(parent, name.clone(), new_parent, new_name)
            .map_err(Into::into)
    }

    fn rename_replace(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> FuseBackendResult<RenameReplaceResult> {
        self.metadata
            .rename_replace_in_dir(parent, name.clone(), new_parent, new_name)
            .map_err(Into::into)
    }

    fn read_file(&self, inode: InodeId, offset: u64, len: usize) -> FuseBackendResult<Vec<u8>> {
        let Some(attr) = self
            .metadata
            .get_attr(inode)
            .map_err(FuseBackendError::from)?
        else {
            return Err(FuseBackendError::Metadata(MetadError::NotFound));
        };
        let plan = self.cached_read_body_plan(inode, attr.generation, offset, len)?;
        let read =
            self.objects
                .read_blocks(self.block_cache.as_ref(), plan.output_len, &plan.blocks)?;
        Ok(read.bytes)
    }

    fn read_file_with_pipeline(
        &self,
        inode: InodeId,
        offset: u64,
        len: usize,
        pipeline: &mut FileReadPipeline,
    ) -> FuseBackendResult<Vec<u8>> {
        let Some(attr) = self
            .metadata
            .get_attr(inode)
            .map_err(FuseBackendError::from)?
        else {
            return Err(FuseBackendError::Metadata(MetadError::NotFound));
        };
        if len == 0 || offset >= attr.size {
            return Ok(Vec::new());
        }
        let plan = self.cached_read_body_plan(inode, attr.generation, offset, len)?;
        let read = pipeline.read_blocks(
            &self.objects,
            self.block_cache.as_ref(),
            attr.size,
            offset,
            plan.output_len,
            &plan.blocks,
        )?;
        if let Some(hint) = read.readahead {
            self.prefetch_read_blocks(inode, attr.generation, hint.offset, hint.len);
        }
        let read = read.blocks;
        Ok(read.bytes)
    }

    fn read_file_with_known_attr_pipeline(
        &self,
        attr: &InodeAttr,
        offset: u64,
        len: usize,
        pipeline: &mut FileReadPipeline,
    ) -> FuseBackendResult<Vec<u8>> {
        if len == 0 || offset >= attr.size {
            return Ok(Vec::new());
        }
        let plan = self.cached_read_body_plan(attr.inode, attr.generation, offset, len)?;
        let read = pipeline.read_blocks(
            &self.objects,
            self.block_cache.as_ref(),
            attr.size,
            offset,
            plan.output_len,
            &plan.blocks,
        )?;
        if let Some(hint) = read.readahead {
            self.prefetch_read_blocks(attr.inode, attr.generation, hint.offset, hint.len);
        }
        Ok(read.blocks.bytes)
    }

    fn read_file_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> FuseBackendResult<Vec<u8>> {
        self.metadata
            .read_file_at_snapshot(snapshot_id, inode, offset, len)
            .map_err(Into::into)
    }

    fn read_symlink(&self, inode: InodeId) -> FuseBackendResult<Vec<u8>> {
        self.metadata.read_symlink(inode).map_err(Into::into)
    }

    fn read_symlink_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> FuseBackendResult<Vec<u8>> {
        self.metadata
            .read_symlink_at_snapshot(snapshot_id, inode)
            .map_err(Into::into)
    }

    fn update_root_attrs(&self, changes: UpdateAttr) -> FuseBackendResult<InodeAttr> {
        self.metadata.update_root_attrs(changes).map_err(Into::into)
    }

    fn update_attrs(
        &self,
        parent: InodeId,
        name: &DentryName,
        changes: UpdateAttr,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .update_attrs(parent, name.clone(), changes)
            .map_err(Into::into)
    }

    fn set_xattr(
        &self,
        inode: InodeId,
        name: &[u8],
        value: Vec<u8>,
        mode: XattrSetMode,
    ) -> FuseBackendResult<()> {
        self.metadata
            .set_xattr(inode, name, value, mode)
            .map_err(Into::into)
    }

    fn get_xattr(&self, inode: InodeId, name: &[u8]) -> FuseBackendResult<Option<Vec<u8>>> {
        self.metadata.get_xattr(inode, name).map_err(Into::into)
    }

    fn list_xattr(&self, inode: InodeId) -> FuseBackendResult<Vec<Vec<u8>>> {
        self.metadata.list_xattr(inode).map_err(Into::into)
    }

    fn remove_xattr(&self, inode: InodeId, name: &[u8]) -> FuseBackendResult<()> {
        self.metadata.remove_xattr(inode, name).map_err(Into::into)
    }

    fn get_advisory_lock(
        &self,
        request: AdvisoryLockRequest,
    ) -> FuseBackendResult<Option<AdvisoryLock>> {
        self.metadata.get_advisory_lock(request).map_err(Into::into)
    }

    fn set_advisory_lock(&self, request: AdvisoryLockRequest) -> FuseBackendResult<()> {
        self.metadata.set_advisory_lock(request).map_err(Into::into)
    }

    fn create_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .create_dir(parent, name, mode, uid, gid)
            .map_err(Into::into)
    }

    fn create_file(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .create_file_in_dir(parent, name, mode, uid, gid)
            .map_err(Into::into)
    }

    fn create_file_prepared(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<(DentryWithAttr, Self::Prepared)> {
        self.metadata
            .create_file_prepared_in_dir(parent, name, mode, uid, gid)
            .map(|created| (created.entry, created.prepared))
            .map_err(Into::into)
    }

    fn create_symlink(
        &self,
        parent: InodeId,
        name: DentryName,
        target: Vec<u8>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .create_symlink(parent, name, target, mode, uid, gid)
            .map_err(Into::into)
    }

    fn create_special_node(
        &self,
        parent: InodeId,
        name: DentryName,
        spec: SpecialNodeSpec,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .create_special_node(parent, name, spec)
            .map_err(Into::into)
    }

    fn link(
        &self,
        inode: InodeId,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .link(inode, new_parent, new_name)
            .map_err(Into::into)
    }

    fn remove_file(&self, parent: InodeId, name: &DentryName) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .remove_file(parent, name.clone())
            .map_err(Into::into)
    }

    fn remove_empty_dir(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .remove_empty_dir(parent, name.clone())
            .map_err(Into::into)
    }

    fn prepare_artifact_replace(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> FuseBackendResult<Self::Prepared> {
        self.metadata
            .prepare_artifact(parent, name, true)
            .map_err(Into::into)
    }

    fn stage_prepared_artifact_ranges(
        &self,
        prepared: &Self::Prepared,
        manifest_id: &str,
        ranges: &[PublishArtifactRange],
        block_index_base: u64,
    ) -> FuseBackendResult<ChunkedWrite> {
        let dirty_ranges = ranges
            .iter()
            .filter(|range| !range.bytes.is_empty())
            .map(|range| ChunkWriteRange {
                logical_offset: range.offset,
                bytes: range.bytes.clone(),
            })
            .collect::<Vec<_>>();
        put_chunked_ranges_parallel(
            &self.objects,
            &dirty_ranges,
            ChunkWriteOptions {
                manifest_id: manifest_id.to_owned(),
                mount: prepared.mount,
                inode: prepared.inode.get(),
                generation: prepared.generation,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
            block_index_base,
            self.upload_workers,
            self.block_cache
                .as_ref()
                .map(|cache| cache as &(dyn BlockCache + Sync)),
        )
        .map_err(Into::into)
    }

    fn new_write_pipeline(
        &self,
        prepared: &Self::Prepared,
        manifest_id: &str,
    ) -> FuseBackendResult<FileWritePipeline> {
        FileWritePipeline::new(ChunkWriteOptions {
            manifest_id: manifest_id.to_owned(),
            mount: prepared.mount,
            inode: prepared.inode.get(),
            generation: prepared.generation,
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size: DEFAULT_BLOCK_SIZE,
        })
        .map_err(Into::into)
    }

    fn stage_prepared_artifact_ranges_async(
        &self,
        prepared: &Self::Prepared,
        manifest_id: &str,
        ranges: &[PublishArtifactRange],
        block_index_base: u64,
    ) -> FuseBackendResult<PendingChunkedWrite> {
        let (Some(writeback_cache), Some(writeback_uploader)) =
            (&self.writeback_cache, &self.writeback_uploader)
        else {
            return self.stage_prepared_artifact_ranges_direct_pending(
                prepared,
                manifest_id,
                ranges,
                block_index_base,
            );
        };
        let mut upload_ranges: Vec<WritebackUploadRange> = Vec::new();
        for range in ranges.iter().filter(|range| !range.bytes.is_empty()) {
            let key = format!(
                "{manifest_id}:{}:{}:{}",
                prepared.generation,
                range.offset,
                range.bytes.len()
            );
            let ticket = match writeback_cache.stage(key, &range.bytes) {
                Ok(ticket) => ticket,
                Err(_) => {
                    for range in upload_ranges {
                        let _ = writeback_cache.remove(&range.ticket);
                    }
                    let _ = writeback_uploader.record_fallback();
                    return self.stage_prepared_artifact_ranges_direct_pending(
                        prepared,
                        manifest_id,
                        ranges,
                        block_index_base,
                    );
                }
            };
            upload_ranges.push(WritebackUploadRange {
                logical_offset: range.offset,
                ticket,
            });
        }
        if upload_ranges.is_empty() {
            return Ok(PendingChunkedWrite::ready(Ok(ChunkedWrite {
                manifest_id: manifest_id.to_owned(),
                size: 0,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE as u64,
                chunks: Vec::new(),
                object_puts: 0,
                object_put_bytes: 0,
            })));
        }
        let request = ObjectWritebackRequest {
            ranges: upload_ranges.clone(),
            options: ChunkWriteOptions {
                manifest_id: manifest_id.to_owned(),
                mount: prepared.mount,
                inode: prepared.inode.get(),
                generation: prepared.generation,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
            block_index_base,
        };
        match writeback_uploader.submit(request) {
            Ok(pending) => Ok(pending),
            Err(_) => {
                for range in upload_ranges {
                    let _ = writeback_cache.remove(&range.ticket);
                }
                let _ = writeback_uploader.record_fallback();
                self.stage_prepared_artifact_ranges_direct_pending(
                    prepared,
                    manifest_id,
                    ranges,
                    block_index_base,
                )
            }
        }
    }

    fn cleanup_staged_objects(
        &self,
        staged: &nokv_object::StagedObjectSet,
    ) -> FuseBackendResult<()> {
        self.objects
            .delete_staged(staged)
            .map(|_| ())
            .map_err(Into::into)
    }

    fn read_session_object_blocks(
        &self,
        output_len: usize,
        blocks: &[ObjectReadBlock],
    ) -> FuseBackendResult<Vec<u8>> {
        let read = self
            .objects
            .read_blocks(self.block_cache.as_ref(), output_len, blocks)?;
        Ok(read.bytes)
    }

    fn publish_prepared_artifact_staged_session(
        &self,
        prepared: Self::Prepared,
        request: PublishArtifactStagedSession,
    ) -> FuseBackendResult<RenameReplaceResult> {
        if prepared.parent != request.parent || prepared.name != request.name {
            let _ = self.objects.delete_staged(&request.staged);
            return Err(FuseBackendError::Metadata(
                MetadError::InvalidPreparedArtifact(
                    "prepared artifact target does not match staged publish session".to_owned(),
                ),
            ));
        }
        let staged = request.staged.clone();
        let cache_new_file_plan = !prepared.replace;
        let cache_chunks = request.chunks.clone();
        let cache_size = request.size;
        let result = self
            .metadata
            .publish_prepared_artifact_staged_session(prepared, request)
            .map_err(|err| {
                let _ = self.objects.delete_staged(&staged);
                FuseBackendError::from(err)
            })?;
        if cache_new_file_plan {
            let _ = self.cache_new_file_staged_read_plan(
                result.entry.attr.inode,
                result.entry.attr.generation,
                cache_size,
                &cache_chunks,
            );
        }
        Ok(result)
    }

    fn object_pipeline_stats(&self) -> FuseBackendResult<FuseObjectPipelineStats> {
        self.collect_object_pipeline_stats()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use nokv_client::MetadataClientOptions;
    use nokv_meta::PublishArtifactRange;
    use nokv_object::{MemoryObjectStore, ObjectStore};
    use nokv_types::DentryName;

    use super::*;

    #[test]
    fn client_backend_reports_writeback_pipeline_stats() {
        let temp = tempfile::tempdir().unwrap();
        let metadata = MetadataClient::new(MetadataClientOptions::new(SocketAddr::from((
            [127, 0, 0, 1],
            9,
        ))));
        let objects = MemoryObjectStore::new();
        let backend = ClientFuseBackend::new(
            metadata,
            objects.clone(),
            &FuseOptions {
                writeback: crate::filesystem::FuseWritebackOptions {
                    root: temp.path().join("writeback"),
                    workers: 1,
                    upload_workers_per_request: 1,
                    ..crate::filesystem::FuseWritebackOptions::default()
                },
                ..FuseOptions::default()
            },
        )
        .unwrap();
        let prepared = ClientPreparedArtifact {
            mount: 1,
            parent: InodeId::new(1).unwrap(),
            name: DentryName::new("checkpoint.bin").unwrap(),
            path: Some("/checkpoint.bin".to_owned()),
            inode: InodeId::new(42).unwrap(),
            generation: 7,
            mtime_ms: 1,
            ctime_ms: 1,
            replace: true,
            dentry_version: Some(1),
            old_generation: None,
        };

        let pending = backend
            .stage_prepared_artifact_ranges_async(
                &prepared,
                "checkpoint.bin",
                &[PublishArtifactRange {
                    offset: 0,
                    bytes: b"checkpoint".to_vec(),
                }],
                0,
            )
            .unwrap();
        let written = pending.wait().unwrap();
        assert_eq!(written.object_puts, 1);
        assert!(objects
            .head(&nokv_object::ObjectKey::new("blocks/1/42/7/0/0").unwrap())
            .unwrap()
            .is_some());

        let stats = backend.object_pipeline_stats().unwrap();
        let writeback = stats.writeback.unwrap();
        assert_eq!(writeback.enqueued, 1);
        assert_eq!(writeback.fallback, 0);
        assert_eq!(writeback.completed, 1);
        assert_eq!(writeback.failed, 0);
        assert_eq!(writeback.staged_bytes, 10);
        assert_eq!(writeback.uploaded_bytes, 10);
        let cache = stats.writeback_cache.unwrap();
        assert_eq!(cache.staged, 1);
        assert_eq!(cache.staged_bytes, 10);
        assert_eq!(cache.removed, 1);
        assert_eq!(cache.removed_bytes, 10);
        assert_eq!(cache.active_items, 0);
        assert_eq!(cache.active_bytes, 0);
    }

    #[test]
    fn writeback_cache_capacity_falls_back_to_direct_upload() {
        let temp = tempfile::tempdir().unwrap();
        let metadata = MetadataClient::new(MetadataClientOptions::new(SocketAddr::from((
            [127, 0, 0, 1],
            9,
        ))));
        let objects = MemoryObjectStore::new();
        let backend = ClientFuseBackend::new(
            metadata,
            objects.clone(),
            &FuseOptions {
                writeback: crate::filesystem::FuseWritebackOptions {
                    root: temp.path().join("writeback"),
                    max_bytes: 4,
                    workers: 1,
                    upload_workers_per_request: 1,
                    ..crate::filesystem::FuseWritebackOptions::default()
                },
                ..FuseOptions::default()
            },
        )
        .unwrap();
        let prepared = ClientPreparedArtifact {
            mount: 1,
            parent: InodeId::new(1).unwrap(),
            name: DentryName::new("checkpoint.bin").unwrap(),
            path: Some("/checkpoint.bin".to_owned()),
            inode: InodeId::new(42).unwrap(),
            generation: 7,
            mtime_ms: 1,
            ctime_ms: 1,
            replace: true,
            dentry_version: Some(1),
            old_generation: None,
        };

        let pending = backend
            .stage_prepared_artifact_ranges_async(
                &prepared,
                "checkpoint.bin",
                &[PublishArtifactRange {
                    offset: 0,
                    bytes: b"checkpoint".to_vec(),
                }],
                0,
            )
            .unwrap();
        let written = pending.wait().unwrap();
        assert_eq!(written.object_puts, 1);
        assert!(objects
            .head(&nokv_object::ObjectKey::new("blocks/1/42/7/0/0").unwrap())
            .unwrap()
            .is_some());

        let stats = backend.object_pipeline_stats().unwrap();
        let writeback = stats.writeback.unwrap();
        assert_eq!(writeback.enqueued, 0);
        assert_eq!(writeback.fallback, 1);
        assert_eq!(writeback.completed, 0);
        assert_eq!(writeback.failed, 0);
        let cache = stats.writeback_cache.unwrap();
        assert_eq!(cache.staged, 0);
        assert_eq!(cache.active_items, 0);
        assert_eq!(cache.active_bytes, 0);
    }

    #[test]
    fn client_backend_reports_read_plan_cache_hits() {
        let metadata = MetadataClient::new(MetadataClientOptions::new(SocketAddr::from((
            [127, 0, 0, 1],
            9,
        ))));
        let backend =
            ClientFuseBackend::new(metadata, MemoryObjectStore::new(), &FuseOptions::default())
                .unwrap();
        let inode = InodeId::new(42).unwrap();
        backend
            .cache_read_body_plan(
                ObjectReadPlanKey::new(inode.get(), 7, 0, 12),
                ObjectReadPlan::new(
                    12,
                    vec![ObjectReadBlock {
                        object_key: "blocks/demo".to_owned(),
                        object_offset: 0,
                        len: 12,
                        output_offset: 0,
                    }],
                ),
            )
            .unwrap();

        let plan = backend.cached_read_body_plan(inode, 7, 4, 4).unwrap();
        assert_eq!(
            plan.blocks,
            vec![ObjectReadBlock {
                object_key: "blocks/demo".to_owned(),
                object_offset: 4,
                len: 4,
                output_offset: 0,
            }]
        );
        let stats = backend.object_pipeline_stats().unwrap();
        assert_eq!(stats.read_plan_cache_hits, 1);
        assert_eq!(stats.read_plan_cache_misses, 0);
    }

    #[test]
    fn client_backend_caches_new_file_staged_read_plan() {
        let metadata = MetadataClient::new(MetadataClientOptions::new(SocketAddr::from((
            [127, 0, 0, 1],
            9,
        ))));
        let objects = MemoryObjectStore::new();
        let backend =
            ClientFuseBackend::new(metadata, objects.clone(), &FuseOptions::default()).unwrap();
        let inode = InodeId::new(42).unwrap();
        let write = nokv_object::put_chunked_object(
            &objects,
            b"abcdefghijkl",
            ChunkWriteOptions {
                manifest_id: "checkpoint.bin".to_owned(),
                mount: 1,
                inode: inode.get(),
                generation: 7,
                chunk_size: 8,
                block_size: 4,
            },
        )
        .unwrap();

        backend
            .cache_new_file_staged_read_plan(inode, 7, write.size, &write.chunks)
            .unwrap();

        let plan = backend.cached_read_body_plan(inode, 7, 4, 4).unwrap();
        let read = objects
            .read_blocks(
                None::<&nokv_object::MemoryBlockCache>,
                plan.output_len,
                &plan.blocks,
            )
            .unwrap();
        assert_eq!(read.bytes, b"efgh");
        let stats = backend.object_pipeline_stats().unwrap();
        assert_eq!(stats.read_plan_cache_hits, 1);
        assert_eq!(stats.read_plan_cache_misses, 0);
    }
}
