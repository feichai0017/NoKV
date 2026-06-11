use super::*;

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn new(mount: MountId, metadata: M, objects: O) -> Self {
        Self {
            mount,
            metadata,
            objects,
            allocator_gate: Mutex::new(()),
            backup_gate: Mutex::new(()),
            path_resolution_cache: new_path_resolution_cache_shards(),
            path_index_lookup_cache: new_path_index_lookup_cache_shards(),
            path_index_validation_cache: new_path_index_validation_cache_shards(),
            advisory_locks: Mutex::new(AdvisoryLockTable::default()),
            clock: AtomicU64::new(1),
            reserved_version: AtomicU64::new(1),
            next_inode: AtomicU64::new(InodeId::ROOT_RAW + 1),
            reserved_next_inode: AtomicU64::new(InodeId::ROOT_RAW + 1),
            epoch: AtomicU64::new(1),
            block_cache: MemoryBlockCache::default(),
            block_cache_enabled: AtomicBool::new(true),
            watch_logging_enabled: AtomicBool::new(false),
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
            path_index_lookup_total: AtomicU64::new(0),
            path_index_hit_total: AtomicU64::new(0),
            path_index_miss_total: AtomicU64::new(0),
            path_index_stale_total: AtomicU64::new(0),
            path_index_scan_stale_total: AtomicU64::new(0),
            path_index_fallback_total: AtomicU64::new(0),
            create_files_batch_total: AtomicU64::new(0),
            create_files_entry_total: AtomicU64::new(0),
            create_dirs_batch_total: AtomicU64::new(0),
            create_dirs_entry_total: AtomicU64::new(0),
            read_dir_plus_total: AtomicU64::new(0),
            read_dir_plus_entry_total: AtomicU64::new(0),
            read_dir_plus_projection_hit_total: AtomicU64::new(0),
        }
    }

    pub fn open_existing(mount: MountId, metadata: M, objects: O) -> Result<Self, MetadError> {
        let allocator = recover_allocator_state(&metadata, mount)?;
        Ok(Self {
            mount,
            metadata,
            objects,
            allocator_gate: Mutex::new(()),
            backup_gate: Mutex::new(()),
            path_resolution_cache: new_path_resolution_cache_shards(),
            path_index_lookup_cache: new_path_index_lookup_cache_shards(),
            path_index_validation_cache: new_path_index_validation_cache_shards(),
            advisory_locks: Mutex::new(AdvisoryLockTable::default()),
            clock: AtomicU64::new(allocator.last_commit_version),
            reserved_version: AtomicU64::new(allocator.last_commit_version),
            next_inode: AtomicU64::new(allocator.next_inode),
            reserved_next_inode: AtomicU64::new(allocator.next_inode),
            epoch: AtomicU64::new(allocator.epoch),
            block_cache: MemoryBlockCache::default(),
            block_cache_enabled: AtomicBool::new(true),
            watch_logging_enabled: AtomicBool::new(false),
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
            path_index_lookup_total: AtomicU64::new(0),
            path_index_hit_total: AtomicU64::new(0),
            path_index_miss_total: AtomicU64::new(0),
            path_index_stale_total: AtomicU64::new(0),
            path_index_scan_stale_total: AtomicU64::new(0),
            path_index_fallback_total: AtomicU64::new(0),
            create_files_batch_total: AtomicU64::new(0),
            create_files_entry_total: AtomicU64::new(0),
            create_dirs_batch_total: AtomicU64::new(0),
            create_dirs_entry_total: AtomicU64::new(0),
            read_dir_plus_total: AtomicU64::new(0),
            read_dir_plus_entry_total: AtomicU64::new(0),
            read_dir_plus_projection_hit_total: AtomicU64::new(0),
        })
    }

    pub fn object_stats(&self) -> ObjectTransferStats {
        ObjectTransferStats {
            object_puts: self.object_puts.load(Ordering::Relaxed),
            object_put_bytes: self.object_put_bytes.load(Ordering::Relaxed),
            object_gets: self.object_gets.load(Ordering::Relaxed),
            object_get_bytes: self.object_get_bytes.load(Ordering::Relaxed),
            coalesced_gets: self.coalesced_gets.load(Ordering::Relaxed),
            coalesced_get_bytes: self.coalesced_get_bytes.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_hit_bytes: self.cache_hit_bytes.load(Ordering::Relaxed),
            prefetch_enqueued: 0,
            prefetch_dropped: 0,
            prefetch_completed: 0,
            prefetch_failed: 0,
            prefetch_object_gets: 0,
            prefetch_object_get_bytes: 0,
            prefetch_cache_hits: 0,
            prefetch_cache_hit_bytes: 0,
            read_plan_cache_hits: 0,
            read_plan_cache_misses: 0,
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

    pub fn metadata_service_stats(&self) -> MetadataServiceStats {
        MetadataServiceStats {
            path_index_lookup_total: self.path_index_lookup_total.load(Ordering::Relaxed),
            path_index_hit_total: self.path_index_hit_total.load(Ordering::Relaxed),
            path_index_miss_total: self.path_index_miss_total.load(Ordering::Relaxed),
            path_index_stale_total: self.path_index_stale_total.load(Ordering::Relaxed),
            path_index_scan_stale_total: self.path_index_scan_stale_total.load(Ordering::Relaxed),
            path_index_fallback_total: self.path_index_fallback_total.load(Ordering::Relaxed),
            create_files_batch_total: self.create_files_batch_total.load(Ordering::Relaxed),
            create_files_entry_total: self.create_files_entry_total.load(Ordering::Relaxed),
            create_dirs_batch_total: self.create_dirs_batch_total.load(Ordering::Relaxed),
            create_dirs_entry_total: self.create_dirs_entry_total.load(Ordering::Relaxed),
            read_dir_plus_total: self.read_dir_plus_total.load(Ordering::Relaxed),
            read_dir_plus_entry_total: self.read_dir_plus_entry_total.load(Ordering::Relaxed),
            read_dir_plus_projection_hit_total: self
                .read_dir_plus_projection_hit_total
                .load(Ordering::Relaxed),
        }
    }

    pub fn mount_id(&self) -> MountId {
        self.mount
    }

    pub fn metadata_store(&self) -> &M {
        &self.metadata
    }

    pub fn set_block_cache_enabled(&self, enabled: bool) {
        self.block_cache_enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn block_cache_enabled(&self) -> bool {
        self.block_cache_enabled.load(Ordering::Relaxed)
    }
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore + MetadataStoreStatsProvider,
    O: ObjectStore,
{
    pub fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.metadata.metadata_store_stats()
    }
}
