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
            clock: AtomicU64::new(1),
            reserved_version: AtomicU64::new(1),
            next_inode: AtomicU64::new(InodeId::ROOT_RAW + 1),
            reserved_next_inode: AtomicU64::new(InodeId::ROOT_RAW + 1),
            block_cache: MemoryBlockCache::default(),
            block_cache_enabled: AtomicBool::new(true),
            object_puts: AtomicU64::new(0),
            object_gets: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            manifest_chunks: AtomicU64::new(0),
            manifest_blocks: AtomicU64::new(0),
            path_index_lookup_total: AtomicU64::new(0),
            path_index_hit_total: AtomicU64::new(0),
            path_index_miss_total: AtomicU64::new(0),
            path_index_stale_total: AtomicU64::new(0),
            path_index_fallback_total: AtomicU64::new(0),
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
            clock: AtomicU64::new(allocator.last_commit_version),
            reserved_version: AtomicU64::new(allocator.last_commit_version),
            next_inode: AtomicU64::new(allocator.next_inode),
            reserved_next_inode: AtomicU64::new(allocator.next_inode),
            block_cache: MemoryBlockCache::default(),
            block_cache_enabled: AtomicBool::new(true),
            object_puts: AtomicU64::new(0),
            object_gets: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            manifest_chunks: AtomicU64::new(0),
            manifest_blocks: AtomicU64::new(0),
            path_index_lookup_total: AtomicU64::new(0),
            path_index_hit_total: AtomicU64::new(0),
            path_index_miss_total: AtomicU64::new(0),
            path_index_stale_total: AtomicU64::new(0),
            path_index_fallback_total: AtomicU64::new(0),
            read_dir_plus_total: AtomicU64::new(0),
            read_dir_plus_entry_total: AtomicU64::new(0),
            read_dir_plus_projection_hit_total: AtomicU64::new(0),
        })
    }

    pub fn object_stats(&self) -> ObjectTransferStats {
        ObjectTransferStats {
            object_puts: self.object_puts.load(Ordering::Relaxed),
            object_gets: self.object_gets.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
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
            path_index_fallback_total: self.path_index_fallback_total.load(Ordering::Relaxed),
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
