use crate::cache::BlockCache;
use crate::chunk::{BlockReadOptions, BlockReadOutcome, ChunkStore};
use crate::pipeline::{
    FileReadPipeline, FileReadRequest, ObjectPrefetchRequest, ObjectReadPlan, ReadAheadHint,
};
use crate::store::{ObjectError, ObjectStore};

use super::placement::{BlockPlacement, DataTransport};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayoutReadOutcome {
    pub bytes: Vec<u8>,
    pub readahead: Option<ReadAheadHint>,
    pub cache_warmup: Option<ObjectPrefetchRequest>,
    pub stats: DataFabricReadStats,
    pub placements: Vec<BlockPlacement>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DataFabricReadStats {
    pub planned_blocks: u64,
    pub local_nvme_hits: u64,
    pub object_fallbacks: u64,
    pub object_gets: u64,
    pub object_get_bytes: u64,
    pub coalesced_ranges: u64,
    pub coalesced_range_bytes: u64,
    pub cache_hits: u64,
    pub cache_hit_bytes: u64,
}

pub struct LayoutReadExecutor<'a, Store> {
    store: &'a Store,
}

impl<'a, Store> LayoutReadExecutor<'a, Store> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }
}

impl<Store> LayoutReadExecutor<'_, Store>
where
    Store: ChunkStore + ObjectStore,
{
    pub fn read_plan<C>(
        &self,
        pipeline: &mut FileReadPipeline,
        cache: Option<&C>,
        file_size: u64,
        offset: u64,
        plan: &ObjectReadPlan,
    ) -> Result<LayoutReadOutcome, ObjectError>
    where
        C: BlockCache + ?Sized,
    {
        self.read_plan_with_options(
            pipeline,
            cache,
            file_size,
            offset,
            plan,
            BlockReadOptions::default(),
        )
    }

    pub fn read_plan_with_options<C>(
        &self,
        pipeline: &mut FileReadPipeline,
        cache: Option<&C>,
        file_size: u64,
        offset: u64,
        plan: &ObjectReadPlan,
        read_options: BlockReadOptions,
    ) -> Result<LayoutReadOutcome, ObjectError>
    where
        C: BlockCache + ?Sized,
    {
        let placements = self.store.resolve_read_placements(&plan.blocks)?;
        let read = pipeline.read_blocks_with_options(
            self.store,
            cache,
            FileReadRequest {
                file_size,
                offset,
                output_len: plan.output_len,
                blocks: &plan.blocks,
            },
            read_options,
        )?;
        let blocks = read.blocks;
        let stats = DataFabricReadStats::from_block_read(&placements, &blocks);
        Ok(LayoutReadOutcome {
            bytes: blocks.bytes,
            readahead: read.readahead,
            cache_warmup: read.cache_warmup,
            stats,
            placements,
        })
    }
}

impl DataFabricReadStats {
    pub fn from_block_read(placements: &[BlockPlacement], blocks: &BlockReadOutcome) -> Self {
        let mut stats = Self {
            planned_blocks: placements.len() as u64,
            object_gets: blocks.object_gets as u64,
            object_get_bytes: blocks.object_get_bytes,
            coalesced_ranges: blocks.coalesced_gets as u64,
            coalesced_range_bytes: blocks.coalesced_get_bytes,
            cache_hits: blocks.cache_hits as u64,
            cache_hit_bytes: blocks.cache_hit_bytes,
            ..Self::default()
        };
        for placement in placements {
            match placement.transport {
                DataTransport::ObjectTcpGet => {
                    stats.object_fallbacks = stats.object_fallbacks.saturating_add(1);
                }
                DataTransport::LocalNvmeRead => {
                    stats.local_nvme_hits = stats.local_nvme_hits.saturating_add(1);
                }
            }
        }
        stats
    }

    pub fn saturating_add_assign(&mut self, other: Self) {
        self.planned_blocks = self.planned_blocks.saturating_add(other.planned_blocks);
        self.local_nvme_hits = self.local_nvme_hits.saturating_add(other.local_nvme_hits);
        self.object_fallbacks = self.object_fallbacks.saturating_add(other.object_fallbacks);
        self.object_gets = self.object_gets.saturating_add(other.object_gets);
        self.object_get_bytes = self.object_get_bytes.saturating_add(other.object_get_bytes);
        self.coalesced_ranges = self.coalesced_ranges.saturating_add(other.coalesced_ranges);
        self.coalesced_range_bytes = self
            .coalesced_range_bytes
            .saturating_add(other.coalesced_range_bytes);
        self.cache_hits = self.cache_hits.saturating_add(other.cache_hits);
        self.cache_hit_bytes = self.cache_hit_bytes.saturating_add(other.cache_hit_bytes);
    }
}
