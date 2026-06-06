use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::mpsc::{self, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use crate::cache::{BlockCache, ObjectBlockCache, WritebackCache, WritebackTicket};
use crate::chunk::{
    put_chunked_ranges_parallel, BlockReadOutcome, ChunkStore, ChunkWriteOptions, ChunkWriteRange,
    ChunkedWrite, DirtyChunkExtent, ObjectReadBlock, StagedObjectSet, StoredChunk,
    DEFAULT_BLOCK_SIZE,
};
use crate::store::{ObjectError, ObjectStore};

#[derive(Clone, Debug)]
pub struct FileWritePipeline {
    options: ChunkWriteOptions,
    staged_chunks: Vec<StoredChunk>,
    staged: StagedObjectSet,
    dirty_extents: Vec<DirtyChunkExtent>,
    next_block_index: u64,
}

#[derive(Clone, Debug, Default)]
pub struct FileReadPipeline {
    options: FileReadPipelineOptions,
    last_read_end: Option<u64>,
    stats: FileReadPipelineStats,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectReadPlan {
    pub output_len: usize,
    pub blocks: Vec<ObjectReadBlock>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ObjectReadPlanKey {
    pub object_id: u64,
    pub generation: u64,
    pub offset: u64,
    pub len: usize,
}

#[derive(Clone, Debug)]
pub struct ObjectReadPlanCache {
    capacity: usize,
    plans: HashMap<ObjectReadPlanKey, ObjectReadPlan>,
    order: VecDeque<ObjectReadPlanKey>,
}

#[derive(Clone)]
pub struct ObjectPrefetcher<O, C = ObjectBlockCache> {
    sender: mpsc::SyncSender<ObjectPrefetchRequest>,
    stats: Arc<Mutex<ObjectPrefetchStats>>,
    _state: PhantomData<(O, C)>,
}

#[derive(Clone)]
pub struct ObjectWritebackUploader<O> {
    sender: mpsc::SyncSender<ObjectWritebackJob>,
    stats: Arc<Mutex<ObjectWritebackStats>>,
    cache: WritebackCache,
    store: O,
    upload_workers_per_request: usize,
    _state: PhantomData<O>,
}

#[derive(Clone, Debug)]
pub struct PendingChunkedWrite {
    inner: Arc<PendingChunkedWriteInner>,
    writeback: Option<PendingWritebackCache>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileReadPipelineOptions {
    pub max_readahead_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectPrefetchOptions {
    pub queue_capacity: usize,
    pub workers: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectWritebackOptions {
    pub queue_capacity: usize,
    pub workers: usize,
    pub upload_workers_per_request: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FileReadPipelineStats {
    pub reads: u64,
    pub read_bytes: u64,
    pub sequential_reads: u64,
    pub readahead_hints: u64,
    pub readahead_hint_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileReadOutcome {
    pub blocks: BlockReadOutcome,
    pub readahead: Option<ReadAheadHint>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadAheadHint {
    pub offset: u64,
    pub len: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectPrefetchRequest {
    pub output_len: usize,
    pub blocks: Vec<ObjectReadBlock>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WritebackUploadRange {
    pub logical_offset: u64,
    pub ticket: WritebackTicket,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectWritebackRequest {
    pub ranges: Vec<WritebackUploadRange>,
    pub options: ChunkWriteOptions,
    pub block_index_base: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ObjectPrefetchStats {
    pub enqueued: u64,
    pub dropped: u64,
    pub completed: u64,
    pub failed: u64,
    pub object_gets: u64,
    pub object_get_bytes: u64,
    pub cache_hits: u64,
    pub cache_hit_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ObjectWritebackStats {
    pub enqueued: u64,
    pub inline: u64,
    pub fallback: u64,
    pub completed: u64,
    pub failed: u64,
    pub staged_bytes: u64,
    pub uploaded_bytes: u64,
}

struct ObjectWritebackJob {
    request: ObjectWritebackRequest,
    pending: PendingChunkedWrite,
}

#[derive(Debug)]
struct PendingChunkedWriteInner {
    state: Mutex<Option<Result<ChunkedWrite, ObjectError>>>,
    ready: Condvar,
}

#[derive(Clone, Debug)]
struct PendingWritebackCache {
    cache: WritebackCache,
    tickets: Vec<WritebackTicket>,
}

impl FileWritePipeline {
    pub fn new(options: ChunkWriteOptions) -> Result<Self, ObjectError> {
        options.validate()?;
        Ok(Self {
            options,
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
}

impl Default for FileReadPipelineOptions {
    fn default() -> Self {
        Self {
            max_readahead_bytes: DEFAULT_BLOCK_SIZE,
        }
    }
}

impl Default for ObjectPrefetchOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 64,
            workers: 1,
        }
    }
}

impl Default for ObjectWritebackOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 64,
            workers: 2,
            upload_workers_per_request: 1,
        }
    }
}

impl PendingChunkedWrite {
    pub fn ready(result: Result<ChunkedWrite, ObjectError>) -> Self {
        let pending = Self {
            inner: Arc::new(PendingChunkedWriteInner {
                state: Mutex::new(None),
                ready: Condvar::new(),
            }),
            writeback: None,
        };
        pending.complete(result);
        pending
    }

    pub fn wait(&self) -> Result<ChunkedWrite, ObjectError> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(ObjectError::from_poisoned_lock)?;
        loop {
            if let Some(result) = state.as_ref() {
                return result.clone();
            }
            state = self
                .inner
                .ready
                .wait(state)
                .map_err(ObjectError::from_poisoned_lock)?;
        }
    }

    pub fn discard_writeback_cache(&self) -> Result<usize, ObjectError> {
        let Some(writeback) = &self.writeback else {
            return Ok(0);
        };
        let mut removed = 0_usize;
        for ticket in &writeback.tickets {
            if writeback.cache.remove(ticket)? {
                removed = removed.saturating_add(1);
            }
        }
        Ok(removed)
    }

    fn complete(&self, result: Result<ChunkedWrite, ObjectError>) {
        if let Ok(mut state) = self.inner.state.lock() {
            *state = Some(result);
            self.inner.ready.notify_all();
        }
    }
}

impl FileReadPipeline {
    pub fn new(options: FileReadPipelineOptions) -> Self {
        Self {
            options,
            last_read_end: None,
            stats: FileReadPipelineStats::default(),
        }
    }

    pub fn read_blocks<S, C>(
        &mut self,
        store: &S,
        cache: Option<&C>,
        file_size: u64,
        offset: u64,
        output_len: usize,
        blocks: &[ObjectReadBlock],
    ) -> Result<FileReadOutcome, ObjectError>
    where
        S: ChunkStore,
        C: BlockCache + ?Sized,
    {
        let read = store.read_blocks(cache, output_len, blocks)?;
        let starts_stream = self.last_read_end.is_none() && offset == 0;
        let sequential = self.last_read_end == Some(offset) || starts_stream;
        let read_end = offset
            .checked_add(u64::try_from(output_len).map_err(|_| ObjectError::InvalidRange)?)
            .ok_or(ObjectError::InvalidRange)?;
        self.last_read_end = Some(read_end);
        self.stats.reads = self.stats.reads.saturating_add(1);
        self.stats.read_bytes = self.stats.read_bytes.saturating_add(output_len as u64);
        let readahead =
            if sequential && read_end < file_size && self.options.max_readahead_bytes > 0 {
                self.stats.sequential_reads = self.stats.sequential_reads.saturating_add(1);
                let len = self
                    .options
                    .max_readahead_bytes
                    .min(output_len)
                    .min(usize::try_from(file_size - read_end).unwrap_or(usize::MAX));
                self.stats.readahead_hints = self.stats.readahead_hints.saturating_add(1);
                self.stats.readahead_hint_bytes =
                    self.stats.readahead_hint_bytes.saturating_add(len as u64);
                Some(ReadAheadHint {
                    offset: read_end,
                    len,
                })
            } else {
                None
            };
        Ok(FileReadOutcome {
            blocks: read,
            readahead,
        })
    }

    pub fn stats(&self) -> FileReadPipelineStats {
        self.stats
    }
}

impl ObjectReadPlan {
    pub fn new(output_len: usize, blocks: Vec<ObjectReadBlock>) -> Self {
        Self { output_len, blocks }
    }
}

impl ObjectReadPlanKey {
    pub fn new(object_id: u64, generation: u64, offset: u64, len: usize) -> Self {
        Self {
            object_id,
            generation,
            offset,
            len,
        }
    }
}

impl ObjectReadPlanCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            plans: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn get(&mut self, key: &ObjectReadPlanKey) -> Option<ObjectReadPlan> {
        let plan = self.plans.get(key)?.clone();
        self.order.retain(|existing| existing != key);
        self.order.push_back(*key);
        Some(plan)
    }

    pub fn insert(&mut self, key: ObjectReadPlanKey, plan: ObjectReadPlan) {
        self.order.retain(|existing| existing != &key);
        self.order.push_back(key);
        self.plans.insert(key, plan);
        while self.plans.len() > self.capacity {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.plans.remove(&oldest);
        }
    }

    pub fn len(&self) -> usize {
        self.plans.len()
    }

    pub fn is_empty(&self) -> bool {
        self.plans.is_empty()
    }
}

impl ObjectPrefetchRequest {
    pub fn new(output_len: usize, blocks: Vec<ObjectReadBlock>) -> Self {
        Self { output_len, blocks }
    }

    pub fn is_empty(&self) -> bool {
        self.output_len == 0 || self.blocks.is_empty()
    }
}

impl<O, C> ObjectPrefetcher<O, C>
where
    O: ChunkStore + Clone + Send + 'static,
    C: BlockCache + Clone + Send + 'static,
{
    pub fn new(store: O, cache: C, options: ObjectPrefetchOptions) -> Self {
        let capacity = options.queue_capacity.max(1);
        let workers = options.workers.max(1);
        let (sender, receiver) = mpsc::sync_channel::<ObjectPrefetchRequest>(capacity);
        let receiver = Arc::new(Mutex::new(receiver));
        let stats = Arc::new(Mutex::new(ObjectPrefetchStats::default()));
        for worker in 0..workers {
            let store = store.clone();
            let cache = cache.clone();
            let receiver = Arc::clone(&receiver);
            let stats = Arc::clone(&stats);
            let name = format!("nokv-prefetch-{worker}");
            let _ = thread::Builder::new().name(name).spawn(move || loop {
                let request = {
                    let Ok(receiver) = receiver.lock() else {
                        break;
                    };
                    match receiver.recv() {
                        Ok(request) => request,
                        Err(_) => break,
                    }
                };
                match store.read_blocks(Some(&cache), request.output_len, &request.blocks) {
                    Ok(outcome) => {
                        if let Ok(mut stats) = stats.lock() {
                            stats.completed = stats.completed.saturating_add(1);
                            stats.object_gets =
                                stats.object_gets.saturating_add(outcome.object_gets as u64);
                            stats.object_get_bytes = stats
                                .object_get_bytes
                                .saturating_add(outcome.object_get_bytes);
                            stats.cache_hits =
                                stats.cache_hits.saturating_add(outcome.cache_hits as u64);
                            stats.cache_hit_bytes = stats
                                .cache_hit_bytes
                                .saturating_add(outcome.cache_hit_bytes);
                        }
                    }
                    Err(_) => {
                        if let Ok(mut stats) = stats.lock() {
                            stats.failed = stats.failed.saturating_add(1);
                        }
                    }
                }
            });
        }
        Self {
            sender,
            stats,
            _state: PhantomData,
        }
    }

    pub fn submit(&self, request: ObjectPrefetchRequest) -> Result<bool, ObjectError> {
        if request.is_empty() {
            return Ok(false);
        }
        match self.sender.try_send(request) {
            Ok(()) => {
                self.with_stats(|stats| {
                    stats.enqueued = stats.enqueued.saturating_add(1);
                })?;
                Ok(true)
            }
            Err(TrySendError::Full(_)) => {
                self.with_stats(|stats| {
                    stats.dropped = stats.dropped.saturating_add(1);
                })?;
                Ok(false)
            }
            Err(TrySendError::Disconnected(_)) => Err(ObjectError::Backend(
                "object prefetch worker stopped".to_owned(),
            )),
        }
    }

    pub fn stats(&self) -> Result<ObjectPrefetchStats, ObjectError> {
        self.stats
            .lock()
            .map_err(ObjectError::from_poisoned_lock)
            .map(|stats| *stats)
    }

    fn with_stats(&self, update: impl FnOnce(&mut ObjectPrefetchStats)) -> Result<(), ObjectError> {
        let mut stats = self.stats.lock().map_err(ObjectError::from_poisoned_lock)?;
        update(&mut stats);
        Ok(())
    }
}

impl<O> ObjectWritebackUploader<O>
where
    O: ObjectStore + Clone + Send + Sync + 'static,
{
    pub fn new(store: O, cache: WritebackCache, options: ObjectWritebackOptions) -> Self {
        let capacity = options.queue_capacity.max(1);
        let workers = options.workers.max(1);
        let (sender, receiver) = mpsc::sync_channel::<ObjectWritebackJob>(capacity);
        let receiver = Arc::new(Mutex::new(receiver));
        let stats = Arc::new(Mutex::new(ObjectWritebackStats::default()));
        for worker in 0..workers {
            let store = store.clone();
            let cache = cache.clone();
            let receiver = Arc::clone(&receiver);
            let stats = Arc::clone(&stats);
            let upload_workers = options.upload_workers_per_request.max(1);
            let name = format!("nokv-writeback-{worker}");
            let _ = thread::Builder::new().name(name).spawn(move || loop {
                let job = {
                    let Ok(receiver) = receiver.lock() else {
                        break;
                    };
                    match receiver.recv() {
                        Ok(job) => job,
                        Err(_) => break,
                    }
                };
                let result = upload_writeback_request(&store, &cache, job.request, upload_workers);
                if let Ok(mut stats) = stats.lock() {
                    match &result {
                        Ok(written) => {
                            stats.completed = stats.completed.saturating_add(1);
                            stats.uploaded_bytes = stats
                                .uploaded_bytes
                                .saturating_add(written.object_put_bytes);
                        }
                        Err(_) => {
                            stats.failed = stats.failed.saturating_add(1);
                        }
                    }
                }
                job.pending.complete(result);
            });
        }
        Self {
            sender,
            stats,
            cache,
            store,
            upload_workers_per_request: options.upload_workers_per_request.max(1),
            _state: PhantomData,
        }
    }

    pub fn submit(
        &self,
        request: ObjectWritebackRequest,
    ) -> Result<PendingChunkedWrite, ObjectError> {
        let staged_bytes = request
            .ranges
            .iter()
            .map(|range| range.ticket.len())
            .sum::<u64>();
        let tickets = request
            .ranges
            .iter()
            .map(|range| range.ticket.clone())
            .collect::<Vec<_>>();
        let pending = PendingChunkedWrite {
            inner: Arc::new(PendingChunkedWriteInner {
                state: Mutex::new(None),
                ready: Condvar::new(),
            }),
            writeback: Some(PendingWritebackCache {
                cache: self.cache.clone(),
                tickets,
            }),
        };
        let job = ObjectWritebackJob {
            request,
            pending: pending.clone(),
        };
        self.with_stats(|stats| {
            stats.enqueued = stats.enqueued.saturating_add(1);
            stats.staged_bytes = stats.staged_bytes.saturating_add(staged_bytes);
        })?;
        match self.sender.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(job)) => {
                self.with_stats(|stats| {
                    stats.inline = stats.inline.saturating_add(1);
                })?;
                let result = upload_writeback_request(
                    &self.store,
                    &self.cache,
                    job.request,
                    self.upload_workers_per_request,
                );
                let stats_result = self.record_upload_result(&result);
                job.pending.complete(result);
                stats_result?;
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err(ObjectError::Backend(
                    "object writeback worker stopped".to_owned(),
                ));
            }
        }
        Ok(pending)
    }

    pub fn stats(&self) -> Result<ObjectWritebackStats, ObjectError> {
        self.stats
            .lock()
            .map_err(ObjectError::from_poisoned_lock)
            .map(|stats| *stats)
    }

    pub fn record_fallback(&self) -> Result<(), ObjectError> {
        self.with_stats(|stats| {
            stats.fallback = stats.fallback.saturating_add(1);
        })
    }

    fn with_stats(
        &self,
        update: impl FnOnce(&mut ObjectWritebackStats),
    ) -> Result<(), ObjectError> {
        let mut stats = self.stats.lock().map_err(ObjectError::from_poisoned_lock)?;
        update(&mut stats);
        Ok(())
    }

    fn record_upload_result(
        &self,
        result: &Result<ChunkedWrite, ObjectError>,
    ) -> Result<(), ObjectError> {
        self.with_stats(|stats| match result {
            Ok(written) => {
                stats.completed = stats.completed.saturating_add(1);
                stats.uploaded_bytes = stats
                    .uploaded_bytes
                    .saturating_add(written.object_put_bytes);
            }
            Err(_) => {
                stats.failed = stats.failed.saturating_add(1);
            }
        })
    }
}

fn upload_writeback_request<O>(
    store: &O,
    cache: &WritebackCache,
    request: ObjectWritebackRequest,
    workers: usize,
) -> Result<ChunkedWrite, ObjectError>
where
    O: ObjectStore + Sync,
{
    let mut ranges = Vec::with_capacity(request.ranges.len());
    for range in &request.ranges {
        ranges.push(ChunkWriteRange {
            logical_offset: range.logical_offset,
            bytes: cache.read(&range.ticket)?,
        });
    }
    let result = put_chunked_ranges_parallel(
        store,
        &ranges,
        request.options,
        request.block_index_base,
        workers,
    );
    if result.is_ok() {
        for range in &request.ranges {
            let _ = cache.remove(&range.ticket);
        }
    }
    result
}
