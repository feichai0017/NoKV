use std::marker::PhantomData;
use std::sync::mpsc::{self, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::cache::{BlockCache, WritebackCache, WritebackTicket};
use crate::chunk::{
    put_chunked_ranges_parallel_with_timings, ChunkWriteOptions, ChunkWriteRange,
    ChunkWriteTimings, ChunkedWrite,
};
use crate::pipeline::ObjectSliceWriter;
use crate::store::{ObjectBytes, ObjectError, ObjectStore};

#[derive(Clone)]
pub struct ObjectWritebackUploader<O> {
    sender: mpsc::SyncSender<ObjectWritebackJob>,
    stats: Arc<Mutex<ObjectWritebackStats>>,
    cache: Option<WritebackCache>,
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
pub struct ObjectWritebackOptions {
    pub queue_capacity: usize,
    pub workers: usize,
    pub upload_workers_per_request: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WritebackUploadRange {
    pub logical_offset: u64,
    source: WritebackUploadSource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum WritebackUploadSource {
    Cache(WritebackTicket),
    InlineBytes(ObjectBytes),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectWritebackRequest {
    pub ranges: Vec<WritebackUploadRange>,
    pub options: ChunkWriteOptions,
    pub block_index_base: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ObjectWritebackStats {
    pub enqueued: u64,
    pub inline: u64,
    pub completed: u64,
    pub failed: u64,
    pub staged_bytes: u64,
    pub uploaded_bytes: u64,
    pub queue_wait_ns: u64,
    pub queue_max_wait_ns: u64,
    pub upload_ns: u64,
    pub upload_max_ns: u64,
    pub collect_ns: u64,
    pub digest_ns: u64,
    pub store_put_ns: u64,
    pub cache_put_ns: u64,
}

struct ObjectWritebackJob {
    request: ObjectWritebackRequest,
    pending: PendingChunkedWrite,
    enqueued_at: Instant,
}

#[derive(Clone, Debug)]
struct ObjectWritebackUploadOutcome {
    written: ChunkedWrite,
    timings: ObjectWritebackTimings,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ObjectWritebackTimings {
    collect_ns: u64,
    chunk: ChunkWriteTimings,
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

impl WritebackUploadRange {
    pub fn cache(logical_offset: u64, ticket: WritebackTicket) -> Self {
        Self {
            logical_offset,
            source: WritebackUploadSource::Cache(ticket),
        }
    }

    pub fn inline_bytes(logical_offset: u64, bytes: impl Into<ObjectBytes>) -> Self {
        Self {
            logical_offset,
            source: WritebackUploadSource::InlineBytes(bytes.into()),
        }
    }

    pub fn len(&self) -> u64 {
        match &self.source {
            WritebackUploadSource::Cache(ticket) => ticket.len(),
            WritebackUploadSource::InlineBytes(bytes) => bytes.len() as u64,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn cache_ticket(&self) -> Option<&WritebackTicket> {
        match &self.source {
            WritebackUploadSource::Cache(ticket) => Some(ticket),
            WritebackUploadSource::InlineBytes(_) => None,
        }
    }

    pub fn into_cache_ticket(self) -> Option<WritebackTicket> {
        match self.source {
            WritebackUploadSource::Cache(ticket) => Some(ticket),
            WritebackUploadSource::InlineBytes(_) => None,
        }
    }
}

fn cache_tickets(request: &ObjectWritebackRequest) -> impl Iterator<Item = &WritebackTicket> {
    request
        .ranges
        .iter()
        .filter_map(WritebackUploadRange::cache_ticket)
}

impl<O> ObjectWritebackUploader<O>
where
    O: ObjectStore + Clone + Send + Sync + 'static,
{
    pub fn new(store: O, cache: WritebackCache, options: ObjectWritebackOptions) -> Self {
        Self::new_with_cache(store, Some(cache), options)
    }

    pub fn direct(store: O, options: ObjectWritebackOptions) -> Self {
        Self::new_with_cache(store, None, options)
    }

    fn new_with_cache(
        store: O,
        cache: Option<WritebackCache>,
        options: ObjectWritebackOptions,
    ) -> Self {
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
                let queue_wait = job.enqueued_at.elapsed();
                let upload_start = Instant::now();
                let result =
                    upload_writeback_request(&store, cache.as_ref(), job.request, upload_workers);
                let upload_elapsed = upload_start.elapsed();
                if let Ok(mut stats) = stats.lock() {
                    record_upload_result(&mut stats, &result, queue_wait, upload_elapsed);
                }
                job.pending.complete(result.map(|outcome| outcome.written));
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
            .map(WritebackUploadRange::len)
            .sum::<u64>();
        let tickets = cache_tickets(&request).cloned().collect::<Vec<_>>();
        let writeback = if tickets.is_empty() {
            None
        } else {
            Some(PendingWritebackCache {
                cache: self.cache.clone().ok_or_else(|| {
                    ObjectError::Backend(
                        "cache-backed writeback request requires a cache".to_owned(),
                    )
                })?,
                tickets,
            })
        };
        let pending = PendingChunkedWrite {
            inner: Arc::new(PendingChunkedWriteInner {
                state: Mutex::new(None),
                ready: Condvar::new(),
            }),
            writeback,
        };
        let job = ObjectWritebackJob {
            request,
            pending: pending.clone(),
            enqueued_at: Instant::now(),
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
                let queue_wait = job.enqueued_at.elapsed();
                let upload_start = Instant::now();
                let result = upload_writeback_request(
                    &self.store,
                    self.cache.as_ref(),
                    job.request,
                    self.upload_workers_per_request,
                );
                let upload_elapsed = upload_start.elapsed();
                let stats_result = self.record_upload_result(&result, queue_wait, upload_elapsed);
                job.pending.complete(result.map(|outcome| outcome.written));
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
        result: &Result<ObjectWritebackUploadOutcome, ObjectError>,
        queue_wait: Duration,
        upload_elapsed: Duration,
    ) -> Result<(), ObjectError> {
        self.with_stats(|stats| {
            record_upload_result(stats, result, queue_wait, upload_elapsed);
        })
    }
}

fn record_upload_result(
    stats: &mut ObjectWritebackStats,
    result: &Result<ObjectWritebackUploadOutcome, ObjectError>,
    queue_wait: Duration,
    upload_elapsed: Duration,
) {
    match result {
        Ok(outcome) => {
            stats.completed = stats.completed.saturating_add(1);
            stats.uploaded_bytes = stats
                .uploaded_bytes
                .saturating_add(outcome.written.object_put_bytes);
            stats.collect_ns = stats.collect_ns.saturating_add(outcome.timings.collect_ns);
            stats.digest_ns = stats
                .digest_ns
                .saturating_add(outcome.timings.chunk.digest_ns);
            stats.store_put_ns = stats
                .store_put_ns
                .saturating_add(outcome.timings.chunk.store_put_ns);
            stats.cache_put_ns = stats
                .cache_put_ns
                .saturating_add(outcome.timings.chunk.cache_put_ns);
        }
        Err(_) => {
            stats.failed = stats.failed.saturating_add(1);
        }
    }
    record_writeback_timing(stats, queue_wait, upload_elapsed);
}

fn record_writeback_timing(
    stats: &mut ObjectWritebackStats,
    queue_wait: Duration,
    upload_elapsed: Duration,
) {
    let queue_wait_ns = duration_ns(queue_wait);
    let upload_ns = duration_ns(upload_elapsed);
    stats.queue_wait_ns = stats.queue_wait_ns.saturating_add(queue_wait_ns);
    stats.queue_max_wait_ns = stats.queue_max_wait_ns.max(queue_wait_ns);
    stats.upload_ns = stats.upload_ns.saturating_add(upload_ns);
    stats.upload_max_ns = stats.upload_max_ns.max(upload_ns);
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn upload_writeback_request<O>(
    store: &O,
    cache: Option<&WritebackCache>,
    request: ObjectWritebackRequest,
    workers: usize,
) -> Result<ObjectWritebackUploadOutcome, ObjectError>
where
    O: ObjectStore + Sync,
{
    let ObjectWritebackRequest {
        ranges,
        options,
        block_index_base,
    } = request;
    let collect_start = Instant::now();
    let (ranges, cache_tickets) = collect_upload_ranges(ranges, cache, options.block_size)?;
    let collect_ns = duration_ns(collect_start.elapsed());
    let result = put_chunked_ranges_parallel_with_timings(
        store,
        ranges,
        options,
        block_index_base,
        workers,
        None::<&(dyn BlockCache + Sync)>,
    )
    .map(|(written, chunk)| ObjectWritebackUploadOutcome {
        written,
        timings: ObjectWritebackTimings { collect_ns, chunk },
    });
    if result.is_ok() {
        if let Some(cache) = cache {
            for ticket in cache_tickets {
                let _ = cache.remove(&ticket);
            }
        }
    }
    result
}

fn collect_upload_ranges(
    upload_ranges: Vec<WritebackUploadRange>,
    cache: Option<&WritebackCache>,
    block_size: usize,
) -> Result<(Vec<ChunkWriteRange>, Vec<WritebackTicket>), ObjectError> {
    if cache.is_none() && inline_ranges_are_block_framed(&upload_ranges, block_size)? {
        return Ok((collect_inline_ranges(upload_ranges), Vec::new()));
    }

    let mut writer = ObjectSliceWriter::new(block_size)?;
    let mut cache_tickets = Vec::new();
    for range in upload_ranges {
        let logical_offset = range.logical_offset;
        match range.source {
            WritebackUploadSource::Cache(ticket) => {
                let bytes = cache
                    .ok_or_else(|| {
                        ObjectError::Backend("writeback cache source requires a cache".to_owned())
                    })?
                    .read(&ticket)?;
                cache_tickets.push(ticket);
                writer.write_at(logical_offset, bytes)?;
            }
            WritebackUploadSource::InlineBytes(bytes) => {
                writer.write_at(logical_offset, bytes)?;
            }
        }
    }
    Ok((writer.finish()?, cache_tickets))
}

fn inline_ranges_are_block_framed(
    upload_ranges: &[WritebackUploadRange],
    block_size: usize,
) -> Result<bool, ObjectError> {
    if block_size == 0 {
        return Err(ObjectError::InvalidChunkLayout);
    }
    let block_size = u64::try_from(block_size).map_err(|_| ObjectError::InvalidChunkLayout)?;
    let mut previous_end = None;
    let mut has_payload = false;
    for range in upload_ranges {
        let WritebackUploadSource::InlineBytes(bytes) = &range.source else {
            return Ok(false);
        };
        if bytes.is_empty() {
            continue;
        }
        if range.logical_offset % block_size != 0 {
            return Ok(false);
        }
        let end = range
            .logical_offset
            .checked_add(bytes.len() as u64)
            .ok_or(ObjectError::InvalidRange)?;
        if let Some(previous_end) = previous_end {
            if range.logical_offset < previous_end {
                return Ok(false);
            }
            if range.logical_offset == previous_end && previous_end % block_size != 0 {
                return Ok(false);
            }
        }
        previous_end = Some(end);
        has_payload = true;
    }
    Ok(has_payload)
}

fn collect_inline_ranges(upload_ranges: Vec<WritebackUploadRange>) -> Vec<ChunkWriteRange> {
    upload_ranges
        .into_iter()
        .filter_map(|range| match range.source {
            WritebackUploadSource::InlineBytes(bytes) if !bytes.is_empty() => {
                Some(ChunkWriteRange {
                    logical_offset: range.logical_offset,
                    bytes,
                })
            }
            _ => None,
        })
        .collect()
}
