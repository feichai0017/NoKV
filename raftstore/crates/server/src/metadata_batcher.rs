use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_raftnode::MetadataCommandExecutor;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinSet;

const COMMIT_BATCH_MAX: usize = 64;
const COMMIT_BATCH_COALESCE_DELAY: Duration = Duration::from_micros(250);
const COMMIT_BATCH_CHANNEL: usize = 1024;
const COMMIT_BATCH_INFLIGHT: usize = 4;

#[derive(Clone)]
pub struct MetadataCommitBatcher<E> {
    tx: mpsc::Sender<BatchItem>,
    _engine: std::marker::PhantomData<E>,
}

struct BatchItem {
    request: metadatapb::MetadataCommitRequest,
    reply: oneshot::Sender<nokv_metadata_state::Result<metadatapb::MetadataCommitResponse>>,
    enqueued_at: Instant,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataBatchMetricsSnapshot {
    pub requests_total: u64,
    pub queue_depth_current: u64,
    pub queue_depth_max: u64,
    pub enqueue_block_ns_total: u64,
    pub enqueue_block_ns_max: u64,
    pub batches_total: u64,
    pub batch_commands_total: u64,
    pub batch_commands_max: u64,
    pub batch_wait_ns_total: u64,
    pub batch_wait_ns_max: u64,
    pub inflight_batches_current: u64,
    pub inflight_batches_max: u64,
    pub pipeline_block_ns_total: u64,
    pub pipeline_block_ns_max: u64,
    pub batch_execute_ns_total: u64,
    pub batch_execute_ns_max: u64,
}

struct MetadataBatchMetrics {
    requests_total: AtomicU64,
    queue_depth_current: AtomicU64,
    queue_depth_max: AtomicU64,
    enqueue_block_ns_total: AtomicU64,
    enqueue_block_ns_max: AtomicU64,
    batches_total: AtomicU64,
    batch_commands_total: AtomicU64,
    batch_commands_max: AtomicU64,
    batch_wait_ns_total: AtomicU64,
    batch_wait_ns_max: AtomicU64,
    inflight_batches_current: AtomicU64,
    inflight_batches_max: AtomicU64,
    pipeline_block_ns_total: AtomicU64,
    pipeline_block_ns_max: AtomicU64,
    batch_execute_ns_total: AtomicU64,
    batch_execute_ns_max: AtomicU64,
}

impl<E> MetadataCommitBatcher<E>
where
    E: MetadataCommandExecutor,
{
    pub fn new(engine: E) -> Self {
        let (tx, rx) = mpsc::channel(COMMIT_BATCH_CHANNEL);
        tokio::spawn(run_commit_batcher(engine, rx));
        Self {
            tx,
            _engine: std::marker::PhantomData,
        }
    }

    pub async fn execute(
        &self,
        request: metadatapb::MetadataCommitRequest,
    ) -> nokv_metadata_state::Result<metadatapb::MetadataCommitResponse> {
        let (reply, wait) = oneshot::channel();
        let metrics = metadata_batch_metrics();
        metrics.requests_total.fetch_add(1, Ordering::Relaxed);
        let queued = metrics.queue_depth_current.fetch_add(1, Ordering::Relaxed) + 1;
        record_max(&metrics.queue_depth_max, queued);
        let enqueue_started = Instant::now();
        let send_result = self
            .tx
            .send(BatchItem {
                request,
                reply,
                enqueued_at: Instant::now(),
            })
            .await;
        let enqueue_block_ns = duration_ns(enqueue_started.elapsed());
        metrics
            .enqueue_block_ns_total
            .fetch_add(enqueue_block_ns, Ordering::Relaxed);
        record_max(&metrics.enqueue_block_ns_max, enqueue_block_ns);
        send_result.map_err(|_| {
            metrics.queue_depth_current.fetch_sub(1, Ordering::Relaxed);
            nokv_metadata_state::Error::Backend("metadata batcher stopped".to_owned())
        })?;
        wait.await.map_err(|_| {
            nokv_metadata_state::Error::Backend("metadata batcher response dropped".to_owned())
        })?
    }
}

async fn run_commit_batcher<E>(engine: E, mut rx: mpsc::Receiver<BatchItem>)
where
    E: MetadataCommandExecutor,
{
    let permits = Arc::new(Semaphore::new(COMMIT_BATCH_INFLIGHT));
    let mut flush_tasks = JoinSet::new();
    while let Some(first) = rx.recv().await {
        while flush_tasks.try_join_next().is_some() {}
        let batch = collect_commit_batch(first, &mut rx).await;
        metadata_batch_metrics()
            .queue_depth_current
            .fetch_sub(batch.len() as u64, Ordering::Relaxed);
        let permit_started = Instant::now();
        let permit = match permits.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                fail_commit_batch(batch, "metadata batch pipeline stopped".to_owned());
                break;
            }
        };
        let block_ns = duration_ns(permit_started.elapsed());
        let metrics = metadata_batch_metrics();
        metrics
            .pipeline_block_ns_total
            .fetch_add(block_ns, Ordering::Relaxed);
        record_max(&metrics.pipeline_block_ns_max, block_ns);
        let engine = engine.clone();
        flush_tasks.spawn(async move {
            let _permit = permit;
            record_inflight_start();
            flush_commit_batch(&engine, batch).await;
            record_inflight_finish();
        });
    }
    while flush_tasks.join_next().await.is_some() {}
}

async fn collect_commit_batch(
    first: BatchItem,
    rx: &mut mpsc::Receiver<BatchItem>,
) -> Vec<BatchItem> {
    let mut batch = vec![first];
    drain_ready_commit_items(&mut batch, rx);
    if batch.len() > 1 || batch.len() >= COMMIT_BATCH_MAX {
        return batch;
    }
    let delay = tokio::time::sleep(COMMIT_BATCH_COALESCE_DELAY);
    tokio::pin!(delay);
    loop {
        if batch.len() >= COMMIT_BATCH_MAX {
            break;
        }
        tokio::select! {
            maybe_item = rx.recv() => {
                match maybe_item {
                    Some(item) => {
                        batch.push(item);
                        drain_ready_commit_items(&mut batch, rx);
                    }
                    None => break,
                }
            }
            _ = &mut delay => break,
        }
    }
    batch
}

fn drain_ready_commit_items(batch: &mut Vec<BatchItem>, rx: &mut mpsc::Receiver<BatchItem>) {
    while batch.len() < COMMIT_BATCH_MAX {
        match rx.try_recv() {
            Ok(item) => batch.push(item),
            Err(mpsc::error::TryRecvError::Empty)
            | Err(mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
}

async fn flush_commit_batch<E>(engine: &E, batch: Vec<BatchItem>)
where
    E: MetadataCommandExecutor,
{
    let batch_size = batch.len() as u64;
    let started_at = Instant::now();
    let first_enqueued_at = batch
        .first()
        .map(|item| item.enqueued_at)
        .unwrap_or(started_at);
    let metrics = metadata_batch_metrics();
    metrics.batches_total.fetch_add(1, Ordering::Relaxed);
    metrics
        .batch_commands_total
        .fetch_add(batch_size, Ordering::Relaxed);
    record_max(&metrics.batch_commands_max, batch_size);
    let wait_ns = duration_ns(started_at.saturating_duration_since(first_enqueued_at));
    metrics
        .batch_wait_ns_total
        .fetch_add(wait_ns, Ordering::Relaxed);
    record_max(&metrics.batch_wait_ns_max, wait_ns);

    let requests = batch
        .iter()
        .map(|item| item.request.clone())
        .collect::<Vec<_>>();
    let result = engine.execute_metadata_commands(&requests).await;
    let execute_ns = duration_ns(started_at.elapsed());
    metrics
        .batch_execute_ns_total
        .fetch_add(execute_ns, Ordering::Relaxed);
    record_max(&metrics.batch_execute_ns_max, execute_ns);
    match result {
        Ok(responses) if responses.len() == batch.len() => {
            for (item, response) in batch.into_iter().zip(responses) {
                let _ = item.reply.send(Ok(response));
            }
        }
        Ok(responses) => {
            let err = nokv_metadata_state::Error::Backend(format!(
                "metadata batch response length {} expected {}",
                responses.len(),
                batch.len()
            ));
            fail_commit_batch(batch, err.to_string());
        }
        Err(err) => {
            fail_commit_batch(batch, err.to_string());
        }
    }
}

fn fail_commit_batch(batch: Vec<BatchItem>, message: String) {
    for item in batch {
        let _ = item
            .reply
            .send(Err(nokv_metadata_state::Error::Backend(message.clone())));
    }
}

pub fn metadata_batch_metrics_snapshot() -> MetadataBatchMetricsSnapshot {
    let metrics = metadata_batch_metrics();
    MetadataBatchMetricsSnapshot {
        requests_total: metrics.requests_total.load(Ordering::Relaxed),
        queue_depth_current: metrics.queue_depth_current.load(Ordering::Relaxed),
        queue_depth_max: metrics.queue_depth_max.load(Ordering::Relaxed),
        enqueue_block_ns_total: metrics.enqueue_block_ns_total.load(Ordering::Relaxed),
        enqueue_block_ns_max: metrics.enqueue_block_ns_max.load(Ordering::Relaxed),
        batches_total: metrics.batches_total.load(Ordering::Relaxed),
        batch_commands_total: metrics.batch_commands_total.load(Ordering::Relaxed),
        batch_commands_max: metrics.batch_commands_max.load(Ordering::Relaxed),
        batch_wait_ns_total: metrics.batch_wait_ns_total.load(Ordering::Relaxed),
        batch_wait_ns_max: metrics.batch_wait_ns_max.load(Ordering::Relaxed),
        inflight_batches_current: metrics.inflight_batches_current.load(Ordering::Relaxed),
        inflight_batches_max: metrics.inflight_batches_max.load(Ordering::Relaxed),
        pipeline_block_ns_total: metrics.pipeline_block_ns_total.load(Ordering::Relaxed),
        pipeline_block_ns_max: metrics.pipeline_block_ns_max.load(Ordering::Relaxed),
        batch_execute_ns_total: metrics.batch_execute_ns_total.load(Ordering::Relaxed),
        batch_execute_ns_max: metrics.batch_execute_ns_max.load(Ordering::Relaxed),
    }
}

fn metadata_batch_metrics() -> &'static MetadataBatchMetrics {
    static METRICS: OnceLock<MetadataBatchMetrics> = OnceLock::new();
    METRICS.get_or_init(MetadataBatchMetrics::default)
}

impl Default for MetadataBatchMetrics {
    fn default() -> Self {
        Self {
            requests_total: AtomicU64::new(0),
            queue_depth_current: AtomicU64::new(0),
            queue_depth_max: AtomicU64::new(0),
            enqueue_block_ns_total: AtomicU64::new(0),
            enqueue_block_ns_max: AtomicU64::new(0),
            batches_total: AtomicU64::new(0),
            batch_commands_total: AtomicU64::new(0),
            batch_commands_max: AtomicU64::new(0),
            batch_wait_ns_total: AtomicU64::new(0),
            batch_wait_ns_max: AtomicU64::new(0),
            inflight_batches_current: AtomicU64::new(0),
            inflight_batches_max: AtomicU64::new(0),
            pipeline_block_ns_total: AtomicU64::new(0),
            pipeline_block_ns_max: AtomicU64::new(0),
            batch_execute_ns_total: AtomicU64::new(0),
            batch_execute_ns_max: AtomicU64::new(0),
        }
    }
}

fn record_inflight_start() {
    let metrics = metadata_batch_metrics();
    let current = metrics
        .inflight_batches_current
        .fetch_add(1, Ordering::Relaxed)
        + 1;
    record_max(&metrics.inflight_batches_max, current);
}

fn record_inflight_finish() {
    metadata_batch_metrics()
        .inflight_batches_current
        .fetch_sub(1, Ordering::Relaxed);
}

fn record_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(next) => current = next,
        }
    }
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn record_max_keeps_largest_value() {
        let value = AtomicU64::new(5);
        record_max(&value, 3);
        assert_eq!(value.load(Ordering::Relaxed), 5);
        record_max(&value, 9);
        assert_eq!(value.load(Ordering::Relaxed), 9);
    }

    #[derive(Clone, Default)]
    struct DelayedExecutor {
        active: Arc<AtomicU64>,
        active_max: Arc<AtomicU64>,
    }

    impl MetadataCommandExecutor for DelayedExecutor {
        fn execute_metadata_command<'a>(
            &'a self,
            req: &'a metadatapb::MetadataCommitRequest,
        ) -> impl std::future::Future<
            Output = nokv_metadata_state::Result<metadatapb::MetadataCommitResponse>,
        > + Send
               + 'a {
            async move {
                let mut responses = self
                    .execute_metadata_commands(std::slice::from_ref(req))
                    .await?;
                Ok(responses.remove(0))
            }
        }

        fn execute_metadata_commands<'a>(
            &'a self,
            reqs: &'a [metadatapb::MetadataCommitRequest],
        ) -> impl std::future::Future<
            Output = nokv_metadata_state::Result<Vec<metadatapb::MetadataCommitResponse>>,
        > + Send
               + 'a {
            async move {
                let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
                record_max(&self.active_max, active);
                tokio::time::sleep(Duration::from_millis(20)).await;
                self.active.fetch_sub(1, Ordering::Relaxed);
                Ok((0..reqs.len())
                    .map(|_| metadatapb::MetadataCommitResponse::default())
                    .collect())
            }
        }
    }

    #[tokio::test]
    async fn commit_batcher_pipelines_multiple_flushes() {
        let executor = DelayedExecutor::default();
        let batcher = MetadataCommitBatcher::new(executor.clone());
        let mut tasks = Vec::new();
        for _ in 0..(COMMIT_BATCH_MAX * 2 + 1) {
            let batcher = batcher.clone();
            tasks.push(tokio::spawn(async move {
                batcher
                    .execute(metadatapb::MetadataCommitRequest::default())
                    .await
                    .unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert!(executor.active_max.load(Ordering::Relaxed) > 1);
        let snapshot = metadata_batch_metrics_snapshot();
        assert!(snapshot.inflight_batches_max > 1);
    }
}
