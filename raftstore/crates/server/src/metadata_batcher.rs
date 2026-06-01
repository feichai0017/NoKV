use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use nokv_proto::nokv::metadata::v1 as metadatapb;
use nokv_raftnode::MetadataCommandExecutor;
use tokio::sync::{mpsc, oneshot};

const COMMIT_BATCH_MAX: usize = 32;
const COMMIT_BATCH_DELAY: Duration = Duration::from_micros(750);
const COMMIT_BATCH_CHANNEL: usize = 1024;

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
    pub batches_total: u64,
    pub batch_commands_total: u64,
    pub batch_commands_max: u64,
    pub batch_wait_ns_total: u64,
    pub batch_wait_ns_max: u64,
    pub batch_execute_ns_total: u64,
    pub batch_execute_ns_max: u64,
}

struct MetadataBatchMetrics {
    requests_total: AtomicU64,
    batches_total: AtomicU64,
    batch_commands_total: AtomicU64,
    batch_commands_max: AtomicU64,
    batch_wait_ns_total: AtomicU64,
    batch_wait_ns_max: AtomicU64,
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
        metadata_batch_metrics()
            .requests_total
            .fetch_add(1, Ordering::Relaxed);
        self.tx
            .send(BatchItem {
                request,
                reply,
                enqueued_at: Instant::now(),
            })
            .await
            .map_err(|_| {
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
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        let delay = tokio::time::sleep(COMMIT_BATCH_DELAY);
        tokio::pin!(delay);
        loop {
            if batch.len() >= COMMIT_BATCH_MAX {
                break;
            }
            tokio::select! {
                maybe_item = rx.recv() => {
                    match maybe_item {
                        Some(item) => batch.push(item),
                        None => break,
                    }
                }
                _ = &mut delay => break,
            }
        }
        flush_commit_batch(&engine, batch).await;
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
            for item in batch {
                let _ = item
                    .reply
                    .send(Err(nokv_metadata_state::Error::Backend(err.to_string())));
            }
        }
        Err(err) => {
            let message = err.to_string();
            for item in batch {
                let _ = item
                    .reply
                    .send(Err(nokv_metadata_state::Error::Backend(message.clone())));
            }
        }
    }
}

pub fn metadata_batch_metrics_snapshot() -> MetadataBatchMetricsSnapshot {
    let metrics = metadata_batch_metrics();
    MetadataBatchMetricsSnapshot {
        requests_total: metrics.requests_total.load(Ordering::Relaxed),
        batches_total: metrics.batches_total.load(Ordering::Relaxed),
        batch_commands_total: metrics.batch_commands_total.load(Ordering::Relaxed),
        batch_commands_max: metrics.batch_commands_max.load(Ordering::Relaxed),
        batch_wait_ns_total: metrics.batch_wait_ns_total.load(Ordering::Relaxed),
        batch_wait_ns_max: metrics.batch_wait_ns_max.load(Ordering::Relaxed),
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
            batches_total: AtomicU64::new(0),
            batch_commands_total: AtomicU64::new(0),
            batch_commands_max: AtomicU64::new(0),
            batch_wait_ns_total: AtomicU64::new(0),
            batch_wait_ns_max: AtomicU64::new(0),
            batch_execute_ns_total: AtomicU64::new(0),
            batch_execute_ns_max: AtomicU64::new(0),
        }
    }
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

    #[test]
    fn record_max_keeps_largest_value() {
        let value = AtomicU64::new(5);
        record_max(&value, 3);
        assert_eq!(value.load(Ordering::Relaxed), 5);
        record_max(&value, 9);
        assert_eq!(value.load(Ordering::Relaxed), 9);
    }
}
