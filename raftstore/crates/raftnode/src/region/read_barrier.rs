use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::Raft;
use tokio::sync::{watch, Mutex};

use super::openraft_check_leader_error;
use crate::{metrics, Error, NodeId, RaftStoreConfig};

#[derive(Clone, Default)]
pub(super) struct ReadBarrier {
    active: Arc<Mutex<Option<ActiveReadBarrier>>>,
}

struct ActiveReadBarrier {
    receiver: watch::Receiver<Option<ReadBarrierOutcome>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReadBarrierOutcome {
    Confirmed,
    NotLeader { leader_id: Option<NodeId> },
    OpenRaft(String),
}

impl ReadBarrier {
    pub(super) async fn ensure_linearizable(
        &self,
        raft: Raft<RaftStoreConfig>,
    ) -> Result<(), Error> {
        self.confirm(move || ensure_linearizable_with_retry(raft))
            .await
    }

    async fn confirm<F, Fut>(&self, confirm: F) -> Result<(), Error>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), Error>> + Send + 'static,
    {
        metrics::record_read_barrier_request();
        let receiver = {
            let mut active = self.active.lock().await;
            if let Some(barrier) = active.as_ref() {
                metrics::record_read_barrier_shared();
                barrier.receiver.clone()
            } else {
                let (sender, receiver) = watch::channel(None);
                *active = Some(ActiveReadBarrier {
                    receiver: receiver.clone(),
                });
                let active = self.active.clone();
                tokio::spawn(async move {
                    let started = Instant::now();
                    let result = confirm().await;
                    metrics::record_read_barrier_execution(started.elapsed(), result.is_ok());
                    let outcome = ReadBarrierOutcome::from_result(result);
                    let mut active = active.lock().await;
                    active.take();
                    drop(active);
                    let _ = sender.send(Some(outcome));
                });
                receiver
            }
        };
        await_read_barrier(receiver).await
    }
}

async fn ensure_linearizable_with_retry(raft: Raft<RaftStoreConfig>) -> Result<(), Error> {
    let mut last_error = None;
    for attempt in 1..=50 {
        match raft.ensure_linearizable().await {
            Ok(_) => return Ok(()),
            Err(err) => {
                let err = openraft_check_leader_error(err);
                if matches!(err, Error::NotLeader { leader_id: Some(_) }) {
                    return Err(err);
                }
                last_error = Some(err);
                if attempt < 50 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    let err = last_error
        .map(|err| err.to_string())
        .unwrap_or_else(|| "linearizable read did not complete".to_owned());
    Err(Error::OpenRaft(err))
}

async fn await_read_barrier(
    mut receiver: watch::Receiver<Option<ReadBarrierOutcome>>,
) -> Result<(), Error> {
    loop {
        if let Some(outcome) = receiver.borrow().clone() {
            return outcome.into_result();
        }
        receiver
            .changed()
            .await
            .map_err(|_| Error::OpenRaft("read barrier closed before completion".to_owned()))?;
    }
}

impl ReadBarrierOutcome {
    fn from_result(result: Result<(), Error>) -> Self {
        match result {
            Ok(()) => Self::Confirmed,
            Err(Error::NotLeader { leader_id }) => Self::NotLeader { leader_id },
            Err(err) => Self::OpenRaft(err.to_string()),
        }
    }

    fn into_result(self) -> Result<(), Error> {
        match self {
            Self::Confirmed => Ok(()),
            Self::NotLeader { leader_id } => Err(Error::NotLeader { leader_id }),
            Self::OpenRaft(err) => Err(Error::OpenRaft(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn read_barrier_shares_concurrent_confirmations() {
        let before = metrics::raftnode_metrics_snapshot();
        let barrier = ReadBarrier::default();
        let executions = Arc::new(AtomicU64::new(0));
        let mut tasks = Vec::new();

        for _ in 0..8 {
            let barrier = barrier.clone();
            let executions = executions.clone();
            tasks.push(tokio::spawn(async move {
                barrier
                    .confirm(move || async move {
                        executions.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok(())
                    })
                    .await
                    .unwrap();
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(executions.load(Ordering::Relaxed), 1);
        let after = metrics::raftnode_metrics_snapshot();
        assert_eq!(
            after.read_barrier_requests_total - before.read_barrier_requests_total,
            8
        );
        assert_eq!(
            after.read_barrier_executions_total - before.read_barrier_executions_total,
            1
        );
        assert_eq!(
            after.read_barrier_shared_total - before.read_barrier_shared_total,
            7
        );
    }
}
