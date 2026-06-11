//! Background metadata-archive worker.
//!
//! Periodically publishes a metadata checkpoint to the object store for off-node
//! disaster recovery (see [`crate::service`]'s `backup_metadata`). The loop here
//! owns only scheduling; the durable archive format and the object writes stay in
//! the service layer, mirroring how [`crate::gc`] wraps service-level cleanup.

use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::command::{MetadataCheckpointStore, MetadataStore};
use crate::service::{MetadataArchiveConfig, MetadataBackupOutcome, NoKvFs};
use nokv_object::ObjectStore;

const DEFAULT_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataBackupOptions {
    pub config: MetadataArchiveConfig,
    pub interval: Duration,
    pub run_immediately: bool,
}

impl MetadataBackupOptions {
    pub fn new(config: MetadataArchiveConfig) -> Self {
        Self {
            config,
            interval: DEFAULT_INTERVAL,
            run_immediately: true,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetadataBackupWorkerState {
    pub iterations: u64,
    pub last_outcome: Option<MetadataBackupOutcome>,
    pub last_error: Option<String>,
}

pub struct MetadataBackupWorker {
    stop: Arc<(Mutex<bool>, Condvar)>,
    state: Arc<Mutex<MetadataBackupWorkerState>>,
    handle: Option<JoinHandle<()>>,
}

impl MetadataBackupWorker {
    pub fn spawn<M, O>(service: Arc<NoKvFs<M, O>>, options: MetadataBackupOptions) -> Self
    where
        M: MetadataStore + MetadataCheckpointStore + Send + Sync + 'static,
        O: ObjectStore + Send + Sync + 'static,
    {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let state = Arc::new(Mutex::new(MetadataBackupWorkerState::default()));
        let worker_stop = Arc::clone(&stop);
        let worker_state = Arc::clone(&state);
        let handle = thread::spawn(move || {
            if options.run_immediately {
                run_once(&service, &options.config, &worker_state);
            }
            loop {
                let (lock, cvar) = &*worker_stop;
                let stopped = match lock.lock() {
                    Ok(stopped) => stopped,
                    Err(_) => break,
                };
                if *stopped {
                    break;
                }
                let (stopped, _) = match cvar.wait_timeout(stopped, options.interval) {
                    Ok(waited) => waited,
                    Err(_) => break,
                };
                if *stopped {
                    break;
                }
                drop(stopped);
                run_once(&service, &options.config, &worker_state);
            }
        });
        Self {
            stop,
            state,
            handle: Some(handle),
        }
    }

    pub fn state(&self) -> MetadataBackupWorkerState {
        self.state
            .lock()
            .map(|state| state.clone())
            .unwrap_or_else(|err| err.into_inner().clone())
    }

    pub fn stop(&mut self) {
        let (lock, cvar) = &*self.stop;
        if let Ok(mut stopped) = lock.lock() {
            *stopped = true;
            cvar.notify_all();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for MetadataBackupWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run_once<M, O>(
    service: &Arc<NoKvFs<M, O>>,
    config: &MetadataArchiveConfig,
    state: &Arc<Mutex<MetadataBackupWorkerState>>,
) where
    M: MetadataStore + MetadataCheckpointStore,
    O: ObjectStore,
{
    let outcome = service.backup_metadata(config);
    if let Ok(mut state) = state.lock() {
        state.iterations += 1;
        match outcome {
            Ok(outcome) => {
                state.last_outcome = Some(outcome);
                state.last_error = None;
            }
            Err(err) => {
                state.last_error = Some(err.to_string());
            }
        }
    }
}
