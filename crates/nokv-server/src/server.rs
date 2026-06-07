use std::error::Error;
use std::fmt;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use nokv_meta::holtstore::HoltMetadataStore;
use nokv_meta::{
    HistoryGcWorker, HistoryGcWorkerState, MetadError, NoKvFs, ObjectGcWorker, ObjectGcWorkerState,
};
use nokv_object::{ObjectError, S3ObjectStore};

use crate::http;
use crate::metadata::ServerMetadataStore;
use crate::options::{MetadataMode, ServerOptions};
use crate::rpc;

const DEFAULT_ROOT_MODE: u32 = 0o755;
const SERVER_CONNECTION_WORKERS: usize = 256;
const SERVER_CONNECTION_QUEUE: usize = 1024;

pub struct Server {
    service: Arc<NoKvFs<ServerMetadataStore, S3ObjectStore>>,
    metadata_mode: MetadataMode,
    object_gc: ObjectGcWorker,
    history_gc: HistoryGcWorker,
    framed_rpc_workers: rpc::RpcWorkerPool,
    #[cfg(test)]
    _test_meta_dir: Option<tempfile::TempDir>,
}

#[derive(Debug)]
pub enum ServerError {
    Io(io::Error),
    Metadata(MetadError),
    Object(ObjectError),
}

pub fn run(options: ServerOptions) -> Result<(), ServerError> {
    let bind = options.bind;
    let server = Server::open(options)?;
    let listener = TcpListener::bind(bind).map_err(ServerError::Io)?;
    server.serve(listener)
}

impl Server {
    pub fn open(options: ServerOptions) -> Result<Self, ServerError> {
        let objects = options.object.open()?;
        Self::open_with_objects(options, objects)
    }

    fn open_with_objects(
        options: ServerOptions,
        objects: S3ObjectStore,
    ) -> Result<Self, ServerError> {
        let metadata_state_path = default_metadata_state_path(&options.meta_path);
        let metadata = match options.metadata_mode {
            MetadataMode::Local => {
                let store =
                    HoltMetadataStore::open_file(&metadata_state_path).map_err(MetadError::from)?;
                ServerMetadataStore::direct(store)
            }
        };
        let service = Arc::new(NoKvFs::open_existing(options.mount, metadata, objects)?);
        service.bootstrap_root(DEFAULT_ROOT_MODE, options.uid, options.gid)?;
        let object_gc = ObjectGcWorker::spawn(Arc::clone(&service), options.object_gc);
        let history_gc = HistoryGcWorker::spawn(Arc::clone(&service), options.history_gc);
        let framed_rpc_workers = rpc::RpcWorkerPool::new(
            rpc::default_framed_rpc_worker_count(),
            rpc::default_framed_rpc_queue_capacity(),
        );
        Ok(Self {
            service,
            metadata_mode: options.metadata_mode,
            object_gc,
            history_gc,
            framed_rpc_workers,
            #[cfg(test)]
            _test_meta_dir: None,
        })
    }

    pub fn serve(self, listener: TcpListener) -> Result<(), ServerError> {
        let server = Arc::new(self);
        let workers = ConnectionWorkerPool::new(
            Arc::clone(&server),
            SERVER_CONNECTION_WORKERS,
            SERVER_CONNECTION_QUEUE,
        )?;
        for stream in listener.incoming() {
            let stream = stream.map_err(ServerError::Io)?;
            workers.submit(stream)?;
        }
        Ok(())
    }

    pub(crate) fn service(&self) -> &NoKvFs<ServerMetadataStore, S3ObjectStore> {
        &self.service
    }

    pub(crate) fn framed_rpc_workers(&self) -> &rpc::RpcWorkerPool {
        &self.framed_rpc_workers
    }

    pub(crate) fn refresh_metadata_view(&self) -> Result<(), ServerError> {
        self.service
            .refresh_allocator_state()
            .map_err(ServerError::Metadata)
    }

    pub fn stats_json(&self) -> String {
        let objects = self.service.object_stats();
        let metadata = self.service.metadata_store_stats();
        let metadata_service = self.service.metadata_service_stats();
        let object_gc = self.object_gc.state();
        let history_gc = self.history_gc.state();
        format!(
            "{{\"ready\":true,\"metadata_mode\":\"{}\",\"block_cache_enabled\":{},\"object_puts\":{},\"object_put_bytes\":{},\"object_gets\":{},\"object_get_bytes\":{},\"coalesced_gets\":{},\"coalesced_get_bytes\":{},\"cache_hits\":{},\"cache_hit_bytes\":{},\"prefetch_enqueued\":{},\"prefetch_dropped\":{},\"prefetch_completed\":{},\"prefetch_failed\":{},\"prefetch_object_gets\":{},\"prefetch_object_get_bytes\":{},\"prefetch_cache_hits\":{},\"prefetch_cache_hit_bytes\":{},\"read_plan_cache_hits\":{},\"read_plan_cache_misses\":{},\"object_writeback_enqueued\":{},\"object_writeback_inline\":{},\"object_writeback_fallback\":{},\"object_writeback_completed\":{},\"object_writeback_failed\":{},\"object_writeback_staged_bytes\":{},\"object_writeback_uploaded_bytes\":{},\"object_writeback_queue_wait_ns\":{},\"object_writeback_queue_max_wait_ns\":{},\"object_writeback_upload_ns\":{},\"object_writeback_upload_max_ns\":{},\"manifest_chunks\":{},\"manifest_blocks\":{},\"metadata_store\":{},\"metadata_service\":{},\"object_gc\":{},\"history_gc\":{}}}\n",
            self.metadata_mode.as_str(),
            self.service.block_cache_enabled(),
            objects.object_puts,
            objects.object_put_bytes,
            objects.object_gets,
            objects.object_get_bytes,
            objects.coalesced_gets,
            objects.coalesced_get_bytes,
            objects.cache_hits,
            objects.cache_hit_bytes,
            objects.prefetch_enqueued,
            objects.prefetch_dropped,
            objects.prefetch_completed,
            objects.prefetch_failed,
            objects.prefetch_object_gets,
            objects.prefetch_object_get_bytes,
            objects.prefetch_cache_hits,
            objects.prefetch_cache_hit_bytes,
            objects.read_plan_cache_hits,
            objects.read_plan_cache_misses,
            objects.object_writeback_enqueued,
            objects.object_writeback_inline,
            objects.object_writeback_fallback,
            objects.object_writeback_completed,
            objects.object_writeback_failed,
            objects.object_writeback_staged_bytes,
            objects.object_writeback_uploaded_bytes,
            objects.object_writeback_queue_wait_ns,
            objects.object_writeback_queue_max_wait_ns,
            objects.object_writeback_upload_ns,
            objects.object_writeback_upload_max_ns,
            objects.manifest_chunks,
            objects.manifest_blocks,
            metadata_store_json(&metadata),
            metadata_service_json(&metadata_service),
            object_gc_json(&object_gc),
            history_gc_json(&history_gc),
        )
    }

    pub fn run_manual_gc(&self, limit: usize) -> Result<String, ServerError> {
        let object = self.service.cleanup_pending_objects(limit)?;
        let history = self.service.cleanup_history(limit)?;
        Ok(format!(
            r#"{{"object_gc":{{"scanned":{},"blocked_by_snapshots":{},"attempted":{},"deleted":{},"missing":{},"records_removed":{}}},"history_gc":{{"scanned":{},"removed":{},"retained_by_snapshots":{}}}}}
"#,
            object.scanned,
            object.blocked_by_snapshots,
            object.attempted,
            object.deleted,
            object.missing,
            object.records_removed,
            history.scanned,
            history.removed,
            history.retained_by_snapshots,
        ))
    }

    pub fn run_manual_checkpoint(&self) -> Result<String, ServerError> {
        self.service
            .metadata_store()
            .checkpoint()
            .map_err(|err| ServerError::Metadata(MetadError::from(err)))?;
        Ok(format!(
            r#"{{"metadata_mode":"{}"}}
"#,
            self.metadata_mode.as_str(),
        ))
    }
}

struct ConnectionWorkerPool {
    sender: mpsc::SyncSender<TcpStream>,
}

impl ConnectionWorkerPool {
    fn new(server: Arc<Server>, workers: usize, queue: usize) -> Result<Self, ServerError> {
        let (sender, receiver) = mpsc::sync_channel::<TcpStream>(queue.max(workers));
        let receiver = Arc::new(Mutex::new(receiver));
        for worker in 0..workers {
            let server = Arc::clone(&server);
            let receiver = Arc::clone(&receiver);
            thread::Builder::new()
                .name(format!("nokv-conn-{worker}"))
                .spawn(move || loop {
                    let stream = {
                        let receiver = match receiver.lock() {
                            Ok(receiver) => receiver,
                            Err(_) => return,
                        };
                        receiver.recv()
                    };
                    match stream {
                        Ok(stream) => {
                            if let Err(err) = http::handle_stream(Arc::clone(&server), stream) {
                                eprintln!("nokv-server connection failed: {err}");
                            }
                        }
                        Err(_) => return,
                    }
                })
                .map_err(ServerError::Io)?;
        }
        Ok(Self { sender })
    }

    fn submit(&self, stream: TcpStream) -> Result<(), ServerError> {
        self.sender.send(stream).map_err(|_| {
            ServerError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "nokv connection worker pool stopped",
            ))
        })
    }
}

fn default_metadata_state_path(meta_path: &Path) -> PathBuf {
    meta_path.join("metadata-state.holt")
}

fn metadata_store_json(stats: &nokv_meta::MetadataStoreStats) -> String {
    format!(
        "{{\"get_total\":{},\"get_user_strong_total\":{},\"get_write_plan_local_total\":{},\"get_snapshot_total\":{},\"scan_total\":{},\"scan_user_strong_total\":{},\"scan_write_plan_local_total\":{},\"scan_snapshot_total\":{},\"scan_cache_hit_total\":{},\"scan_key_visited_total\":{},\"scan_key_returned_total\":{},\"history_lookup_total\":{},\"active_snapshot_pin_total\":{},\"commit_total\":{},\"dedupe_hit_total\":{},\"predicate_total\":{},\"prefix_empty_predicate_total\":{},\"current_put_total\":{},\"current_delete_total\":{},\"history_write_total\":{},\"watch_write_total\":{},\"dedupe_write_total\":{},\"commit_prepare_ns_total\":{},\"atomic_apply_total\":{},\"atomic_apply_command_total\":{},\"atomic_apply_max_batch\":{},\"atomic_apply_ns_total\":{}}}",
        stats.get_total,
        stats.get_user_strong_total,
        stats.get_write_plan_local_total,
        stats.get_snapshot_total,
        stats.scan_total,
        stats.scan_user_strong_total,
        stats.scan_write_plan_local_total,
        stats.scan_snapshot_total,
        stats.scan_cache_hit_total,
        stats.scan_key_visited_total,
        stats.scan_key_returned_total,
        stats.history_lookup_total,
        stats.active_snapshot_pin_total,
        stats.commit_total,
        stats.dedupe_hit_total,
        stats.predicate_total,
        stats.prefix_empty_predicate_total,
        stats.current_put_total,
        stats.current_delete_total,
        stats.history_write_total,
        stats.watch_write_total,
        stats.dedupe_write_total,
        stats.commit_prepare_ns_total,
        stats.atomic_apply_total,
        stats.atomic_apply_command_total,
        stats.atomic_apply_max_batch,
        stats.atomic_apply_ns_total,
    )
}

fn metadata_service_json(stats: &nokv_meta::MetadataServiceStats) -> String {
    format!(
        "{{\"path_index_lookup_total\":{},\"path_index_hit_total\":{},\"path_index_miss_total\":{},\"path_index_stale_total\":{},\"path_index_scan_stale_total\":{},\"path_index_fallback_total\":{},\"create_files_batch_total\":{},\"create_files_entry_total\":{},\"create_dirs_batch_total\":{},\"create_dirs_entry_total\":{},\"read_dir_plus_total\":{},\"read_dir_plus_entry_total\":{},\"read_dir_plus_projection_hit_total\":{}}}",
        stats.path_index_lookup_total,
        stats.path_index_hit_total,
        stats.path_index_miss_total,
        stats.path_index_stale_total,
        stats.path_index_scan_stale_total,
        stats.path_index_fallback_total,
        stats.create_files_batch_total,
        stats.create_files_entry_total,
        stats.create_dirs_batch_total,
        stats.create_dirs_entry_total,
        stats.read_dir_plus_total,
        stats.read_dir_plus_entry_total,
        stats.read_dir_plus_projection_hit_total,
    )
}

fn object_gc_json(state: &ObjectGcWorkerState) -> String {
    format!(
        "{{\"iterations\":{},\"last_error\":{}}}",
        state.iterations,
        json_string_or_null(state.last_error.as_deref())
    )
}

fn history_gc_json(state: &HistoryGcWorkerState) -> String {
    format!(
        "{{\"iterations\":{},\"last_error\":{}}}",
        state.iterations,
        json_string_or_null(state.last_error.as_deref())
    )
}

fn json_string_or_null(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("\"{}\"", escape_json_string(value)),
        None => "null".to_owned(),
    }
}

fn escape_json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out
}

impl From<MetadError> for ServerError {
    fn from(err: MetadError) -> Self {
        Self::Metadata(err)
    }
}

impl From<ObjectError> for ServerError {
    fn from(err: ObjectError) -> Self {
        Self::Object(err)
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Metadata(err) => write!(f, "{err}"),
            Self::Object(err) => write!(f, "{err}"),
        }
    }
}

impl Error for ServerError {}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::path::Path;
    use std::time::Duration;

    use nokv_meta::{HistoryGcOptions, ObjectGcOptions};
    use nokv_object::{ObjectStoreConfig, S3ObjectStoreOptions};
    use nokv_types::MountId;
    use tempfile::tempdir;

    use crate::MetadataMode;

    pub(crate) fn test_options(root: &Path) -> ServerOptions {
        ServerOptions {
            bind: crate::options::DEFAULT_SERVER_BIND,
            mount: MountId::new(1).unwrap(),
            meta_path: root.join("meta"),
            metadata_mode: MetadataMode::Local,
            metadata_checkpoint_archive_prefix: None,
            object: ObjectStoreConfig::s3(S3ObjectStoreOptions {
                bucket: "test".to_owned(),
                root: "/".to_owned(),
                region: "auto".to_owned(),
                endpoint: Some("http://127.0.0.1:1".to_owned()),
                access_key_id: Some("test".to_owned()),
                secret_access_key: Some("test".to_owned()),
                session_token: None,
                virtual_host_style: false,
                skip_signature: true,
            }),
            uid: 1000,
            gid: 1000,
            object_gc: ObjectGcOptions {
                interval: Duration::from_secs(3600),
                limit: 128,
                run_immediately: false,
            },
            history_gc: HistoryGcOptions {
                interval: Duration::from_secs(3600),
                limit: 128,
                run_immediately: false,
            },
        }
    }

    pub(crate) fn test_server() -> Server {
        let dir = tempdir().unwrap();
        let mut server = Server::open(test_options(dir.path())).unwrap();
        server._test_meta_dir = Some(dir);
        server
    }

    #[test]
    fn manual_gc_reports_empty_outcomes() {
        let server = test_server();
        assert!(server.stats_json().contains("\"metadata_mode\":\"local\""));
        let body = server.run_manual_gc(128).unwrap();
        assert!(body.contains("\"object_gc\""));
        assert!(body.contains("\"history_gc\""));
    }
}
