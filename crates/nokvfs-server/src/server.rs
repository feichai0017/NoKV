use std::error::Error;
use std::fmt;
use std::io;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use nokvfs_cluster::{
    compact_log_to_checkpoint, AppendMetadataBatchRequest, AppendMetadataBatchResponse,
    ApplyFrontier, CheckpointArtifact, CheckpointCatalog, CheckpointManifest,
    FileAppliedFrontierStore, FileCheckpointCatalog, FileSharedLog, FileSharedLogOptions, LogIndex,
    LogPosition, MetadataLogEntry, SharedLogError, SharedLogMetadataStore, SharedLogRuntimeStats,
    SharedMetadataLog,
};
use nokvfs_meta::holtstore::HoltMetadataStore;
use nokvfs_meta::{
    HistoryGcWorker, HistoryGcWorkerState, MetadError, NoKvFs, ObjectGcWorker, ObjectGcWorkerState,
};
use nokvfs_object::{ObjectError, S3ObjectStore};

use crate::http;
use crate::metadata::{FileLoggedMetadataStore, ServerMetadataLogStatus, ServerMetadataStore};
use crate::options::ServerOptions;

const DEFAULT_ROOT_MODE: u32 = 0o755;

pub struct Server {
    service: Arc<NoKvFs<ServerMetadataStore, S3ObjectStore>>,
    metadata_log_enabled: bool,
    metadata_log_sync: nokvfs_cluster::FileSharedLogSync,
    metadata_log_status: Option<Arc<dyn ServerMetadataLogStatus>>,
    metadata_log: Option<Arc<FileLoggedMetadataStore>>,
    metadata_checkpoint: Option<FileCheckpointCatalog>,
    object_gc: ObjectGcWorker,
    history_gc: HistoryGcWorker,
}

#[derive(Debug)]
pub enum ServerError {
    Io(io::Error),
    Metadata(MetadError),
    Object(ObjectError),
    SharedLog(SharedLogError),
}

pub fn run(options: ServerOptions) -> Result<(), ServerError> {
    let bind = options.bind;
    let server = Server::open(options)?;
    let listener = TcpListener::bind(bind).map_err(ServerError::Io)?;
    server.serve(listener)
}

impl Server {
    pub fn open(options: ServerOptions) -> Result<Self, ServerError> {
        let metadata =
            HoltMetadataStore::open_file(&options.meta_path).map_err(MetadError::from)?;
        let mut metadata_log = None;
        let mut metadata_log_status = None;
        let mut metadata_checkpoint = None;
        let metadata = match options.metadata_log_path.as_ref() {
            Some(path) => {
                let log = FileSharedLog::open(
                    path,
                    FileSharedLogOptions {
                        sync: options.metadata_log_sync,
                    },
                )?;
                let frontier = FileAppliedFrontierStore::open(metadata_apply_frontier_path(path))?;
                let checkpoint = FileCheckpointCatalog::open(metadata_checkpoint_path(path))?;
                let (logged, _replay) = SharedLogMetadataStore::recover_with_frontier_store(
                    metadata,
                    log,
                    options.metadata_log_term,
                    options.mount,
                    frontier,
                )
                .map_err(|err| ServerError::SharedLog(SharedLogError::Backend(err.to_string())))?;
                let logged = Arc::new(logged);
                let status: Arc<dyn ServerMetadataLogStatus> = logged.clone();
                metadata_log = Some(Arc::clone(&logged));
                metadata_log_status = Some(status);
                metadata_checkpoint = Some(checkpoint);
                ServerMetadataStore::shared_logged(logged)
            }
            None => ServerMetadataStore::direct(metadata),
        };
        let objects = options.object.open()?;
        let service = Arc::new(NoKvFs::open_existing(options.mount, metadata, objects)?);
        service.bootstrap_root(DEFAULT_ROOT_MODE, options.uid, options.gid)?;
        let object_gc = ObjectGcWorker::spawn(Arc::clone(&service), options.object_gc);
        let history_gc = HistoryGcWorker::spawn(Arc::clone(&service), options.history_gc);
        Ok(Self {
            service,
            metadata_log_enabled: metadata_log.is_some(),
            metadata_log_sync: options.metadata_log_sync,
            metadata_log_status,
            metadata_log,
            metadata_checkpoint,
            object_gc,
            history_gc,
        })
    }

    pub fn serve(self, listener: TcpListener) -> Result<(), ServerError> {
        let server = Arc::new(self);
        for stream in listener.incoming() {
            let stream = stream.map_err(ServerError::Io)?;
            let server = Arc::clone(&server);
            thread::spawn(move || {
                if let Err(err) = http::handle_stream(server, stream) {
                    eprintln!("nokvfs-server connection failed: {err}");
                }
            });
        }
        Ok(())
    }

    pub(crate) fn service(&self) -> &NoKvFs<ServerMetadataStore, S3ObjectStore> {
        &self.service
    }

    pub(crate) fn metadata_log_applied_position(&self) -> Option<LogPosition> {
        self.metadata_log_frontier()
            .map(|frontier| frontier.position)
    }

    pub(crate) fn ensure_metadata_log_applied(
        &self,
        position: LogPosition,
    ) -> Result<(), ServerError> {
        let Some(metadata_log) = self.metadata_log_status.as_ref() else {
            return Ok(());
        };
        metadata_log
            .ensure_applied(position)
            .map_err(ServerError::SharedLog)
    }

    pub(crate) fn read_metadata_log_tail(
        &self,
        start: LogIndex,
        limit: usize,
    ) -> Result<(Vec<MetadataLogEntry>, Option<LogPosition>), ServerError> {
        let Some(metadata_log) = self.metadata_log.as_ref() else {
            return Err(ServerError::SharedLog(SharedLogError::Backend(
                "metadata log is disabled".to_owned(),
            )));
        };
        let entries = metadata_log.log().read_from(start, limit)?;
        Ok((entries, metadata_log.log().committed_position()))
    }

    pub(crate) fn append_metadata_log_batch(
        &self,
        request: AppendMetadataBatchRequest,
    ) -> Result<AppendMetadataBatchResponse, ServerError> {
        if request.mount != self.service.mount_id() {
            return Err(ServerError::SharedLog(SharedLogError::Backend(format!(
                "metadata log append mount {} does not match server mount {}",
                request.mount.get(),
                self.service.mount_id().get(),
            ))));
        }
        let Some(metadata_log) = self.metadata_log.as_ref() else {
            return Err(ServerError::SharedLog(SharedLogError::Backend(
                "metadata log is disabled".to_owned(),
            )));
        };
        let receipts =
            metadata_log
                .log()
                .append_batch(request.term, request.mount, &request.commands)?;
        metadata_log
            .replay_committed_tail(0)
            .map_err(|err| ServerError::SharedLog(SharedLogError::Backend(err.to_string())))?;
        self.service.refresh_allocator_state()?;
        AppendMetadataBatchResponse::from_receipts(receipts).map_err(ServerError::SharedLog)
    }

    pub fn stats_json(&self) -> String {
        let objects = self.service.object_stats();
        let metadata = self.service.metadata_store_stats();
        let metadata_service = self.service.metadata_service_stats();
        let object_gc = self.object_gc.state();
        let history_gc = self.history_gc.state();
        format!(
            "{{\"ready\":true,\"block_cache_enabled\":{},\"object_puts\":{},\"object_gets\":{},\"cache_hits\":{},\"manifest_chunks\":{},\"manifest_blocks\":{},\"metadata_store\":{},\"metadata_log\":{},\"metadata_service\":{},\"object_gc\":{},\"history_gc\":{}}}\n",
            self.service.block_cache_enabled(),
            objects.object_puts,
            objects.object_gets,
            objects.cache_hits,
            objects.manifest_chunks,
            objects.manifest_blocks,
            metadata_store_json(&metadata),
            metadata_log_json(
                self.metadata_log_enabled,
                self.metadata_log_sync,
                self.metadata_log_frontier(),
                self.metadata_log_runtime_stats(),
            ),
            metadata_service_json(&metadata_service),
            object_gc_json(&object_gc),
            history_gc_json(&history_gc),
        )
    }

    pub fn run_manual_gc(&self, limit: usize) -> Result<String, ServerError> {
        let object = self.service.cleanup_pending_objects(limit)?;
        let history = self.service.cleanup_history(limit)?;
        let metadata_log = self.compact_metadata_log()?;
        Ok(format!(
            "{{\"object_gc\":{{\"scanned\":{},\"blocked_by_snapshots\":{},\"attempted\":{},\"deleted\":{},\"missing\":{},\"records_removed\":{}}},\"history_gc\":{{\"scanned\":{},\"removed\":{},\"retained_by_snapshots\":{}}},\"metadata_log\":{}}}\n",
            object.scanned,
            object.blocked_by_snapshots,
            object.attempted,
            object.deleted,
            object.missing,
            object.records_removed,
            history.scanned,
            history.removed,
            history.retained_by_snapshots,
            metadata_log_gc_json(self.metadata_log_enabled, metadata_log),
        ))
    }

    fn compact_metadata_log(&self) -> Result<Option<CheckpointManifest>, ServerError> {
        let Some(metadata_log) = self.metadata_log.as_ref() else {
            return Ok(None);
        };
        let Some(checkpoints) = self.metadata_checkpoint.as_ref() else {
            return Ok(None);
        };
        let Some(applied) = metadata_log.applied_frontier() else {
            return Ok(None);
        };
        let target = applied
            .position
            .index
            .get()
            .checked_add(1)
            .ok_or_else(|| {
                ServerError::SharedLog(SharedLogError::Backend(format!(
                    "metadata log index overflow after {}",
                    applied.position.index.get()
                )))
            })
            .and_then(|next| LogIndex::new(next).map_err(ServerError::SharedLog))?;
        let Some(frontier) = metadata_log
            .checkpoint_frontier(target)
            .map_err(ServerError::SharedLog)?
        else {
            return Ok(None);
        };
        metadata_log
            .inner()
            .checkpoint()
            .map_err(MetadError::from)?;
        let mount = self.service.mount_id();
        let checkpoint_id = metadata_checkpoint_id(mount, &frontier);
        let artifact = metadata_checkpoint_artifact(&checkpoint_id)?;
        let manifest = CheckpointManifest::new(checkpoint_id, mount, frontier, artifact)?;
        checkpoints.publish(manifest.clone())?;
        let outcome = compact_log_to_checkpoint(metadata_log.log(), manifest)
            .map_err(ServerError::SharedLog)?;
        Ok(outcome.manifest)
    }
}

fn metadata_apply_frontier_path(log_path: &Path) -> PathBuf {
    let mut path = log_path.to_path_buf();
    let file_name = log_path
        .file_name()
        .map(|name| {
            let mut name = name.to_os_string();
            name.push(".apply");
            name
        })
        .unwrap_or_else(|| "metadata.log.apply".into());
    path.set_file_name(file_name);
    path
}

fn metadata_checkpoint_path(log_path: &Path) -> PathBuf {
    let mut path = log_path.to_path_buf();
    let file_name = log_path
        .file_name()
        .map(|name| {
            let mut name = name.to_os_string();
            name.push(".checkpoint");
            name
        })
        .unwrap_or_else(|| "metadata.log.checkpoint".into());
    path.set_file_name(file_name);
    path
}

fn metadata_checkpoint_id(
    mount: nokvfs_types::MountId,
    frontier: &nokvfs_cluster::CheckpointFrontier,
) -> Vec<u8> {
    format!(
        "mount-{}-term-{}-index-{}",
        mount.get(),
        frontier.applied_position.term.get(),
        frontier.applied_position.index.get()
    )
    .into_bytes()
}

fn metadata_checkpoint_artifact(id: &[u8]) -> Result<CheckpointArtifact, SharedLogError> {
    CheckpointArtifact::new(
        format!("local-holt:{}", String::from_utf8_lossy(id)).into_bytes(),
        Vec::new(),
        0,
    )
}

impl Server {
    fn metadata_log_frontier(&self) -> Option<ApplyFrontier> {
        self.metadata_log_status
            .as_ref()
            .and_then(|metadata_log| metadata_log.applied_frontier())
    }

    fn metadata_log_runtime_stats(&self) -> Option<SharedLogRuntimeStats> {
        self.metadata_log_status
            .as_ref()
            .map(|metadata_log| metadata_log.runtime_stats())
    }
}

fn metadata_log_json(
    enabled: bool,
    sync: nokvfs_cluster::FileSharedLogSync,
    frontier: Option<ApplyFrontier>,
    runtime: Option<SharedLogRuntimeStats>,
) -> String {
    let runtime = runtime.unwrap_or_default();
    match frontier {
        Some(frontier) => format!(
            "{{\"enabled\":true,\"sync\":\"{}\",\"applied_term\":{},\"applied_index\":{},\"commit_version\":{},\"commit_entry_total\":{},\"commit_command_total\":{},\"max_commands_per_entry\":{},\"stale_read_total\":{}}}",
            metadata_log_sync_name(sync),
            frontier.position.term.get(),
            frontier.position.index.get(),
            frontier.commit_version.get(),
            runtime.commit_entry_total,
            runtime.commit_command_total,
            runtime.max_commands_per_entry,
            runtime.stale_read_total,
        ),
        None if enabled => format!(
            "{{\"enabled\":true,\"sync\":\"{}\",\"applied_term\":null,\"applied_index\":null,\"commit_version\":null,\"commit_entry_total\":{},\"commit_command_total\":{},\"max_commands_per_entry\":{},\"stale_read_total\":{}}}",
            metadata_log_sync_name(sync),
            runtime.commit_entry_total,
            runtime.commit_command_total,
            runtime.max_commands_per_entry,
            runtime.stale_read_total,
        ),
        None => {
            "{\"enabled\":false,\"commit_entry_total\":0,\"commit_command_total\":0,\"max_commands_per_entry\":0,\"stale_read_total\":0}".to_owned()
        }
    }
}

fn metadata_log_sync_name(sync: nokvfs_cluster::FileSharedLogSync) -> &'static str {
    match sync {
        nokvfs_cluster::FileSharedLogSync::Data => "data",
        nokvfs_cluster::FileSharedLogSync::None => "none",
    }
}

fn metadata_log_gc_json(enabled: bool, manifest: Option<CheckpointManifest>) -> String {
    match manifest {
        Some(manifest) => format!(
            "{{\"enabled\":true,\"checkpoint_id\":\"{}\",\"checkpoint_uri\":\"{}\",\"checkpoint_digest\":\"{}\",\"checkpoint_size_bytes\":{},\"durable_term\":{},\"durable_index\":{},\"applied_term\":{},\"applied_index\":{},\"min_retained_index\":{},\"max_commit_version\":{},\"compacted_through\":{}}}",
            escape_json_string(&String::from_utf8_lossy(&manifest.id)),
            escape_json_string(&String::from_utf8_lossy(&manifest.artifact.uri)),
            escape_json_string(&String::from_utf8_lossy(&manifest.artifact.digest)),
            manifest.artifact.size_bytes,
            manifest.frontier.durable_position.term.get(),
            manifest.frontier.durable_position.index.get(),
            manifest.frontier.applied_position.term.get(),
            manifest.frontier.applied_position.index.get(),
            manifest.frontier.min_retained_index.get(),
            manifest.frontier.max_commit_version.get(),
            manifest
                .frontier
                .compact_through()
                .map(|index| index.get().to_string())
                .unwrap_or_else(|| "null".to_owned()),
        ),
        None if enabled => {
            "{\"enabled\":true,\"checkpoint_id\":null,\"checkpoint_uri\":null,\"checkpoint_digest\":null,\"checkpoint_size_bytes\":null,\"durable_term\":null,\"durable_index\":null,\"applied_term\":null,\"applied_index\":null,\"min_retained_index\":null,\"max_commit_version\":null,\"compacted_through\":null}".to_owned()
        }
        None => "{\"enabled\":false}".to_owned(),
    }
}

fn metadata_store_json(stats: &nokvfs_meta::MetadataStoreStats) -> String {
    format!(
        "{{\"get_total\":{},\"get_user_strong_total\":{},\"get_write_plan_local_total\":{},\"get_snapshot_total\":{},\"scan_total\":{},\"scan_user_strong_total\":{},\"scan_write_plan_local_total\":{},\"scan_snapshot_total\":{},\"scan_key_visited_total\":{},\"scan_key_returned_total\":{},\"active_snapshot_pin_total\":{},\"commit_total\":{},\"dedupe_hit_total\":{},\"predicate_total\":{},\"prefix_empty_predicate_total\":{},\"current_put_total\":{},\"current_delete_total\":{},\"history_write_total\":{},\"watch_write_total\":{},\"dedupe_write_total\":{},\"commit_prepare_ns_total\":{},\"atomic_apply_ns_total\":{}}}",
        stats.get_total,
        stats.get_user_strong_total,
        stats.get_write_plan_local_total,
        stats.get_snapshot_total,
        stats.scan_total,
        stats.scan_user_strong_total,
        stats.scan_write_plan_local_total,
        stats.scan_snapshot_total,
        stats.scan_key_visited_total,
        stats.scan_key_returned_total,
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
        stats.atomic_apply_ns_total,
    )
}

fn metadata_service_json(stats: &nokvfs_meta::MetadataServiceStats) -> String {
    format!(
        "{{\"path_index_lookup_total\":{},\"path_index_hit_total\":{},\"path_index_miss_total\":{},\"path_index_stale_total\":{},\"path_index_fallback_total\":{},\"create_files_batch_total\":{},\"create_files_entry_total\":{},\"create_dirs_batch_total\":{},\"create_dirs_entry_total\":{},\"read_dir_plus_total\":{},\"read_dir_plus_entry_total\":{},\"read_dir_plus_projection_hit_total\":{}}}",
        stats.path_index_lookup_total,
        stats.path_index_hit_total,
        stats.path_index_miss_total,
        stats.path_index_stale_total,
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

impl From<SharedLogError> for ServerError {
    fn from(err: SharedLogError) -> Self {
        Self::SharedLog(err)
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Metadata(err) => write!(f, "{err}"),
            Self::Object(err) => write!(f, "{err}"),
            Self::SharedLog(err) => write!(f, "{err}"),
        }
    }
}

impl Error for ServerError {}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use nokvfs_cluster::{
        FileSharedLogSync, InMemoryQuorumLog, LogTerm, NodeId, QuorumNodeLog, ReadFreshness,
        SharedMetadataLog,
    };
    use nokvfs_meta::holtstore::HoltMetadataStore;
    use nokvfs_meta::{HistoryGcOptions, ObjectGcOptions};
    use nokvfs_object::{ObjectStoreConfig, S3ObjectStoreOptions};
    use nokvfs_types::MountId;
    use tempfile::tempdir;

    use crate::metadata::{ServerMetadataBackend, ServerMetadataLogStatus, ServerMetadataStore};

    fn node(raw: u64) -> NodeId {
        NodeId::new(raw).unwrap()
    }

    pub(crate) fn test_options(root: &Path, metadata_log_path: Option<PathBuf>) -> ServerOptions {
        ServerOptions {
            bind: crate::options::DEFAULT_SERVER_BIND,
            mount: MountId::new(1).unwrap(),
            meta_path: root.join("meta"),
            metadata_log_path,
            metadata_log_term: LogTerm::new(1).unwrap(),
            metadata_log_sync: FileSharedLogSync::Data,
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
        Server::open(test_options(dir.path(), None)).unwrap()
    }

    fn test_server_with_shared_metadata(
        root: &Path,
        metadata: ServerMetadataStore,
        metadata_log_status: Arc<dyn ServerMetadataLogStatus>,
        bootstrap_root: bool,
    ) -> Server {
        let options = test_options(root, None);
        let objects = options.object.open().unwrap();
        let service = Arc::new(NoKvFs::open_existing(options.mount, metadata, objects).unwrap());
        if bootstrap_root {
            service
                .bootstrap_root(DEFAULT_ROOT_MODE, options.uid, options.gid)
                .unwrap();
        }
        let object_gc = ObjectGcWorker::spawn(Arc::clone(&service), options.object_gc);
        let history_gc = HistoryGcWorker::spawn(Arc::clone(&service), options.history_gc);
        Server {
            service,
            metadata_log_enabled: true,
            metadata_log_sync: options.metadata_log_sync,
            metadata_log_status: Some(metadata_log_status),
            metadata_log: None,
            metadata_checkpoint: None,
            object_gc,
            history_gc,
        }
    }

    #[test]
    fn manual_gc_reports_empty_outcomes() {
        let server = test_server();
        assert!(server
            .stats_json()
            .contains("\"metadata_log\":{\"enabled\":false,\"commit_entry_total\":0"));
        let body = server.run_manual_gc(128).unwrap();
        assert!(body.contains("\"object_gc\""));
        assert!(body.contains("\"history_gc\""));
        assert!(body.contains("\"metadata_log\":{\"enabled\":false}"));
    }

    #[test]
    fn manual_gc_compacts_metadata_log_through_applied_frontier() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let server = Server::open(test_options(dir.path(), Some(metadata_log.clone()))).unwrap();
        server
            .service()
            .create_dir_path("/runs", 0o755, 1000, 1000)
            .unwrap();
        let applied = server.metadata_log_frontier().unwrap();

        let body = server.run_manual_gc(128).unwrap();
        assert!(body.contains("\"metadata_log\":{\"enabled\":true"));
        let expected_checkpoint_id = format!(
            "\"checkpoint_id\":\"mount-{}-term-{}-index-{}\"",
            server.service().mount_id().get(),
            applied.position.term.get(),
            applied.position.index.get()
        );
        assert!(body.contains(&expected_checkpoint_id));
        assert!(body.contains("\"checkpoint_uri\":\"local-holt:"));
        assert!(body.contains("\"checkpoint_size_bytes\":0"));
        assert!(body.contains("\"compacted_through\":"));
        let checkpoint_path = metadata_checkpoint_path(&metadata_log);
        assert!(checkpoint_path.is_file());
        let checkpoint = FileCheckpointCatalog::open(&checkpoint_path)
            .unwrap()
            .latest_for_mount(server.service().mount_id())
            .unwrap()
            .expect("manual GC should publish checkpoint manifest");
        assert_eq!(
            checkpoint.id,
            metadata_checkpoint_id(server.service().mount_id(), &checkpoint.frontier)
        );
        assert_eq!(
            checkpoint.artifact.uri,
            metadata_checkpoint_artifact(&checkpoint.id).unwrap().uri
        );
        assert_eq!(
            checkpoint.frontier.applied_position.index,
            applied.position.index
        );
        let log = server.metadata_log.as_ref().unwrap().log();
        assert!(matches!(
            log.read_from(applied.position.index, 0),
            Err(SharedLogError::Compacted { .. })
        ));
    }

    #[test]
    fn stats_reports_metadata_log_sync_policy() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let mut options = test_options(dir.path(), Some(metadata_log));
        options.metadata_log_term = LogTerm::new(7).unwrap();
        options.metadata_log_sync = FileSharedLogSync::None;
        let server = Server::open(options).unwrap();
        server
            .service()
            .create_dir_path("/runs", 0o755, 1000, 1000)
            .unwrap();

        let stats = server.stats_json();
        assert!(stats.contains("\"metadata_log\":{\"enabled\":true,\"sync\":\"none\""));
        assert!(stats.contains("\"applied_term\":7"));
        assert!(stats.contains("\"commit_entry_total\":"));
        assert!(stats.contains("\"commit_command_total\":"));
        assert!(stats.contains("\"max_commands_per_entry\":"));
        let runtime = server.metadata_log_runtime_stats().unwrap();
        assert!(runtime.commit_entry_total >= 1);
        assert_eq!(runtime.commit_entry_total, runtime.commit_command_total);
        assert_eq!(runtime.max_commands_per_entry, 1);
    }

    #[test]
    fn server_metadata_log_preserves_namespace_after_reopen() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        {
            let server =
                Server::open(test_options(dir.path(), Some(metadata_log.clone()))).unwrap();
            server
                .service()
                .create_dir_path("/runs", 0o755, 1000, 1000)
                .unwrap();
            let stats = server.stats_json();
            assert!(stats.contains("\"metadata_log\":{\"enabled\":true"));
            assert!(stats.contains("\"applied_index\":"));
            assert!(stats.contains("\"commit_version\":"));
        }
        let apply_marker = dir.path().join("metadata.log.apply");
        assert!(apply_marker.is_file());

        let reopened = Server::open(test_options(dir.path(), Some(metadata_log))).unwrap();
        assert!(reopened.stats_json().contains("\"applied_index\":"));
        let entry = reopened
            .service()
            .lookup_path("/runs")
            .unwrap()
            .expect("created directory should survive metadata-log reopen");
        assert_eq!(entry.attr.file_type, nokvfs_types::FileType::Directory);
    }

    #[test]
    fn server_quorum_learner_enforces_require_applied_until_local_tail_replayed() {
        let dir = tempdir().unwrap();
        let log = Arc::new(
            InMemoryQuorumLog::with_learners([node(1), node(2), node(3)], [node(4)]).unwrap(),
        );
        log.set_node_available(node(4), false).unwrap();
        let mount = MountId::new(1).unwrap();
        let term = LogTerm::new(1).unwrap();
        let leader_logged = Arc::new(SharedLogMetadataStore::new(
            HoltMetadataStore::open_memory().unwrap(),
            QuorumNodeLog::new(Arc::clone(&log), node(1)).unwrap(),
            term,
            mount,
        ));
        let learner_logged = Arc::new(SharedLogMetadataStore::new(
            HoltMetadataStore::open_memory().unwrap(),
            QuorumNodeLog::new(Arc::clone(&log), node(4)).unwrap(),
            term,
            mount,
        ));
        let leader_backend: Arc<dyn ServerMetadataBackend> = leader_logged.clone();
        let leader_status: Arc<dyn ServerMetadataLogStatus> = leader_logged.clone();
        let leader = test_server_with_shared_metadata(
            dir.path(),
            ServerMetadataStore::shared_logged(leader_backend),
            leader_status,
            true,
        );

        leader
            .service()
            .create_dir_path("/runs", 0o755, 1000, 1000)
            .unwrap();
        let position = leader
            .metadata_log_applied_position()
            .expect("leader write should publish metadata position");

        assert!(matches!(
            learner_logged.ensure_read_freshness(ReadFreshness::AppliedThrough(position)),
            Err(SharedLogError::ReadNotFresh {
                required,
                applied: None,
            }) if required == position
        ));

        log.set_node_available(node(4), true).unwrap();
        log.sync_learner(node(4)).unwrap();
        let replay = learner_logged.replay_committed_tail(0).unwrap();

        assert!(replay.entries >= 2);
        assert!(replay.commands >= 2);
        let learner_backend: Arc<dyn ServerMetadataBackend> = learner_logged.clone();
        let learner_status: Arc<dyn ServerMetadataLogStatus> = learner_logged.clone();
        let learner = test_server_with_shared_metadata(
            dir.path(),
            ServerMetadataStore::shared_logged(learner_backend),
            learner_status,
            false,
        );
        learner.ensure_metadata_log_applied(position).unwrap();
        let entry = learner
            .service()
            .lookup_path("/runs")
            .unwrap()
            .expect("learner should serve the synced directory");
        assert_eq!(entry.attr.file_type, nokvfs_types::FileType::Directory);
    }
}
