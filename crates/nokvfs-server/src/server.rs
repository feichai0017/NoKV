use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use nokvfs_cluster::{
    compact_log_to_checkpoint, AppendMetadataBatchRequest, AppendMetadataBatchResponse,
    ApplyFrontier, CheckpointArtifact, CheckpointCatalog, CheckpointManifest,
    FileAppliedFrontierStore, FileCheckpointCatalog, FileMembershipCatalog, FileSharedLog,
    FileSharedLogOptions, InstallCheckpointRequest, InstallCheckpointResponse, LogIndex,
    LogPosition, MembershipCatalog, MetadataLogEntry, MetadataMembership, NodeId, SharedLogError,
    SharedLogMetadataStore, SharedLogRuntimeStats, SharedMetadataLog,
};
use nokvfs_meta::holtstore::HoltMetadataStore;
use nokvfs_meta::{
    HistoryGcWorker, HistoryGcWorkerState, MetadError, NoKvFs, ObjectGcWorker, ObjectGcWorkerState,
};
use nokvfs_object::{ObjectError, S3ObjectStore};
use sha2::{Digest, Sha256};

use crate::http;
use crate::metadata::{FileLoggedMetadataStore, ServerMetadataLogStatus, ServerMetadataStore};
use crate::options::ServerOptions;

const DEFAULT_ROOT_MODE: u32 = 0o755;

pub struct Server {
    service: Arc<NoKvFs<ServerMetadataStore, S3ObjectStore>>,
    metadata_log_enabled: bool,
    metadata_log_node: NodeId,
    metadata_membership: Option<MetadataMembership>,
    metadata_log_sync: nokvfs_cluster::FileSharedLogSync,
    metadata_log_status: Option<Arc<dyn ServerMetadataLogStatus>>,
    metadata_log: Option<Arc<FileLoggedMetadataStore>>,
    metadata_checkpoint: Option<FileCheckpointCatalog>,
    metadata_checkpoint_artifact_dir: Option<PathBuf>,
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
        let mut checkpoint_artifact_dir = None;
        let mut metadata_membership = None;
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
                let artifact_dir = metadata_checkpoint_artifact_dir(path);
                fs::create_dir_all(&artifact_dir).map_err(ServerError::Io)?;
                let membership_catalog =
                    FileMembershipCatalog::open(metadata_membership_path(path))?;
                let membership = metadata_membership_for_node(
                    &membership_catalog,
                    options.mount,
                    options.metadata_log_term,
                    options.metadata_log_node,
                )?;
                let log_term = membership.term;
                let (logged, _replay) = SharedLogMetadataStore::recover_with_frontier_store(
                    metadata,
                    log,
                    log_term,
                    options.mount,
                    frontier,
                )
                .map_err(|err| ServerError::SharedLog(SharedLogError::Backend(err.to_string())))?;
                let logged = Arc::new(logged);
                let status: Arc<dyn ServerMetadataLogStatus> = logged.clone();
                metadata_log = Some(Arc::clone(&logged));
                metadata_log_status = Some(status);
                metadata_checkpoint = Some(checkpoint);
                checkpoint_artifact_dir = Some(artifact_dir);
                metadata_membership = Some(membership);
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
            metadata_log_node: options.metadata_log_node,
            metadata_membership,
            metadata_log_sync: options.metadata_log_sync,
            metadata_log_status,
            metadata_log,
            metadata_checkpoint,
            metadata_checkpoint_artifact_dir: checkpoint_artifact_dir,
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
        self.authorize_metadata_log_leader(request.leader)?;
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

    pub(crate) fn latest_metadata_checkpoint(
        &self,
        mount: nokvfs_types::MountId,
    ) -> Result<Option<CheckpointManifest>, ServerError> {
        if mount != self.service.mount_id() {
            return Err(ServerError::SharedLog(SharedLogError::Backend(format!(
                "metadata checkpoint mount {} does not match server mount {}",
                mount.get(),
                self.service.mount_id().get(),
            ))));
        }
        let Some(checkpoints) = self.metadata_checkpoint.as_ref() else {
            return Err(ServerError::SharedLog(SharedLogError::Backend(
                "metadata checkpoint catalog is disabled".to_owned(),
            )));
        };
        checkpoints
            .latest_for_mount(mount)
            .map_err(ServerError::SharedLog)
    }

    pub(crate) fn plan_metadata_bootstrap(
        &self,
        leader: NodeId,
        learner: NodeId,
        mount: nokvfs_types::MountId,
    ) -> Result<nokvfs_cluster::InstallCheckpointRequest, ServerError> {
        self.authorize_metadata_log_leader(leader)?;
        if mount != self.service.mount_id() {
            return Err(ServerError::SharedLog(SharedLogError::Backend(format!(
                "metadata bootstrap mount {} does not match server mount {}",
                mount.get(),
                self.service.mount_id().get(),
            ))));
        }
        let Some(metadata_log) = self.metadata_log.as_ref() else {
            return Err(ServerError::SharedLog(SharedLogError::Backend(
                "metadata log is disabled".to_owned(),
            )));
        };
        let checkpoint = self
            .latest_metadata_checkpoint(mount)?
            .ok_or(SharedLogError::CheckpointRequired {
                node: learner,
                compacted: LogIndex::ZERO,
            })
            .map_err(ServerError::SharedLog)?;
        let replay_start = checkpoint.frontier.min_retained_index;
        let tail = metadata_log.log().read_from(replay_start, 0)?;
        let replayed_index = tail
            .last()
            .map(|entry| entry.position.index)
            .unwrap_or(checkpoint.frontier.applied_position.index);
        Ok(nokvfs_cluster::InstallCheckpointRequest::from_plan(
            leader,
            nokvfs_cluster::LearnerBootstrapPlan {
                node: learner,
                checkpoint,
                replay_start,
                replayed_index,
            },
        ))
    }

    pub(crate) fn install_metadata_checkpoint(
        &self,
        request: InstallCheckpointRequest,
    ) -> Result<InstallCheckpointResponse, ServerError> {
        self.authorize_metadata_log_leader(request.leader)?;
        if request.plan.node != self.metadata_log_node {
            return Err(ServerError::SharedLog(SharedLogError::Backend(format!(
                "metadata checkpoint learner {} does not match server node {}",
                request.plan.node.get(),
                self.metadata_log_node.get()
            ))));
        }
        if request.plan.checkpoint.mount != self.service.mount_id() {
            return Err(ServerError::SharedLog(SharedLogError::Backend(format!(
                "metadata checkpoint mount {} does not match server mount {}",
                request.plan.checkpoint.mount.get(),
                self.service.mount_id().get()
            ))));
        }
        let Some(metadata_log) = self.metadata_log.as_ref() else {
            return Err(ServerError::SharedLog(SharedLogError::Backend(
                "metadata log is disabled".to_owned(),
            )));
        };

        let image = read_metadata_checkpoint_artifact(&request.plan.checkpoint.artifact)?;
        let frontier = ApplyFrontier {
            position: request.plan.checkpoint.frontier.applied_position,
            commit_version: request.plan.checkpoint.frontier.max_commit_version,
        };
        metadata_log
            .install_checkpoint_state(frontier, |store| store.install_checkpoint_image(&image))
            .map_err(ServerError::SharedLog)?;
        let replay = metadata_log
            .replay_committed_tail(0)
            .map_err(|err| ServerError::SharedLog(SharedLogError::Backend(err.to_string())))?;
        let replayed_index = replay
            .frontier
            .map(|frontier| frontier.position.index)
            .unwrap_or(request.plan.checkpoint.frontier.applied_position.index);
        if replayed_index < request.plan.replayed_index {
            return Err(ServerError::SharedLog(SharedLogError::Backend(format!(
                "metadata checkpoint replay reached index {}, expected at least {}",
                replayed_index.get(),
                request.plan.replayed_index.get()
            ))));
        }
        Ok(InstallCheckpointResponse {
            learner: request.plan.node,
            replay_start: request.plan.replay_start,
            replayed_index,
        })
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

    fn authorize_metadata_log_leader(&self, leader: NodeId) -> Result<(), ServerError> {
        if let Some(membership) = self.metadata_membership.as_ref() {
            return membership
                .authorize_leader(leader)
                .map_err(ServerError::SharedLog);
        }
        if leader != self.metadata_log_node {
            return Err(ServerError::SharedLog(SharedLogError::UnauthorizedLeader {
                expected: self.metadata_log_node,
                proposed: leader,
            }));
        }
        Ok(())
    }

    fn compact_metadata_log(&self) -> Result<Option<CheckpointManifest>, ServerError> {
        let Some(metadata_log) = self.metadata_log.as_ref() else {
            return Ok(None);
        };
        let Some(checkpoints) = self.metadata_checkpoint.as_ref() else {
            return Ok(None);
        };
        let Some(checkpoint_artifact_dir) = self.metadata_checkpoint_artifact_dir.as_ref() else {
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
        let image = metadata_log
            .inner()
            .export_checkpoint_image()
            .map_err(MetadError::from)?;
        let artifact =
            write_metadata_checkpoint_artifact(checkpoint_artifact_dir, &checkpoint_id, &image)?;
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

fn metadata_checkpoint_artifact_dir(log_path: &Path) -> PathBuf {
    let mut path = log_path.to_path_buf();
    let file_name = log_path
        .file_name()
        .map(|name| {
            let mut name = name.to_os_string();
            name.push(".checkpoint-images");
            name
        })
        .unwrap_or_else(|| "metadata.log.checkpoint-images".into());
    path.set_file_name(file_name);
    path
}

fn metadata_membership_path(log_path: &Path) -> PathBuf {
    let mut path = log_path.to_path_buf();
    let file_name = log_path
        .file_name()
        .map(|name| {
            let mut name = name.to_os_string();
            name.push(".membership");
            name
        })
        .unwrap_or_else(|| "metadata.log.membership".into());
    path.set_file_name(file_name);
    path
}

fn metadata_membership_for_node(
    catalog: &FileMembershipCatalog,
    mount: nokvfs_types::MountId,
    fallback_term: nokvfs_cluster::LogTerm,
    node: NodeId,
) -> Result<MetadataMembership, ServerError> {
    let membership = match catalog.latest_for_mount(mount)? {
        Some(membership) => membership,
        None => {
            let membership = MetadataMembership::single_voter(mount, fallback_term, node)?;
            catalog.publish(membership.clone())?;
            membership
        }
    };
    if !membership.is_voter(node) && !membership.is_learner(node) {
        return Err(ServerError::SharedLog(SharedLogError::UnknownNode(node)));
    }
    Ok(membership)
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

fn write_metadata_checkpoint_artifact(
    dir: &Path,
    id: &[u8],
    image: &[u8],
) -> Result<CheckpointArtifact, SharedLogError> {
    fs::create_dir_all(dir).map_err(|err| {
        SharedLogError::Backend(format!(
            "create metadata checkpoint artifact dir {}: {err}",
            dir.display()
        ))
    })?;
    let name = String::from_utf8(id.to_vec()).map_err(|err| {
        SharedLogError::Backend(format!("metadata checkpoint id is not utf-8: {err}"))
    })?;
    let path = dir.join(format!("{name}.nkmeta"));
    fs::write(&path, image).map_err(|err| {
        SharedLogError::Backend(format!(
            "write metadata checkpoint artifact {}: {err}",
            path.display()
        ))
    })?;
    let digest = Sha256::digest(image).to_vec();
    CheckpointArtifact::new(
        format!("file:{}", path.display()).into_bytes(),
        digest,
        image.len() as u64,
    )
}

fn read_metadata_checkpoint_artifact(
    artifact: &CheckpointArtifact,
) -> Result<Vec<u8>, SharedLogError> {
    let uri = std::str::from_utf8(&artifact.uri).map_err(|err| {
        SharedLogError::Backend(format!(
            "metadata checkpoint artifact URI is not utf-8: {err}"
        ))
    })?;
    let path = uri.strip_prefix("file:").ok_or_else(|| {
        SharedLogError::Backend(format!(
            "unsupported metadata checkpoint artifact URI scheme: {uri}"
        ))
    })?;
    let image = fs::read(path).map_err(|err| {
        SharedLogError::Backend(format!("read metadata checkpoint artifact {path}: {err}"))
    })?;
    if image.len() as u64 != artifact.size_bytes {
        return Err(SharedLogError::Backend(format!(
            "metadata checkpoint artifact size mismatch: expected {}, got {}",
            artifact.size_bytes,
            image.len()
        )));
    }
    let digest = Sha256::digest(&image);
    if digest.as_slice() != artifact.digest.as_slice() {
        return Err(SharedLogError::Backend(
            "metadata checkpoint artifact digest mismatch".to_owned(),
        ));
    }
    Ok(image)
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
    use nokvfs_meta::command::{MetadataStore, ReadPurpose, ScanRequest};
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
            metadata_log_node: NodeId::new(1).unwrap(),
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
            metadata_log_node: options.metadata_log_node,
            metadata_membership: Some(
                MetadataMembership::single_voter(
                    options.mount,
                    options.metadata_log_term,
                    options.metadata_log_node,
                )
                .unwrap(),
            ),
            metadata_log_sync: options.metadata_log_sync,
            metadata_log_status: Some(metadata_log_status),
            metadata_log: None,
            metadata_checkpoint: None,
            metadata_checkpoint_artifact_dir: None,
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
        assert!(body.contains("\"checkpoint_uri\":\"file:"));
        assert!(!body.contains("\"checkpoint_size_bytes\":0"));
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
        assert!(checkpoint.artifact.size_bytes > 0);
        assert!(!checkpoint.artifact.digest.is_empty());
        let uri = String::from_utf8(checkpoint.artifact.uri.clone()).unwrap();
        let image_path = uri.strip_prefix("file:").expect("checkpoint image URI");
        let image = fs::read(image_path).unwrap();
        assert_eq!(image.len() as u64, checkpoint.artifact.size_bytes);
        let restored = HoltMetadataStore::open_memory().unwrap();
        restored.install_checkpoint_image(&image).unwrap();
        let restored_dentries = restored
            .scan(ScanRequest {
                family: nokvfs_types::RecordFamily::Dentry,
                prefix: Vec::new(),
                start_after: None,
                version: nokvfs_meta::Version::new(u64::MAX).unwrap(),
                limit: 10,
                purpose: ReadPurpose::UserStrong,
            })
            .unwrap();
        assert!(!restored_dentries.is_empty());
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
    fn server_publishes_metadata_log_membership() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let mut options = test_options(dir.path(), Some(metadata_log.clone()));
        options.metadata_log_node = node(4);
        options.metadata_log_term = LogTerm::new(7).unwrap();
        let server = Server::open(options).unwrap();

        let catalog = FileMembershipCatalog::open(metadata_membership_path(&metadata_log)).unwrap();
        let membership = catalog
            .latest_for_mount(server.service().mount_id())
            .unwrap()
            .expect("server should publish metadata log membership");
        assert_eq!(membership.mount, server.service().mount_id());
        assert_eq!(membership.term, LogTerm::new(7).unwrap());
        assert_eq!(membership.leader, node(4));
        assert_eq!(membership.voters, vec![node(4)]);
        assert!(membership.learners.is_empty());
    }

    #[test]
    fn server_preserves_existing_metadata_log_membership_for_learner() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let catalog = FileMembershipCatalog::open(metadata_membership_path(&metadata_log)).unwrap();
        let membership = MetadataMembership::new(
            MountId::new(1).unwrap(),
            LogTerm::new(7).unwrap(),
            node(1),
            [node(1)],
            [node(4)],
        )
        .unwrap();
        catalog.publish(membership.clone()).unwrap();
        let mut options = test_options(dir.path(), Some(metadata_log.clone()));
        options.metadata_log_node = node(4);
        options.metadata_log_term = LogTerm::new(99).unwrap();

        let server = Server::open(options).unwrap();

        assert_eq!(server.metadata_membership.as_ref(), Some(&membership));
        let stored = FileMembershipCatalog::open(metadata_membership_path(&metadata_log))
            .unwrap()
            .latest_for_mount(server.service().mount_id())
            .unwrap()
            .expect("existing membership should survive server open");
        assert_eq!(stored, membership);
        assert!(matches!(
            server.plan_metadata_bootstrap(node(1), node(4), server.service().mount_id()),
            Err(ServerError::SharedLog(
                SharedLogError::CheckpointRequired { .. }
            ))
        ));
        assert!(matches!(
            server.plan_metadata_bootstrap(node(4), node(4), server.service().mount_id()),
            Err(ServerError::SharedLog(
                SharedLogError::UnauthorizedLeader { expected, proposed }
            )) if expected == node(1) && proposed == node(4)
        ));
    }

    #[test]
    fn server_rejects_metadata_log_node_outside_existing_membership() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let catalog = FileMembershipCatalog::open(metadata_membership_path(&metadata_log)).unwrap();
        catalog
            .publish(
                MetadataMembership::new(
                    MountId::new(1).unwrap(),
                    LogTerm::new(7).unwrap(),
                    node(1),
                    [node(1)],
                    [node(4)],
                )
                .unwrap(),
            )
            .unwrap();
        let mut options = test_options(dir.path(), Some(metadata_log));
        options.metadata_log_node = node(9);

        assert!(matches!(
            Server::open(options),
            Err(ServerError::SharedLog(SharedLogError::UnknownNode(unknown))) if unknown == node(9)
        ));
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
