use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use nokvfs_cluster::{
    compact_log_to_checkpoint, AppendMetadataBatchRequest, AppendMetadataBatchResponse,
    AppliedFrontierStore, ApplyFrontier, CheckpointArtifact, CheckpointCatalog, CheckpointManifest,
    FileAppliedFrontierStore, FileCheckpointCatalog, FileMembershipCatalog,
    FileMetadataRaftLogOptions, FileMetadataRaftLogSync, FileSharedLog, FileSharedLogOptions,
    InstallCheckpointRequest, InstallCheckpointResponse, LogIndex, LogPosition, MembershipCatalog,
    MetadataLogEntry, MetadataMembership, MetadataRaftRpcNetworkFactory, NodeId,
    OpenRaftMetadataStats, OpenRaftMetadataStatsHandle, SharedLogError, SharedLogMetadataStore,
    SharedLogRuntimeStats, SharedMetadataLog,
};
use nokvfs_meta::holtstore::HoltMetadataStore;
use nokvfs_meta::{
    HistoryGcWorker, HistoryGcWorkerState, MetadError, NoKvFs, ObjectGcWorker, ObjectGcWorkerState,
};
use nokvfs_object::{ObjectError, ObjectKey, ObjectStore, S3ObjectStore};
use sha2::{Digest, Sha256};

use crate::http;
use crate::metadata::{
    FileLoggedMetadataStore, OpenRaftLoggedMetadataStore, ServerMetadataLogStatus,
    ServerMetadataStore,
};
use crate::options::ServerOptions;
use crate::replication::{MajorityMetadataLog, MajorityMetadataLogReplicationStats};
use crate::rpc;

const DEFAULT_ROOT_MODE: u32 = 0o755;
const SERVER_CONNECTION_WORKERS: usize = 64;
const SERVER_CONNECTION_QUEUE: usize = 1024;
type CheckpointObjectStore = Arc<dyn ObjectStore + Send + Sync>;

pub struct Server {
    service: Arc<NoKvFs<ServerMetadataStore, S3ObjectStore>>,
    metadata_log_enabled: bool,
    metadata_log_node: NodeId,
    metadata_membership: Option<MetadataMembership>,
    metadata_log_peers: BTreeMap<NodeId, SocketAddr>,
    metadata_log_sync: nokvfs_cluster::FileSharedLogSync,
    metadata_raft: Option<OpenRaftMetadataStatsHandle>,
    metadata_log_status: Option<Arc<dyn ServerMetadataLogStatus>>,
    metadata_log: Option<Arc<FileLoggedMetadataStore>>,
    metadata_checkpoint: Option<Arc<FileCheckpointCatalog>>,
    metadata_checkpoint_objects: Option<CheckpointObjectStore>,
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
        let objects = options.object.open()?;
        let checkpoint_objects: CheckpointObjectStore = Arc::new(objects.clone());
        Self::open_with_checkpoint_objects(options, objects, checkpoint_objects)
    }

    #[cfg(test)]
    pub(crate) fn open_with_test_checkpoint_objects(
        options: ServerOptions,
        checkpoint_objects: nokvfs_object::MemoryObjectStore,
    ) -> Result<Self, ServerError> {
        let objects = options.object.open()?;
        Self::open_with_checkpoint_objects(options, objects, Arc::new(checkpoint_objects))
    }

    fn open_with_checkpoint_objects(
        options: ServerOptions,
        objects: S3ObjectStore,
        checkpoint_objects: CheckpointObjectStore,
    ) -> Result<Self, ServerError> {
        let metadata =
            HoltMetadataStore::open_file(&options.meta_path).map_err(MetadError::from)?;
        let mut metadata_log = None;
        let mut metadata_log_status = None;
        let mut metadata_checkpoint = None;
        let mut metadata_checkpoint_objects = None;
        let mut metadata_membership = None;
        let mut metadata_raft = None;
        let bootstrap_root;
        let metadata_log_peers = options
            .metadata_log_peers
            .iter()
            .map(|peer| (peer.node, peer.address))
            .collect::<BTreeMap<_, _>>();
        let metadata = match options.metadata_log_path.as_ref() {
            Some(path) => {
                let local_log = Arc::new(FileSharedLog::open(
                    path,
                    FileSharedLogOptions {
                        sync: options.metadata_log_sync,
                    },
                )?);
                let frontier = FileAppliedFrontierStore::open(metadata_apply_frontier_path(path))?;
                let checkpoint =
                    Arc::new(FileCheckpointCatalog::open(metadata_checkpoint_path(path))?);
                let membership_catalog =
                    FileMembershipCatalog::open(metadata_membership_path(path))?;
                let membership = metadata_membership_for_node(
                    &membership_catalog,
                    options.mount,
                    options.metadata_log_term,
                    options.metadata_log_node,
                    options.metadata_log_leader,
                    &options.metadata_log_voters,
                    &options.metadata_log_learners,
                )?;
                let log_term = membership.term;
                install_startup_checkpoint_if_required(
                    &metadata,
                    local_log.as_ref(),
                    &frontier,
                    checkpoint.as_ref(),
                    checkpoint_objects.as_ref(),
                    options.mount,
                    options.metadata_log_node,
                )?;
                let log = MajorityMetadataLog::new(
                    options.metadata_log_node,
                    membership.clone(),
                    Arc::clone(&local_log),
                    &options.metadata_log_peers,
                    Some(Arc::clone(&checkpoint)),
                );
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
                metadata_checkpoint_objects = Some(checkpoint_objects);
                metadata_membership = Some(membership);
                bootstrap_root = metadata_membership
                    .as_ref()
                    .map(|membership| membership.leader == options.metadata_log_node)
                    .unwrap_or(true);
                ServerMetadataStore::shared_logged(logged)
            }
            None => {
                let voters = metadata_raft_voters_for_options(&options)?;
                let voter_count = voters.len();
                let openraft =
                    OpenRaftLoggedMetadataStore::new_initialized_voter_group_with_file_log_and_network(
                        metadata,
                        options.metadata_log_node,
                        default_metadata_raft_log_path(&options.meta_path),
                        FileMetadataRaftLogOptions {
                            sync: metadata_raft_log_sync(options.metadata_log_sync),
                        },
                        MetadataRaftRpcNetworkFactory::new(
                            rpc::MetadataRaftFramedRpcClient::default(),
                        ),
                        &voters,
                    )
                    .map_err(MetadError::from)?;
                if voter_count == 1 {
                    openraft
                        .wait_for_current_leader(options.metadata_log_node, Duration::from_secs(3))
                        .map_err(MetadError::from)?;
                    bootstrap_root = true;
                } else {
                    bootstrap_root = false;
                }
                metadata_raft = Some(openraft.stats_handle());
                ServerMetadataStore::openraft(openraft)
            }
        };
        let service = Arc::new(NoKvFs::open_existing(options.mount, metadata, objects)?);
        if bootstrap_root {
            service.bootstrap_root(DEFAULT_ROOT_MODE, options.uid, options.gid)?;
        }
        let object_gc = ObjectGcWorker::spawn(Arc::clone(&service), options.object_gc);
        let history_gc = HistoryGcWorker::spawn(Arc::clone(&service), options.history_gc);
        Ok(Self {
            service,
            metadata_log_enabled: metadata_log.is_some(),
            metadata_log_node: options.metadata_log_node,
            metadata_membership,
            metadata_log_peers,
            metadata_log_sync: options.metadata_log_sync,
            metadata_raft,
            metadata_log_status,
            metadata_log,
            metadata_checkpoint,
            metadata_checkpoint_objects,
            object_gc,
            history_gc,
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

    pub(crate) fn refresh_metadata_view(&self) -> Result<(), ServerError> {
        self.service
            .refresh_allocator_state()
            .map_err(ServerError::Metadata)
    }

    #[cfg(test)]
    pub(crate) fn shutdown_metadata_raft(&self) -> Result<(), ServerError> {
        self.service
            .metadata_store()
            .shutdown_openraft()
            .map_err(|err| ServerError::Metadata(MetadError::from(err)))
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
        if request.entry.mount != self.service.mount_id() {
            return Err(ServerError::SharedLog(SharedLogError::Backend(format!(
                "metadata log append mount {} does not match server mount {}",
                request.entry.mount.get(),
                self.service.mount_id().get(),
            ))));
        }
        let Some(metadata_log) = self.metadata_log.as_ref() else {
            return Err(ServerError::SharedLog(SharedLogError::Backend(
                "metadata log is disabled".to_owned(),
            )));
        };
        let receipts = metadata_log.log().append_entry(request.entry)?;
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

        let Some(checkpoint_objects) = self.metadata_checkpoint_objects.as_ref() else {
            return Err(ServerError::SharedLog(SharedLogError::Backend(
                "metadata checkpoint object store is disabled".to_owned(),
            )));
        };
        let image = read_metadata_checkpoint_artifact(
            checkpoint_objects.as_ref(),
            &request.plan.checkpoint.artifact,
        )?;
        let frontier = ApplyFrontier {
            position: request.plan.checkpoint.frontier.applied_position,
            commit_version: request.plan.checkpoint.frontier.max_commit_version,
        };
        metadata_log
            .install_checkpoint_state(frontier, |store| store.install_checkpoint_image(&image))
            .map_err(ServerError::SharedLog)?;
        if let Some(compact_through) = request.plan.checkpoint.frontier.compact_through() {
            metadata_log.log().compact_through(compact_through)?;
        }
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
        self.service.refresh_allocator_state()?;
        Ok(InstallCheckpointResponse {
            learner: request.plan.node,
            replay_start: request.plan.replay_start,
            replayed_index,
        })
    }

    pub(crate) fn bootstrap_metadata_peer(
        &self,
        learner: NodeId,
    ) -> Result<InstallCheckpointResponse, ServerError> {
        self.authorize_metadata_log_leader(self.metadata_log_node)?;
        if let Some(membership) = self.metadata_membership.as_ref() {
            if !membership.is_voter(learner) && !membership.is_learner(learner) {
                return Err(ServerError::SharedLog(SharedLogError::UnknownNode(learner)));
            }
        }
        let address = self
            .metadata_log_peers
            .get(&learner)
            .copied()
            .ok_or(ServerError::SharedLog(SharedLogError::UnknownNode(learner)))?;
        let request =
            self.plan_metadata_bootstrap(self.metadata_log_node, learner, self.service.mount_id())?;
        let checkpoint_only = InstallCheckpointRequest::from_plan(
            request.leader,
            nokvfs_cluster::LearnerBootstrapPlan {
                node: request.plan.node,
                checkpoint: request.plan.checkpoint.clone(),
                replay_start: request.plan.replay_start,
                replayed_index: request.plan.checkpoint.frontier.applied_position.index,
            },
        );
        let mut install = rpc::call_install_metadata_checkpoint(address, checkpoint_only)?;
        let Some(metadata_log) = self.metadata_log.as_ref() else {
            return Err(ServerError::SharedLog(SharedLogError::Backend(
                "metadata log is disabled".to_owned(),
            )));
        };
        for entry in metadata_log.log().read_from(request.plan.replay_start, 0)? {
            if entry.position.index > request.plan.replayed_index {
                break;
            }
            rpc::call_append_metadata_log(address, self.metadata_log_node, &entry)?;
            install.replayed_index = entry.position.index;
        }
        if install.replayed_index < request.plan.replayed_index {
            return Err(ServerError::SharedLog(SharedLogError::Backend(format!(
                "metadata peer bootstrap reached index {}, expected at least {}",
                install.replayed_index.get(),
                request.plan.replayed_index.get()
            ))));
        }
        Ok(install)
    }

    pub fn stats_json(&self) -> String {
        let objects = self.service.object_stats();
        let metadata = self.service.metadata_store_stats();
        let metadata_service = self.service.metadata_service_stats();
        let object_gc = self.object_gc.state();
        let history_gc = self.history_gc.state();
        format!(
            "{{\"ready\":true,\"block_cache_enabled\":{},\"object_puts\":{},\"object_put_bytes\":{},\"object_gets\":{},\"object_get_bytes\":{},\"cache_hits\":{},\"cache_hit_bytes\":{},\"manifest_chunks\":{},\"manifest_blocks\":{},\"metadata_store\":{},\"metadata_raft\":{},\"metadata_log\":{},\"metadata_service\":{},\"object_gc\":{},\"history_gc\":{}}}\n",
            self.service.block_cache_enabled(),
            objects.object_puts,
            objects.object_put_bytes,
            objects.object_gets,
            objects.object_get_bytes,
            objects.cache_hits,
            objects.cache_hit_bytes,
            objects.manifest_chunks,
            objects.manifest_blocks,
            metadata_store_json(&metadata),
            metadata_raft_json(self.metadata_raft.as_ref().map(|handle| handle.stats())),
            metadata_log_json(
                self.metadata_log_enabled,
                self.metadata_log_sync,
                self.metadata_log_frontier(),
                self.metadata_log_runtime_stats(),
                self.metadata_log_replication_stats(),
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
        let Some(checkpoint_objects) = self.metadata_checkpoint_objects.as_ref() else {
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
            write_metadata_checkpoint_artifact(checkpoint_objects.as_ref(), &checkpoint_id, &image)
                .map_err(ServerError::SharedLog)?;
        let manifest = CheckpointManifest::new(checkpoint_id, mount, frontier, artifact)?;
        checkpoints.publish(manifest.clone())?;
        let outcome = compact_log_to_checkpoint(metadata_log.log(), manifest)
            .map_err(ServerError::SharedLog)?;
        Ok(outcome.manifest)
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
                .name(format!("nokvfs-conn-{worker}"))
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
                                eprintln!("nokvfs-server connection failed: {err}");
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
                "nokvfs connection worker pool stopped",
            ))
        })
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

fn default_metadata_raft_log_path(meta_path: &Path) -> PathBuf {
    meta_path.join("metadata-raft.log")
}

fn metadata_raft_log_sync(sync: nokvfs_cluster::FileSharedLogSync) -> FileMetadataRaftLogSync {
    match sync {
        nokvfs_cluster::FileSharedLogSync::Data => FileMetadataRaftLogSync::Data,
        nokvfs_cluster::FileSharedLogSync::None => FileMetadataRaftLogSync::None,
    }
}

fn metadata_raft_voters_for_options(
    options: &ServerOptions,
) -> Result<BTreeMap<NodeId, String>, ServerError> {
    if !options.metadata_log_learners.is_empty() {
        return Err(ServerError::SharedLog(SharedLogError::Backend(
            "OpenRaft metadata learners are not wired yet".to_owned(),
        )));
    }
    let configured_voters = if options.metadata_log_voters.is_empty() {
        vec![options.metadata_log_node]
    } else {
        options.metadata_log_voters.clone()
    };
    let mut voters = BTreeMap::new();
    for node in configured_voters {
        let address = metadata_raft_node_address(options, node)?;
        if voters.insert(node, address).is_some() {
            return Err(ServerError::SharedLog(SharedLogError::DuplicateNode(node)));
        }
    }
    if voters.is_empty() {
        return Err(ServerError::SharedLog(SharedLogError::NoVoters));
    }
    if !voters.contains_key(&options.metadata_log_node) {
        return Err(ServerError::SharedLog(SharedLogError::UnknownNode(
            options.metadata_log_node,
        )));
    }
    Ok(voters)
}

fn metadata_raft_node_address(
    options: &ServerOptions,
    node: NodeId,
) -> Result<String, ServerError> {
    if node == options.metadata_log_node {
        return Ok(options.bind.to_string());
    }
    options
        .metadata_log_peers
        .iter()
        .find(|peer| peer.node == node)
        .map(|peer| peer.address.to_string())
        .ok_or(ServerError::SharedLog(SharedLogError::UnknownNode(node)))
}

fn metadata_membership_for_node(
    catalog: &FileMembershipCatalog,
    mount: nokvfs_types::MountId,
    fallback_term: nokvfs_cluster::LogTerm,
    node: NodeId,
    leader: NodeId,
    configured_voters: &[NodeId],
    configured_learners: &[NodeId],
) -> Result<MetadataMembership, ServerError> {
    let membership = match catalog.latest_for_mount(mount)? {
        Some(membership) => membership,
        None => {
            let membership = if configured_voters.is_empty() {
                let voters = [node];
                MetadataMembership::new(
                    mount,
                    fallback_term,
                    leader,
                    voters,
                    configured_learners.iter().copied(),
                )?
            } else {
                MetadataMembership::new(
                    mount,
                    fallback_term,
                    leader,
                    configured_voters.iter().copied(),
                    configured_learners.iter().copied(),
                )?
            };
            catalog.publish(membership.clone())?;
            membership
        }
    };
    if !membership.is_voter(node) && !membership.is_learner(node) {
        return Err(ServerError::SharedLog(SharedLogError::UnknownNode(node)));
    }
    Ok(membership)
}

fn install_startup_checkpoint_if_required(
    metadata: &HoltMetadataStore,
    log: &FileSharedLog,
    frontier: &FileAppliedFrontierStore,
    checkpoints: &FileCheckpointCatalog,
    objects: &dyn ObjectStore,
    mount: nokvfs_types::MountId,
    node: NodeId,
) -> Result<(), ServerError> {
    if frontier.load()?.is_some() {
        return Ok(());
    }
    let first_index = LogIndex::new(1)?;
    let compacted = match log.read_from(first_index, 1) {
        Ok(_) => return Ok(()),
        Err(SharedLogError::Compacted { compacted, .. }) => compacted,
        Err(err) => return Err(ServerError::SharedLog(err)),
    };
    let checkpoint = checkpoints
        .latest_for_mount(mount)?
        .ok_or(SharedLogError::CheckpointRequired { node, compacted })
        .map_err(ServerError::SharedLog)?;
    let checkpoint_compacted = checkpoint
        .frontier
        .compact_through()
        .unwrap_or(LogIndex::ZERO);
    if checkpoint_compacted < compacted {
        return Err(ServerError::SharedLog(SharedLogError::CheckpointTooOld {
            node,
            checkpoint_compacted,
            required: compacted,
        }));
    }
    let image = read_metadata_checkpoint_artifact(objects, &checkpoint.artifact)?;
    metadata
        .install_checkpoint_image(&image)
        .map_err(|err| ServerError::SharedLog(SharedLogError::Backend(err.to_string())))?;
    frontier.save(ApplyFrontier {
        position: checkpoint.frontier.applied_position,
        commit_version: checkpoint.frontier.max_commit_version,
    })?;
    Ok(())
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
    objects: &dyn ObjectStore,
    id: &[u8],
    image: &[u8],
) -> Result<CheckpointArtifact, SharedLogError> {
    let name = String::from_utf8(id.to_vec()).map_err(|err| {
        SharedLogError::Backend(format!("metadata checkpoint id is not utf-8: {err}"))
    })?;
    let key = ObjectKey::new(format!("metadata-checkpoints/{name}.nkmeta"))
        .map_err(|err| SharedLogError::Backend(err.to_string()))?;
    objects
        .put(&key, image)
        .map_err(|err| SharedLogError::Backend(err.to_string()))?;
    let digest = Sha256::digest(image).to_vec();
    CheckpointArtifact::new(
        format!("object:{}", key.as_str()).into_bytes(),
        digest,
        image.len() as u64,
    )
}

fn read_metadata_checkpoint_artifact(
    objects: &dyn ObjectStore,
    artifact: &CheckpointArtifact,
) -> Result<Vec<u8>, SharedLogError> {
    let uri = std::str::from_utf8(&artifact.uri).map_err(|err| {
        SharedLogError::Backend(format!(
            "metadata checkpoint artifact URI is not utf-8: {err}"
        ))
    })?;
    let key = uri.strip_prefix("object:").ok_or_else(|| {
        SharedLogError::Backend(format!(
            "unsupported metadata checkpoint artifact URI scheme: {uri}"
        ))
    })?;
    let key =
        ObjectKey::new(key.to_owned()).map_err(|err| SharedLogError::Backend(err.to_string()))?;
    let image = objects
        .get(&key, None)
        .map_err(|err| SharedLogError::Backend(err.to_string()))?;
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

    fn metadata_log_replication_stats(&self) -> MajorityMetadataLogReplicationStats {
        self.metadata_log
            .as_ref()
            .map(|metadata_log| metadata_log.log().replication_stats())
            .unwrap_or_default()
    }
}

fn metadata_raft_json(stats: Option<OpenRaftMetadataStats>) -> String {
    match stats {
        Some(stats) => format!(
            "{{\"enabled\":true,\"node_id\":{},\"current_term\":{},\"state\":\"{}\",\"current_leader\":{},\"last_log_index\":{},\"last_applied_index\":{},\"snapshot_index\":{},\"purged_index\":{},\"millis_since_quorum_ack\":{},\"voter_count\":{},\"learner_count\":{}}}",
            stats.node_id,
            stats.current_term,
            escape_json_string(&stats.state),
            optional_u64_json(stats.current_leader),
            optional_u64_json(stats.last_log_index),
            optional_u64_json(stats.last_applied_index),
            optional_u64_json(stats.snapshot_index),
            optional_u64_json(stats.purged_index),
            optional_u64_json(stats.millis_since_quorum_ack),
            stats.voter_count,
            stats.learner_count,
        ),
        None => {
            "{\"enabled\":false,\"node_id\":null,\"current_term\":null,\"state\":null,\"current_leader\":null,\"last_log_index\":null,\"last_applied_index\":null,\"snapshot_index\":null,\"purged_index\":null,\"millis_since_quorum_ack\":null,\"voter_count\":0,\"learner_count\":0}".to_owned()
        }
    }
}

fn optional_u64_json(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_owned())
}

fn metadata_log_json(
    enabled: bool,
    sync: nokvfs_cluster::FileSharedLogSync,
    frontier: Option<ApplyFrontier>,
    runtime: Option<SharedLogRuntimeStats>,
    replication: MajorityMetadataLogReplicationStats,
) -> String {
    let runtime = runtime.unwrap_or_default();
    match frontier {
        Some(frontier) => format!(
            "{{\"enabled\":true,\"sync\":\"{}\",\"applied_term\":{},\"applied_index\":{},\"commit_version\":{},\"commit_entry_total\":{},\"commit_command_total\":{},\"max_commands_per_entry\":{},\"precheck_command_total\":{},\"precheck_predicate_total\":{},\"precheck_ns_total\":{},\"stale_read_total\":{},\"remote_voter_append_total\":{},\"remote_voter_append_success_total\":{},\"remote_voter_append_failure_total\":{},\"remote_voter_quorum_success_total\":{},\"remote_voter_quorum_failure_total\":{},\"remote_voter_quorum_wait_ns_total\":{},\"learner_wakeup_total\":{},\"learner_wakeup_coalesced_total\":{},\"learner_wakeup_disconnected_total\":{},\"learner_catchup_success_total\":{},\"learner_catchup_failure_total\":{},\"learner_catchup_ns_total\":{}}}",
            metadata_log_sync_name(sync),
            frontier.position.term.get(),
            frontier.position.index.get(),
            frontier.commit_version.get(),
            runtime.commit_entry_total,
            runtime.commit_command_total,
            runtime.max_commands_per_entry,
            runtime.precheck_command_total,
            runtime.precheck_predicate_total,
            runtime.precheck_ns_total,
            runtime.stale_read_total,
            replication.remote_voter_append_total,
            replication.remote_voter_append_success_total,
            replication.remote_voter_append_failure_total,
            replication.remote_voter_quorum_success_total,
            replication.remote_voter_quorum_failure_total,
            replication.remote_voter_quorum_wait_ns_total,
            replication.learner_wakeup_total,
            replication.learner_wakeup_coalesced_total,
            replication.learner_wakeup_disconnected_total,
            replication.learner_catchup_success_total,
            replication.learner_catchup_failure_total,
            replication.learner_catchup_ns_total,
        ),
        None if enabled => format!(
            "{{\"enabled\":true,\"sync\":\"{}\",\"applied_term\":null,\"applied_index\":null,\"commit_version\":null,\"commit_entry_total\":{},\"commit_command_total\":{},\"max_commands_per_entry\":{},\"precheck_command_total\":{},\"precheck_predicate_total\":{},\"precheck_ns_total\":{},\"stale_read_total\":{},\"remote_voter_append_total\":{},\"remote_voter_append_success_total\":{},\"remote_voter_append_failure_total\":{},\"remote_voter_quorum_success_total\":{},\"remote_voter_quorum_failure_total\":{},\"remote_voter_quorum_wait_ns_total\":{},\"learner_wakeup_total\":{},\"learner_wakeup_coalesced_total\":{},\"learner_wakeup_disconnected_total\":{},\"learner_catchup_success_total\":{},\"learner_catchup_failure_total\":{},\"learner_catchup_ns_total\":{}}}",
            metadata_log_sync_name(sync),
            runtime.commit_entry_total,
            runtime.commit_command_total,
            runtime.max_commands_per_entry,
            runtime.precheck_command_total,
            runtime.precheck_predicate_total,
            runtime.precheck_ns_total,
            runtime.stale_read_total,
            replication.remote_voter_append_total,
            replication.remote_voter_append_success_total,
            replication.remote_voter_append_failure_total,
            replication.remote_voter_quorum_success_total,
            replication.remote_voter_quorum_failure_total,
            replication.remote_voter_quorum_wait_ns_total,
            replication.learner_wakeup_total,
            replication.learner_wakeup_coalesced_total,
            replication.learner_wakeup_disconnected_total,
            replication.learner_catchup_success_total,
            replication.learner_catchup_failure_total,
            replication.learner_catchup_ns_total,
        ),
        None => {
            "{\"enabled\":false,\"commit_entry_total\":0,\"commit_command_total\":0,\"max_commands_per_entry\":0,\"precheck_command_total\":0,\"precheck_predicate_total\":0,\"precheck_ns_total\":0,\"stale_read_total\":0,\"remote_voter_append_total\":0,\"remote_voter_append_success_total\":0,\"remote_voter_append_failure_total\":0,\"remote_voter_quorum_success_total\":0,\"remote_voter_quorum_failure_total\":0,\"remote_voter_quorum_wait_ns_total\":0,\"learner_wakeup_total\":0,\"learner_wakeup_coalesced_total\":0,\"learner_wakeup_disconnected_total\":0,\"learner_catchup_success_total\":0,\"learner_catchup_failure_total\":0,\"learner_catchup_ns_total\":0}".to_owned()
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
        "{{\"get_total\":{},\"get_user_strong_total\":{},\"get_write_plan_local_total\":{},\"get_snapshot_total\":{},\"scan_total\":{},\"scan_user_strong_total\":{},\"scan_write_plan_local_total\":{},\"scan_snapshot_total\":{},\"scan_key_visited_total\":{},\"scan_key_returned_total\":{},\"history_lookup_total\":{},\"active_snapshot_pin_total\":{},\"commit_total\":{},\"dedupe_hit_total\":{},\"predicate_total\":{},\"prefix_empty_predicate_total\":{},\"current_put_total\":{},\"current_delete_total\":{},\"history_write_total\":{},\"watch_write_total\":{},\"dedupe_write_total\":{},\"commit_prepare_ns_total\":{},\"atomic_apply_total\":{},\"atomic_apply_command_total\":{},\"atomic_apply_max_batch\":{},\"atomic_apply_ns_total\":{}}}",
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

fn metadata_service_json(stats: &nokvfs_meta::MetadataServiceStats) -> String {
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
    use std::fs;

    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use nokvfs_cluster::{
        FileSharedLogSync, InMemoryQuorumLog, LogTerm, NodeId, QuorumNodeLog, ReadFreshness,
        SharedMetadataLog,
    };
    use nokvfs_meta::command::{MetadataStore, ReadPurpose, ScanRequest};
    use nokvfs_meta::holtstore::HoltMetadataStore;
    use nokvfs_meta::{HistoryGcOptions, ObjectGcOptions};
    use nokvfs_object::{MemoryObjectStore, ObjectStoreConfig, S3ObjectStoreOptions};
    use nokvfs_types::MountId;
    use tempfile::tempdir;

    use crate::metadata::{ServerMetadataBackend, ServerMetadataLogStatus, ServerMetadataStore};
    use crate::MetadataLogPeerOptions;

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
            metadata_log_leader: NodeId::new(1).unwrap(),
            metadata_log_term: LogTerm::new(1).unwrap(),
            metadata_log_voters: Vec::new(),
            metadata_log_learners: Vec::new(),
            metadata_log_peers: Vec::new(),
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

    pub(crate) fn test_checkpoint_objects() -> MemoryObjectStore {
        MemoryObjectStore::new()
    }

    pub(crate) fn open_test_server_with_checkpoint_objects(
        root: &Path,
        metadata_log_path: Option<PathBuf>,
        checkpoint_objects: &MemoryObjectStore,
    ) -> Server {
        Server::open_with_test_checkpoint_objects(
            test_options(root, metadata_log_path),
            checkpoint_objects.clone(),
        )
        .unwrap()
    }

    fn start_openraft_test_server(
        root: &Path,
        listener: TcpListener,
        node_id: NodeId,
        voters: &[NodeId],
        peers: Vec<MetadataLogPeerOptions>,
    ) -> RunningTestServer {
        let address = listener.local_addr().unwrap();
        let mut options = test_options(root, None);
        options.bind = address;
        options.metadata_log_node = node_id;
        options.metadata_log_voters = voters.to_vec();
        options.metadata_log_peers = peers;
        let server = Arc::new(Server::open(options).unwrap());
        RunningTestServer::spawn(server, listener, address)
    }

    struct RunningTestServer {
        server: Arc<Server>,
        address: SocketAddr,
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl RunningTestServer {
        fn spawn(server: Arc<Server>, listener: TcpListener, address: SocketAddr) -> Self {
            listener.set_nonblocking(true).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let thread_stop = Arc::clone(&stop);
            let thread_server = Arc::clone(&server);
            let thread = thread::spawn(move || {
                while !thread_stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let server = Arc::clone(&thread_server);
                            thread::spawn(move || {
                                let _ = crate::http::handle_stream(server, stream);
                            });
                        }
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            });
            Self {
                server,
                address,
                stop,
                thread: Some(thread),
            }
        }

        fn stop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.address);
            if let Some(thread) = self.thread.take() {
                thread.join().unwrap();
            }
        }
    }

    impl Drop for RunningTestServer {
        fn drop(&mut self) {
            self.stop();
        }
    }

    pub(crate) fn publish_test_metadata_membership(
        metadata_log_path: &Path,
        membership: MetadataMembership,
    ) {
        FileMembershipCatalog::open(metadata_membership_path(metadata_log_path))
            .unwrap()
            .publish(membership)
            .unwrap();
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
            metadata_log_peers: BTreeMap::new(),
            metadata_log_sync: options.metadata_log_sync,
            metadata_raft: None,
            metadata_log_status: Some(metadata_log_status),
            metadata_log: None,
            metadata_checkpoint: None,
            metadata_checkpoint_objects: None,
            object_gc,
            history_gc,
        }
    }

    #[test]
    fn manual_gc_reports_empty_outcomes() {
        let server = test_server();
        assert!(server
            .stats_json()
            .contains("\"metadata_raft\":{\"enabled\":true,\"node_id\":1"));
        assert!(server
            .stats_json()
            .contains("\"metadata_log\":{\"enabled\":false,\"commit_entry_total\":0"));
        let body = server.run_manual_gc(128).unwrap();
        assert!(body.contains("\"object_gc\""));
        assert!(body.contains("\"history_gc\""));
        assert!(body.contains("\"metadata_log\":{\"enabled\":false}"));
    }

    #[test]
    fn three_openraft_metadata_servers_replicate_client_write() {
        let (_dirs, mut servers) = start_three_openraft_test_servers();
        let leader = wait_openraft_server_leader(&servers, None);
        servers
            .get(&leader)
            .unwrap()
            .server
            .service()
            .bootstrap_root(DEFAULT_ROOT_MODE, 1000, 1000)
            .unwrap();
        servers
            .get(&leader)
            .unwrap()
            .server
            .service()
            .create_dir_path("/runs", 0o755, 1000, 1000)
            .unwrap();

        wait_path_on_openraft_servers(&servers, "/runs", None);
        for server in servers.values_mut() {
            server.server.shutdown_metadata_raft().unwrap();
            server.stop();
        }
    }

    #[test]
    fn three_openraft_metadata_servers_elect_new_leader_after_leader_crash() {
        let (_dirs, mut servers) = start_three_openraft_test_servers();
        let leader = wait_openraft_server_leader(&servers, None);
        servers
            .get(&leader)
            .unwrap()
            .server
            .service()
            .bootstrap_root(DEFAULT_ROOT_MODE, 1000, 1000)
            .unwrap();
        servers
            .get(&leader)
            .unwrap()
            .server
            .service()
            .create_dir_path("/before-crash", 0o755, 1000, 1000)
            .unwrap();
        wait_path_on_openraft_servers(&servers, "/before-crash", None);

        let failed = servers.get_mut(&leader).unwrap();
        failed.server.shutdown_metadata_raft().unwrap();
        failed.stop();

        let new_leader = wait_openraft_server_leader(&servers, Some(leader));
        servers
            .get(&new_leader)
            .unwrap()
            .server
            .service()
            .create_dir_path("/after-crash", 0o755, 1000, 1000)
            .unwrap();

        wait_path_on_openraft_servers(&servers, "/after-crash", Some(leader));
        for (id, server) in servers.iter_mut() {
            if *id != leader {
                server.server.shutdown_metadata_raft().unwrap();
                server.stop();
            }
        }
    }

    #[test]
    fn manual_gc_compacts_metadata_log_through_applied_frontier() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let checkpoint_objects = test_checkpoint_objects();
        let server = open_test_server_with_checkpoint_objects(
            dir.path(),
            Some(metadata_log.clone()),
            &checkpoint_objects,
        );
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
        assert!(body.contains("\"checkpoint_uri\":\"object:"));
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
        let image =
            read_metadata_checkpoint_artifact(&checkpoint_objects, &checkpoint.artifact).unwrap();
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

    fn start_three_openraft_test_servers(
    ) -> (Vec<tempfile::TempDir>, BTreeMap<u64, RunningTestServer>) {
        let mut listeners = BTreeMap::new();
        let mut addresses = BTreeMap::new();
        for id in 1..=3 {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            addresses.insert(id, listener.local_addr().unwrap());
            listeners.insert(id, listener);
        }
        let voters = vec![node(1), node(2), node(3)];
        let mut dirs = Vec::new();
        let mut servers = BTreeMap::new();
        for id in 1..=3 {
            let dir = tempdir().unwrap();
            let peers = addresses
                .iter()
                .filter(|(peer_id, _)| **peer_id != id)
                .map(|(peer_id, address)| MetadataLogPeerOptions {
                    node: node(*peer_id),
                    address: *address,
                })
                .collect::<Vec<_>>();
            let listener = listeners.remove(&id).unwrap();
            let running =
                start_openraft_test_server(dir.path(), listener, node(id), &voters, peers);
            dirs.push(dir);
            servers.insert(id, running);
        }
        (dirs, servers)
    }

    fn wait_openraft_server_leader(
        servers: &BTreeMap<u64, RunningTestServer>,
        excluded: Option<u64>,
    ) -> u64 {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            for (id, running) in servers {
                if excluded == Some(*id) {
                    continue;
                }
                let Some(handle) = running.server.metadata_raft.as_ref() else {
                    continue;
                };
                let Some(leader) = handle.stats().current_leader else {
                    continue;
                };
                if excluded != Some(leader) && servers.contains_key(&leader) {
                    return leader;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "OpenRaft metadata servers did not elect a usable leader"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_path_on_openraft_servers(
        servers: &BTreeMap<u64, RunningTestServer>,
        path: &str,
        excluded: Option<u64>,
    ) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let all_visible =
                servers
                    .iter()
                    .filter(|(id, _)| excluded != Some(**id))
                    .all(|(_, running)| {
                        running.server.refresh_metadata_view().unwrap();
                        running
                            .server
                            .service()
                            .lookup_path(path)
                            .unwrap()
                            .is_some()
                    });
            if all_visible {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "OpenRaft metadata write did not replicate to all live servers: {}",
                openraft_server_path_states(servers, path, excluded)
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn openraft_server_path_states(
        servers: &BTreeMap<u64, RunningTestServer>,
        path: &str,
        excluded: Option<u64>,
    ) -> String {
        servers
            .iter()
            .filter(|(id, _)| excluded != Some(**id))
            .map(|(id, running)| {
                let stats = running.server.metadata_raft.as_ref().unwrap().stats();
                running.server.refresh_metadata_view().unwrap();
                let visible = running
                    .server
                    .service()
                    .lookup_path(path)
                    .unwrap()
                    .is_some();
                format!(
                    "node={id} leader={:?} state={} applied={:?} visible={visible}",
                    stats.current_leader, stats.state, stats.last_applied_index
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
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
        assert!(stats.contains("\"remote_voter_append_total\":"));
        assert!(stats.contains("\"remote_voter_quorum_wait_ns_total\":"));
        assert!(stats.contains("\"learner_wakeup_total\":"));
        assert!(stats.contains("\"learner_catchup_ns_total\":"));
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
        options.metadata_log_leader = node(4);
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
    fn server_publishes_configured_metadata_log_membership() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let catalog = FileMembershipCatalog::open(metadata_membership_path(&metadata_log)).unwrap();
        let mount = MountId::new(1).unwrap();
        let membership = metadata_membership_for_node(
            &catalog,
            mount,
            LogTerm::new(9).unwrap(),
            node(2),
            node(1),
            &[node(1), node(2), node(3)],
            &[node(4)],
        )
        .unwrap();

        assert_eq!(membership.mount, mount);
        assert_eq!(membership.term, LogTerm::new(9).unwrap());
        assert_eq!(membership.leader, node(1));
        assert_eq!(membership.voters, vec![node(1), node(2), node(3)]);
        assert_eq!(membership.learners, vec![node(4)]);

        let membership = catalog
            .latest_for_mount(mount)
            .unwrap()
            .expect("server should publish configured metadata log membership");
        assert_eq!(membership.mount, mount);
        assert_eq!(membership.term, LogTerm::new(9).unwrap());
        assert_eq!(membership.leader, node(1));
        assert_eq!(membership.voters, vec![node(1), node(2), node(3)]);
        assert_eq!(membership.learners, vec![node(4)]);
    }

    #[test]
    fn server_uses_local_voter_when_only_learners_are_configured() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let mut options = test_options(dir.path(), Some(metadata_log.clone()));
        options.metadata_log_node = node(2);
        options.metadata_log_leader = node(2);
        options.metadata_log_learners = vec![node(4)];
        let server = Server::open(options).unwrap();

        let catalog = FileMembershipCatalog::open(metadata_membership_path(&metadata_log)).unwrap();
        let membership = catalog
            .latest_for_mount(server.service().mount_id())
            .unwrap()
            .expect("server should publish metadata log membership");
        assert_eq!(membership.leader, node(2));
        assert_eq!(membership.voters, vec![node(2)]);
        assert_eq!(membership.learners, vec![node(4)]);
    }

    #[test]
    fn server_rejects_configured_metadata_leader_outside_voters() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let mut options = test_options(dir.path(), Some(metadata_log));
        options.metadata_log_node = node(4);
        options.metadata_log_leader = node(4);
        options.metadata_log_voters = vec![node(1), node(2), node(3)];
        options.metadata_log_learners = vec![node(4)];

        assert!(matches!(
            Server::open(options),
            Err(ServerError::SharedLog(SharedLogError::LeaderNotVoter(leader))) if leader == node(4)
        ));
    }

    #[test]
    fn server_rejects_metadata_peer_bootstrap_outside_membership() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let server = Server::open(test_options(dir.path(), Some(metadata_log))).unwrap();

        assert!(matches!(
            server.bootstrap_metadata_peer(node(9)),
            Err(ServerError::SharedLog(SharedLogError::UnknownNode(unknown))) if unknown == node(9)
        ));
    }

    #[test]
    fn server_rejects_metadata_peer_bootstrap_without_peer_address() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let mut options = test_options(dir.path(), Some(metadata_log));
        options.metadata_log_learners = vec![node(2)];
        let server = Server::open(options).unwrap();

        assert!(matches!(
            server.bootstrap_metadata_peer(node(2)),
            Err(ServerError::SharedLog(SharedLogError::UnknownNode(unknown))) if unknown == node(2)
        ));
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
    fn server_rejects_fresh_startup_from_compacted_log_without_checkpoint() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let server = Server::open(test_options(dir.path(), Some(metadata_log.clone()))).unwrap();
        server
            .service()
            .create_dir_path("/runs", 0o755, 1000, 1000)
            .unwrap();
        let applied = server
            .metadata_log_frontier()
            .expect("create should apply through the metadata log");
        server
            .metadata_log
            .as_ref()
            .unwrap()
            .log()
            .compact_through(applied.position.index)
            .unwrap();
        drop(server);
        remove_path_if_exists(&dir.path().join("meta"));
        remove_path_if_exists(&metadata_apply_frontier_path(&metadata_log));

        assert!(matches!(
            Server::open(test_options(dir.path(), Some(metadata_log))),
            Err(ServerError::SharedLog(SharedLogError::CheckpointRequired {
                compacted,
                ..
            })) if compacted == applied.position.index
        ));
    }

    #[test]
    fn server_installs_checkpoint_before_replay_for_fresh_metadata_after_compaction() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let checkpoint_objects = test_checkpoint_objects();
        {
            let server = open_test_server_with_checkpoint_objects(
                dir.path(),
                Some(metadata_log.clone()),
                &checkpoint_objects,
            );
            server
                .service()
                .create_dir_path("/before-checkpoint", 0o755, 1000, 1000)
                .unwrap();
            server.run_manual_gc(128).unwrap();
            server
                .service()
                .create_dir_path("/after-checkpoint", 0o755, 1000, 1000)
                .unwrap();
        }
        remove_path_if_exists(&dir.path().join("meta"));
        remove_path_if_exists(&metadata_apply_frontier_path(&metadata_log));

        let reopened = open_test_server_with_checkpoint_objects(
            dir.path(),
            Some(metadata_log),
            &checkpoint_objects,
        );

        let before = reopened
            .service()
            .lookup_path("/before-checkpoint")
            .unwrap()
            .expect("checkpoint image should restore pre-compaction namespace");
        let after = reopened
            .service()
            .lookup_path("/after-checkpoint")
            .unwrap()
            .expect("retained log tail should replay after checkpoint install");
        assert_eq!(before.attr.file_type, nokvfs_types::FileType::Directory);
        assert_eq!(after.attr.file_type, nokvfs_types::FileType::Directory);
        assert!(metadata_apply_frontier_path(&dir.path().join("metadata.log")).is_file());
    }

    #[test]
    fn server_installs_leader_checkpoint_into_fresh_follower_store() {
        let dir = tempdir().unwrap();
        let checkpoint_objects = test_checkpoint_objects();
        let leader_log = dir.path().join("leader-metadata.log");
        let follower_log = dir.path().join("follower-metadata.log");
        let leader = open_test_server_with_checkpoint_objects(
            dir.path(),
            Some(leader_log),
            &checkpoint_objects,
        );
        leader
            .service()
            .create_dir_path("/runs", 0o755, 1000, 1000)
            .unwrap();
        leader
            .service()
            .create_dir_path("/runs/1", 0o755, 1000, 1000)
            .unwrap();
        leader.run_manual_gc(128).unwrap();

        let mut follower_options = test_options(dir.path(), Some(follower_log));
        follower_options.metadata_log_node = node(3);
        follower_options.metadata_log_leader = node(1);
        follower_options.metadata_log_voters = vec![node(1)];
        follower_options.metadata_log_learners = vec![node(3)];
        let follower =
            Server::open_with_test_checkpoint_objects(follower_options, checkpoint_objects)
                .unwrap();
        let request = leader
            .plan_metadata_bootstrap(node(1), node(3), leader.service().mount_id())
            .unwrap();

        follower.install_metadata_checkpoint(request).unwrap();

        let entry = follower
            .service()
            .lookup_path("/runs/1")
            .unwrap()
            .expect("fresh follower should read checkpoint namespace");
        assert_eq!(entry.attr.file_type, nokvfs_types::FileType::Directory);
    }

    #[test]
    fn server_bootstrap_installs_checkpoint_then_replays_retained_tail() {
        let dir = tempdir().unwrap();
        let checkpoint_objects = test_checkpoint_objects();
        let leader_log = dir.path().join("leader-metadata.log");
        let follower_log = dir.path().join("follower-metadata.log");
        let leader = open_test_server_with_checkpoint_objects(
            dir.path(),
            Some(leader_log),
            &checkpoint_objects,
        );
        leader
            .service()
            .create_dir_path("/runs", 0o755, 1000, 1000)
            .unwrap();
        leader
            .service()
            .create_dir_path("/runs/before-checkpoint", 0o755, 1000, 1000)
            .unwrap();
        leader.run_manual_gc(128).unwrap();
        leader
            .service()
            .create_dir_path("/runs/after-checkpoint", 0o755, 1000, 1000)
            .unwrap();

        let mut follower_options = test_options(dir.path(), Some(follower_log));
        follower_options.metadata_log_node = node(3);
        follower_options.metadata_log_leader = node(1);
        follower_options.metadata_log_voters = vec![node(1)];
        follower_options.metadata_log_learners = vec![node(3)];
        let follower =
            Server::open_with_test_checkpoint_objects(follower_options, checkpoint_objects)
                .unwrap();
        let request = leader
            .plan_metadata_bootstrap(node(1), node(3), leader.service().mount_id())
            .unwrap();
        assert!(
            request.plan.replayed_index > request.plan.checkpoint.frontier.applied_position.index,
            "test must include retained tail entries after the checkpoint"
        );

        let checkpoint_only = InstallCheckpointRequest::from_plan(
            request.leader,
            nokvfs_cluster::LearnerBootstrapPlan {
                node: request.plan.node,
                checkpoint: request.plan.checkpoint.clone(),
                replay_start: request.plan.replay_start,
                replayed_index: request.plan.checkpoint.frontier.applied_position.index,
            },
        );
        follower
            .install_metadata_checkpoint(checkpoint_only)
            .unwrap();
        let (tail, _) = leader
            .read_metadata_log_tail(request.plan.replay_start, 0)
            .unwrap();
        assert!(!tail.is_empty());
        for entry in tail {
            if entry.position.index > request.plan.replayed_index {
                break;
            }
            follower
                .append_metadata_log_batch(AppendMetadataBatchRequest::new(node(1), entry).unwrap())
                .unwrap();
        }

        let before = follower
            .service()
            .lookup_path("/runs/before-checkpoint")
            .unwrap()
            .expect("checkpoint image should install pre-compaction namespace");
        let after = follower
            .service()
            .lookup_path("/runs/after-checkpoint")
            .unwrap()
            .expect("retained tail should replay after checkpoint install");
        assert_eq!(before.attr.file_type, nokvfs_types::FileType::Directory);
        assert_eq!(after.attr.file_type, nokvfs_types::FileType::Directory);
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

    fn remove_path_if_exists(path: &Path) {
        match fs::metadata(path) {
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path).unwrap(),
            Ok(_) => fs::remove_file(path).unwrap(),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => panic!("stat {}: {err}", path.display()),
        }
    }
}
