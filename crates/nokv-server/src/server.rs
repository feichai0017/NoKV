use std::error::Error;
use std::fmt;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use nokv_cluster::{
    FileMetadataRaftLogOptions, LogIndex, LogPosition, LogTerm, MetadataRaftError,
    MetadataRaftRpcNetworkFactory, NodeId, OpenRaftMetadataStats, OpenRaftMetadataStatsHandle,
};
use nokv_meta::holtstore::HoltMetadataStore;
use nokv_meta::{
    HistoryGcWorker, HistoryGcWorkerState, MetadError, NoKvFs, ObjectGcWorker, ObjectGcWorkerState,
};
use nokv_object::{ObjectError, ObjectKey, ObjectStore, S3ObjectStore};

use crate::http;
use crate::metadata::{OpenRaftLoggedMetadataStore, ServerMetadataStore};
use crate::options::ServerOptions;
use crate::rpc;

const DEFAULT_ROOT_MODE: u32 = 0o755;
const SERVER_CONNECTION_WORKERS: usize = 64;
const SERVER_CONNECTION_QUEUE: usize = 1024;

pub struct Server {
    service: Arc<NoKvFs<ServerMetadataStore, S3ObjectStore>>,
    metadata_raft: OpenRaftMetadataStatsHandle,
    metadata_checkpoint_archive_prefix: Option<String>,
    checkpoint_archive_objects: S3ObjectStore,
    object_gc: ObjectGcWorker,
    history_gc: HistoryGcWorker,
    #[cfg(test)]
    _test_meta_dir: Option<tempfile::TempDir>,
}

#[derive(Debug)]
pub enum ServerError {
    Io(io::Error),
    Metadata(MetadError),
    Object(ObjectError),
    MetadataRaft(MetadataRaftError),
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
        let metadata = HoltMetadataStore::open_raft_materialized(&metadata_state_path)
            .map_err(MetadError::from)?;
        let voters = metadata_raft_voters_for_options(&options)?;
        let learners = metadata_raft_learners_for_options(&options, &voters)?;
        validate_metadata_raft_local_node(&options, &voters, &learners)?;
        let voter_count = voters.len();
        let log_path = default_metadata_raft_log_path(&options.meta_path);
        let log_options = FileMetadataRaftLogOptions {
            sync: options.metadata_raft_log_sync,
        };
        let network =
            MetadataRaftRpcNetworkFactory::new(rpc::MetadataRaftFramedRpcClient::default());
        let bootstrap_node = metadata_raft_bootstrap_node(&voters)?;
        let (openraft, _initialized_membership) = if options.metadata_raft_node == bootstrap_node {
            OpenRaftLoggedMetadataStore::new_voter_group_with_file_log_and_network(
                metadata,
                options.metadata_raft_node,
                log_path,
                log_options,
                network,
                &voters,
            )
        } else {
            OpenRaftLoggedMetadataStore::new_uninitialized_with_file_log_and_network(
                metadata,
                options.metadata_raft_node,
                log_path,
                log_options,
                network,
            )
            .map(|store| (store, false))
        }
        .map_err(MetadError::from)?;
        let metadata_raft = openraft.stats_handle();
        let bootstrap_root = if voter_count == 1 {
            openraft
                .wait_for_current_leader(options.metadata_raft_node, Duration::from_secs(3))
                .map_err(MetadError::from)?;
            true
        } else {
            false
        };
        let metadata_checkpoint_archive_prefix = options.metadata_checkpoint_archive_prefix.clone();
        let checkpoint_archive_objects = objects.clone();
        let metadata = ServerMetadataStore::openraft(openraft);
        let service = Arc::new(NoKvFs::open_existing(options.mount, metadata, objects)?);
        if bootstrap_root {
            service.bootstrap_root(DEFAULT_ROOT_MODE, options.uid, options.gid)?;
        }
        let object_gc = ObjectGcWorker::spawn(Arc::clone(&service), options.object_gc);
        let history_gc = HistoryGcWorker::spawn(Arc::clone(&service), options.history_gc);
        Ok(Self {
            service,
            metadata_raft,
            metadata_checkpoint_archive_prefix,
            checkpoint_archive_objects,
            object_gc,
            history_gc,
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

    pub(crate) fn refresh_metadata_view(&self) -> Result<(), ServerError> {
        self.service
            .refresh_allocator_state()
            .map_err(ServerError::Metadata)
    }

    pub(crate) fn metadata_raft_applied_position(&self) -> Option<LogPosition> {
        let stats = self.metadata_raft.stats();
        let term = LogTerm::new(stats.current_term.max(1)).ok()?;
        let index = LogIndex::new(stats.last_applied_index?).ok()?;
        Some(LogPosition { term, index })
    }

    pub(crate) fn ensure_metadata_raft_applied(
        &self,
        required: LogPosition,
    ) -> Result<(), ServerError> {
        let applied = self.metadata_raft_applied_position();
        if applied
            .map(|position| position.index >= required.index)
            .unwrap_or(false)
        {
            return Ok(());
        }
        Err(ServerError::MetadataRaft(MetadataRaftError::ReadNotFresh {
            required,
            applied,
        }))
    }

    pub(crate) fn add_metadata_raft_learner(
        &self,
        node: NodeId,
        address: String,
        blocking: bool,
    ) -> Result<LogPosition, ServerError> {
        self.service
            .metadata_store()
            .add_metadata_raft_learner(node, address, blocking)
            .map_err(MetadError::from)
            .map_err(ServerError::Metadata)
    }

    #[cfg(test)]
    pub(crate) fn shutdown_metadata_raft(&self) -> Result<(), ServerError> {
        self.service
            .metadata_store()
            .shutdown_openraft()
            .map_err(|err| ServerError::Metadata(MetadError::from(err)))
    }

    pub fn stats_json(&self) -> String {
        let objects = self.service.object_stats();
        let metadata = self.service.metadata_store_stats();
        let metadata_service = self.service.metadata_service_stats();
        let object_gc = self.object_gc.state();
        let history_gc = self.history_gc.state();
        format!(
            "{{\"ready\":true,\"block_cache_enabled\":{},\"object_puts\":{},\"object_put_bytes\":{},\"object_gets\":{},\"object_get_bytes\":{},\"cache_hits\":{},\"cache_hit_bytes\":{},\"manifest_chunks\":{},\"manifest_blocks\":{},\"metadata_store\":{},\"metadata_raft\":{},\"metadata_service\":{},\"object_gc\":{},\"history_gc\":{}}}\n",
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
            metadata_raft_json(self.metadata_raft.stats()),
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
            .trigger_openraft_snapshot()
            .map_err(|err| ServerError::Metadata(MetadError::from(err)))?;
        let stats = self.metadata_raft.stats();
        let archive = match &self.metadata_checkpoint_archive_prefix {
            Some(prefix) => {
                let image = self
                    .service
                    .metadata_store()
                    .export_openraft_checkpoint_image()
                    .map_err(|err| ServerError::Metadata(MetadError::from(err)))?;
                let key =
                    metadata_checkpoint_archive_key(prefix, stats.node_id, stats.snapshot_index);
                let key = ObjectKey::new(key).map_err(ServerError::Object)?;
                let info = self
                    .checkpoint_archive_objects
                    .put(&key, &image)
                    .map_err(ServerError::Object)?;
                format!(
                    "{{\"enabled\":true,\"key\":\"{}\",\"bytes\":{}}}",
                    escape_json_string(info.key.as_str()),
                    info.size
                )
            }
            None => "{\"enabled\":false,\"key\":null,\"bytes\":0}".to_owned(),
        };
        Ok(format!(
            r#"{{"metadata_raft":{{"node_id":{},"snapshot_index":{},"last_applied_index":{}}},"archive":{}}}
"#,
            stats.node_id,
            optional_u64_json(stats.snapshot_index),
            optional_u64_json(stats.last_applied_index),
            archive,
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

fn default_metadata_raft_log_path(meta_path: &Path) -> PathBuf {
    meta_path.join("metadata-raft.log")
}

fn default_metadata_state_path(meta_path: &Path) -> PathBuf {
    meta_path.join("metadata-state.holt")
}

fn metadata_raft_voters_for_options(
    options: &ServerOptions,
) -> Result<std::collections::BTreeMap<NodeId, String>, ServerError> {
    let configured_voters = if options.metadata_raft_voters.is_empty() {
        vec![options.metadata_raft_node]
    } else {
        options.metadata_raft_voters.clone()
    };
    let mut voters = std::collections::BTreeMap::new();
    for node in configured_voters {
        let address = metadata_raft_node_address(options, node)?;
        if voters.insert(node, address).is_some() {
            return Err(ServerError::MetadataRaft(MetadataRaftError::DuplicateNode(
                node,
            )));
        }
    }
    if voters.is_empty() {
        return Err(ServerError::MetadataRaft(MetadataRaftError::NoVoters));
    }
    Ok(voters)
}

fn metadata_raft_learners_for_options(
    options: &ServerOptions,
    voters: &std::collections::BTreeMap<NodeId, String>,
) -> Result<std::collections::BTreeMap<NodeId, String>, ServerError> {
    let mut learners = std::collections::BTreeMap::new();
    for node in &options.metadata_raft_learners {
        if voters.contains_key(node) {
            return Err(ServerError::MetadataRaft(MetadataRaftError::DuplicateNode(
                *node,
            )));
        }
        let address = metadata_raft_node_address(options, *node)?;
        if learners.insert(*node, address).is_some() {
            return Err(ServerError::MetadataRaft(MetadataRaftError::DuplicateNode(
                *node,
            )));
        }
    }
    Ok(learners)
}

fn validate_metadata_raft_local_node(
    options: &ServerOptions,
    voters: &std::collections::BTreeMap<NodeId, String>,
    learners: &std::collections::BTreeMap<NodeId, String>,
) -> Result<(), ServerError> {
    if voters.contains_key(&options.metadata_raft_node)
        || learners.contains_key(&options.metadata_raft_node)
    {
        return Ok(());
    }
    Err(ServerError::MetadataRaft(MetadataRaftError::UnknownNode(
        options.metadata_raft_node,
    )))
}

fn metadata_raft_bootstrap_node(
    voters: &std::collections::BTreeMap<NodeId, String>,
) -> Result<NodeId, ServerError> {
    voters
        .keys()
        .next()
        .copied()
        .ok_or(ServerError::MetadataRaft(MetadataRaftError::NoVoters))
}

fn metadata_raft_node_address(
    options: &ServerOptions,
    node: NodeId,
) -> Result<String, ServerError> {
    if node == options.metadata_raft_node {
        return Ok(options.bind.to_string());
    }
    options
        .metadata_raft_peers
        .iter()
        .find(|peer| peer.node == node)
        .map(|peer| peer.address.to_string())
        .ok_or(ServerError::MetadataRaft(MetadataRaftError::UnknownNode(
            node,
        )))
}

fn metadata_checkpoint_archive_key(
    prefix: &str,
    node_id: u64,
    snapshot_index: Option<u64>,
) -> String {
    let prefix = prefix.trim_matches('/');
    let snapshot_index = snapshot_index.unwrap_or(0);
    format!("{prefix}/node-{node_id}/snapshot-{snapshot_index}.nkfsc")
}

fn metadata_raft_json(stats: OpenRaftMetadataStats) -> String {
    format!(
        "{{\"enabled\":true,\"node_id\":{},\"current_term\":{},\"state\":\"{}\",\"current_leader\":{},\"last_log_index\":{},\"last_applied_index\":{},\"snapshot_index\":{},\"purged_index\":{},\"millis_since_quorum_ack\":{},\"voter_count\":{},\"learner_count\":{},\"proposal_batch_total\":{},\"proposal_command_total\":{},\"proposal_max_batch\":{},\"proposal_ns_total\":{},\"proposal_queue_wait_ns_total\":{},\"proposal_queue_max_wait_ns\":{}}}",
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
        stats.proposal_batch_total,
        stats.proposal_command_total,
        stats.proposal_max_batch,
        stats.proposal_ns_total,
        stats.proposal_queue_wait_ns_total,
        stats.proposal_queue_max_wait_ns,
    )
}

fn optional_u64_json(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_owned())
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

impl From<MetadataRaftError> for ServerError {
    fn from(err: MetadataRaftError) -> Self {
        Self::MetadataRaft(err)
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Metadata(err) => write!(f, "{err}"),
            Self::Object(err) => write!(f, "{err}"),
            Self::MetadataRaft(err) => write!(f, "{err}"),
        }
    }
}

impl Error for ServerError {}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use nokv_cluster::{FileMetadataRaftLogSync, NodeId};
    use nokv_meta::{HistoryGcOptions, ObjectGcOptions};
    use nokv_object::{ObjectStoreConfig, S3ObjectStoreOptions};
    use nokv_types::MountId;
    use tempfile::tempdir;

    use crate::MetadataRaftPeerOptions;

    fn node(raw: u64) -> NodeId {
        NodeId::new(raw).unwrap()
    }

    pub(crate) fn test_options(root: &Path) -> ServerOptions {
        ServerOptions {
            bind: crate::options::DEFAULT_SERVER_BIND,
            mount: MountId::new(1).unwrap(),
            meta_path: root.join("meta"),
            metadata_raft_node: NodeId::new(1).unwrap(),
            metadata_raft_voters: Vec::new(),
            metadata_raft_learners: Vec::new(),
            metadata_raft_peers: Vec::new(),
            metadata_raft_log_sync: FileMetadataRaftLogSync::Data,
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

    fn start_openraft_test_server(
        root: &Path,
        listener: TcpListener,
        node_id: NodeId,
        voters: &[NodeId],
        peers: Vec<MetadataRaftPeerOptions>,
    ) -> RunningTestServer {
        let address = listener.local_addr().unwrap();
        let mut options = test_options(root);
        options.bind = address;
        options.metadata_raft_node = node_id;
        options.metadata_raft_voters = voters.to_vec();
        options.metadata_raft_peers = peers;
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

    #[test]
    fn manual_gc_reports_empty_outcomes() {
        let server = test_server();
        assert!(server
            .stats_json()
            .contains("\"metadata_raft\":{\"enabled\":true,\"node_id\":1"));
        let body = server.run_manual_gc(128).unwrap();
        assert!(body.contains("\"object_gc\""));
        assert!(body.contains("\"history_gc\""));
    }

    #[test]
    fn three_openraft_metadata_servers_replicate_client_write() {
        let (_dirs, mut servers) = start_three_openraft_test_servers();
        wait_openraft_server_leader(&servers, None);
        bootstrap_root_on_openraft_servers(&servers, None);
        create_dir_on_openraft_servers(&servers, "/runs", None);

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
        bootstrap_root_on_openraft_servers(&servers, None);
        create_dir_on_openraft_servers(&servers, "/before-crash", None);
        wait_path_on_openraft_servers(&servers, "/before-crash", None);

        let failed = servers.get_mut(&leader).unwrap();
        failed.server.shutdown_metadata_raft().unwrap();
        failed.stop();

        wait_openraft_server_leader(&servers, Some(leader));
        create_dir_on_openraft_servers(&servers, "/after-crash", Some(leader));

        wait_path_on_openraft_servers(&servers, "/after-crash", Some(leader));
        for (id, server) in servers.iter_mut() {
            if *id != leader {
                server.server.shutdown_metadata_raft().unwrap();
                server.stop();
            }
        }
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
                .map(|(peer_id, address)| MetadataRaftPeerOptions {
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

    fn bootstrap_root_on_openraft_servers(
        servers: &BTreeMap<u64, RunningTestServer>,
        excluded: Option<u64>,
    ) {
        retry_openraft_metadata_write(servers, excluded, |running| {
            running
                .server
                .service()
                .bootstrap_root(DEFAULT_ROOT_MODE, 1000, 1000)
                .map(|_| ())
        });
    }

    fn create_dir_on_openraft_servers(
        servers: &BTreeMap<u64, RunningTestServer>,
        path: &str,
        excluded: Option<u64>,
    ) {
        retry_openraft_metadata_write(servers, excluded, |running| {
            running
                .server
                .service()
                .create_dir_path(path, 0o755, 1000, 1000)
                .map(|_| ())
        });
    }

    fn retry_openraft_metadata_write(
        servers: &BTreeMap<u64, RunningTestServer>,
        excluded: Option<u64>,
        mut write: impl FnMut(&RunningTestServer) -> Result<(), MetadError>,
    ) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut last_forward = None;
        loop {
            for (id, running) in servers {
                if excluded == Some(*id) {
                    continue;
                }
                match write(running) {
                    Ok(()) => return,
                    Err(err) if is_forward_to_leader(&err) => {
                        last_forward = Some(err.to_string());
                    }
                    Err(err) => panic!("OpenRaft metadata write failed on node {id}: {err}"),
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "OpenRaft metadata write never reached a leader; last forward={last_forward:?}"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn is_forward_to_leader(err: &MetadError) -> bool {
        matches!(
            err,
            MetadError::Metadata(nokv_meta::MetadataError::ForwardToLeader { .. })
        )
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
                let Some(leader) = running.server.metadata_raft.stats().current_leader else {
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
                let stats = running.server.metadata_raft.stats();
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
}
