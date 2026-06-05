//! NoKV workload benchmark harness.
//!
//! This binary intentionally reports workload shape and durability caveats with
//! every result. It runs a real `metad` process boundary with the service client.
//! The metadata HA smoke workload also starts real multi-server OpenRaft
//! metadata groups and verifies read-your-writes from a replicated peer.

use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::hint::black_box;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nokvfs_client::{ArtifactMetadata, MetadataClient, MetadataClientOptions, NoKvFsClient};
use nokvfs_cluster::{FileMetadataRaftLogSync, NodeId};
use nokvfs_meta::{
    DentryWithAttr, HistoryGcOptions, MetadataServiceStats, MetadataStoreStats, ObjectGcOptions,
    ObjectTransferStats, RenameReplaceResult,
};
use nokvfs_object::{ObjectStoreConfig, S3ObjectStore, S3ObjectStoreOptions};
use nokvfs_server::{MetadataRaftPeerOptions, Server, ServerOptions};
use nokvfs_types::{MountId, PathMetadata};

const DEFAULT_MODE_DIR: u32 = 0o755;
const DEFAULT_MODE_FILE: u32 = 0o644;
const DEFAULT_UID: u32 = 1000;
const DEFAULT_GID: u32 = 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Profile {
    Smoke,
    Standard,
    Long,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    All,
    MetadataSmoke,
    MetadataHaSmoke,
    MetadataHaFaultSmoke,
    MdtestEasy,
    MdtestHard,
    MetadataNegativeLookup,
    ArtifactIndexLookup,
    MetadataConcurrentRead,
    CheckpointPublish,
    TrainingRead,
    MlperfDlio,
    DemoDataset,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Config {
    profile: Profile,
    workload: Workload,
    root: PathBuf,
    object_backend: ObjectBackendKind,
    s3: S3ObjectStoreOptions,
    object_concurrency: usize,
    read_repeats: usize,
    block_cache: bool,
    metadata_raft_log_sync: FileMetadataRaftLogSync,
    checkpoint_bytes: Option<usize>,
    sample_bytes: Option<usize>,
    keep: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObjectBackendKind {
    S3,
    RustFs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkloadShape {
    dirs: usize,
    files_per_dir: usize,
    shared_files: usize,
    checkpoints: usize,
    checkpoint_bytes: usize,
    dataset_dirs: usize,
    dataset_files_per_dir: usize,
    dataset_file_bytes: usize,
}

#[derive(Clone, Debug)]
struct ResultRow {
    workload: &'static str,
    profile: Profile,
    operations: usize,
    seconds: f64,
    ops_per_second: f64,
    mb_per_second: f64,
    samples_per_second: f64,
    object_puts: u64,
    object_put_bytes: u64,
    object_gets: u64,
    object_get_bytes: u64,
    cache_hits: u64,
    cache_hit_bytes: u64,
    cache_hit_rate: f64,
    manifest_chunks: u64,
    manifest_blocks: u64,
    metadata_commits: u64,
    metadata_dedupe_hits: u64,
    metadata_predicates: u64,
    metadata_prefix_empty_predicates: u64,
    metadata_raft_current_term: u64,
    metadata_raft_current_leader: u64,
    metadata_raft_last_log_index: u64,
    metadata_raft_last_applied_index: u64,
    metadata_raft_snapshot_index: u64,
    metadata_raft_purged_index: u64,
    metadata_raft_millis_since_quorum_ack: u64,
    metadata_raft_voters: u64,
    metadata_raft_learners: u64,
    metadata_gets: u64,
    metadata_get_user_strong: u64,
    metadata_get_write_plan_local: u64,
    metadata_get_snapshot: u64,
    metadata_scans: u64,
    metadata_scan_user_strong: u64,
    metadata_scan_write_plan_local: u64,
    metadata_scan_snapshot: u64,
    metadata_scan_visited: u64,
    metadata_scan_returned: u64,
    metadata_history_lookups: u64,
    metadata_current_puts: u64,
    metadata_current_deletes: u64,
    metadata_history_writes: u64,
    metadata_watch_writes: u64,
    metadata_dedupe_writes: u64,
    metadata_commit_prepare_ns: u64,
    metadata_atomic_applies: u64,
    metadata_atomic_apply_commands: u64,
    metadata_atomic_apply_max_batch: u64,
    metadata_atomic_apply_ns: u64,
    path_index_lookups: u64,
    path_index_hits: u64,
    path_index_misses: u64,
    path_index_stale: u64,
    path_index_scan_stale: u64,
    path_index_fallback: u64,
    path_index_hit_rate: f64,
    create_files_batches: u64,
    create_files_entries: u64,
    create_dirs_batches: u64,
    create_dirs_entries: u64,
    read_dir_plus_calls: u64,
    read_dir_plus_entries: u64,
    read_dir_plus_projection_hits: u64,
    read_dir_plus_projection_hit_rate: f64,
    object_concurrency: usize,
    read_repeats: usize,
    block_cache: bool,
    checksum: u64,
    shape: String,
    caveat: String,
}

#[derive(Clone, Debug)]
struct RowInput {
    workload: &'static str,
    profile: Profile,
    operations: usize,
    seconds: f64,
    bytes: u64,
    samples: usize,
    stats: BenchStats,
    object_concurrency: usize,
    read_repeats: usize,
    block_cache: bool,
    checksum: u64,
    shape: String,
    caveat: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BenchStats {
    object: ObjectTransferStats,
    metadata_store: MetadataStoreStats,
    metadata_raft: MetadataRaftBenchStats,
    metadata_service: MetadataServiceStats,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MetadataRaftBenchStats {
    current_term: u64,
    current_leader: u64,
    last_log_index: u64,
    last_applied_index: u64,
    snapshot_index: u64,
    purged_index: u64,
    millis_since_quorum_ack: u64,
    voter_count: u64,
    learner_count: u64,
}

#[derive(Clone, Debug)]
struct MetadataHaClusterConfig {
    root: PathBuf,
    object: ObjectStoreConfig,
    voters: Vec<NodeId>,
    peers: Vec<MetadataRaftPeerOptions>,
    sync: FileMetadataRaftLogSync,
}

#[derive(Debug)]
enum BenchError {
    MissingValue(&'static str),
    UnknownOption(String),
    InvalidProfile(String),
    InvalidWorkload(String),
    Io(String),
    Client(String),
}

trait BenchClient: Sync {
    fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<(), BenchError>;
    fn mkdir(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, BenchError>;
    fn mkdirs(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, BenchError> {
        paths
            .iter()
            .map(|path| self.mkdir(path, mode, uid, gid))
            .collect()
    }
    fn create_file(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, BenchError>;
    fn create_files(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, BenchError> {
        paths
            .iter()
            .map(|path| self.create_file(path, mode, uid, gid))
            .collect()
    }
    fn put_artifact(
        &self,
        path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, BenchError>;
    fn rename_replace(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<RenameReplaceResult, BenchError>;
    fn lookup(&self, path: &str) -> Result<Option<DentryWithAttr>, BenchError>;
    fn stat_path(&self, path: &str) -> Result<Option<PathMetadata>, BenchError>;
    fn list(&self, path: &str) -> Result<Vec<DentryWithAttr>, BenchError>;
    fn list_indexed(&self, path: &str) -> Result<Vec<DentryWithAttr>, BenchError>;
    fn cat(&self, path: &str) -> Result<Vec<u8>, BenchError>;
    fn stats(&self) -> Result<BenchStats, BenchError>;
}

struct ServiceBenchClient {
    client: NoKvFsClient<S3ObjectStore>,
    stats_addr: SocketAddr,
}

impl BenchClient for ServiceBenchClient {
    fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<(), BenchError> {
        self.client
            .metadata()
            .bootstrap_root(mode, uid, gid)
            .map_err(from_client)
    }

    fn mkdir(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, BenchError> {
        self.client
            .metadata()
            .mkdir(path, mode, uid, gid)
            .map_err(from_client)
    }

    fn mkdirs(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, BenchError> {
        self.client
            .metadata()
            .mkdirs(paths, mode, uid, gid)
            .map_err(from_client)?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(from_client)
    }

    fn create_file(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, BenchError> {
        self.client
            .metadata()
            .create_file(path, mode, uid, gid)
            .map_err(from_client)
    }

    fn create_files(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, BenchError> {
        self.client
            .metadata()
            .create_files(paths, mode, uid, gid)
            .map_err(from_client)?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(from_client)
    }

    fn put_artifact(
        &self,
        path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, BenchError> {
        NoKvFsClient::put_artifact(&self.client, path, bytes, metadata).map_err(from_client)
    }

    fn rename_replace(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<RenameReplaceResult, BenchError> {
        self.client
            .metadata()
            .rename_replace(source, destination)
            .map_err(from_client)
    }

    fn lookup(&self, path: &str) -> Result<Option<DentryWithAttr>, BenchError> {
        self.client.metadata().lookup(path).map_err(from_client)
    }

    fn stat_path(&self, path: &str) -> Result<Option<PathMetadata>, BenchError> {
        self.client.metadata().stat_path(path).map_err(from_client)
    }

    fn list(&self, path: &str) -> Result<Vec<DentryWithAttr>, BenchError> {
        self.client.metadata().list(path).map_err(from_client)
    }

    fn list_indexed(&self, path: &str) -> Result<Vec<DentryWithAttr>, BenchError> {
        self.client
            .metadata()
            .list_indexed(path)
            .map_err(from_client)
    }

    fn cat(&self, path: &str) -> Result<Vec<u8>, BenchError> {
        NoKvFsClient::cat(&self.client, path).map_err(from_client)
    }

    fn stats(&self) -> Result<BenchStats, BenchError> {
        let mut stats = fetch_server_stats(self.stats_addr)?;
        stats.object = NoKvFsClient::object_stats(&self.client);
        Ok(stats)
    }
}

fn main() {
    if let Err(err) = run(env::args().skip(1).collect()) {
        eprintln!("error: {err}");
        eprintln!(
            "\nUsage: nokv-fs-bench [--profile smoke|standard|long] \
             [--workload all|metadata-smoke|metadata-ha-smoke|metadata-ha-fault-smoke|mdtest-easy|mdtest-hard|metadata-negative-lookup|artifact-index-lookup|metadata-concurrent-read|checkpoint-publish|training-read|mlperf-dlio|demo-dataset] \
             [--root PATH] [--object-backend s3|rustfs] \
             [--object-concurrency N] [--checkpoint-bytes N] [--sample-bytes N] \
             [--read-repeats N] [--block-cache on|off] \
             [--metadata-raft-log-sync data|none] [--keep]"
        );
        std::process::exit(2);
    }
}

fn run(args: Vec<String>) -> Result<(), BenchError> {
    let config = parse(args)?;
    let shape = shape(&config);
    fs::create_dir_all(&config.root).map_err(from_io)?;

    println!("{}", csv_header());
    for workload in expand_workloads(config.workload) {
        let row = run_one(&config, &shape, workload)?;
        println!("{}", csv_row(&row));
    }

    if !config.keep {
        fs::remove_dir_all(&config.root).map_err(from_io)?;
    }
    Ok(())
}

fn run_one(
    config: &Config,
    shape: &WorkloadShape,
    workload: Workload,
) -> Result<ResultRow, BenchError> {
    if workload == Workload::MetadataHaSmoke {
        return bench_metadata_ha_smoke(config, shape);
    }
    if workload == Workload::MetadataHaFaultSmoke {
        return bench_metadata_ha_fault_smoke(config, shape);
    }
    let label = workload_name(workload);
    let client = client_for(config, label)?;
    client.bootstrap_root(DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    match workload {
        Workload::MdtestEasy => bench_mdtest_easy(client.as_ref(), config, shape),
        Workload::MdtestHard => bench_mdtest_hard(client.as_ref(), config, shape),
        Workload::MetadataNegativeLookup => {
            bench_metadata_negative_lookup(client.as_ref(), config, shape)
        }
        Workload::ArtifactIndexLookup => {
            bench_artifact_index_lookup(client.as_ref(), config, shape)
        }
        Workload::MetadataConcurrentRead => {
            bench_metadata_concurrent_read(client.as_ref(), config, shape)
        }
        Workload::CheckpointPublish => bench_checkpoint_publish(client.as_ref(), config, shape),
        Workload::TrainingRead => bench_training_read(client.as_ref(), config, shape),
        Workload::MlperfDlio => bench_mlperf_dlio(client.as_ref(), config, shape),
        Workload::DemoDataset => bench_demo_dataset(client.as_ref(), config, shape),
        Workload::MetadataHaSmoke => unreachable!("metadata-ha-smoke executes before client setup"),
        Workload::MetadataHaFaultSmoke => {
            unreachable!("metadata-ha-fault-smoke executes before client setup")
        }
        Workload::MetadataSmoke => unreachable!("metadata-smoke expands before execution"),
        Workload::All => unreachable!("all expands before execution"),
    }
}

fn bench_mdtest_easy(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    let before = client.stats()?;
    let start = Instant::now();
    let mut checksum = 0_u64;
    client.mkdir("/mdtest-easy", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let dir_paths = (0..shape.dirs)
        .map(|dir| format!("/mdtest-easy/dir-{dir:05}"))
        .collect::<Vec<_>>();
    for entry in client.mkdirs(&dir_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)? {
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }
    let mut file_paths = Vec::with_capacity(shape.dirs * shape.files_per_dir);
    for dir in 0..shape.dirs {
        let dir_path = format!("/mdtest-easy/dir-{dir:05}");
        checksum = checksum.wrapping_add(dir as u64);
        file_paths
            .extend((0..shape.files_per_dir).map(|file| format!("{dir_path}/file-{file:05}")));
    }
    for entry in client.create_files(&file_paths, DEFAULT_MODE_FILE, DEFAULT_UID, DEFAULT_GID)? {
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }
    let operations = shape.dirs + shape.dirs * shape.files_per_dir + 1;
    Ok(row(RowInput {
        workload: "mdtest-easy",
        profile: config.profile,
        operations,
        seconds: start.elapsed().as_secs_f64(),
        bytes: 0,
        samples: 0,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "dirs={} files_per_dir={} file_body=metadata-only",
            shape.dirs, shape.files_per_dir
        ),
        caveat: metadata_only_caveat(config),
    }))
}

fn bench_mdtest_hard(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir("/mdtest-hard", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let before = client.stats()?;
    let start = Instant::now();
    let mut checksum = 0_u64;
    let paths = (0..shape.shared_files)
        .map(|file| format!("/mdtest-hard/file-{file:06}"))
        .collect::<Vec<_>>();
    for entry in client.create_files(&paths, DEFAULT_MODE_FILE, DEFAULT_UID, DEFAULT_GID)? {
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }
    Ok(row(RowInput {
        workload: "mdtest-hard",
        profile: config.profile,
        operations: shape.shared_files,
        seconds: start.elapsed().as_secs_f64(),
        bytes: 0,
        samples: 0,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "shared_dir_files={} file_body=metadata-only",
            shape.shared_files
        ),
        caveat: metadata_only_caveat(config),
    }))
}

fn bench_metadata_negative_lookup(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir(
        "/metadata-negative-lookup",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;
    let dir_paths = (0..shape.dirs)
        .map(|dir| format!("/metadata-negative-lookup/dir-{dir:05}"))
        .collect::<Vec<_>>();
    let mut checksum = 0_u64;
    for entry in client.mkdirs(&dir_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)? {
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }

    let mut present_paths = Vec::with_capacity(shape.dirs * shape.files_per_dir);
    for dir in 0..shape.dirs {
        let dir_path = format!("/metadata-negative-lookup/dir-{dir:05}");
        present_paths
            .extend((0..shape.files_per_dir).map(|file| format!("{dir_path}/present-{file:05}")));
    }
    for entry in client.create_files(&present_paths, DEFAULT_MODE_FILE, DEFAULT_UID, DEFAULT_GID)? {
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }

    let before = client.stats()?;
    let start = Instant::now();
    let mut probes = 0_usize;
    for dir in 0..shape.dirs {
        for file in 0..shape.files_per_dir {
            let path = format!("/metadata-negative-lookup/dir-{dir:05}/missing-{file:05}");
            if client.lookup(&path)?.is_some() {
                return Err(BenchError::Client(format!(
                    "negative lookup unexpectedly found {path}"
                )));
            }
            checksum = checksum.wrapping_add(((dir as u64) << 32) ^ file as u64);
            probes += 1;
        }
    }

    Ok(row(RowInput {
        workload: "metadata-negative-lookup",
        profile: config.profile,
        operations: probes,
        seconds: start.elapsed().as_secs_f64(),
        bytes: 0,
        samples: 0,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "dirs={} missing_per_dir={} setup_present_per_dir={} file_body=metadata-only",
            shape.dirs, shape.files_per_dir, shape.files_per_dir
        ),
        caveat: metadata_only_caveat(config),
    }))
}

fn bench_artifact_index_lookup(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir(
        "/artifact-index",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;
    let run_paths = (0..shape.dirs)
        .map(|run| format!("/artifact-index/run-{run:05}"))
        .collect::<Vec<_>>();
    client.mkdirs(&run_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;

    let mut artifact_paths = Vec::with_capacity(shape.dirs * shape.files_per_dir);
    for run in 0..shape.dirs {
        let run_path = format!("/artifact-index/run-{run:05}");
        for artifact in 0..shape.files_per_dir {
            let path = format!("{run_path}/artifact-{artifact:05}.bin");
            let manifest_id = format!("artifact-index/run-{run:05}/artifact-{artifact:05}");
            client.put_artifact(
                &path,
                vec![artifact as u8],
                artifact_metadata("artifact-index", &manifest_id),
            )?;
            artifact_paths.push(path);
        }
    }

    let before = client.stats()?;
    let start = Instant::now();
    let mut checksum = 0_u64;
    for path in &artifact_paths {
        let metadata = client.stat_path(path)?.ok_or_else(|| {
            BenchError::Client(format!("artifact stat missed indexed path {path}"))
        })?;
        checksum = checksum
            .wrapping_add(metadata.attr.inode.get())
            .wrapping_add(metadata.attr.generation);
    }
    for run_path in &run_paths {
        let entries = client.list_indexed(run_path)?;
        if entries.len() != shape.files_per_dir {
            return Err(BenchError::Client(format!(
                "indexed list for {run_path} returned {} entries, expected {}",
                entries.len(),
                shape.files_per_dir
            )));
        }
        checksum = checksum.wrapping_add(entries.len() as u64);
    }

    Ok(row(RowInput {
        workload: "artifact-index-lookup",
        profile: config.profile,
        operations: artifact_paths.len() + run_paths.len(),
        seconds: start.elapsed().as_secs_f64(),
        bytes: 0,
        samples: 0,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "runs={} artifacts_per_run={} timed_ops=stat_path_plus_indexed_list",
            shape.dirs, shape.files_per_dir
        ),
        caveat: metadata_only_caveat(config),
    }))
}

fn bench_metadata_concurrent_read(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir(
        "/metadata-concurrent-read",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;
    let run_paths = (0..shape.dirs)
        .map(|run| format!("/metadata-concurrent-read/run-{run:05}"))
        .collect::<Vec<_>>();
    client.mkdirs(&run_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;

    let mut artifact_paths = Vec::with_capacity(shape.dirs * shape.files_per_dir);
    for run in 0..shape.dirs {
        let run_path = format!("/metadata-concurrent-read/run-{run:05}");
        for artifact in 0..shape.files_per_dir {
            let path = format!("{run_path}/artifact-{artifact:05}.bin");
            let manifest_id =
                format!("metadata-concurrent-read/run-{run:05}/artifact-{artifact:05}");
            client.put_artifact(
                &path,
                vec![artifact as u8],
                artifact_metadata("metadata-concurrent-read", &manifest_id),
            )?;
            artifact_paths.push(path);
        }
    }

    for path in &artifact_paths {
        client
            .stat_path(path)?
            .ok_or_else(|| BenchError::Client(format!("warmup stat missed indexed path {path}")))?;
    }
    for run_path in &run_paths {
        let entries = client.list_indexed(run_path)?;
        if entries.len() != shape.files_per_dir {
            return Err(BenchError::Client(format!(
                "warmup indexed list for {run_path} returned {} entries, expected {}",
                entries.len(),
                shape.files_per_dir
            )));
        }
    }

    let stat_ops = artifact_paths.len() * config.read_repeats;
    let list_ops = run_paths.len() * config.read_repeats;
    let operations = stat_ops + list_ops;
    let before = client.stats()?;
    let start = Instant::now();
    let checksum = run_parallel(operations, config.object_concurrency, |op| {
        if op < stat_ops {
            let path = &artifact_paths[op % artifact_paths.len()];
            let metadata = client.stat_path(path)?.ok_or_else(|| {
                BenchError::Client(format!("concurrent stat missed indexed path {path}"))
            })?;
            Ok(metadata
                .attr
                .inode
                .get()
                .wrapping_add(metadata.attr.generation))
        } else {
            let index = op - stat_ops;
            let run_path = &run_paths[index % run_paths.len()];
            let entries = client.list_indexed(run_path)?;
            if entries.len() != shape.files_per_dir {
                return Err(BenchError::Client(format!(
                    "concurrent indexed list for {run_path} returned {} entries, expected {}",
                    entries.len(),
                    shape.files_per_dir
                )));
            }
            Ok(entries.len() as u64)
        }
    })?;

    Ok(row(RowInput {
        workload: "metadata-concurrent-read",
        profile: config.profile,
        operations,
        seconds: start.elapsed().as_secs_f64(),
        bytes: 0,
        samples: 0,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "runs={} artifacts_per_run={} read_repeats={} workers={} timed_ops=concurrent_stat_path_plus_indexed_list",
            shape.dirs, shape.files_per_dir, config.read_repeats, config.object_concurrency
        ),
        caveat: metadata_only_caveat(config),
    }))
}

fn bench_metadata_ha_smoke(
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    let workload = "metadata-ha-smoke";
    let root = config.root.join(workload);
    fs::create_dir_all(&root).map_err(from_io)?;
    let object = object_config_for(config, workload);
    let objects = object.clone().open().map_err(from_client)?;
    let node_1 = NodeId::new(1).expect("benchmark node id is non-zero");
    let node_2 = NodeId::new(2).expect("benchmark node id is non-zero");
    let node_3 = NodeId::new(3).expect("benchmark node id is non-zero");
    let voters = vec![node_1, node_2, node_3];

    let listener_2 = TcpListener::bind("127.0.0.1:0").map_err(from_io)?;
    let addr_2 = listener_2.local_addr().map_err(from_io)?;
    let listener_3 = TcpListener::bind("127.0.0.1:0").map_err(from_io)?;
    let addr_3 = listener_3.local_addr().map_err(from_io)?;
    let listener_1 = TcpListener::bind("127.0.0.1:0").map_err(from_io)?;
    let addr_1 = listener_1.local_addr().map_err(from_io)?;
    let peers = vec![
        MetadataRaftPeerOptions {
            node: node_1,
            address: addr_1,
        },
        MetadataRaftPeerOptions {
            node: node_2,
            address: addr_2,
        },
        MetadataRaftPeerOptions {
            node: node_3,
            address: addr_3,
        },
    ];
    let cluster = MetadataHaClusterConfig {
        root: root.clone(),
        object,
        voters,
        peers,
        sync: config.metadata_raft_log_sync,
    };

    start_metadata_raft_server(listener_2, server_options_for_node(&cluster, node_2)?)?;
    wait_for_health(addr_2)?;
    start_metadata_raft_server(listener_3, server_options_for_node(&cluster, node_3)?)?;
    wait_for_health(addr_3)?;
    start_metadata_raft_server(listener_1, server_options_for_node(&cluster, node_1)?)?;
    wait_for_health(addr_1)?;

    let cluster_nodes = [(node_1, addr_1), (node_2, addr_2), (node_3, addr_3)];
    let (leader, leader_addr) = wait_metadata_raft_leader(&cluster_nodes)?;
    let write_addr = cluster_nodes
        .iter()
        .find_map(|(node, address)| (*node != leader).then_some(*address))
        .unwrap_or(leader_addr);
    let peer_read_addr = cluster_nodes
        .iter()
        .find_map(|(node, address)| (*node != leader && *address != write_addr).then_some(*address))
        .unwrap_or(write_addr);
    let metadata = MetadataClient::new(
        MetadataClientOptions::new(write_addr).with_read_endpoints(vec![peer_read_addr]),
    );
    let client = NoKvFsClient::new(metadata, objects);
    client
        .metadata()
        .bootstrap_root(DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
        .map_err(|err| BenchError::Client(format!("metadata-ha-smoke bootstrap root: {err}")))?;
    let before = fetch_server_stats(leader_addr)?;
    let start = Instant::now();
    let mut checksum = 0_u64;
    let files = shape.shared_files.clamp(16, 512);
    client
        .metadata()
        .mkdir("/ha", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
        .map_err(|err| BenchError::Client(format!("metadata-ha-smoke mkdir /ha: {err}")))?;
    let paths = (0..files)
        .map(|index| format!("/ha/file-{index:06}"))
        .collect::<Vec<_>>();
    for entry in client
        .metadata()
        .create_files(&paths, DEFAULT_MODE_FILE, DEFAULT_UID, DEFAULT_GID)
        .map_err(|err| BenchError::Client(format!("metadata-ha-smoke create files RPC: {err}")))?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            BenchError::Client(format!("metadata-ha-smoke create files result: {err}"))
        })?
    {
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }
    let observed = client
        .metadata()
        .observed_metadata_position()
        .ok_or_else(|| {
            BenchError::Client("metadata HA write returned no log position".to_owned())
        })?;
    let peer_reader = MetadataClient::connect(peer_read_addr);
    peer_reader.observe_metadata_position(observed);
    let peer_entries = peer_reader
        .list("/ha")
        .map_err(|err| BenchError::Client(format!("metadata-ha-smoke peer list /ha: {err}")))?;
    if peer_entries.len() != files {
        return Err(BenchError::Client(format!(
            "metadata HA peer read returned {} entries, expected {files}",
            peer_entries.len()
        )));
    }
    checksum = checksum.wrapping_add(peer_entries.len() as u64);
    let stats = stats_delta(before, fetch_server_stats(leader_addr)?);
    Ok(row(RowInput {
        workload,
        profile: config.profile,
        operations: files + 2,
        seconds: start.elapsed().as_secs_f64(),
        bytes: 0,
        samples: 0,
        stats,
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "voters=3 leader={} write_endpoint={} peer_read={} files={files}",
            leader.get(),
            write_addr,
            peer_read_addr
        ),
        caveat: format!(
            "OpenRaft metadata HA smoke over three metadata server processes, sync={}, peer read requires observed log position",
            metadata_raft_log_sync_name(config.metadata_raft_log_sync)
        ),
    }))
}

fn bench_metadata_ha_fault_smoke(
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    let workload = "metadata-ha-fault-smoke";
    let root = config.root.join(workload);
    fs::create_dir_all(&root).map_err(from_io)?;
    let object = object_config_for(config, workload);
    let objects = object.clone().open().map_err(from_client)?;
    let node_1 = NodeId::new(1).expect("benchmark node id is non-zero");
    let node_2 = NodeId::new(2).expect("benchmark node id is non-zero");
    let node_3 = NodeId::new(3).expect("benchmark node id is non-zero");
    let voters = vec![node_1, node_2, node_3];

    let listener_2 = TcpListener::bind("127.0.0.1:0").map_err(from_io)?;
    let addr_2 = listener_2.local_addr().map_err(from_io)?;
    let addr_3 = reserve_local_addr()?;
    let listener_1 = TcpListener::bind("127.0.0.1:0").map_err(from_io)?;
    let addr_1 = listener_1.local_addr().map_err(from_io)?;
    let peers = vec![
        MetadataRaftPeerOptions {
            node: node_1,
            address: addr_1,
        },
        MetadataRaftPeerOptions {
            node: node_2,
            address: addr_2,
        },
        MetadataRaftPeerOptions {
            node: node_3,
            address: addr_3,
        },
    ];
    let cluster = MetadataHaClusterConfig {
        root: root.clone(),
        object,
        voters,
        peers,
        sync: config.metadata_raft_log_sync,
    };

    start_metadata_raft_server(listener_2, server_options_for_node(&cluster, node_2)?)?;
    wait_for_health(addr_2)?;
    start_metadata_raft_server(listener_1, server_options_for_node(&cluster, node_1)?)?;
    wait_for_health(addr_1)?;

    let metadata =
        MetadataClient::new(MetadataClientOptions::new(addr_1).with_read_endpoints(vec![addr_2]));
    let client = NoKvFsClient::new(metadata, objects);
    client
        .metadata()
        .bootstrap_root(DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
        .map_err(|err| BenchError::Client(format!("metadata-ha-fault bootstrap root: {err}")))?;
    let before = fetch_server_stats(addr_1)?;
    let start = Instant::now();
    let mut checksum = 0_u64;
    let files = shape.shared_files.clamp(16, 512);
    client
        .metadata()
        .mkdir("/ha-fault", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
        .map_err(|err| BenchError::Client(format!("metadata-ha-fault mkdir /ha-fault: {err}")))?;
    let paths = (0..files)
        .map(|index| format!("/ha-fault/file-{index:06}"))
        .collect::<Vec<_>>();
    for entry in client
        .metadata()
        .create_files(&paths, DEFAULT_MODE_FILE, DEFAULT_UID, DEFAULT_GID)
        .map_err(|err| BenchError::Client(format!("metadata-ha-fault create files RPC: {err}")))?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            BenchError::Client(format!("metadata-ha-fault create files result: {err}"))
        })?
    {
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }
    let first_observed = client
        .metadata()
        .observed_metadata_position()
        .ok_or_else(|| {
            BenchError::Client("metadata HA degraded write returned no log position".to_owned())
        })?;
    let peer_2 = MetadataClient::connect(addr_2);
    peer_2.observe_metadata_position(first_observed);
    let peer_2_entries = peer_2
        .list("/ha-fault")
        .map_err(|err| BenchError::Client(format!("metadata-ha-fault peer2 list: {err}")))?;
    if peer_2_entries.len() != files {
        return Err(BenchError::Client(format!(
            "metadata HA surviving peer read returned {} entries, expected {files}",
            peer_2_entries.len()
        )));
    }

    let listener_3 = TcpListener::bind(addr_3).map_err(from_io)?;
    start_metadata_raft_server(listener_3, server_options_for_node(&cluster, node_3)?)?;
    wait_for_health(addr_3)?;
    let recovered = client
        .metadata()
        .create_file(
            "/ha-fault/recovered-after-outage",
            DEFAULT_MODE_FILE,
            DEFAULT_UID,
            DEFAULT_GID,
        )
        .map_err(|err| BenchError::Client(format!("metadata-ha-fault recovery create: {err}")))?;
    checksum = checksum.wrapping_add(recovered.attr.inode.get());
    let recovered_observed = client
        .metadata()
        .observed_metadata_position()
        .ok_or_else(|| {
            BenchError::Client("metadata HA recovery write returned no log position".to_owned())
        })?;
    let peer_3 = MetadataClient::connect(addr_3);
    peer_3.observe_metadata_position(recovered_observed);
    let peer_3_entries = peer_3
        .list("/ha-fault")
        .map_err(|err| BenchError::Client(format!("metadata-ha-fault peer3 list: {err}")))?;
    let expected_after_recovery = files + 1;
    if peer_3_entries.len() != expected_after_recovery {
        return Err(BenchError::Client(format!(
            "metadata HA recovered peer read returned {} entries, expected {expected_after_recovery}",
            peer_3_entries.len()
        )));
    }
    checksum = checksum
        .wrapping_add(peer_2_entries.len() as u64)
        .wrapping_add(peer_3_entries.len() as u64);
    let stats = stats_delta(before, fetch_server_stats(addr_1)?);
    Ok(row(RowInput {
        workload,
        profile: config.profile,
        operations: files + 4,
        seconds: start.elapsed().as_secs_f64(),
        bytes: 0,
        samples: 0,
        stats,
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "voters=3 leader=1 surviving_peer=2 recovered_peer=3 files={files}"
        ),
        caveat: format!(
            "OpenRaft metadata HA fault smoke: node3 unavailable for initial quorum commit, then restarted and caught up, sync={}",
            metadata_raft_log_sync_name(config.metadata_raft_log_sync)
        ),
    }))
}

fn bench_checkpoint_publish(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir("/checkpoints", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let first = checkpoint_payload(0, shape.checkpoint_bytes);
    client.put_artifact(
        "/checkpoints/latest.ckpt",
        first,
        artifact_metadata("checkpoint", "checkpoints/latest-initial"),
    )?;

    let before = client.stats()?;
    let start = Instant::now();
    let stage_checksum = run_parallel(shape.checkpoints, config.object_concurrency, |step| {
        let stage_path = format!("/checkpoints/.stage-{step:06}");
        let manifest_id = format!("checkpoints/stage-{step:06}");
        let bytes = checkpoint_payload(step, shape.checkpoint_bytes);
        let staged = client.put_artifact(
            &stage_path,
            bytes,
            artifact_metadata("checkpoint", &manifest_id),
        )?;
        Ok(staged.attr.inode.get())
    })?;

    let mut checksum = stage_checksum;
    for step in 0..shape.checkpoints {
        let stage_path = format!("/checkpoints/.stage-{step:06}");
        let result = client.rename_replace(&stage_path, "/checkpoints/latest.ckpt")?;
        checksum = checksum
            .wrapping_add(result.entry.attr.inode.get())
            .wrapping_add(
                result
                    .replaced
                    .map(|entry| entry.attr.inode.get())
                    .unwrap_or(0),
            );
    }
    Ok(row(RowInput {
        workload: "checkpoint-publish",
        profile: config.profile,
        operations: shape.checkpoints * 2,
        seconds: start.elapsed().as_secs_f64(),
        bytes: (shape.checkpoints * shape.checkpoint_bytes) as u64,
        samples: 0,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "iterations={} payload_bytes={} ops=count_parallel_put_plus_atomic_replace",
            shape.checkpoints, shape.checkpoint_bytes
        ),
        caveat: object_caveat(config, "object put plus metadata rename-replace"),
    }))
}

fn bench_training_read(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir("/dataset", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let shard_paths = (0..shape.dataset_dirs)
        .map(|shard| format!("/dataset/shard-{shard:04}"))
        .collect::<Vec<_>>();
    client.mkdirs(&shard_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    for shard in 0..shape.dataset_dirs {
        let shard_path = format!("/dataset/shard-{shard:04}");
        for file in 0..shape.dataset_files_per_dir {
            let path = format!("{shard_path}/sample-{file:05}.bin");
            let manifest_id = format!("dataset/shard-{shard:04}/sample-{file:05}.bin");
            let payload = dataset_payload(shard, file, shape.dataset_file_bytes);
            client.put_artifact(&path, payload, artifact_metadata("dataset", &manifest_id))?;
        }
    }

    let before = client.stats()?;
    let start = Instant::now();
    let checksum = run_parallel(shape.dataset_dirs, config.object_concurrency, |shard| {
        let shard_path = format!("/dataset/shard-{shard:04}");
        let entries = client.list(&shard_path)?;
        let mut checksum = entries.len() as u64;
        if let Some(first) = entries.first() {
            let name = String::from_utf8_lossy(first.dentry.name.as_bytes());
            let path = format!("{shard_path}/{name}");
            for _ in 0..config.read_repeats {
                let bytes = client.cat(&path)?;
                checksum =
                    checksum.wrapping_add(bytes.iter().map(|byte| *byte as u64).sum::<u64>());
            }
        }
        Ok(checksum)
    })?;
    black_box(checksum);
    Ok(row(RowInput {
        workload: "training-read",
        profile: config.profile,
        operations: shape.dataset_dirs * (1 + config.read_repeats),
        seconds: start.elapsed().as_secs_f64(),
        bytes: (shape.dataset_dirs * shape.dataset_file_bytes * config.read_repeats) as u64,
        samples: shape.dataset_dirs * config.read_repeats,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "dataset_dirs={} files_per_dir={} sample_bytes={} timed_ops=list_plus_repeated_read_per_dir",
            shape.dataset_dirs, shape.dataset_files_per_dir, shape.dataset_file_bytes
        ),
        caveat: object_caveat(config, "warm object reads after deterministic dataset seed"),
    }))
}

fn bench_mlperf_dlio(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir("/mlperf-dlio", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    client.mkdir(
        "/mlperf-dlio/dataset",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;
    client.mkdir(
        "/mlperf-dlio/checkpoints",
        DEFAULT_MODE_DIR,
        DEFAULT_UID,
        DEFAULT_GID,
    )?;

    let shard_paths = (0..shape.dataset_dirs)
        .map(|shard| format!("/mlperf-dlio/dataset/shard-{shard:04}"))
        .collect::<Vec<_>>();
    client.mkdirs(&shard_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    for shard in 0..shape.dataset_dirs {
        let shard_path = format!("/mlperf-dlio/dataset/shard-{shard:04}");
        for file in 0..shape.dataset_files_per_dir {
            let path = format!("{shard_path}/sample-{file:05}.bin");
            let manifest_id = format!("mlperf-dlio/dataset/shard-{shard:04}/sample-{file:05}");
            let sample_payload = dataset_payload(shard, file, shape.dataset_file_bytes);
            client.put_artifact(
                &path,
                sample_payload,
                artifact_metadata("mlperf-dlio-dataset", &manifest_id),
            )?;
        }
    }

    client.put_artifact(
        "/mlperf-dlio/checkpoints/latest.ckpt",
        checkpoint_payload(0, shape.checkpoint_bytes),
        artifact_metadata(
            "mlperf-dlio-checkpoint",
            "mlperf-dlio/checkpoints/latest-initial",
        ),
    )?;

    let checkpoint_steps = shape.checkpoints.max(1) / 4;
    let before = client.stats()?;
    let start = Instant::now();
    let mut checksum = run_parallel(shape.dataset_dirs, config.object_concurrency, |shard| {
        let shard_path = format!("/mlperf-dlio/dataset/shard-{shard:04}");
        let entries = client.list(&shard_path)?;
        let mut checksum = entries.len() as u64;
        if let Some(first) = entries.first() {
            let name = String::from_utf8_lossy(first.dentry.name.as_bytes());
            let path = format!("{shard_path}/{name}");
            for _ in 0..config.read_repeats {
                let bytes = client.cat(&path)?;
                checksum = checksum.wrapping_add(bytes.len() as u64);
            }
        }
        Ok(checksum)
    })?;
    let stage_checksum = run_parallel(checkpoint_steps, config.object_concurrency, |step| {
        let stage_path = format!("/mlperf-dlio/checkpoints/.stage-{step:06}");
        let manifest_id = format!("mlperf-dlio/checkpoints/stage-{step:06}");
        let entry = client.put_artifact(
            &stage_path,
            checkpoint_payload(step, shape.checkpoint_bytes),
            artifact_metadata("mlperf-dlio-checkpoint", &manifest_id),
        )?;
        Ok(entry.attr.inode.get())
    })?;
    checksum = checksum.wrapping_add(stage_checksum);
    for step in 0..checkpoint_steps {
        let stage_path = format!("/mlperf-dlio/checkpoints/.stage-{step:06}");
        let result = client.rename_replace(&stage_path, "/mlperf-dlio/checkpoints/latest.ckpt")?;
        checksum = checksum.wrapping_add(result.entry.attr.inode.get());
    }
    black_box(checksum);
    Ok(row(RowInput {
        workload: "mlperf-dlio",
        profile: config.profile,
        operations: shape.dataset_dirs * (1 + config.read_repeats) + checkpoint_steps * 2,
        seconds: start.elapsed().as_secs_f64(),
        bytes: (shape.dataset_dirs * shape.dataset_file_bytes * config.read_repeats
            + checkpoint_steps * shape.checkpoint_bytes) as u64,
        samples: shape.dataset_dirs * config.read_repeats,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "dlio_style_generated dataset_dirs={} files_per_dir={} sample_bytes={} checkpoint_steps={} checkpoint_bytes={}",
            shape.dataset_dirs,
            shape.dataset_files_per_dir,
            shape.dataset_file_bytes,
            checkpoint_steps,
            shape.checkpoint_bytes
        ),
        caveat: object_caveat(config, "MLPerf Storage/DLIO-style generated training read plus checkpoint write"),
    }))
}

fn bench_demo_dataset(
    client: &dyn BenchClient,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client.mkdir("/demo-dataset", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    let classes = shape.dataset_dirs.clamp(1, 8);
    let samples_per_class = shape.dataset_files_per_dir.clamp(1, 32);
    let sample_bytes = shape.dataset_file_bytes.clamp(128, 4096);
    let payload = vec![23_u8; sample_bytes];
    let class_paths = (0..classes)
        .map(|class| format!("/demo-dataset/class-{class:03}"))
        .collect::<Vec<_>>();
    client.mkdirs(&class_paths, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)?;
    for class in 0..classes {
        let class_path = format!("/demo-dataset/class-{class:03}");
        for sample in 0..samples_per_class {
            let path = format!("{class_path}/sample-{sample:05}.bin");
            let manifest_id = format!("demo-dataset/class-{class:03}/sample-{sample:05}");
            client.put_artifact(
                &path,
                payload.clone(),
                artifact_metadata("demo-dataset", &manifest_id),
            )?;
        }
    }

    let before = client.stats()?;
    let start = Instant::now();
    let mut checksum = 0_u64;
    for class in 0..classes {
        let class_path = format!("/demo-dataset/class-{class:03}");
        let entries = client.list(&class_path)?;
        for entry in entries.iter().take(2) {
            let name = String::from_utf8_lossy(entry.dentry.name.as_bytes());
            let path = format!("{class_path}/{name}");
            checksum = checksum.wrapping_add(client.cat(&path)?.len() as u64);
        }
    }
    Ok(row(RowInput {
        workload: "demo-dataset",
        profile: config.profile,
        operations: classes * 3,
        seconds: start.elapsed().as_secs_f64(),
        bytes: (classes * 2 * sample_bytes) as u64,
        samples: classes * 2,
        stats: stats_delta(before, client.stats()?),
        object_concurrency: config.object_concurrency,
        read_repeats: config.read_repeats,
        block_cache: config.block_cache,
        checksum,
        shape: format!(
            "public_dataset_demo_shape classes={} samples_per_class={} sample_bytes={} timed=list_plus_two_reads_per_class",
            classes, samples_per_class, sample_bytes
        ),
        caveat: object_caveat(config, "small public-dataset-shaped demo without external download"),
    }))
}

fn row(input: RowInput) -> ResultRow {
    let cache_total = input.stats.object.object_gets + input.stats.object.cache_hits;
    let path_index_lookups = input.stats.metadata_service.path_index_lookup_total;
    let path_index_hits = input.stats.metadata_service.path_index_hit_total;
    let read_dir_plus_entries = input.stats.metadata_service.read_dir_plus_entry_total;
    let read_dir_plus_projection_hits = input
        .stats
        .metadata_service
        .read_dir_plus_projection_hit_total;
    ResultRow {
        workload: input.workload,
        profile: input.profile,
        operations: input.operations,
        seconds: input.seconds,
        ops_per_second: input.operations as f64 / input.seconds.max(f64::MIN_POSITIVE),
        mb_per_second: input.bytes as f64 / 1_048_576_f64 / input.seconds.max(f64::MIN_POSITIVE),
        samples_per_second: input.samples as f64 / input.seconds.max(f64::MIN_POSITIVE),
        object_puts: input.stats.object.object_puts,
        object_put_bytes: input.stats.object.object_put_bytes,
        object_gets: input.stats.object.object_gets,
        object_get_bytes: input.stats.object.object_get_bytes,
        cache_hits: input.stats.object.cache_hits,
        cache_hit_bytes: input.stats.object.cache_hit_bytes,
        cache_hit_rate: if cache_total == 0 {
            0.0
        } else {
            input.stats.object.cache_hits as f64 / cache_total as f64
        },
        manifest_chunks: input.stats.object.manifest_chunks,
        manifest_blocks: input.stats.object.manifest_blocks,
        metadata_commits: input.stats.metadata_store.commit_total,
        metadata_dedupe_hits: input.stats.metadata_store.dedupe_hit_total,
        metadata_predicates: input.stats.metadata_store.predicate_total,
        metadata_prefix_empty_predicates: input.stats.metadata_store.prefix_empty_predicate_total,
        metadata_raft_current_term: input.stats.metadata_raft.current_term,
        metadata_raft_current_leader: input.stats.metadata_raft.current_leader,
        metadata_raft_last_log_index: input.stats.metadata_raft.last_log_index,
        metadata_raft_last_applied_index: input.stats.metadata_raft.last_applied_index,
        metadata_raft_snapshot_index: input.stats.metadata_raft.snapshot_index,
        metadata_raft_purged_index: input.stats.metadata_raft.purged_index,
        metadata_raft_millis_since_quorum_ack: input.stats.metadata_raft.millis_since_quorum_ack,
        metadata_raft_voters: input.stats.metadata_raft.voter_count,
        metadata_raft_learners: input.stats.metadata_raft.learner_count,
        metadata_gets: input.stats.metadata_store.get_total,
        metadata_get_user_strong: input.stats.metadata_store.get_user_strong_total,
        metadata_get_write_plan_local: input.stats.metadata_store.get_write_plan_local_total,
        metadata_get_snapshot: input.stats.metadata_store.get_snapshot_total,
        metadata_scans: input.stats.metadata_store.scan_total,
        metadata_scan_user_strong: input.stats.metadata_store.scan_user_strong_total,
        metadata_scan_write_plan_local: input.stats.metadata_store.scan_write_plan_local_total,
        metadata_scan_snapshot: input.stats.metadata_store.scan_snapshot_total,
        metadata_scan_visited: input.stats.metadata_store.scan_key_visited_total,
        metadata_scan_returned: input.stats.metadata_store.scan_key_returned_total,
        metadata_history_lookups: input.stats.metadata_store.history_lookup_total,
        metadata_current_puts: input.stats.metadata_store.current_put_total,
        metadata_current_deletes: input.stats.metadata_store.current_delete_total,
        metadata_history_writes: input.stats.metadata_store.history_write_total,
        metadata_watch_writes: input.stats.metadata_store.watch_write_total,
        metadata_dedupe_writes: input.stats.metadata_store.dedupe_write_total,
        metadata_commit_prepare_ns: input.stats.metadata_store.commit_prepare_ns_total,
        metadata_atomic_applies: input.stats.metadata_store.atomic_apply_total,
        metadata_atomic_apply_commands: input.stats.metadata_store.atomic_apply_command_total,
        metadata_atomic_apply_max_batch: input.stats.metadata_store.atomic_apply_max_batch,
        metadata_atomic_apply_ns: input.stats.metadata_store.atomic_apply_ns_total,
        path_index_lookups,
        path_index_hits,
        path_index_misses: input.stats.metadata_service.path_index_miss_total,
        path_index_stale: input.stats.metadata_service.path_index_stale_total,
        path_index_scan_stale: input.stats.metadata_service.path_index_scan_stale_total,
        path_index_fallback: input.stats.metadata_service.path_index_fallback_total,
        path_index_hit_rate: ratio(path_index_hits, path_index_lookups),
        create_files_batches: input.stats.metadata_service.create_files_batch_total,
        create_files_entries: input.stats.metadata_service.create_files_entry_total,
        create_dirs_batches: input.stats.metadata_service.create_dirs_batch_total,
        create_dirs_entries: input.stats.metadata_service.create_dirs_entry_total,
        read_dir_plus_calls: input.stats.metadata_service.read_dir_plus_total,
        read_dir_plus_entries,
        read_dir_plus_projection_hits,
        read_dir_plus_projection_hit_rate: ratio(
            read_dir_plus_projection_hits,
            read_dir_plus_entries,
        ),
        object_concurrency: input.object_concurrency,
        read_repeats: input.read_repeats,
        block_cache: input.block_cache,
        checksum: input.checksum,
        shape: input.shape,
        caveat: input.caveat,
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn csv_header() -> &'static str {
    "workload,profile,operations,seconds,ops_per_second,mb_per_second,samples_per_second,object_puts,object_put_bytes,object_gets,object_get_bytes,cache_hits,cache_hit_bytes,cache_hit_rate,manifest_chunks,manifest_blocks,metadata_commits,metadata_dedupe_hits,metadata_predicates,metadata_prefix_empty_predicates,metadata_raft_current_term,metadata_raft_current_leader,metadata_raft_last_log_index,metadata_raft_last_applied_index,metadata_raft_snapshot_index,metadata_raft_purged_index,metadata_raft_millis_since_quorum_ack,metadata_raft_voters,metadata_raft_learners,metadata_gets,metadata_get_user_strong,metadata_get_write_plan_local,metadata_get_snapshot,metadata_scans,metadata_scan_user_strong,metadata_scan_write_plan_local,metadata_scan_snapshot,metadata_scan_visited,metadata_scan_returned,metadata_history_lookups,metadata_current_puts,metadata_current_deletes,metadata_history_writes,metadata_watch_writes,metadata_dedupe_writes,metadata_commit_prepare_ns,metadata_atomic_applies,metadata_atomic_apply_commands,metadata_atomic_apply_max_batch,metadata_atomic_apply_ns,path_index_lookups,path_index_hits,path_index_misses,path_index_stale,path_index_scan_stale,path_index_fallback,path_index_hit_rate,create_files_batches,create_files_entries,create_dirs_batches,create_dirs_entries,read_dir_plus_calls,read_dir_plus_entries,read_dir_plus_projection_hits,read_dir_plus_projection_hit_rate,object_concurrency,read_repeats,block_cache,checksum,shape,caveat"
}

fn csv_row(row: &ResultRow) -> String {
    [
        row.workload.to_owned(),
        profile_name(row.profile).to_owned(),
        row.operations.to_string(),
        format!("{:.6}", row.seconds),
        format!("{:.2}", row.ops_per_second),
        format!("{:.2}", row.mb_per_second),
        format!("{:.2}", row.samples_per_second),
        row.object_puts.to_string(),
        row.object_put_bytes.to_string(),
        row.object_gets.to_string(),
        row.object_get_bytes.to_string(),
        row.cache_hits.to_string(),
        row.cache_hit_bytes.to_string(),
        format!("{:.4}", row.cache_hit_rate),
        row.manifest_chunks.to_string(),
        row.manifest_blocks.to_string(),
        row.metadata_commits.to_string(),
        row.metadata_dedupe_hits.to_string(),
        row.metadata_predicates.to_string(),
        row.metadata_prefix_empty_predicates.to_string(),
        row.metadata_raft_current_term.to_string(),
        row.metadata_raft_current_leader.to_string(),
        row.metadata_raft_last_log_index.to_string(),
        row.metadata_raft_last_applied_index.to_string(),
        row.metadata_raft_snapshot_index.to_string(),
        row.metadata_raft_purged_index.to_string(),
        row.metadata_raft_millis_since_quorum_ack.to_string(),
        row.metadata_raft_voters.to_string(),
        row.metadata_raft_learners.to_string(),
        row.metadata_gets.to_string(),
        row.metadata_get_user_strong.to_string(),
        row.metadata_get_write_plan_local.to_string(),
        row.metadata_get_snapshot.to_string(),
        row.metadata_scans.to_string(),
        row.metadata_scan_user_strong.to_string(),
        row.metadata_scan_write_plan_local.to_string(),
        row.metadata_scan_snapshot.to_string(),
        row.metadata_scan_visited.to_string(),
        row.metadata_scan_returned.to_string(),
        row.metadata_history_lookups.to_string(),
        row.metadata_current_puts.to_string(),
        row.metadata_current_deletes.to_string(),
        row.metadata_history_writes.to_string(),
        row.metadata_watch_writes.to_string(),
        row.metadata_dedupe_writes.to_string(),
        row.metadata_commit_prepare_ns.to_string(),
        row.metadata_atomic_applies.to_string(),
        row.metadata_atomic_apply_commands.to_string(),
        row.metadata_atomic_apply_max_batch.to_string(),
        row.metadata_atomic_apply_ns.to_string(),
        row.path_index_lookups.to_string(),
        row.path_index_hits.to_string(),
        row.path_index_misses.to_string(),
        row.path_index_stale.to_string(),
        row.path_index_scan_stale.to_string(),
        row.path_index_fallback.to_string(),
        format!("{:.4}", row.path_index_hit_rate),
        row.create_files_batches.to_string(),
        row.create_files_entries.to_string(),
        row.create_dirs_batches.to_string(),
        row.create_dirs_entries.to_string(),
        row.read_dir_plus_calls.to_string(),
        row.read_dir_plus_entries.to_string(),
        row.read_dir_plus_projection_hits.to_string(),
        format!("{:.4}", row.read_dir_plus_projection_hit_rate),
        row.object_concurrency.to_string(),
        row.read_repeats.to_string(),
        row.block_cache.to_string(),
        row.checksum.to_string(),
        csv_field(&row.shape),
        csv_field(&row.caveat),
    ]
    .join(",")
}

fn stats_delta(before: BenchStats, after: BenchStats) -> BenchStats {
    let atomic_apply_delta = after
        .metadata_store
        .atomic_apply_total
        .saturating_sub(before.metadata_store.atomic_apply_total);
    BenchStats {
        object: ObjectTransferStats {
            object_puts: after
                .object
                .object_puts
                .saturating_sub(before.object.object_puts),
            object_put_bytes: after
                .object
                .object_put_bytes
                .saturating_sub(before.object.object_put_bytes),
            object_gets: after
                .object
                .object_gets
                .saturating_sub(before.object.object_gets),
            object_get_bytes: after
                .object
                .object_get_bytes
                .saturating_sub(before.object.object_get_bytes),
            cache_hits: after
                .object
                .cache_hits
                .saturating_sub(before.object.cache_hits),
            cache_hit_bytes: after
                .object
                .cache_hit_bytes
                .saturating_sub(before.object.cache_hit_bytes),
            manifest_chunks: after
                .object
                .manifest_chunks
                .saturating_sub(before.object.manifest_chunks),
            manifest_blocks: after
                .object
                .manifest_blocks
                .saturating_sub(before.object.manifest_blocks),
        },
        metadata_store: MetadataStoreStats {
            get_total: after
                .metadata_store
                .get_total
                .saturating_sub(before.metadata_store.get_total),
            get_user_strong_total: after
                .metadata_store
                .get_user_strong_total
                .saturating_sub(before.metadata_store.get_user_strong_total),
            get_write_plan_local_total: after
                .metadata_store
                .get_write_plan_local_total
                .saturating_sub(before.metadata_store.get_write_plan_local_total),
            get_snapshot_total: after
                .metadata_store
                .get_snapshot_total
                .saturating_sub(before.metadata_store.get_snapshot_total),
            scan_total: after
                .metadata_store
                .scan_total
                .saturating_sub(before.metadata_store.scan_total),
            scan_user_strong_total: after
                .metadata_store
                .scan_user_strong_total
                .saturating_sub(before.metadata_store.scan_user_strong_total),
            scan_write_plan_local_total: after
                .metadata_store
                .scan_write_plan_local_total
                .saturating_sub(before.metadata_store.scan_write_plan_local_total),
            scan_snapshot_total: after
                .metadata_store
                .scan_snapshot_total
                .saturating_sub(before.metadata_store.scan_snapshot_total),
            scan_key_visited_total: after
                .metadata_store
                .scan_key_visited_total
                .saturating_sub(before.metadata_store.scan_key_visited_total),
            scan_key_returned_total: after
                .metadata_store
                .scan_key_returned_total
                .saturating_sub(before.metadata_store.scan_key_returned_total),
            history_lookup_total: after
                .metadata_store
                .history_lookup_total
                .saturating_sub(before.metadata_store.history_lookup_total),
            active_snapshot_pin_total: after.metadata_store.active_snapshot_pin_total,
            commit_total: after
                .metadata_store
                .commit_total
                .saturating_sub(before.metadata_store.commit_total),
            dedupe_hit_total: after
                .metadata_store
                .dedupe_hit_total
                .saturating_sub(before.metadata_store.dedupe_hit_total),
            predicate_total: after
                .metadata_store
                .predicate_total
                .saturating_sub(before.metadata_store.predicate_total),
            prefix_empty_predicate_total: after
                .metadata_store
                .prefix_empty_predicate_total
                .saturating_sub(before.metadata_store.prefix_empty_predicate_total),
            current_put_total: after
                .metadata_store
                .current_put_total
                .saturating_sub(before.metadata_store.current_put_total),
            current_delete_total: after
                .metadata_store
                .current_delete_total
                .saturating_sub(before.metadata_store.current_delete_total),
            history_write_total: after
                .metadata_store
                .history_write_total
                .saturating_sub(before.metadata_store.history_write_total),
            watch_write_total: after
                .metadata_store
                .watch_write_total
                .saturating_sub(before.metadata_store.watch_write_total),
            dedupe_write_total: after
                .metadata_store
                .dedupe_write_total
                .saturating_sub(before.metadata_store.dedupe_write_total),
            commit_prepare_ns_total: after
                .metadata_store
                .commit_prepare_ns_total
                .saturating_sub(before.metadata_store.commit_prepare_ns_total),
            atomic_apply_total: atomic_apply_delta,
            atomic_apply_command_total: after
                .metadata_store
                .atomic_apply_command_total
                .saturating_sub(before.metadata_store.atomic_apply_command_total),
            atomic_apply_max_batch: if atomic_apply_delta == 0 {
                0
            } else {
                after.metadata_store.atomic_apply_max_batch
            },
            atomic_apply_ns_total: after
                .metadata_store
                .atomic_apply_ns_total
                .saturating_sub(before.metadata_store.atomic_apply_ns_total),
        },
        metadata_raft: after.metadata_raft,
        metadata_service: MetadataServiceStats {
            path_index_lookup_total: after
                .metadata_service
                .path_index_lookup_total
                .saturating_sub(before.metadata_service.path_index_lookup_total),
            path_index_hit_total: after
                .metadata_service
                .path_index_hit_total
                .saturating_sub(before.metadata_service.path_index_hit_total),
            path_index_miss_total: after
                .metadata_service
                .path_index_miss_total
                .saturating_sub(before.metadata_service.path_index_miss_total),
            path_index_stale_total: after
                .metadata_service
                .path_index_stale_total
                .saturating_sub(before.metadata_service.path_index_stale_total),
            path_index_scan_stale_total: after
                .metadata_service
                .path_index_scan_stale_total
                .saturating_sub(before.metadata_service.path_index_scan_stale_total),
            path_index_fallback_total: after
                .metadata_service
                .path_index_fallback_total
                .saturating_sub(before.metadata_service.path_index_fallback_total),
            create_files_batch_total: after
                .metadata_service
                .create_files_batch_total
                .saturating_sub(before.metadata_service.create_files_batch_total),
            create_files_entry_total: after
                .metadata_service
                .create_files_entry_total
                .saturating_sub(before.metadata_service.create_files_entry_total),
            create_dirs_batch_total: after
                .metadata_service
                .create_dirs_batch_total
                .saturating_sub(before.metadata_service.create_dirs_batch_total),
            create_dirs_entry_total: after
                .metadata_service
                .create_dirs_entry_total
                .saturating_sub(before.metadata_service.create_dirs_entry_total),
            read_dir_plus_total: after
                .metadata_service
                .read_dir_plus_total
                .saturating_sub(before.metadata_service.read_dir_plus_total),
            read_dir_plus_entry_total: after
                .metadata_service
                .read_dir_plus_entry_total
                .saturating_sub(before.metadata_service.read_dir_plus_entry_total),
            read_dir_plus_projection_hit_total: after
                .metadata_service
                .read_dir_plus_projection_hit_total
                .saturating_sub(before.metadata_service.read_dir_plus_projection_hit_total),
        },
    }
}

fn metadata_only_caveat(config: &Config) -> String {
    format!(
        "metadata-only on Holt metadata service, object_backend={}, {}",
        object_backend_name(config.object_backend),
        metadata_raft_caveat(config)
    )
}

fn object_caveat(config: &Config, path: &str) -> String {
    let cache = if config.block_cache {
        "cache=on"
    } else {
        "cache=off"
    };
    match config.object_backend {
        ObjectBackendKind::RustFs => {
            format!(
                "{path}, Holt metadata service, RustFS S3-compatible backend over configured endpoint, object_concurrency={}, read_repeats={}, {cache}, {}",
                config.object_concurrency,
                config.read_repeats,
                metadata_raft_caveat(config)
            )
        }
        ObjectBackendKind::S3 => {
            format!(
                "{path}, Holt metadata service, generic S3-compatible backend over configured endpoint, object_concurrency={}, read_repeats={}, {cache}, {}",
                config.object_concurrency,
                config.read_repeats,
                metadata_raft_caveat(config)
            )
        }
    }
}

fn metadata_raft_caveat(config: &Config) -> String {
    format!(
        "OpenRaft metadata group sync={}",
        metadata_raft_log_sync_name(config.metadata_raft_log_sync)
    )
}

fn client_for(config: &Config, workload: &str) -> Result<Box<dyn BenchClient>, BenchError> {
    service_client_for(config, workload)
}

fn service_client_for(config: &Config, workload: &str) -> Result<Box<dyn BenchClient>, BenchError> {
    let meta = config.root.join(workload).join("meta");
    let object = object_config_for(config, workload);
    let objects = object.clone().open().map_err(from_client)?;
    let listener = TcpListener::bind("127.0.0.1:0").map_err(from_io)?;
    let bind = listener.local_addr().map_err(from_io)?;
    let server = Server::open(ServerOptions {
        bind,
        mount: MountId::new(1).expect("mount id is non-zero"),
        meta_path: meta,
        metadata_raft_node: nokvfs_cluster::NodeId::new(1)
            .expect("benchmark metadata raft node is non-zero"),
        metadata_raft_voters: Vec::new(),
        metadata_raft_peers: Vec::new(),
        metadata_raft_log_sync: config.metadata_raft_log_sync,
        metadata_checkpoint_archive_prefix: None,
        object,
        uid: DEFAULT_UID,
        gid: DEFAULT_GID,
        object_gc: ObjectGcOptions {
            interval: Duration::from_secs(3600),
            limit: 1024,
            run_immediately: false,
        },
        history_gc: HistoryGcOptions {
            interval: Duration::from_secs(3600),
            limit: 1024,
            run_immediately: false,
        },
    })
    .map_err(from_client)?;
    thread::spawn(move || {
        let _ = server.serve(listener);
    });
    let mut client = NoKvFsClient::connect(bind, objects);
    client.set_block_cache_enabled(config.block_cache);
    Ok(Box::new(ServiceBenchClient {
        client,
        stats_addr: bind,
    }))
}

fn server_options_for_node(
    cluster: &MetadataHaClusterConfig,
    node: NodeId,
) -> Result<ServerOptions, BenchError> {
    let node_root = cluster.root.join(format!("node-{}", node.get()));
    fs::create_dir_all(&node_root).map_err(from_io)?;
    Ok(ServerOptions {
        bind: cluster
            .peers
            .iter()
            .find(|peer| peer.node == node)
            .map(|peer| peer.address)
            .ok_or_else(|| {
                BenchError::Client(format!("missing metadata peer for node {}", node.get()))
            })?,
        mount: MountId::new(1).expect("mount id is non-zero"),
        meta_path: node_root.join("meta"),
        metadata_raft_node: node,
        metadata_raft_voters: cluster.voters.clone(),
        metadata_raft_peers: cluster
            .peers
            .iter()
            .copied()
            .filter(|peer| peer.node != node)
            .collect(),
        metadata_raft_log_sync: cluster.sync,
        metadata_checkpoint_archive_prefix: None,
        object: cluster.object.clone(),
        uid: DEFAULT_UID,
        gid: DEFAULT_GID,
        object_gc: ObjectGcOptions {
            interval: Duration::from_secs(3600),
            limit: 1024,
            run_immediately: false,
        },
        history_gc: HistoryGcOptions {
            interval: Duration::from_secs(3600),
            limit: 1024,
            run_immediately: false,
        },
    })
}

fn start_metadata_raft_server(
    listener: TcpListener,
    options: ServerOptions,
) -> Result<(), BenchError> {
    let server = Server::open(options).map_err(from_client)?;
    thread::spawn(move || {
        let _ = server.serve(listener);
    });
    Ok(())
}

fn wait_for_health(address: SocketAddr) -> Result<(), BenchError> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match TcpStream::connect(address) {
            Ok(mut stream) => {
                stream
                    .write_all(
                        b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                    )
                    .map_err(from_io)?;
                let mut response = String::new();
                stream.read_to_string(&mut response).map_err(from_io)?;
                if response.contains("200 OK") && response.contains("ok") {
                    return Ok(());
                }
            }
            Err(err) if Instant::now() < deadline => {
                let _ = err;
            }
            Err(err) => return Err(from_io(err)),
        }
        if Instant::now() >= deadline {
            return Err(BenchError::Client(format!(
                "metadata server {address} did not become healthy"
            )));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_metadata_raft_leader(
    nodes: &[(NodeId, SocketAddr)],
) -> Result<(NodeId, SocketAddr), BenchError> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        for (_, address) in nodes {
            let Ok(stats) = fetch_server_stats(*address) else {
                continue;
            };
            if stats.metadata_raft.current_leader == 0 {
                continue;
            }
            let leader = NodeId::new(stats.metadata_raft.current_leader).map_err(from_client)?;
            let leader_addr = nodes
                .iter()
                .find_map(|(candidate, address)| (*candidate == leader).then_some(*address))
                .ok_or_else(|| {
                    BenchError::Client(format!(
                        "metadata raft leader {} is not in benchmark node set",
                        leader.get()
                    ))
                })?;
            return Ok((leader, leader_addr));
        }
        if Instant::now() >= deadline {
            return Err(BenchError::Client(
                "metadata raft cluster did not elect a leader".to_owned(),
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn reserve_local_addr() -> Result<SocketAddr, BenchError> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(from_io)?;
    listener.local_addr().map_err(from_io)
}

fn fetch_server_stats(address: SocketAddr) -> Result<BenchStats, BenchError> {
    let mut stream = TcpStream::connect(address).map_err(from_io)?;
    stream
        .write_all(b"GET /stats HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .map_err(from_io)?;
    let mut response = String::new();
    stream.read_to_string(&mut response).map_err(from_io)?;
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .ok_or_else(|| BenchError::Client("stats response missing body".to_owned()))?;
    Ok(BenchStats {
        object: ObjectTransferStats {
            object_puts: json_u64(body, "object_puts")?,
            object_put_bytes: json_u64(body, "object_put_bytes")?,
            object_gets: json_u64(body, "object_gets")?,
            object_get_bytes: json_u64(body, "object_get_bytes")?,
            cache_hits: json_u64(body, "cache_hits")?,
            cache_hit_bytes: json_u64(body, "cache_hit_bytes")?,
            manifest_chunks: json_u64(body, "manifest_chunks")?,
            manifest_blocks: json_u64(body, "manifest_blocks")?,
        },
        metadata_store: MetadataStoreStats {
            get_total: json_u64(body, "get_total")?,
            get_user_strong_total: json_u64(body, "get_user_strong_total")?,
            get_write_plan_local_total: json_u64(body, "get_write_plan_local_total")?,
            get_snapshot_total: json_u64(body, "get_snapshot_total")?,
            scan_total: json_u64(body, "scan_total")?,
            scan_user_strong_total: json_u64(body, "scan_user_strong_total")?,
            scan_write_plan_local_total: json_u64(body, "scan_write_plan_local_total")?,
            scan_snapshot_total: json_u64(body, "scan_snapshot_total")?,
            scan_key_visited_total: json_u64(body, "scan_key_visited_total")?,
            scan_key_returned_total: json_u64(body, "scan_key_returned_total")?,
            history_lookup_total: json_u64(body, "history_lookup_total")?,
            active_snapshot_pin_total: json_u64(body, "active_snapshot_pin_total")?,
            commit_total: json_u64(body, "commit_total")?,
            dedupe_hit_total: json_u64(body, "dedupe_hit_total")?,
            predicate_total: json_u64(body, "predicate_total")?,
            prefix_empty_predicate_total: json_u64(body, "prefix_empty_predicate_total")?,
            current_put_total: json_u64(body, "current_put_total")?,
            current_delete_total: json_u64(body, "current_delete_total")?,
            history_write_total: json_u64(body, "history_write_total")?,
            watch_write_total: json_u64(body, "watch_write_total")?,
            dedupe_write_total: json_u64(body, "dedupe_write_total")?,
            commit_prepare_ns_total: json_u64(body, "commit_prepare_ns_total")?,
            atomic_apply_total: json_u64(body, "atomic_apply_total")?,
            atomic_apply_command_total: json_u64(body, "atomic_apply_command_total")?,
            atomic_apply_max_batch: json_u64(body, "atomic_apply_max_batch")?,
            atomic_apply_ns_total: json_u64(body, "atomic_apply_ns_total")?,
        },
        metadata_raft: MetadataRaftBenchStats {
            current_term: json_u64(body, "current_term")?,
            current_leader: json_u64_or_zero(body, "current_leader"),
            last_log_index: json_u64_or_zero(body, "last_log_index"),
            last_applied_index: json_u64_or_zero(body, "last_applied_index"),
            snapshot_index: json_u64_or_zero(body, "snapshot_index"),
            purged_index: json_u64_or_zero(body, "purged_index"),
            millis_since_quorum_ack: json_u64_or_zero(body, "millis_since_quorum_ack"),
            voter_count: json_u64(body, "voter_count")?,
            learner_count: json_u64(body, "learner_count")?,
        },
        metadata_service: MetadataServiceStats {
            path_index_lookup_total: json_u64(body, "path_index_lookup_total")?,
            path_index_hit_total: json_u64(body, "path_index_hit_total")?,
            path_index_miss_total: json_u64(body, "path_index_miss_total")?,
            path_index_stale_total: json_u64(body, "path_index_stale_total")?,
            path_index_scan_stale_total: json_u64(body, "path_index_scan_stale_total")?,
            path_index_fallback_total: json_u64(body, "path_index_fallback_total")?,
            create_files_batch_total: json_u64(body, "create_files_batch_total")?,
            create_files_entry_total: json_u64(body, "create_files_entry_total")?,
            create_dirs_batch_total: json_u64(body, "create_dirs_batch_total")?,
            create_dirs_entry_total: json_u64(body, "create_dirs_entry_total")?,
            read_dir_plus_total: json_u64(body, "read_dir_plus_total")?,
            read_dir_plus_entry_total: json_u64(body, "read_dir_plus_entry_total")?,
            read_dir_plus_projection_hit_total: json_u64(
                body,
                "read_dir_plus_projection_hit_total",
            )?,
        },
    })
}

fn json_u64(body: &str, key: &str) -> Result<u64, BenchError> {
    let needle = format!("\"{key}\":");
    let start = body
        .find(&needle)
        .ok_or_else(|| BenchError::Client(format!("stats response missing {key}")))?
        + needle.len();
    let digits = body[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    if digits.is_empty() {
        return Err(BenchError::Client(format!(
            "stats response has non-numeric {key}"
        )));
    }
    digits
        .parse()
        .map_err(|err| BenchError::Client(format!("invalid stats value for {key}: {err}")))
}

fn json_u64_or_zero(body: &str, key: &str) -> u64 {
    json_u64(body, key).unwrap_or(0)
}

fn artifact_metadata(producer: &str, manifest_id: &str) -> ArtifactMetadata {
    ArtifactMetadata {
        producer: producer.to_owned(),
        digest_uri: format!("sha256:{}", stable_id_hash(manifest_id)),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: manifest_id.to_owned(),
        mode: DEFAULT_MODE_FILE,
        uid: DEFAULT_UID,
        gid: DEFAULT_GID,
    }
}

fn stable_id_hash(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    format!("{hash:016x}")
}

fn checkpoint_payload(seed: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|offset| ((seed + offset) % 251) as u8)
        .collect()
}

fn dataset_payload(shard: usize, file: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|offset| ((shard * 31 + file * 17 + offset) % 251) as u8)
        .collect()
}

fn run_parallel<F>(total: usize, concurrency: usize, worker: F) -> Result<u64, BenchError>
where
    F: Fn(usize) -> Result<u64, BenchError> + Sync,
{
    if total == 0 {
        return Ok(0);
    }
    let workers = concurrency.max(1).min(total);
    let next = AtomicUsize::new(0);
    let checksum = AtomicU64::new(0);
    let error = Mutex::new(None);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                if error.lock().expect("error lock").is_some() {
                    break;
                }
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= total {
                    break;
                }
                match worker(index) {
                    Ok(value) => {
                        checksum.fetch_add(value, Ordering::Relaxed);
                    }
                    Err(err) => {
                        *error.lock().expect("error lock") = Some(err);
                        break;
                    }
                }
            });
        }
    });
    if let Some(err) = error.into_inner().expect("error lock") {
        return Err(err);
    }
    Ok(checksum.load(Ordering::Relaxed))
}

fn parse(args: Vec<String>) -> Result<Config, BenchError> {
    let mut profile = Profile::Smoke;
    let mut workload = Workload::All;
    let mut root = default_root();
    let mut object_backend = ObjectBackendKind::RustFs;
    let mut s3 = S3ObjectStoreOptions::new("");
    let mut object_concurrency = 1_usize;
    let mut read_repeats = 1_usize;
    let mut block_cache = true;
    let mut metadata_raft_log_sync = FileMetadataRaftLogSync::Data;
    let mut checkpoint_bytes = None;
    let mut sample_bytes = None;
    let mut keep = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--profile" => {
                index += 1;
                profile = parse_profile(value(&args, index, "--profile")?)?;
            }
            "--workload" => {
                index += 1;
                workload = parse_workload(value(&args, index, "--workload")?)?;
            }
            "--root" => {
                index += 1;
                root = PathBuf::from(value(&args, index, "--root")?);
            }
            "--object-backend" => {
                index += 1;
                object_backend = parse_object_backend(value(&args, index, "--object-backend")?)?;
            }
            "--s3-bucket" => {
                index += 1;
                s3.bucket = value(&args, index, "--s3-bucket")?.to_owned();
            }
            "--s3-endpoint" => {
                index += 1;
                s3.endpoint = Some(value(&args, index, "--s3-endpoint")?.to_owned());
            }
            "--s3-region" => {
                index += 1;
                s3.region = value(&args, index, "--s3-region")?.to_owned();
            }
            "--s3-access-key-id" => {
                index += 1;
                s3.access_key_id = Some(value(&args, index, "--s3-access-key-id")?.to_owned());
            }
            "--s3-secret-access-key" => {
                index += 1;
                s3.secret_access_key =
                    Some(value(&args, index, "--s3-secret-access-key")?.to_owned());
            }
            "--s3-session-token" => {
                index += 1;
                s3.session_token = Some(value(&args, index, "--s3-session-token")?.to_owned());
            }
            "--s3-root" => {
                index += 1;
                s3.root = value(&args, index, "--s3-root")?.to_owned();
            }
            "--s3-virtual-host-style" => {
                s3.virtual_host_style = true;
            }
            "--s3-skip-signature" => {
                s3.skip_signature = true;
            }
            "--object-concurrency" => {
                index += 1;
                object_concurrency = parse_positive_usize(
                    value(&args, index, "--object-concurrency")?,
                    "--object-concurrency",
                )?;
            }
            "--checkpoint-bytes" => {
                index += 1;
                checkpoint_bytes = Some(parse_positive_usize(
                    value(&args, index, "--checkpoint-bytes")?,
                    "--checkpoint-bytes",
                )?);
            }
            "--sample-bytes" => {
                index += 1;
                sample_bytes = Some(parse_positive_usize(
                    value(&args, index, "--sample-bytes")?,
                    "--sample-bytes",
                )?);
            }
            "--read-repeats" => {
                index += 1;
                read_repeats =
                    parse_positive_usize(value(&args, index, "--read-repeats")?, "--read-repeats")?;
            }
            "--block-cache" => {
                index += 1;
                block_cache = parse_block_cache(value(&args, index, "--block-cache")?)?;
            }
            "--metadata-raft-log-sync" => {
                index += 1;
                metadata_raft_log_sync =
                    parse_metadata_raft_log_sync(value(&args, index, "--metadata-raft-log-sync")?)?;
            }
            "--keep" => keep = true,
            "--help" | "-h" => {
                return Err(BenchError::UnknownOption("--help".to_owned()));
            }
            option => return Err(BenchError::UnknownOption(option.to_owned())),
        }
        index += 1;
    }
    if object_backend == ObjectBackendKind::RustFs {
        apply_rustfs_defaults(&mut s3);
    }
    Ok(Config {
        profile,
        workload,
        root,
        object_backend,
        s3,
        object_concurrency,
        read_repeats,
        block_cache,
        metadata_raft_log_sync,
        checkpoint_bytes,
        sample_bytes,
        keep,
    })
}

fn object_config_for(config: &Config, workload: &str) -> ObjectStoreConfig {
    let mut options = config.s3.clone();
    if config.object_backend == ObjectBackendKind::RustFs {
        apply_rustfs_defaults(&mut options);
    }
    if options.root == "/" {
        options.root = format!("/nokv-fs-bench/{workload}");
    }
    ObjectStoreConfig::s3(options)
}

fn parse_object_backend(raw: &str) -> Result<ObjectBackendKind, BenchError> {
    match raw {
        "s3" => Ok(ObjectBackendKind::S3),
        "rustfs" => Ok(ObjectBackendKind::RustFs),
        _ => Err(BenchError::UnknownOption(format!("--object-backend {raw}"))),
    }
}

fn parse_positive_usize(raw: &str, option: &'static str) -> Result<usize, BenchError> {
    raw.parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| BenchError::UnknownOption(format!("{option} {raw}")))
}

fn parse_block_cache(raw: &str) -> Result<bool, BenchError> {
    parse_on_off(raw).map_err(|_| BenchError::UnknownOption(format!("--block-cache {raw}")))
}

fn parse_on_off(raw: &str) -> Result<bool, BenchError> {
    match raw {
        "on" | "true" | "1" => Ok(true),
        "off" | "false" | "0" => Ok(false),
        _ => Err(BenchError::UnknownOption(raw.to_owned())),
    }
}

fn parse_metadata_raft_log_sync(raw: &str) -> Result<FileMetadataRaftLogSync, BenchError> {
    match raw {
        "data" => Ok(FileMetadataRaftLogSync::Data),
        "none" => Ok(FileMetadataRaftLogSync::None),
        _ => Err(BenchError::UnknownOption(format!(
            "--metadata-raft-log-sync {raw}"
        ))),
    }
}

fn value<'a>(
    args: &'a [String],
    index: usize,
    option: &'static str,
) -> Result<&'a str, BenchError> {
    args.get(index)
        .map(String::as_str)
        .ok_or(BenchError::MissingValue(option))
}

fn parse_profile(raw: &str) -> Result<Profile, BenchError> {
    match raw {
        "smoke" => Ok(Profile::Smoke),
        "standard" => Ok(Profile::Standard),
        "long" => Ok(Profile::Long),
        _ => Err(BenchError::InvalidProfile(raw.to_owned())),
    }
}

fn parse_workload(raw: &str) -> Result<Workload, BenchError> {
    match raw {
        "all" => Ok(Workload::All),
        "metadata-smoke" => Ok(Workload::MetadataSmoke),
        "metadata-ha-smoke" => Ok(Workload::MetadataHaSmoke),
        "metadata-ha-fault-smoke" => Ok(Workload::MetadataHaFaultSmoke),
        "mdtest-easy" => Ok(Workload::MdtestEasy),
        "mdtest-hard" => Ok(Workload::MdtestHard),
        "metadata-negative-lookup" => Ok(Workload::MetadataNegativeLookup),
        "artifact-index-lookup" => Ok(Workload::ArtifactIndexLookup),
        "metadata-concurrent-read" => Ok(Workload::MetadataConcurrentRead),
        "checkpoint-publish" => Ok(Workload::CheckpointPublish),
        "training-read" => Ok(Workload::TrainingRead),
        "mlperf-dlio" => Ok(Workload::MlperfDlio),
        "demo-dataset" => Ok(Workload::DemoDataset),
        _ => Err(BenchError::InvalidWorkload(raw.to_owned())),
    }
}

fn expand_workloads(workload: Workload) -> Vec<Workload> {
    match workload {
        Workload::All => vec![
            Workload::MetadataHaSmoke,
            Workload::MetadataHaFaultSmoke,
            Workload::MdtestEasy,
            Workload::MdtestHard,
            Workload::MetadataNegativeLookup,
            Workload::ArtifactIndexLookup,
            Workload::MetadataConcurrentRead,
            Workload::CheckpointPublish,
            Workload::TrainingRead,
            Workload::MlperfDlio,
            Workload::DemoDataset,
        ],
        Workload::MetadataSmoke => vec![
            Workload::MdtestEasy,
            Workload::MdtestHard,
            Workload::MetadataNegativeLookup,
            Workload::MetadataConcurrentRead,
        ],
        other => vec![other],
    }
}

fn shape(config: &Config) -> WorkloadShape {
    let mut shape = match config.profile {
        Profile::Smoke => WorkloadShape {
            dirs: 8,
            files_per_dir: 64,
            shared_files: 512,
            checkpoints: 128,
            checkpoint_bytes: 4096,
            dataset_dirs: 8,
            dataset_files_per_dir: 64,
            dataset_file_bytes: 512,
        },
        Profile::Standard => WorkloadShape {
            dirs: 32,
            files_per_dir: 256,
            shared_files: 8192,
            checkpoints: 1024,
            checkpoint_bytes: 1024 * 1024,
            dataset_dirs: 32,
            dataset_files_per_dir: 256,
            dataset_file_bytes: 16 * 1024,
        },
        Profile::Long => WorkloadShape {
            dirs: 64,
            files_per_dir: 1024,
            shared_files: 65536,
            checkpoints: 4096,
            checkpoint_bytes: 8 * 1024 * 1024,
            dataset_dirs: 64,
            dataset_files_per_dir: 1024,
            dataset_file_bytes: 256 * 1024,
        },
    };
    if let Some(bytes) = config.checkpoint_bytes {
        shape.checkpoint_bytes = bytes;
    }
    if let Some(bytes) = config.sample_bytes {
        shape.dataset_file_bytes = bytes;
    }
    shape
}

fn workload_name(workload: Workload) -> &'static str {
    match workload {
        Workload::All => "all",
        Workload::MetadataSmoke => "metadata-smoke",
        Workload::MetadataHaSmoke => "metadata-ha-smoke",
        Workload::MetadataHaFaultSmoke => "metadata-ha-fault-smoke",
        Workload::MdtestEasy => "mdtest-easy",
        Workload::MdtestHard => "mdtest-hard",
        Workload::MetadataNegativeLookup => "metadata-negative-lookup",
        Workload::ArtifactIndexLookup => "artifact-index-lookup",
        Workload::MetadataConcurrentRead => "metadata-concurrent-read",
        Workload::CheckpointPublish => "checkpoint-publish",
        Workload::TrainingRead => "training-read",
        Workload::MlperfDlio => "mlperf-dlio",
        Workload::DemoDataset => "demo-dataset",
    }
}

fn profile_name(profile: Profile) -> &'static str {
    match profile {
        Profile::Smoke => "smoke",
        Profile::Standard => "standard",
        Profile::Long => "long",
    }
}

fn object_backend_name(backend: ObjectBackendKind) -> &'static str {
    match backend {
        ObjectBackendKind::S3 => "s3",
        ObjectBackendKind::RustFs => "rustfs",
    }
}

fn metadata_raft_log_sync_name(sync: FileMetadataRaftLogSync) -> &'static str {
    match sync {
        FileMetadataRaftLogSync::Data => "data",
        FileMetadataRaftLogSync::None => "none",
    }
}

fn apply_rustfs_defaults(options: &mut S3ObjectStoreOptions) {
    if options.bucket.is_empty() {
        options.bucket = "nokv".to_owned();
    }
    if options.region.is_empty() || options.region == "us-east-1" {
        options.region = "auto".to_owned();
    }
    if options.endpoint.is_none() {
        options.endpoint = Some("http://127.0.0.1:9000".to_owned());
    }
    if options.access_key_id.is_none() {
        options.access_key_id = Some("rustfsadmin".to_owned());
    }
    if options.secret_access_key.is_none() {
        options.secret_access_key = Some("rustfsadmin".to_owned());
    }
}

fn default_root() -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    env::temp_dir().join(format!("nokv-fs-bench-{now}"))
}

fn csv_field(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn from_io(err: impl Error) -> BenchError {
    BenchError::Io(err.to_string())
}

fn from_client(err: impl Error) -> BenchError {
    BenchError::Client(err.to_string())
}

impl fmt::Display for BenchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingValue(option) => write!(f, "{option} requires a value"),
            Self::UnknownOption(option) => write!(f, "unknown option {option}"),
            Self::InvalidProfile(profile) => write!(f, "invalid profile {profile}"),
            Self::InvalidWorkload(workload) => write!(f, "invalid workload {workload}"),
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Client(err) => write!(f, "{err}"),
        }
    }
}

impl Error for BenchError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(value: &str) -> String {
        value.to_owned()
    }

    #[test]
    fn parse_defaults_to_smoke_all_with_temp_root() {
        let config = parse(Vec::new()).unwrap();
        assert_eq!(config.profile, Profile::Smoke);
        assert_eq!(config.workload, Workload::All);
        assert_eq!(config.object_backend, ObjectBackendKind::RustFs);
        assert_eq!(config.s3.bucket, "nokv");
        assert_eq!(config.s3.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(config.metadata_raft_log_sync, FileMetadataRaftLogSync::Data);
        assert!(!config.keep);
        assert!(config.root.to_string_lossy().contains("nokv-fs-bench"));
    }

    #[test]
    fn parse_profile_workload_root_and_keep() {
        let config = parse(vec![
            s("--profile"),
            s("standard"),
            s("--workload"),
            s("training-read"),
            s("--root"),
            s("/tmp/nokv-fs-bench"),
            s("--keep"),
        ])
        .unwrap();
        assert_eq!(config.profile, Profile::Standard);
        assert_eq!(config.workload, Workload::TrainingRead);
        assert_eq!(config.root, PathBuf::from("/tmp/nokv-fs-bench"));
        assert!(config.keep);
        assert_eq!(config.object_concurrency, 1);
        assert_eq!(config.read_repeats, 1);
        assert!(config.block_cache);
    }

    #[test]
    fn parse_rustfs_object_options() {
        let config = parse(vec![
            s("--object-backend"),
            s("rustfs"),
            s("--s3-bucket"),
            s("nokv"),
            s("--s3-endpoint"),
            s("http://127.0.0.1:9000"),
            s("--s3-access-key-id"),
            s("access"),
            s("--s3-secret-access-key"),
            s("secret"),
        ])
        .unwrap();
        assert_eq!(config.object_backend, ObjectBackendKind::RustFs);
        assert_eq!(config.s3.bucket, "nokv");
        assert_eq!(config.s3.region, "auto");
        assert_eq!(config.s3.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
    }

    #[test]
    fn parse_s3_does_not_inherit_rustfs_endpoint() {
        let config = parse(vec![
            s("--s3-bucket"),
            s("training-artifacts"),
            s("--object-backend"),
            s("s3"),
        ])
        .unwrap();
        assert_eq!(config.object_backend, ObjectBackendKind::S3);
        assert_eq!(config.s3.bucket, "training-artifacts");
        assert_eq!(config.s3.region, "us-east-1");
        assert_eq!(config.s3.endpoint, None);
    }

    #[test]
    fn parse_object_size_concurrency_and_cache_options() {
        let config = parse(vec![
            s("--object-concurrency"),
            s("8"),
            s("--checkpoint-bytes"),
            s("1048576"),
            s("--sample-bytes"),
            s("65536"),
            s("--read-repeats"),
            s("3"),
            s("--block-cache"),
            s("off"),
        ])
        .unwrap();
        assert_eq!(config.object_concurrency, 8);
        assert_eq!(config.checkpoint_bytes, Some(1_048_576));
        assert_eq!(config.sample_bytes, Some(65_536));
        assert_eq!(config.read_repeats, 3);
        assert!(!config.block_cache);
    }

    #[test]
    fn parse_metadata_raft_options() {
        let config = parse(vec![s("--metadata-raft-log-sync"), s("none")]).unwrap();
        assert_eq!(config.metadata_raft_log_sync, FileMetadataRaftLogSync::None);
    }

    #[test]
    fn parse_metadata_ha_smoke_workload() {
        let config = parse(vec![s("--workload"), s("metadata-ha-smoke")]).unwrap();
        assert_eq!(config.workload, Workload::MetadataHaSmoke);
    }

    #[test]
    fn parse_metadata_ha_fault_smoke_workload() {
        let config = parse(vec![s("--workload"), s("metadata-ha-fault-smoke")]).unwrap();
        assert_eq!(config.workload, Workload::MetadataHaFaultSmoke);
    }

    #[test]
    fn parse_metadata_negative_lookup_workload() {
        let config = parse(vec![s("--workload"), s("metadata-negative-lookup")]).unwrap();
        assert_eq!(config.workload, Workload::MetadataNegativeLookup);
    }

    #[test]
    fn parse_artifact_index_lookup_workload() {
        let config = parse(vec![s("--workload"), s("artifact-index-lookup")]).unwrap();
        assert_eq!(config.workload, Workload::ArtifactIndexLookup);
    }

    #[test]
    fn parse_metadata_concurrent_read_workload() {
        let config = parse(vec![s("--workload"), s("metadata-concurrent-read")]).unwrap();
        assert_eq!(config.workload, Workload::MetadataConcurrentRead);
    }

    #[test]
    fn shape_applies_object_size_overrides() {
        let config = parse(vec![
            s("--profile"),
            s("standard"),
            s("--checkpoint-bytes"),
            s("8192"),
            s("--sample-bytes"),
            s("4096"),
        ])
        .unwrap();
        let shape = shape(&config);
        assert_eq!(shape.checkpoint_bytes, 8192);
        assert_eq!(shape.dataset_file_bytes, 4096);
    }

    #[test]
    fn stats_json_parser_reads_metadata_fields() {
        let body = r#"{"object_puts":41,"object_put_bytes":42,"object_gets":43,"object_get_bytes":44,"cache_hits":45,"cache_hit_bytes":46,"manifest_chunks":47,"manifest_blocks":48,"metadata_store":{"get_total":2,"get_user_strong_total":32,"get_write_plan_local_total":33,"get_snapshot_total":34,"scan_total":3,"scan_user_strong_total":35,"scan_write_plan_local_total":36,"scan_snapshot_total":37,"scan_key_visited_total":4,"scan_key_returned_total":5,"history_lookup_total":40,"active_snapshot_pin_total":0,"commit_total":6,"dedupe_hit_total":7,"predicate_total":8,"prefix_empty_predicate_total":9,"current_put_total":10,"current_delete_total":11,"history_write_total":12,"watch_write_total":13,"dedupe_write_total":14,"commit_prepare_ns_total":15,"atomic_apply_total":16,"atomic_apply_command_total":17,"atomic_apply_max_batch":18,"atomic_apply_ns_total":19},"metadata_raft":{"enabled":true,"node_id":1,"current_term":20,"state":"Leader","current_leader":1,"last_log_index":21,"last_applied_index":22,"snapshot_index":23,"purged_index":24,"millis_since_quorum_ack":25,"voter_count":3,"learner_count":1},"metadata_service":{"path_index_lookup_total":26,"path_index_hit_total":27,"path_index_miss_total":28,"path_index_stale_total":29,"path_index_scan_stale_total":30,"path_index_fallback_total":31,"create_files_batch_total":32,"create_files_entry_total":33,"create_dirs_batch_total":34,"create_dirs_entry_total":35,"read_dir_plus_total":36,"read_dir_plus_entry_total":37,"read_dir_plus_projection_hit_total":38}}"#;

        assert_eq!(json_u64(body, "object_put_bytes").unwrap(), 42);
        assert_eq!(json_u64(body, "object_get_bytes").unwrap(), 44);
        assert_eq!(json_u64(body, "cache_hit_bytes").unwrap(), 46);
        assert_eq!(json_u64(body, "commit_total").unwrap(), 6);
        assert_eq!(json_u64(body, "get_write_plan_local_total").unwrap(), 33);
        assert_eq!(json_u64(body, "scan_write_plan_local_total").unwrap(), 36);
        assert_eq!(json_u64(body, "scan_key_visited_total").unwrap(), 4);
        assert_eq!(json_u64(body, "history_lookup_total").unwrap(), 40);
        assert_eq!(json_u64(body, "atomic_apply_total").unwrap(), 16);
        assert_eq!(json_u64(body, "atomic_apply_command_total").unwrap(), 17);
        assert_eq!(json_u64(body, "atomic_apply_max_batch").unwrap(), 18);
        assert_eq!(json_u64(body, "atomic_apply_ns_total").unwrap(), 19);
        assert_eq!(json_u64(body, "current_term").unwrap(), 20);
        assert_eq!(json_u64(body, "last_log_index").unwrap(), 21);
        assert_eq!(json_u64(body, "last_applied_index").unwrap(), 22);
        assert_eq!(json_u64(body, "millis_since_quorum_ack").unwrap(), 25);
        assert_eq!(json_u64(body, "voter_count").unwrap(), 3);
        assert_eq!(json_u64(body, "learner_count").unwrap(), 1);
        assert_eq!(json_u64(body, "path_index_hit_total").unwrap(), 27);
        assert_eq!(json_u64(body, "path_index_scan_stale_total").unwrap(), 30);
        assert_eq!(json_u64(body, "create_files_batch_total").unwrap(), 32);
        assert_eq!(json_u64(body, "create_dirs_entry_total").unwrap(), 35);
        assert_eq!(
            json_u64(body, "read_dir_plus_projection_hit_total").unwrap(),
            38
        );
        assert!(json_u64(body, "missing_total").is_err());
    }

    #[test]
    fn csv_row_reports_hot_path_attribution() {
        let row = row(RowInput {
            workload: "mdtest-easy",
            profile: Profile::Smoke,
            operations: 10,
            seconds: 0.5,
            bytes: 0,
            samples: 0,
            stats: BenchStats {
                object: ObjectTransferStats::default(),
                metadata_store: MetadataStoreStats {
                    commit_total: 4,
                    dedupe_hit_total: 1,
                    predicate_total: 8,
                    prefix_empty_predicate_total: 2,
                    history_lookup_total: 5,
                    current_put_total: 12,
                    atomic_apply_total: 2,
                    atomic_apply_command_total: 4,
                    atomic_apply_max_batch: 3,
                    ..MetadataStoreStats::default()
                },
                metadata_raft: MetadataRaftBenchStats::default(),
                metadata_service: MetadataServiceStats {
                    path_index_lookup_total: 4,
                    path_index_hit_total: 3,
                    path_index_miss_total: 1,
                    path_index_scan_stale_total: 2,
                    read_dir_plus_total: 2,
                    read_dir_plus_entry_total: 8,
                    read_dir_plus_projection_hit_total: 6,
                    ..MetadataServiceStats::default()
                },
            },
            object_concurrency: 1,
            read_repeats: 1,
            block_cache: true,
            checksum: 99,
            shape: "shape".to_owned(),
            caveat: "caveat".to_owned(),
        });

        assert_eq!(row.metadata_dedupe_hits, 1);
        assert_eq!(row.metadata_prefix_empty_predicates, 2);
        assert_eq!(row.metadata_history_lookups, 5);
        assert_eq!(row.metadata_atomic_applies, 2);
        assert_eq!(row.metadata_atomic_apply_commands, 4);
        assert_eq!(row.metadata_atomic_apply_max_batch, 3);
        assert_eq!(row.path_index_lookups, 4);
        assert_eq!(row.path_index_misses, 1);
        assert_eq!(row.path_index_scan_stale, 2);
        assert_eq!(row.path_index_hit_rate, 0.75);
        assert_eq!(row.read_dir_plus_calls, 2);
        assert_eq!(row.read_dir_plus_projection_hit_rate, 0.75);

        let header = csv_header();
        let record = csv_row(&row);
        assert!(header.contains("metadata_prefix_empty_predicates"));
        assert!(header.contains("metadata_history_lookups"));
        assert!(header.contains("metadata_atomic_apply_max_batch"));
        assert!(header.contains("metadata_raft_last_applied_index"));
        assert!(header.contains("metadata_raft_millis_since_quorum_ack"));
        assert!(header.contains("path_index_hit_rate"));
        assert!(header.contains("path_index_scan_stale"));
        assert!(header.contains("read_dir_plus_projection_hit_rate"));
        assert_eq!(header.split(',').count(), record.split(',').count());
    }

    #[test]
    fn stats_delta_clears_atomic_apply_max_when_window_has_no_apply() {
        let before = BenchStats {
            metadata_store: MetadataStoreStats {
                atomic_apply_total: 3,
                atomic_apply_command_total: 16,
                atomic_apply_max_batch: 8,
                ..MetadataStoreStats::default()
            },
            ..BenchStats::default()
        };
        let after = before;

        let delta = stats_delta(before, after);

        assert_eq!(delta.metadata_store.atomic_apply_total, 0);
        assert_eq!(delta.metadata_store.atomic_apply_command_total, 0);
        assert_eq!(delta.metadata_store.atomic_apply_max_batch, 0);
    }

    #[test]
    fn workload_all_expands_to_industry_and_training_paths() {
        assert_eq!(
            expand_workloads(Workload::All),
            vec![
                Workload::MetadataHaSmoke,
                Workload::MetadataHaFaultSmoke,
                Workload::MdtestEasy,
                Workload::MdtestHard,
                Workload::MetadataNegativeLookup,
                Workload::ArtifactIndexLookup,
                Workload::MetadataConcurrentRead,
                Workload::CheckpointPublish,
                Workload::TrainingRead,
                Workload::MlperfDlio,
                Workload::DemoDataset
            ]
        );
    }

    #[test]
    fn metadata_smoke_expands_to_metadata_only_paths() {
        assert_eq!(
            expand_workloads(Workload::MetadataSmoke),
            vec![
                Workload::MdtestEasy,
                Workload::MdtestHard,
                Workload::MetadataNegativeLookup,
                Workload::MetadataConcurrentRead
            ]
        );
    }

    #[test]
    fn rejects_removed_local_object_options() {
        assert!(matches!(
            parse(vec![s("--object-backend"), s("local")]),
            Err(BenchError::UnknownOption(_))
        ));
        assert!(matches!(
            parse(vec![s("--objects"), s("/tmp/objects")]),
            Err(BenchError::UnknownOption(_))
        ));
    }
}
