//! NoKV-FS local workload benchmark harness.
//!
//! This binary intentionally reports workload shape and durability caveats with
//! every result. The current target is local Holt metadata plus a configured
//! S3-compatible object store; it is a correctness and local-path baseline, not
//! a distributed cluster benchmark.

use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use nokvfs_client::{ArtifactMetadata, NoKvFsClient};
use nokvfs_meta::holtstore::HoltMetadataStore;
use nokvfs_meta::NoKvFs;
use nokvfs_object::{ObjectStoreConfig, S3ObjectStore, S3ObjectStoreOptions};
use nokvfs_types::MountId;

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
    MdtestEasy,
    MdtestHard,
    CheckpointPublish,
    TrainingRead,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Config {
    profile: Profile,
    workload: Workload,
    root: PathBuf,
    object_backend: ObjectBackendKind,
    s3: S3ObjectStoreOptions,
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
    checksum: u64,
    shape: String,
    caveat: String,
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

type Client = NoKvFsClient<HoltMetadataStore, S3ObjectStore>;

fn main() {
    if let Err(err) = run(env::args().skip(1).collect()) {
        eprintln!("error: {err}");
        eprintln!(
            "\nUsage: nokv-fs-bench [--profile smoke|standard|long] \
             [--workload all|mdtest-easy|mdtest-hard|checkpoint-publish|training-read] \
             [--root PATH] [--object-backend s3|rustfs] [--keep]"
        );
        std::process::exit(2);
    }
}

fn run(args: Vec<String>) -> Result<(), BenchError> {
    let config = parse(args)?;
    let shape = shape(config.profile);
    fs::create_dir_all(&config.root).map_err(from_io)?;

    println!("workload,profile,operations,seconds,ops_per_second,checksum,shape,caveat");
    for workload in expand_workloads(config.workload) {
        let row = run_one(&config, &shape, workload)?;
        println!(
            "{},{},{},{:.6},{:.2},{},{},{}",
            row.workload,
            profile_name(row.profile),
            row.operations,
            row.seconds,
            row.ops_per_second,
            row.checksum,
            csv_field(&row.shape),
            csv_field(&row.caveat)
        );
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
    let label = workload_name(workload);
    let client = client_for(config, label)?;
    client
        .bootstrap_root(DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
        .map_err(from_client)?;
    match workload {
        Workload::MdtestEasy => bench_mdtest_easy(&client, config, shape),
        Workload::MdtestHard => bench_mdtest_hard(&client, config, shape),
        Workload::CheckpointPublish => bench_checkpoint_publish(&client, config, shape),
        Workload::TrainingRead => bench_training_read(&client, config, shape),
        Workload::All => unreachable!("all expands before execution"),
    }
}

fn bench_mdtest_easy(
    client: &Client,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    let start = Instant::now();
    let mut checksum = 0_u64;
    for dir in 0..shape.dirs {
        let dir_path = format!("/mdtest-easy/dir-{dir:05}");
        if dir == 0 {
            client
                .mkdir("/mdtest-easy", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
                .map_err(from_client)?;
        }
        client
            .mkdir(&dir_path, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
            .map_err(from_client)?;
        checksum = checksum.wrapping_add(dir as u64);
        for file in 0..shape.files_per_dir {
            let path = format!("{dir_path}/file-{file:05}");
            let entry = client
                .create_file(&path, DEFAULT_MODE_FILE, DEFAULT_UID, DEFAULT_GID)
                .map_err(from_client)?;
            checksum = checksum.wrapping_add(entry.attr.inode.get());
        }
    }
    let operations = shape.dirs + shape.dirs * shape.files_per_dir + 1;
    Ok(row(
        "mdtest-easy",
        config.profile,
        operations,
        start.elapsed().as_secs_f64(),
        checksum,
        format!(
            "dirs={} files_per_dir={} file_body=metadata-only",
            shape.dirs, shape.files_per_dir
        ),
        metadata_only_caveat(config),
    ))
}

fn bench_mdtest_hard(
    client: &Client,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client
        .mkdir("/mdtest-hard", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
        .map_err(from_client)?;
    let start = Instant::now();
    let mut checksum = 0_u64;
    for file in 0..shape.shared_files {
        let path = format!("/mdtest-hard/file-{file:06}");
        let entry = client
            .create_file(&path, DEFAULT_MODE_FILE, DEFAULT_UID, DEFAULT_GID)
            .map_err(from_client)?;
        checksum = checksum.wrapping_add(entry.attr.inode.get());
    }
    Ok(row(
        "mdtest-hard",
        config.profile,
        shape.shared_files,
        start.elapsed().as_secs_f64(),
        checksum,
        format!(
            "shared_dir_files={} file_body=metadata-only",
            shape.shared_files
        ),
        metadata_only_caveat(config),
    ))
}

fn bench_checkpoint_publish(
    client: &Client,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client
        .mkdir("/checkpoints", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
        .map_err(from_client)?;
    let first = vec![1_u8; 4096];
    client
        .put_artifact(
            "/checkpoints/latest.ckpt",
            first,
            artifact_metadata("checkpoint", "checkpoints/latest-initial", 1),
        )
        .map_err(from_client)?;

    let start = Instant::now();
    let mut checksum = 0_u64;
    for step in 0..shape.checkpoints {
        let stage_path = format!("/checkpoints/.stage-{step:06}");
        let object_ref = format!("checkpoints/stage-{step:06}");
        let bytes = checkpoint_payload(step, 4096);
        let staged = client
            .put_artifact(
                &stage_path,
                bytes,
                artifact_metadata("checkpoint", &object_ref, step as u64 + 2),
            )
            .map_err(from_client)?;
        let result = client
            .rename_replace(&stage_path, "/checkpoints/latest.ckpt")
            .map_err(from_client)?;
        checksum = checksum
            .wrapping_add(staged.attr.inode.get())
            .wrapping_add(result.entry.attr.inode.get())
            .wrapping_add(
                result
                    .replaced
                    .map(|entry| entry.attr.inode.get())
                    .unwrap_or(0),
            );
    }
    Ok(row(
        "checkpoint-publish",
        config.profile,
        shape.checkpoints * 2,
        start.elapsed().as_secs_f64(),
        checksum,
        format!(
            "iterations={} payload_bytes=4096 ops=count_put_plus_atomic_replace",
            shape.checkpoints
        ),
        object_caveat(config, "object put plus metadata rename-replace"),
    ))
}

fn bench_training_read(
    client: &Client,
    config: &Config,
    shape: &WorkloadShape,
) -> Result<ResultRow, BenchError> {
    client
        .mkdir("/dataset", DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
        .map_err(from_client)?;
    let payload = vec![7_u8; shape.dataset_file_bytes];
    for shard in 0..shape.dataset_dirs {
        let shard_path = format!("/dataset/shard-{shard:04}");
        client
            .mkdir(&shard_path, DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
            .map_err(from_client)?;
        for file in 0..shape.dataset_files_per_dir {
            let path = format!("{shard_path}/sample-{file:05}.bin");
            let object_ref = format!("dataset/shard-{shard:04}/sample-{file:05}.bin");
            client
                .put_artifact(
                    &path,
                    payload.clone(),
                    artifact_metadata("dataset", &object_ref, 1),
                )
                .map_err(from_client)?;
        }
    }

    let start = Instant::now();
    let mut checksum = 0_u64;
    for shard in 0..shape.dataset_dirs {
        let shard_path = format!("/dataset/shard-{shard:04}");
        let entries = client.list(&shard_path).map_err(from_client)?;
        checksum = checksum.wrapping_add(entries.len() as u64);
        if let Some(first) = entries.first() {
            let name = String::from_utf8_lossy(first.dentry.name.as_bytes());
            let path = format!("{shard_path}/{name}");
            let bytes = client.cat(&path).map_err(from_client)?;
            checksum = checksum.wrapping_add(bytes.iter().map(|byte| *byte as u64).sum::<u64>());
        }
    }
    black_box(checksum);
    Ok(row(
        "training-read",
        config.profile,
        shape.dataset_dirs * 2,
        start.elapsed().as_secs_f64(),
        checksum,
        format!(
            "dataset_dirs={} files_per_dir={} sample_bytes={} timed_ops=list_plus_one_read_per_dir",
            shape.dataset_dirs, shape.dataset_files_per_dir, shape.dataset_file_bytes
        ),
        object_caveat(config, "warm object reads after deterministic dataset seed"),
    ))
}

fn row(
    workload: &'static str,
    profile: Profile,
    operations: usize,
    seconds: f64,
    checksum: u64,
    shape: String,
    caveat: String,
) -> ResultRow {
    ResultRow {
        workload,
        profile,
        operations,
        seconds,
        ops_per_second: operations as f64 / seconds.max(f64::MIN_POSITIVE),
        checksum,
        shape,
        caveat,
    }
}

fn metadata_only_caveat(config: &Config) -> String {
    format!(
        "metadata-only on local Holt, object_backend={}, no distributed replication",
        object_backend_name(config.object_backend)
    )
}

fn object_caveat(config: &Config, path: &str) -> String {
    match config.object_backend {
        ObjectBackendKind::RustFs => {
            format!("{path}, RustFS S3-compatible backend over configured endpoint")
        }
        ObjectBackendKind::S3 => {
            format!("{path}, generic S3-compatible backend over configured endpoint")
        }
    }
}

fn client_for(config: &Config, workload: &str) -> Result<Client, BenchError> {
    let meta = config.root.join(workload).join("meta");
    let objects = object_config_for(config, workload)
        .open()
        .map_err(from_client)?;
    let metadata = HoltMetadataStore::open_file(meta).map_err(from_client)?;
    let service = NoKvFs::new(
        MountId::new(1).expect("mount id is non-zero"),
        metadata,
        objects,
    );
    Ok(NoKvFsClient::new(service))
}

fn artifact_metadata(producer: &str, object_ref: &str, generation: u64) -> ArtifactMetadata {
    ArtifactMetadata {
        producer: producer.to_owned(),
        digest_uri: format!("sha256:{generation:016x}"),
        content_type: "application/octet-stream".to_owned(),
        object_ref: object_ref.to_owned(),
        generation,
        mode: DEFAULT_MODE_FILE,
        uid: DEFAULT_UID,
        gid: DEFAULT_GID,
    }
}

fn checkpoint_payload(seed: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|offset| ((seed + offset) % 251) as u8)
        .collect()
}

fn parse(args: Vec<String>) -> Result<Config, BenchError> {
    let mut profile = Profile::Smoke;
    let mut workload = Workload::All;
    let mut root = default_root();
    let mut object_backend = ObjectBackendKind::RustFs;
    let mut s3 = S3ObjectStoreOptions::new("");
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
        "mdtest-easy" => Ok(Workload::MdtestEasy),
        "mdtest-hard" => Ok(Workload::MdtestHard),
        "checkpoint-publish" => Ok(Workload::CheckpointPublish),
        "training-read" => Ok(Workload::TrainingRead),
        _ => Err(BenchError::InvalidWorkload(raw.to_owned())),
    }
}

fn expand_workloads(workload: Workload) -> Vec<Workload> {
    match workload {
        Workload::All => vec![
            Workload::MdtestEasy,
            Workload::MdtestHard,
            Workload::CheckpointPublish,
            Workload::TrainingRead,
        ],
        other => vec![other],
    }
}

fn shape(profile: Profile) -> WorkloadShape {
    match profile {
        Profile::Smoke => WorkloadShape {
            dirs: 8,
            files_per_dir: 64,
            shared_files: 512,
            checkpoints: 128,
            dataset_dirs: 8,
            dataset_files_per_dir: 64,
            dataset_file_bytes: 512,
        },
        Profile::Standard => WorkloadShape {
            dirs: 32,
            files_per_dir: 256,
            shared_files: 8192,
            checkpoints: 1024,
            dataset_dirs: 32,
            dataset_files_per_dir: 256,
            dataset_file_bytes: 1024,
        },
        Profile::Long => WorkloadShape {
            dirs: 64,
            files_per_dir: 1024,
            shared_files: 65536,
            checkpoints: 4096,
            dataset_dirs: 64,
            dataset_files_per_dir: 1024,
            dataset_file_bytes: 4096,
        },
    }
}

fn workload_name(workload: Workload) -> &'static str {
    match workload {
        Workload::All => "all",
        Workload::MdtestEasy => "mdtest-easy",
        Workload::MdtestHard => "mdtest-hard",
        Workload::CheckpointPublish => "checkpoint-publish",
        Workload::TrainingRead => "training-read",
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
    fn workload_all_expands_to_industry_and_training_paths() {
        assert_eq!(
            expand_workloads(Workload::All),
            vec![
                Workload::MdtestEasy,
                Workload::MdtestHard,
                Workload::CheckpointPublish,
                Workload::TrainingRead
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
