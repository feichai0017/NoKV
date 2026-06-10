//! Minimal NoKV command line interface.

use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::time::Duration;

use nokv_client::{ArtifactMetadata, MetadataClient, MetadataClientOptions, NoKvFsClient};
use nokv_meta::{HistoryGcOptions, ObjectGcOptions, SubtreeDeltaKind};
use nokv_object::{
    BlockCachePolicy, ConfiguredObjectStore, DiskBlockCacheOptions, FileReadPipelineOptions,
    HotFillMode, LocalObjectStoreOptions, MemoryBlockCacheOptions, ObjectKey, ObjectStoreConfig,
    S3ObjectStoreOptions, TieredObjectStoreOptions,
};
use nokv_server::{ServerOptions, DEFAULT_METADATA_CHECKPOINT_ARCHIVE_PREFIX, DEFAULT_SERVER_BIND};
use nokv_types::{FileType, MountId};
use sha2::{Digest, Sha256};

const DEFAULT_MODE_DIR: u32 = 0o755;
const DEFAULT_MODE_FILE: u32 = 0o644;
const DEFAULT_UID: u32 = 1000;
const DEFAULT_GID: u32 = 1000;
const DEFAULT_GC_LIMIT: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Config {
    meta: PathBuf,
    metadata_checkpoint_archive_prefix: Option<String>,
    object: ObjectStoreConfig,
    mount: MountId,
    uid: u32,
    gid: u32,
    object_gc_interval: Duration,
    object_gc_limit: usize,
    history_gc_interval: Duration,
    history_gc_limit: usize,
    server_bind: SocketAddr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObjectBackendKind {
    S3,
    RustFs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Command {
    Init,
    Mkdir {
        path: String,
    },
    PutArtifact {
        path: String,
        source: PathBuf,
    },
    Ls {
        path: String,
    },
    Cat {
        path: String,
    },
    Rm {
        path: String,
    },
    Rmdir {
        path: String,
    },
    Rename {
        source: String,
        destination: String,
    },
    RenameReplace {
        source: String,
        destination: String,
    },
    Mount {
        mountpoint: PathBuf,
        options: MountCliOptions,
    },
    MountSnapshot {
        snapshot_id: u64,
        mountpoint: PathBuf,
        options: MountCliOptions,
    },
    Serve,
    Gc {
        limit: usize,
    },
    Backup,
    Restore,
    Fsck,
    Snapshot {
        path: String,
    },
    Clone {
        src: String,
        dst: String,
    },
    Diff {
        a: String,
        b: String,
    },
    Rollback {
        path: String,
        snapshot_id: u64,
    },
    CatSnapshot {
        snapshot_id: u64,
        path: String,
    },
    RetireSnapshot {
        snapshot_id: u64,
    },
    RenewSnapshot {
        snapshot_id: u64,
        lease_ms: u64,
    },
    Help,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MountCliOptions {
    read_only: bool,
    threads: usize,
    kernel_cache: bool,
    direct_io: bool,
    stats_bind: Option<SocketAddr>,
    entry_ttl: Duration,
    attr_ttl: Duration,
    block_cache: BlockCachePolicy,
    prefetch: nokv_fuse::FusePrefetchOptions,
    read_pipeline: FileReadPipelineOptions,
    writeback: nokv_fuse::FuseWritebackOptions,
}

#[derive(Debug)]
enum CliError {
    MissingValue(&'static str),
    UnknownOption(String),
    UnknownCommand(String),
    MissingArgument(&'static str),
    TooManyArguments,
    InvalidMount(String),
    InvalidAddress { field: &'static str, value: String },
    InvalidNumber { field: &'static str, value: String },
    Io(String),
    Client(String),
}

impl Default for MountCliOptions {
    fn default() -> Self {
        let defaults = nokv_fuse::FuseOptions::default();
        Self {
            read_only: false,
            threads: defaults.threads,
            kernel_cache: defaults.kernel_cache,
            direct_io: defaults.direct_io,
            stats_bind: defaults.stats_bind,
            entry_ttl: defaults.entry_ttl,
            attr_ttl: defaults.attr_ttl,
            block_cache: defaults.block_cache,
            prefetch: defaults.prefetch,
            read_pipeline: defaults.read_pipeline,
            writeback: defaults.writeback,
        }
    }
}

impl MountCliOptions {
    fn fuse_options(&self, access: nokv_fuse::FuseAccessMode) -> nokv_fuse::FuseOptions {
        nokv_fuse::FuseOptions {
            access,
            entry_ttl: self.entry_ttl,
            attr_ttl: self.attr_ttl,
            threads: self.threads,
            kernel_cache: self.kernel_cache,
            direct_io: self.direct_io,
            stats_bind: self.stats_bind,
            block_cache: self.block_cache.clone(),
            prefetch: self.prefetch.clone(),
            read_pipeline: self.read_pipeline,
            writeback: self.writeback.clone(),
            ..nokv_fuse::FuseOptions::default()
        }
    }
}

type Client = NoKvFsClient<ConfiguredObjectStore>;

fn main() {
    if let Err(err) = run(env::args().skip(1).collect()) {
        eprintln!("error: {err}");
        eprintln!();
        print_help(&mut io::stderr()).ok();
        std::process::exit(2);
    }
}

fn run(args: Vec<String>) -> Result<(), CliError> {
    let (config, command) = parse(args)?;
    match command {
        Command::Init => {
            let client = open_client(&config)?;
            client
                .metadata()
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            println!("initialized mount {}", config.mount.get());
        }
        Command::Mkdir { path } => {
            let client = open_client(&config)?;
            client
                .metadata()
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            let entry = client
                .metadata()
                .mkdir(&path, DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            println!("dir {} inode={}", path, entry.attr.inode.get());
        }
        Command::PutArtifact { path, source } => {
            let mut digest_source = fs::File::open(&source).map_err(from_io)?;
            let digest_uri = artifact_digest_reader(&mut digest_source)?;
            let source_reader = fs::File::open(&source).map_err(from_io)?;
            let client = open_client(&config)?;
            client
                .metadata()
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            let manifest_id = default_manifest_id(&path)?;
            let entry = client
                .put_artifact_from_reader(
                    &path,
                    source_reader,
                    ArtifactMetadata {
                        producer: "nokv".to_owned(),
                        digest_uri,
                        content_type: "application/octet-stream".to_owned(),
                        manifest_id,
                        mode: DEFAULT_MODE_FILE,
                        uid: config.uid,
                        gid: config.gid,
                    },
                )
                .map_err(from_client)?;
            println!(
                "artifact {} inode={} size={}",
                path,
                entry.attr.inode.get(),
                entry.attr.size
            );
        }
        Command::Ls { path } => {
            let client = open_client(&config)?;
            client
                .metadata()
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            for entry in client.metadata().list(&path).map_err(from_client)? {
                println!(
                    "{}\t{}\t{}\t{}",
                    file_type_label(entry.attr.file_type),
                    entry.attr.inode.get(),
                    entry.attr.size,
                    String::from_utf8_lossy(entry.dentry.name.as_bytes())
                );
            }
        }
        Command::Cat { path } => {
            let client = open_client(&config)?;
            let bytes = client.cat(&path).map_err(from_client)?;
            io::stdout().write_all(&bytes).map_err(from_io)?;
        }
        Command::Rm { path } => {
            let client = open_client(&config)?;
            let removed = client.metadata().remove(&path).map_err(from_client)?;
            println!(
                "removed file {} inode={} body={}",
                path,
                removed.attr.inode.get(),
                removed
                    .body
                    .as_ref()
                    .map(|body| body.manifest_id.as_str())
                    .unwrap_or("-")
            );
        }
        Command::Rmdir { path } => {
            let client = open_client(&config)?;
            let removed = client.metadata().rmdir(&path).map_err(from_client)?;
            println!("removed dir {} inode={}", path, removed.attr.inode.get());
        }
        Command::Rename {
            source,
            destination,
        } => {
            let client = open_client(&config)?;
            let renamed = client
                .metadata()
                .rename(&source, &destination)
                .map_err(from_client)?;
            println!(
                "renamed {} -> {} inode={}",
                source,
                destination,
                renamed.attr.inode.get()
            );
        }
        Command::RenameReplace {
            source,
            destination,
        } => {
            let client = open_client(&config)?;
            let result = client
                .metadata()
                .rename_replace(&source, &destination)
                .map_err(from_client)?;
            println!(
                "renamed {} -> {} inode={} replaced_body={}",
                source,
                destination,
                result.entry.attr.inode.get(),
                result
                    .replaced
                    .as_ref()
                    .and_then(|entry| entry.body.as_ref())
                    .map(|body| body.manifest_id.as_str())
                    .unwrap_or("-")
            );
        }
        Command::Mount {
            mountpoint,
            options,
        } => {
            let metadata = MetadataClient::new(MetadataClientOptions::new(config.server_bind));
            let objects = config.object.open().map_err(from_object)?;
            metadata
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            let mount = nokv_fuse::spawn_mount_client(
                metadata,
                objects,
                mountpoint,
                options.fuse_options(if options.read_only {
                    nokv_fuse::FuseAccessMode::ReadOnly
                } else {
                    nokv_fuse::FuseAccessMode::ReadWrite
                }),
            )
            .map_err(from_io)?;
            mount.join().map_err(from_io)?;
        }
        Command::MountSnapshot {
            snapshot_id,
            mountpoint,
            options,
        } => {
            let metadata = MetadataClient::new(MetadataClientOptions::new(config.server_bind));
            let objects = config.object.open().map_err(from_object)?;
            let snapshot = metadata
                .snapshot_pin(snapshot_id)
                .map_err(from_client)?
                .ok_or_else(|| CliError::Client(format!("snapshot {snapshot_id} not found")))?;
            let mount = nokv_fuse::spawn_mount_client(
                metadata,
                objects,
                mountpoint,
                nokv_fuse::FuseOptions {
                    fs_name: format!("nokv-snapshot-{snapshot_id}"),
                    view: nokv_fuse::FuseView::Snapshot {
                        snapshot_id,
                        root: snapshot.root,
                    },
                    ..options.fuse_options(nokv_fuse::FuseAccessMode::ReadOnly)
                },
            )
            .map_err(from_io)?;
            mount.join().map_err(from_io)?;
        }
        Command::Serve => {
            nokv_server::run(ServerOptions {
                bind: config.server_bind,
                mount: config.mount,
                meta_path: config.meta,
                metadata_checkpoint_archive_prefix: config.metadata_checkpoint_archive_prefix,
                object: config.object,
                uid: config.uid,
                gid: config.gid,
                object_gc: ObjectGcOptions {
                    interval: config.object_gc_interval,
                    limit: config.object_gc_limit,
                    run_immediately: false,
                    read_lease_grace: ObjectGcOptions::default().read_lease_grace,
                },
                history_gc: HistoryGcOptions {
                    interval: config.history_gc_interval,
                    limit: config.history_gc_limit,
                    run_immediately: false,
                },
            })
            .map_err(from_io)?;
        }
        Command::Gc { limit } => {
            let body = control_get(&config, &format!("/gc?limit={limit}"))?;
            print!("{body}");
        }
        Command::Backup => {
            let body = control_get(&config, "/backup")?;
            print!("{body}");
        }
        Command::Restore => {
            let report = nokv_server::restore(ServerOptions {
                bind: config.server_bind,
                mount: config.mount,
                meta_path: config.meta,
                metadata_checkpoint_archive_prefix: config.metadata_checkpoint_archive_prefix,
                object: config.object,
                uid: config.uid,
                gid: config.gid,
                object_gc: ObjectGcOptions {
                    interval: config.object_gc_interval,
                    limit: config.object_gc_limit,
                    run_immediately: false,
                    read_lease_grace: ObjectGcOptions::default().read_lease_grace,
                },
                history_gc: HistoryGcOptions {
                    interval: config.history_gc_interval,
                    limit: config.history_gc_limit,
                    run_immediately: false,
                },
            })
            .map_err(from_io)?;
            print!("{report}");
        }
        Command::Fsck => {
            let body = control_get(&config, "/fsck")?;
            print!("{body}");
        }
        Command::Snapshot { path } => {
            let client = open_client(&config)?;
            let snapshot = client
                .metadata()
                .snapshot_subtree_path(&path)
                .map_err(from_client)?;
            println!(
                "snapshot {} id={} version={}",
                path, snapshot.snapshot_id, snapshot.read_version
            );
        }
        Command::Clone { src, dst } => {
            let client = open_client(&config)?;
            let outcome = client
                .metadata()
                .clone_subtree_path(&src, &dst)
                .map_err(from_client)?;
            println!(
                "cloned {} -> {} root={} snapshot={}",
                src,
                dst,
                outcome.root.get(),
                outcome.snapshot_id
            );
        }
        Command::Diff { a, b } => {
            let client = open_client(&config)?;
            let deltas = client
                .metadata()
                .diff_subtrees(&a, &b)
                .map_err(from_client)?;
            for delta in deltas {
                println!(
                    "{}\t{}\t{:+}\t{}",
                    subtree_delta_label(delta.kind),
                    delta.path,
                    delta.size_delta,
                    delta.digest.as_deref().unwrap_or("-")
                );
            }
        }
        Command::Rollback { path, snapshot_id } => {
            let client = open_client(&config)?;
            client
                .metadata()
                .rollback_subtree_path(&path, snapshot_id)
                .map_err(from_client)?;
            println!("rolled back {} to snapshot {}", path, snapshot_id);
        }
        Command::CatSnapshot { snapshot_id, path } => {
            let client = open_client(&config)?;
            let bytes = client
                .cat_snapshot(snapshot_id, &path)
                .map_err(from_client)?;
            io::stdout().write_all(&bytes).map_err(from_io)?;
        }
        Command::RetireSnapshot { snapshot_id } => {
            let client = open_client(&config)?;
            let retired = client
                .metadata()
                .retire_snapshot(snapshot_id)
                .map_err(from_client)?;
            println!("retired_snapshot id={} retired={}", snapshot_id, retired);
        }
        Command::RenewSnapshot {
            snapshot_id,
            lease_ms,
        } => {
            let client = open_client(&config)?;
            let renewed = client
                .metadata()
                .renew_snapshot(snapshot_id, lease_ms)
                .map_err(from_client)?;
            println!(
                "renewed_snapshot id={} renewed={} lease_ms={}",
                snapshot_id, renewed, lease_ms
            );
        }
        Command::Help => {
            print_help(&mut io::stdout()).map_err(from_io)?;
        }
    }
    Ok(())
}

fn open_client(config: &Config) -> Result<Client, CliError> {
    let objects = config.object.open().map_err(from_object)?;
    let metadata = MetadataClient::new(MetadataClientOptions::new(config.server_bind));
    Ok(NoKvFsClient::new(metadata, objects))
}

fn control_get(config: &Config, path: &str) -> Result<String, CliError> {
    let mut stream = TcpStream::connect(config.server_bind).map_err(from_io)?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\n\r\n",
        config.server_bind
    )
    .map_err(from_io)?;
    let mut response = String::new();
    stream.read_to_string(&mut response).map_err(from_io)?;
    let Some((headers, body)) = response.split_once("\r\n\r\n") else {
        return Err(CliError::Client("malformed control response".to_owned()));
    };
    if !headers.starts_with("HTTP/1.1 200 ") {
        return Err(CliError::Client(response));
    }
    Ok(body.to_owned())
}

fn parse(args: Vec<String>) -> Result<(Config, Command), CliError> {
    let mut meta = PathBuf::from(".nokv/meta");
    let mut metadata_checkpoint_archive_prefix =
        Some(DEFAULT_METADATA_CHECKPOINT_ARCHIVE_PREFIX.to_owned());
    let mut object_backend = ObjectBackendKind::RustFs;
    let mut s3 = S3ObjectStoreOptions::new("");
    let mut hot_object_root: Option<PathBuf> = None;
    let mut hot_object_max_bytes: Option<u64> = None;
    let mut hot_fill_mode = HotFillMode::Inline;
    let mut mount = MountId::new(1).expect("default mount id is non-zero");
    let mut uid = DEFAULT_UID;
    let mut gid = DEFAULT_GID;
    let mut object_gc_interval = ObjectGcOptions::default().interval;
    let mut object_gc_limit = ObjectGcOptions::default().limit;
    let mut history_gc_interval = HistoryGcOptions::default().interval;
    let mut history_gc_limit = HistoryGcOptions::default().limit;
    let mut server_bind = DEFAULT_SERVER_BIND;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--meta" => {
                index += 1;
                meta = PathBuf::from(value(&args, index, "--meta")?);
            }
            "--metadata-checkpoint-archive-prefix" => {
                index += 1;
                metadata_checkpoint_archive_prefix = Some(parse_archive_prefix(
                    value(&args, index, "--metadata-checkpoint-archive-prefix")?,
                    "--metadata-checkpoint-archive-prefix",
                )?);
            }
            "--no-metadata-checkpoint-archive" => {
                metadata_checkpoint_archive_prefix = None;
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
            "--hot-object-root" => {
                index += 1;
                hot_object_root = Some(PathBuf::from(value(&args, index, "--hot-object-root")?));
            }
            "--hot-object-max-bytes" => {
                index += 1;
                let max_bytes = parse_u64(
                    value(&args, index, "--hot-object-max-bytes")?,
                    "hot_object_max_bytes",
                )?;
                if max_bytes == 0 {
                    return Err(CliError::InvalidNumber {
                        field: "hot_object_max_bytes",
                        value: max_bytes.to_string(),
                    });
                }
                hot_object_max_bytes = Some(max_bytes);
            }
            "--hot-fill-mode" => {
                index += 1;
                hot_fill_mode = parse_hot_fill_mode(value(&args, index, "--hot-fill-mode")?)?;
            }
            "--mount" => {
                index += 1;
                let raw = value(&args, index, "--mount")?;
                let parsed = raw
                    .parse::<u64>()
                    .map_err(|_| CliError::InvalidMount(raw.to_owned()))?;
                mount = MountId::new(parsed).map_err(|_| CliError::InvalidMount(raw.to_owned()))?;
            }
            "--uid" => {
                index += 1;
                uid = parse_u32(value(&args, index, "--uid")?, "uid")?;
            }
            "--gid" => {
                index += 1;
                gid = parse_u32(value(&args, index, "--gid")?, "gid")?;
            }
            "--object-gc-interval-ms" => {
                index += 1;
                let interval_ms = parse_u64(
                    value(&args, index, "--object-gc-interval-ms")?,
                    "object_gc_interval_ms",
                )?;
                if interval_ms == 0 {
                    return Err(CliError::InvalidNumber {
                        field: "object_gc_interval_ms",
                        value: interval_ms.to_string(),
                    });
                }
                object_gc_interval = Duration::from_millis(interval_ms);
            }
            "--object-gc-limit" => {
                index += 1;
                object_gc_limit =
                    parse_usize(value(&args, index, "--object-gc-limit")?, "object_gc_limit")?;
            }
            "--history-gc-interval-ms" => {
                index += 1;
                let interval_ms = parse_u64(
                    value(&args, index, "--history-gc-interval-ms")?,
                    "history_gc_interval_ms",
                )?;
                if interval_ms == 0 {
                    return Err(CliError::InvalidNumber {
                        field: "history_gc_interval_ms",
                        value: interval_ms.to_string(),
                    });
                }
                history_gc_interval = Duration::from_millis(interval_ms);
            }
            "--history-gc-limit" => {
                index += 1;
                history_gc_limit = parse_usize(
                    value(&args, index, "--history-gc-limit")?,
                    "history_gc_limit",
                )?;
            }
            "--server-bind" => {
                index += 1;
                server_bind =
                    parse_socket_addr(value(&args, index, "--server-bind")?, "server_bind")?;
            }
            "--help" | "-h" => {
                return Ok((
                    Config {
                        meta,
                        metadata_checkpoint_archive_prefix,
                        object: object_config(
                            object_backend,
                            s3,
                            hot_object_root,
                            hot_object_max_bytes,
                            hot_fill_mode,
                        ),
                        mount,
                        uid,
                        gid,
                        object_gc_interval,
                        object_gc_limit,
                        history_gc_interval,
                        history_gc_limit,
                        server_bind,
                    },
                    Command::Help,
                ));
            }
            option if option.starts_with('-') => {
                return Err(CliError::UnknownOption(option.to_owned()));
            }
            _ => break,
        }
        index += 1;
    }

    let command = parse_command(&args[index..])?;
    Ok((
        Config {
            meta,
            metadata_checkpoint_archive_prefix,
            object: object_config(
                object_backend,
                s3,
                hot_object_root,
                hot_object_max_bytes,
                hot_fill_mode,
            ),
            mount,
            uid,
            gid,
            object_gc_interval,
            object_gc_limit,
            history_gc_interval,
            history_gc_limit,
            server_bind,
        },
        command,
    ))
}

fn parse_object_backend(raw: &str) -> Result<ObjectBackendKind, CliError> {
    match raw {
        "s3" => Ok(ObjectBackendKind::S3),
        "rustfs" => Ok(ObjectBackendKind::RustFs),
        _ => Err(CliError::UnknownOption(format!("--object-backend {raw}"))),
    }
}

fn parse_hot_fill_mode(raw: &str) -> Result<HotFillMode, CliError> {
    match raw {
        "inline" => Ok(HotFillMode::Inline),
        "background" => Ok(HotFillMode::Background),
        _ => Err(CliError::UnknownOption(format!("--hot-fill-mode {raw}"))),
    }
}

fn parse_archive_prefix(raw: &str, option: &str) -> Result<String, CliError> {
    let prefix = raw.trim_matches('/');
    ObjectKey::new(prefix).map_err(|_| CliError::UnknownOption(format!("{option} {raw}")))?;
    Ok(prefix.to_owned())
}

fn object_config(
    backend: ObjectBackendKind,
    mut s3: S3ObjectStoreOptions,
    hot_object_root: Option<PathBuf>,
    hot_object_max_bytes: Option<u64>,
    hot_fill_mode: HotFillMode,
) -> ObjectStoreConfig {
    let cold = match backend {
        ObjectBackendKind::S3 => s3,
        ObjectBackendKind::RustFs => {
            apply_rustfs_defaults(&mut s3);
            s3
        }
    };
    match hot_object_root {
        Some(root) => {
            let hot = match hot_object_max_bytes {
                Some(max_bytes) => LocalObjectStoreOptions::new(root).with_max_bytes(max_bytes),
                None => LocalObjectStoreOptions::new(root),
            };
            ObjectStoreConfig::tiered_local_with_options(
                hot,
                cold,
                TieredObjectStoreOptions {
                    hot_fill_mode,
                    ..TieredObjectStoreOptions::default()
                },
            )
        }
        None => ObjectStoreConfig::s3(cold),
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

fn parse_command(args: &[String]) -> Result<Command, CliError> {
    let Some(command) = args.first().map(String::as_str) else {
        return Ok(Command::Help);
    };
    match command {
        "init" => exact_args(args, 1).map(|()| Command::Init),
        "mkdir" => exact_args(args, 2).map(|()| Command::Mkdir {
            path: args[1].clone(),
        }),
        "put-artifact" => exact_args(args, 3).map(|()| Command::PutArtifact {
            path: args[1].clone(),
            source: PathBuf::from(&args[2]),
        }),
        "ls" => exact_args(args, 2).map(|()| Command::Ls {
            path: args[1].clone(),
        }),
        "cat" => exact_args(args, 2).map(|()| Command::Cat {
            path: args[1].clone(),
        }),
        "rm" => exact_args(args, 2).map(|()| Command::Rm {
            path: args[1].clone(),
        }),
        "rmdir" => exact_args(args, 2).map(|()| Command::Rmdir {
            path: args[1].clone(),
        }),
        "rename" => exact_args(args, 3).map(|()| Command::Rename {
            source: args[1].clone(),
            destination: args[2].clone(),
        }),
        "rename-replace" => exact_args(args, 3).map(|()| Command::RenameReplace {
            source: args[1].clone(),
            destination: args[2].clone(),
        }),
        "mount" => {
            let (options, mountpoint) = parse_mount_args(args, 1, true)?;
            Ok(Command::Mount {
                mountpoint,
                options,
            })
        }
        "mount-snapshot" => {
            if args.len() < 3 {
                return Err(CliError::MissingArgument("snapshot id and mountpoint"));
            }
            let (options, mountpoint) = parse_mount_args(args, 2, false)?;
            Ok(Command::MountSnapshot {
                snapshot_id: parse_u64(&args[1], "snapshot_id")?,
                mountpoint,
                options,
            })
        }
        "serve" => exact_args(args, 1).map(|()| Command::Serve),
        "backup" => exact_args(args, 1).map(|()| Command::Backup),
        "restore" => exact_args(args, 1).map(|()| Command::Restore),
        "fsck" => exact_args(args, 1).map(|()| Command::Fsck),
        "gc" => match args.len() {
            1 => Ok(Command::Gc {
                limit: DEFAULT_GC_LIMIT,
            }),
            2 => Ok(Command::Gc {
                limit: parse_usize(&args[1], "limit")?,
            }),
            _ => Err(CliError::TooManyArguments),
        },
        "snapshot" => exact_args(args, 2).map(|()| Command::Snapshot {
            path: args[1].clone(),
        }),
        "clone" => exact_args(args, 3).map(|()| Command::Clone {
            src: args[1].clone(),
            dst: args[2].clone(),
        }),
        "diff" => exact_args(args, 3).map(|()| Command::Diff {
            a: args[1].clone(),
            b: args[2].clone(),
        }),
        "rollback" => {
            exact_args(args, 3)?;
            Ok(Command::Rollback {
                path: args[1].clone(),
                snapshot_id: parse_u64(&args[2], "snapshot_id")?,
            })
        }
        "cat-snapshot" => {
            exact_args(args, 3)?;
            Ok(Command::CatSnapshot {
                snapshot_id: parse_u64(&args[1], "snapshot_id")?,
                path: args[2].clone(),
            })
        }
        "retire-snapshot" => {
            exact_args(args, 2)?;
            Ok(Command::RetireSnapshot {
                snapshot_id: parse_u64(&args[1], "snapshot_id")?,
            })
        }
        "renew-snapshot" => match args.len() {
            2 => Ok(Command::RenewSnapshot {
                snapshot_id: parse_u64(&args[1], "snapshot_id")?,
                lease_ms: nokv_meta::DEFAULT_SNAPSHOT_LEASE_MS,
            }),
            3 => Ok(Command::RenewSnapshot {
                snapshot_id: parse_u64(&args[1], "snapshot_id")?,
                lease_ms: parse_u64(&args[2], "lease_ms")?,
            }),
            _ => Err(CliError::TooManyArguments),
        },
        "help" => Ok(Command::Help),
        other => Err(CliError::UnknownCommand(other.to_owned())),
    }
}

fn default_disk_block_cache_options() -> DiskBlockCacheOptions {
    DiskBlockCacheOptions {
        root: std::env::temp_dir().join(format!("nokv-block-cache-{}", std::process::id())),
        max_bytes: 8 * 1024 * 1024 * 1024,
        max_items: 16 * 1024,
        ttl: None,
    }
}

fn set_block_cache_kind(options: &mut MountCliOptions, raw: &str) -> Result<(), CliError> {
    options.block_cache = match raw {
        "off" => BlockCachePolicy::Off,
        "memory" => BlockCachePolicy::Memory(MemoryBlockCacheOptions::default()),
        "disk" => BlockCachePolicy::Disk(default_disk_block_cache_options()),
        other => {
            return Err(CliError::InvalidNumber {
                field: "block_cache",
                value: other.to_owned(),
            })
        }
    };
    Ok(())
}

fn set_disk_block_cache_root(options: &mut MountCliOptions, root: PathBuf) {
    if !matches!(options.block_cache, BlockCachePolicy::Disk(_)) {
        options.block_cache = BlockCachePolicy::Disk(default_disk_block_cache_options());
    }
    if let BlockCachePolicy::Disk(cache) = &mut options.block_cache {
        cache.root = root;
    }
}

fn set_block_cache_bytes(options: &mut MountCliOptions, max_bytes: u64) {
    match &mut options.block_cache {
        BlockCachePolicy::Memory(cache) => cache.max_bytes = max_bytes,
        BlockCachePolicy::Disk(cache) => cache.max_bytes = max_bytes,
        BlockCachePolicy::Off => {
            options.block_cache = BlockCachePolicy::Memory(MemoryBlockCacheOptions {
                max_bytes,
                ..MemoryBlockCacheOptions::default()
            });
        }
    }
}

fn set_block_cache_items(options: &mut MountCliOptions, max_items: usize) {
    match &mut options.block_cache {
        BlockCachePolicy::Memory(cache) => cache.max_items = max_items,
        BlockCachePolicy::Disk(cache) => cache.max_items = max_items,
        BlockCachePolicy::Off => {
            options.block_cache = BlockCachePolicy::Memory(MemoryBlockCacheOptions {
                max_items,
                ..MemoryBlockCacheOptions::default()
            });
        }
    }
}

fn set_block_cache_ttl(options: &mut MountCliOptions, ttl: Option<Duration>) {
    match &mut options.block_cache {
        BlockCachePolicy::Memory(cache) => cache.ttl = ttl,
        BlockCachePolicy::Disk(cache) => cache.ttl = ttl,
        BlockCachePolicy::Off => {
            options.block_cache = BlockCachePolicy::Memory(MemoryBlockCacheOptions {
                ttl,
                ..MemoryBlockCacheOptions::default()
            });
        }
    }
}

fn parse_mount_args(
    args: &[String],
    start_index: usize,
    allow_read_only: bool,
) -> Result<(MountCliOptions, PathBuf), CliError> {
    let mut options = MountCliOptions::default();
    let mut mountpoint = None;
    let mut index = start_index;
    while index < args.len() {
        match args[index].as_str() {
            "--read-only" if allow_read_only => {
                options.read_only = true;
                index += 1;
            }
            "--read-only" => return Err(CliError::UnknownOption(args[index].clone())),
            "--no-kernel-cache" => {
                options.kernel_cache = false;
                index += 1;
            }
            "--direct-io" => {
                options.direct_io = true;
                index += 1;
            }
            "--stats-bind" => {
                index += 1;
                options.stats_bind = Some(parse_socket_addr(
                    value(args, index, "--stats-bind")?,
                    "stats_bind",
                )?);
                index += 1;
            }
            "--fuse-threads" => {
                index += 1;
                let threads = parse_usize(value(args, index, "--fuse-threads")?, "fuse_threads")?;
                if threads == 0 {
                    return Err(CliError::InvalidNumber {
                        field: "fuse_threads",
                        value: "0".to_owned(),
                    });
                }
                options.threads = threads;
                index += 1;
            }
            "--entry-ttl-ms" => {
                index += 1;
                options.entry_ttl = Duration::from_millis(parse_u64(
                    value(args, index, "--entry-ttl-ms")?,
                    "entry_ttl_ms",
                )?);
                index += 1;
            }
            "--attr-ttl-ms" => {
                index += 1;
                options.attr_ttl = Duration::from_millis(parse_u64(
                    value(args, index, "--attr-ttl-ms")?,
                    "attr_ttl_ms",
                )?);
                index += 1;
            }
            "--no-block-cache" => {
                options.block_cache = BlockCachePolicy::Off;
                index += 1;
            }
            "--block-cache" => {
                index += 1;
                set_block_cache_kind(&mut options, value(args, index, "--block-cache")?)?;
                index += 1;
            }
            "--block-cache-dir" => {
                index += 1;
                set_disk_block_cache_root(
                    &mut options,
                    PathBuf::from(value(args, index, "--block-cache-dir")?),
                );
                index += 1;
            }
            "--block-cache-bytes" => {
                index += 1;
                let max_bytes = parse_u64(
                    value(args, index, "--block-cache-bytes")?,
                    "block_cache_bytes",
                )?;
                set_block_cache_bytes(&mut options, max_bytes);
                index += 1;
            }
            "--block-cache-items" => {
                index += 1;
                let max_items = parse_usize(
                    value(args, index, "--block-cache-items")?,
                    "block_cache_items",
                )?;
                set_block_cache_items(&mut options, max_items);
                index += 1;
            }
            "--block-cache-ttl-ms" => {
                index += 1;
                let ttl_ms = parse_u64(
                    value(args, index, "--block-cache-ttl-ms")?,
                    "block_cache_ttl_ms",
                )?;
                let ttl = (ttl_ms > 0).then(|| Duration::from_millis(ttl_ms));
                set_block_cache_ttl(&mut options, ttl);
                index += 1;
            }
            "--no-prefetch" => {
                options.prefetch.enabled = false;
                index += 1;
            }
            "--prefetch-workers" => {
                index += 1;
                options.prefetch.workers = parse_usize(
                    value(args, index, "--prefetch-workers")?,
                    "prefetch_workers",
                )?;
                index += 1;
            }
            "--prefetch-queue-capacity" => {
                index += 1;
                options.prefetch.queue_capacity = parse_usize(
                    value(args, index, "--prefetch-queue-capacity")?,
                    "prefetch_queue_capacity",
                )?;
                index += 1;
            }
            "--max-readahead-bytes" => {
                index += 1;
                options.read_pipeline.max_readahead_bytes = parse_usize(
                    value(args, index, "--max-readahead-bytes")?,
                    "max_readahead_bytes",
                )?;
                index += 1;
            }
            "--no-writeback-cache" => {
                options.writeback.cache_enabled = false;
                index += 1;
            }
            "--writeback-cache" => {
                options.writeback.cache_enabled = true;
                index += 1;
            }
            "--writeback-cache-dir" => {
                index += 1;
                options.writeback.root =
                    PathBuf::from(value(args, index, "--writeback-cache-dir")?);
                index += 1;
            }
            "--writeback-cache-bytes" => {
                index += 1;
                options.writeback.max_bytes = parse_u64(
                    value(args, index, "--writeback-cache-bytes")?,
                    "writeback_cache_bytes",
                )?;
                index += 1;
            }
            "--writeback-cache-items" => {
                index += 1;
                options.writeback.max_items = parse_usize(
                    value(args, index, "--writeback-cache-items")?,
                    "writeback_cache_items",
                )?;
                index += 1;
            }
            "--writeback-workers" => {
                index += 1;
                options.writeback.workers = parse_usize(
                    value(args, index, "--writeback-workers")?,
                    "writeback_workers",
                )?;
                index += 1;
            }
            "--writeback-queue-capacity" => {
                index += 1;
                options.writeback.queue_capacity = parse_usize(
                    value(args, index, "--writeback-queue-capacity")?,
                    "writeback_queue_capacity",
                )?;
                index += 1;
            }
            "--writeback-upload-workers-per-request" => {
                index += 1;
                options.writeback.upload_workers_per_request = parse_usize(
                    value(args, index, "--writeback-upload-workers-per-request")?,
                    "writeback_upload_workers_per_request",
                )?;
                index += 1;
            }
            option if option.starts_with('-') => {
                return Err(CliError::UnknownOption(option.to_owned()))
            }
            raw => {
                if mountpoint.replace(PathBuf::from(raw)).is_some() {
                    return Err(CliError::TooManyArguments);
                }
                index += 1;
            }
        }
    }
    let mountpoint = mountpoint.ok_or(CliError::MissingArgument("mountpoint"))?;
    Ok((options, mountpoint))
}

fn exact_args(args: &[String], expected: usize) -> Result<(), CliError> {
    if args.len() < expected {
        return Err(CliError::MissingArgument(
            match args.first().map(String::as_str) {
                Some("mkdir") | Some("ls") | Some("cat") | Some("rm") | Some("rmdir") => "path",
                Some("put-artifact") => "path and source",
                Some("snapshot") => "path",
                Some("cat-snapshot") => "snapshot id and path",
                Some("retire-snapshot") => "snapshot id",
                Some("mount-snapshot") => "snapshot id and mountpoint",
                Some("rename") | Some("rename-replace") => "source and destination",
                Some("clone") => "source and destination paths",
                Some("diff") => "two paths",
                Some("rollback") => "path and snapshot id",
                Some("mount") => "mountpoint",
                _ => "argument",
            },
        ));
    }
    if args.len() > expected {
        return Err(CliError::TooManyArguments);
    }
    Ok(())
}

fn value<'a>(args: &'a [String], index: usize, option: &'static str) -> Result<&'a str, CliError> {
    args.get(index)
        .map(String::as_str)
        .ok_or(CliError::MissingValue(option))
}

fn parse_u32(raw: &str, field: &'static str) -> Result<u32, CliError> {
    raw.parse::<u32>().map_err(|_| CliError::InvalidNumber {
        field,
        value: raw.to_owned(),
    })
}

fn parse_u64(raw: &str, field: &'static str) -> Result<u64, CliError> {
    raw.parse::<u64>().map_err(|_| CliError::InvalidNumber {
        field,
        value: raw.to_owned(),
    })
}

fn parse_usize(raw: &str, field: &'static str) -> Result<usize, CliError> {
    raw.parse::<usize>().map_err(|_| CliError::InvalidNumber {
        field,
        value: raw.to_owned(),
    })
}

fn parse_socket_addr(raw: &str, field: &'static str) -> Result<SocketAddr, CliError> {
    raw.parse::<SocketAddr>()
        .map_err(|_| CliError::InvalidAddress {
            field,
            value: raw.to_owned(),
        })
}

fn default_manifest_id(path: &str) -> Result<String, CliError> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Err(CliError::MissingArgument("artifact path"));
    }
    Ok(format!("artifacts/{trimmed}"))
}

fn artifact_digest_reader(reader: &mut impl Read) -> Result<String, CliError> {
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut buffer).map_err(from_io)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn subtree_delta_label(kind: SubtreeDeltaKind) -> &'static str {
    match kind {
        SubtreeDeltaKind::Added => "A",
        SubtreeDeltaKind::Modified => "M",
        SubtreeDeltaKind::Removed => "D",
    }
}

fn file_type_label(file_type: FileType) -> &'static str {
    match file_type {
        FileType::File => "file",
        FileType::Directory => "dir",
        FileType::Symlink => "symlink",
        FileType::NamedPipe => "fifo",
        FileType::CharDevice => "char",
        FileType::BlockDevice => "block",
        FileType::Socket => "socket",
    }
}

fn from_io(err: impl Error) -> CliError {
    CliError::Io(err.to_string())
}

fn from_client(err: impl Error) -> CliError {
    CliError::Client(err.to_string())
}

fn from_object(err: impl Error) -> CliError {
    CliError::Client(err.to_string())
}

fn print_help(out: &mut impl Write) -> io::Result<()> {
    writeln!(
        out,
        "NoKV metadata client/server CLI\n\
\n\
Usage:\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] init\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] mkdir PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] put-artifact PATH SOURCE\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] ls PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] cat PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rm PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rmdir PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rename SOURCE DESTINATION\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rename-replace SOURCE DESTINATION\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] mount [--read-only] [--no-kernel-cache] [--direct-io] [--entry-ttl-ms MS] [--attr-ttl-ms MS] [--writeback-cache] MOUNTPOINT\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] mount-snapshot SNAPSHOT_ID [--no-kernel-cache] [--direct-io] [--entry-ttl-ms MS] [--attr-ttl-ms MS] [--writeback-cache] MOUNTPOINT\n\
  nokv [--meta PATH] [--object-backend s3|rustfs] [--mount ID] serve\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] backup\n\
  nokv [--meta PATH] [--object-backend s3|rustfs] [--mount ID] restore\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] fsck\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] gc [LIMIT]\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] snapshot PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] clone SRC_PATH DST_PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] diff PATH_A PATH_B\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rollback PATH SNAPSHOT_ID\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] cat-snapshot SNAPSHOT_ID PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] retire-snapshot SNAPSHOT_ID\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] renew-snapshot SNAPSHOT_ID [LEASE_MS]\n\
\n\
Object backends:\n\
  --object-backend s3|rustfs\n\
  --s3-bucket BUCKET                S3/RustFS bucket\n\
  --s3-endpoint URL                 S3/RustFS endpoint\n\
  --s3-region REGION                S3 region; RustFS commonly uses auto\n\
  --s3-access-key-id KEY\n\
  --s3-secret-access-key SECRET\n\
  --s3-root PREFIX\n\
  --hot-object-root PATH           Local NVMe-shaped hot object tier; cold S3/RustFS remains durable truth\n\
  --hot-object-max-bytes BYTES     Maximum local hot-tier bytes before LRU eviction\n\
  --hot-fill-mode inline|background  Cold-read hot fill mode; default inline\n\
  --object-gc-interval-ms MS       Background object GC interval for live mount\n\
  --object-gc-limit LIMIT          Max queued object records per GC iteration\n\
  --history-gc-interval-ms MS      Background metadata history GC interval for live mount\n\
  --history-gc-limit LIMIT         Max history records removed per GC iteration\n\
  --server-bind ADDR              Metadata service address for client commands and serve bind\n\
\n\
Mount cache options:\n\
  --no-kernel-cache              Do not ask FUSE to keep file/directory cache on open\n\
  --direct-io                    Ask FUSE to bypass kernel page cache for file handles\n\
  --entry-ttl-ms MS              Kernel dentry cache TTL\n\
  --attr-ttl-ms MS               Kernel attribute cache TTL\n\
  --stats-bind ADDR              Optional mount-local HTTP stats bind address\n\
  --fuse-threads N              FUSE worker thread count for the mount\n\
  --no-block-cache               Disable NoKV object block cache\n\
  --block-cache off|memory|disk  Select NoKV object block cache policy\n\
  --block-cache-dir PATH         Directory for disk object block cache\n\
  --block-cache-bytes N          Max object block cache bytes\n\
  --block-cache-items N          Max object block cache entries\n\
  --block-cache-ttl-ms MS        Object block cache TTL; 0 means no TTL\n\
  --no-prefetch                  Disable sequential object prefetch\n\
  --prefetch-workers N           Sequential object prefetch worker count\n\
  --prefetch-queue-capacity N    Sequential object prefetch queue capacity\n\
  --max-readahead-bytes N        Max sequential read-ahead bytes per hint\n\
  --writeback-cache              Stage completed write blocks in the disk cache before object upload\n\
  --no-writeback-cache           Use the default in-memory write buffer and direct object upload\n\
  --writeback-cache-dir PATH     Directory for staged object upload cache\n\
  --writeback-cache-bytes N      Max staged object upload cache bytes\n\
  --writeback-cache-items N      Max staged object upload cache entries\n\
  --writeback-workers N          Background staged upload worker count\n\
  --writeback-queue-capacity N   Background staged upload queue capacity\n\
  --writeback-upload-workers-per-request N\n\
                                  Parallel object PUT workers per staged upload request\n\
\n\
Defaults:\n\
  --meta .nokv/meta\n\
  --object-backend rustfs\n\
  --s3-bucket nokv\n\
  --s3-endpoint http://127.0.0.1:9000\n\
  --object-gc-interval-ms 30000\n\
  --object-gc-limit 1024\n\
  --history-gc-interval-ms 30000\n\
  --history-gc-limit 1024\n\
  --server-bind 127.0.0.1:7777\n\
  --metadata-checkpoint-archive-prefix metadata/checkpoints\n\
  --no-metadata-checkpoint-archive\n\
  --mount 1"
    )
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingValue(option) => write!(f, "{option} requires a value"),
            Self::UnknownOption(option) => write!(f, "unknown option {option}"),
            Self::UnknownCommand(command) => write!(f, "unknown command {command}"),
            Self::MissingArgument(arg) => write!(f, "missing {arg}"),
            Self::TooManyArguments => write!(f, "too many arguments"),
            Self::InvalidMount(value) => write!(f, "invalid mount id {value}"),
            Self::InvalidAddress { field, value } => write!(f, "invalid {field} address {value}"),
            Self::InvalidNumber { field, value } => write!(f, "invalid {field} value {value}"),
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Client(err) => write!(f, "{err}"),
        }
    }
}

impl Error for CliError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    use nokv_client::NoKvFsClient;
    use nokv_object::{MemoryObjectStore, ObjectKey, ObjectStore};
    use tempfile::tempdir;

    fn s(value: &str) -> String {
        value.to_owned()
    }

    fn fake_server_object_config() -> ObjectStoreConfig {
        ObjectStoreConfig::s3(S3ObjectStoreOptions {
            bucket: "test".to_owned(),
            root: "/".to_owned(),
            region: "auto".to_owned(),
            endpoint: Some("http://127.0.0.1:1".to_owned()),
            access_key_id: Some("test".to_owned()),
            secret_access_key: Some("test".to_owned()),
            session_token: None,
            virtual_host_style: false,
            skip_signature: true,
        })
    }

    fn spawn_test_server() -> SocketAddr {
        let dir = tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        let server = nokv_server::Server::open(ServerOptions {
            bind,
            mount: MountId::new(1).unwrap(),
            meta_path: dir.path().join("meta"),
            metadata_checkpoint_archive_prefix: None,
            object: fake_server_object_config(),
            uid: 1000,
            gid: 1000,
            object_gc: ObjectGcOptions {
                interval: Duration::from_secs(3600),
                limit: 128,
                run_immediately: false,
                read_lease_grace: ObjectGcOptions::default().read_lease_grace,
            },
            history_gc: HistoryGcOptions {
                interval: Duration::from_secs(3600),
                limit: 128,
                run_immediately: false,
            },
        })
        .unwrap();
        thread::spawn(move || {
            let _dir = dir;
            let _ = server.serve(listener);
        });
        bind
    }

    #[test]
    fn parse_defaults_to_rustfs() {
        let (config, command) = parse(vec![s("ls"), s("/")]).unwrap();
        assert_eq!(config.meta, PathBuf::from(".nokv/meta"));
        let options = config.object.options();
        assert_eq!(options.bucket, "nokv");
        assert_eq!(options.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(config.mount.get(), 1);
        assert_eq!(
            config.metadata_checkpoint_archive_prefix.as_deref(),
            Some(DEFAULT_METADATA_CHECKPOINT_ARCHIVE_PREFIX)
        );
        assert_eq!(config.server_bind, DEFAULT_SERVER_BIND);
        assert_eq!(command, Command::Ls { path: s("/") });
    }

    #[test]
    fn service_sdk_round_trip_uses_server_metadata_and_client_object_store() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store.clone());
        let body = b"hello metadata server";
        let entry = client
            .put_artifact(
                "/checkpoint.bin",
                body.to_vec(),
                ArtifactMetadata {
                    producer: "cli-test".to_owned(),
                    digest_uri: "sha256:demo".to_owned(),
                    content_type: "application/octet-stream".to_owned(),
                    manifest_id: "checkpoint.bin".to_owned(),
                    mode: DEFAULT_MODE_FILE,
                    uid: DEFAULT_UID,
                    gid: DEFAULT_GID,
                },
            )
            .unwrap();
        assert_eq!(entry.attr.size, body.len() as u64);
        assert!(store
            .head(
                &ObjectKey::new(format!(
                    "blocks/1/{}/{}/0/0",
                    entry.attr.inode.get(),
                    entry.attr.generation
                ))
                .unwrap()
            )
            .unwrap()
            .is_some());
        assert_eq!(client.cat("/checkpoint.bin").unwrap(), body);
        let listed = client.metadata().list("/").unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].attr.inode, entry.attr.inode);
    }

    #[test]
    fn parse_global_options_before_command() {
        let (config, command) = parse(vec![
            s("--meta"),
            s("/tmp/meta"),
            s("--object-backend"),
            s("rustfs"),
            s("--mount"),
            s("7"),
            s("--object-gc-interval-ms"),
            s("50"),
            s("--object-gc-limit"),
            s("9"),
            s("--history-gc-interval-ms"),
            s("60"),
            s("--history-gc-limit"),
            s("11"),
            s("--server-bind"),
            s("127.0.0.1:17777"),
            s("mkdir"),
            s("/runs"),
        ])
        .unwrap();
        assert_eq!(config.meta, PathBuf::from("/tmp/meta"));
        assert_eq!(config.mount.get(), 7);
        assert_eq!(config.object_gc_interval, Duration::from_millis(50));
        assert_eq!(config.object_gc_limit, 9);
        assert_eq!(config.history_gc_interval, Duration::from_millis(60));
        assert_eq!(config.history_gc_limit, 11);
        assert_eq!(
            config.server_bind,
            "127.0.0.1:17777".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(command, Command::Mkdir { path: s("/runs") });
    }

    #[test]
    fn parse_rejects_zero_object_gc_interval() {
        assert!(matches!(
            parse(vec![
                s("--object-gc-interval-ms"),
                s("0"),
                s("mount"),
                s("/tmp/nokv")
            ]),
            Err(CliError::InvalidNumber {
                field: "object_gc_interval_ms",
                ..
            })
        ));
    }

    #[test]
    fn parse_rejects_zero_history_gc_interval() {
        assert!(matches!(
            parse(vec![
                s("--history-gc-interval-ms"),
                s("0"),
                s("mount"),
                s("/tmp/nokv")
            ]),
            Err(CliError::InvalidNumber {
                field: "history_gc_interval_ms",
                ..
            })
        ));
    }

    #[test]
    fn parse_rejects_invalid_server_bind() {
        assert!(matches!(
            parse(vec![s("--server-bind"), s("localhost"), s("serve")]),
            Err(CliError::InvalidAddress {
                field: "server_bind",
                ..
            })
        ));
    }

    #[test]
    fn parse_rustfs_object_options() {
        let (config, command) = parse(vec![
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
            s("init"),
        ])
        .unwrap();
        assert_eq!(command, Command::Init);
        let options = config.object.options();
        assert_eq!(options.bucket, "nokv");
        assert_eq!(options.region, "auto");
        assert_eq!(options.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(options.access_key_id.as_deref(), Some("access"));
        assert_eq!(options.secret_access_key.as_deref(), Some("secret"));
    }

    #[test]
    fn parse_s3_does_not_inherit_rustfs_endpoint() {
        let (config, command) = parse(vec![
            s("--s3-bucket"),
            s("training-artifacts"),
            s("--object-backend"),
            s("s3"),
            s("init"),
        ])
        .unwrap();
        assert_eq!(command, Command::Init);
        let options = config.object.options();
        assert_eq!(options.bucket, "training-artifacts");
        assert_eq!(options.region, "us-east-1");
        assert_eq!(options.endpoint, None);
        assert_eq!(options.access_key_id, None);
        assert_eq!(options.secret_access_key, None);
    }

    #[test]
    fn parse_hot_object_root_keeps_cold_backend_options() {
        let (config, command) = parse(vec![
            s("--object-backend"),
            s("rustfs"),
            s("--hot-object-root"),
            s("/mnt/nvme/nokv-hot"),
            s("--hot-object-max-bytes"),
            s("1048576"),
            s("--hot-fill-mode"),
            s("background"),
            s("init"),
        ])
        .unwrap();
        assert_eq!(command, Command::Init);
        assert_eq!(
            config.object.local_hot_root().map(PathBuf::from),
            Some(PathBuf::from("/mnt/nvme/nokv-hot"))
        );
        assert_eq!(
            config
                .object
                .local_hot_options()
                .and_then(|options| options.max_bytes),
            Some(1_048_576)
        );
        assert_eq!(
            config.object.tiered_options(),
            Some(TieredObjectStoreOptions {
                hot_fill_mode: HotFillMode::Background,
                ..TieredObjectStoreOptions::default()
            })
        );
        let options = config.object.options();
        assert_eq!(options.bucket, "nokv");
        assert_eq!(options.region, "auto");
        assert_eq!(options.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
    }

    #[test]
    fn default_manifest_id_is_relative_and_stable() {
        assert_eq!(
            default_manifest_id("/runs/1/checkpoint").unwrap(),
            "artifacts/runs/1/checkpoint"
        );
        assert_eq!(
            default_manifest_id("runs/1/checkpoint").unwrap(),
            "artifacts/runs/1/checkpoint"
        );
    }

    #[test]
    fn artifact_digest_reader_is_sha256_uri() {
        let mut reader = io::Cursor::new(b"body".to_vec());
        assert_eq!(
            artifact_digest_reader(&mut reader).unwrap(),
            "sha256:230d8358dc8e8890b4c58deeb62912ee2f20357ae92a5cc861b98e68fe31acb5"
        );
    }

    #[test]
    fn parse_mount_command() {
        let (_config, command) = parse(vec![s("mount"), s("/tmp/nokv")]).unwrap();
        assert_eq!(
            command,
            Command::Mount {
                mountpoint: PathBuf::from("/tmp/nokv"),
                options: MountCliOptions::default(),
            }
        );
        let (_config, command) =
            parse(vec![s("mount"), s("--read-only"), s("/tmp/nokv-ro")]).unwrap();
        assert_eq!(
            command,
            Command::Mount {
                mountpoint: PathBuf::from("/tmp/nokv-ro"),
                options: MountCliOptions {
                    read_only: true,
                    ..MountCliOptions::default()
                },
            }
        );
        let (_config, command) = parse(vec![
            s("mount"),
            s("--no-kernel-cache"),
            s("--direct-io"),
            s("--stats-bind"),
            s("127.0.0.1:0"),
            s("--fuse-threads"),
            s("8"),
            s("--entry-ttl-ms"),
            s("0"),
            s("--attr-ttl-ms"),
            s("250"),
            s("/tmp/nokv-cache"),
        ])
        .unwrap();
        assert_eq!(
            command,
            Command::Mount {
                mountpoint: PathBuf::from("/tmp/nokv-cache"),
                options: MountCliOptions {
                    kernel_cache: false,
                    direct_io: true,
                    stats_bind: Some("127.0.0.1:0".parse().unwrap()),
                    threads: 8,
                    entry_ttl: Duration::from_millis(0),
                    attr_ttl: Duration::from_millis(250),
                    ..MountCliOptions::default()
                },
            }
        );
        let read_cache_options = MountCliOptions {
            block_cache: BlockCachePolicy::Disk(DiskBlockCacheOptions {
                root: PathBuf::from("/tmp/nokv-block-cache"),
                max_bytes: 8192,
                max_items: 64,
                ttl: Some(Duration::from_millis(5000)),
            }),
            prefetch: nokv_fuse::FusePrefetchOptions {
                enabled: false,
                workers: 3,
                queue_capacity: 16,
            },
            read_pipeline: FileReadPipelineOptions {
                max_readahead_bytes: 2 * 1024 * 1024,
            },
            ..MountCliOptions::default()
        };
        let (_config, command) = parse(vec![
            s("mount"),
            s("--block-cache"),
            s("disk"),
            s("--block-cache-dir"),
            s("/tmp/nokv-block-cache"),
            s("--block-cache-bytes"),
            s("8192"),
            s("--block-cache-items"),
            s("64"),
            s("--block-cache-ttl-ms"),
            s("5000"),
            s("--no-prefetch"),
            s("--prefetch-workers"),
            s("3"),
            s("--prefetch-queue-capacity"),
            s("16"),
            s("--max-readahead-bytes"),
            s("2097152"),
            s("/tmp/nokv-read-cache-mount"),
        ])
        .unwrap();
        assert_eq!(
            command,
            Command::Mount {
                mountpoint: PathBuf::from("/tmp/nokv-read-cache-mount"),
                options: read_cache_options,
            }
        );
        let writeback_options = MountCliOptions {
            writeback: nokv_fuse::FuseWritebackOptions {
                cache_enabled: true,
                root: PathBuf::from("/tmp/nokv-writeback"),
                max_bytes: 4096,
                max_items: 32,
                queue_capacity: 128,
                workers: 4,
                upload_workers_per_request: 2,
            },
            ..MountCliOptions::default()
        };
        let (_config, command) = parse(vec![
            s("mount"),
            s("--writeback-cache"),
            s("--writeback-cache-dir"),
            s("/tmp/nokv-writeback"),
            s("--writeback-cache-bytes"),
            s("4096"),
            s("--writeback-cache-items"),
            s("32"),
            s("--writeback-workers"),
            s("4"),
            s("--writeback-queue-capacity"),
            s("128"),
            s("--writeback-upload-workers-per-request"),
            s("2"),
            s("/tmp/nokv-writeback-mount"),
        ])
        .unwrap();
        assert_eq!(
            command,
            Command::Mount {
                mountpoint: PathBuf::from("/tmp/nokv-writeback-mount"),
                options: writeback_options,
            }
        );
        let (_config, command) = parse(vec![
            s("mount-snapshot"),
            s("42"),
            s("--direct-io"),
            s("/tmp/nokv-ro"),
        ])
        .unwrap();
        assert_eq!(
            command,
            Command::MountSnapshot {
                snapshot_id: 42,
                mountpoint: PathBuf::from("/tmp/nokv-ro"),
                options: MountCliOptions {
                    direct_io: true,
                    ..MountCliOptions::default()
                },
            }
        );
        assert!(matches!(
            parse(vec![s("mount-snapshot"), s("bad"), s("/tmp/nokv-ro")]),
            Err(CliError::InvalidNumber {
                field: "snapshot_id",
                ..
            })
        ));
    }

    #[test]
    fn parse_mount_rejects_zero_fuse_threads() {
        assert!(matches!(
            parse(vec![
                s("mount"),
                s("--fuse-threads"),
                s("0"),
                s("/tmp/nokv")
            ]),
            Err(CliError::InvalidNumber {
                field: "fuse_threads",
                ..
            })
        ));
    }

    #[test]
    fn parse_serve_command() {
        let command = parse(vec![s("serve")]).unwrap().1;
        assert_eq!(command, Command::Serve);
        assert!(matches!(
            parse(vec![s("serve"), s("extra")]),
            Err(CliError::TooManyArguments)
        ));
    }

    #[test]
    fn parse_gc_command() {
        assert_eq!(
            parse(vec![s("gc")]).unwrap().1,
            Command::Gc {
                limit: DEFAULT_GC_LIMIT
            }
        );
        assert_eq!(
            parse(vec![s("gc"), s("7")]).unwrap().1,
            Command::Gc { limit: 7 }
        );
        assert!(matches!(
            parse(vec![s("gc"), s("bad")]),
            Err(CliError::InvalidNumber { field: "limit", .. })
        ));
    }

    #[test]
    fn parse_clone_and_diff_commands() {
        assert_eq!(
            parse(vec![s("clone"), s("/base"), s("/forks/agent-1")])
                .unwrap()
                .1,
            Command::Clone {
                src: s("/base"),
                dst: s("/forks/agent-1"),
            }
        );
        assert_eq!(
            parse(vec![s("diff"), s("/base"), s("/forks/agent-1")])
                .unwrap()
                .1,
            Command::Diff {
                a: s("/base"),
                b: s("/forks/agent-1"),
            }
        );
        assert!(matches!(
            parse(vec![s("clone"), s("/base")]),
            Err(CliError::MissingArgument("source and destination paths"))
        ));
        assert!(matches!(
            parse(vec![s("diff"), s("/base"), s("/b"), s("/c")]),
            Err(CliError::TooManyArguments)
        ));
    }

    #[test]
    fn parse_rollback_command() {
        assert_eq!(
            parse(vec![s("rollback"), s("/base"), s("42")]).unwrap().1,
            Command::Rollback {
                path: s("/base"),
                snapshot_id: 42,
            }
        );
        assert!(matches!(
            parse(vec![s("rollback"), s("/base")]),
            Err(CliError::MissingArgument("path and snapshot id"))
        ));
        assert!(matches!(
            parse(vec![s("rollback"), s("/base"), s("bad")]),
            Err(CliError::InvalidNumber {
                field: "snapshot_id",
                ..
            })
        ));
        assert!(matches!(
            parse(vec![s("rollback"), s("/base"), s("1"), s("2")]),
            Err(CliError::TooManyArguments)
        ));
    }

    #[test]
    fn parse_snapshot_commands() {
        assert_eq!(
            parse(vec![s("snapshot"), s("/runs")]).unwrap().1,
            Command::Snapshot { path: s("/runs") }
        );
        assert_eq!(
            parse(vec![s("cat-snapshot"), s("42"), s("/runs/checkpoint")])
                .unwrap()
                .1,
            Command::CatSnapshot {
                snapshot_id: 42,
                path: s("/runs/checkpoint")
            }
        );
        assert_eq!(
            parse(vec![s("retire-snapshot"), s("42")]).unwrap().1,
            Command::RetireSnapshot { snapshot_id: 42 }
        );
        assert!(matches!(
            parse(vec![s("cat-snapshot"), s("bad"), s("/runs/checkpoint")]),
            Err(CliError::InvalidNumber {
                field: "snapshot_id",
                ..
            })
        ));
    }

    #[test]
    fn rejects_removed_local_object_options() {
        assert!(matches!(
            parse(vec![s("--object-backend"), s("local"), s("init")]),
            Err(CliError::UnknownOption(_))
        ));
        assert!(matches!(
            parse(vec![s("--objects"), s("/tmp/objects"), s("init")]),
            Err(CliError::UnknownOption(_))
        ));
    }

    #[test]
    fn parse_mutation_commands() {
        assert_eq!(
            parse(vec![s("rm"), s("/runs/a")]).unwrap().1,
            Command::Rm { path: s("/runs/a") }
        );
        assert_eq!(
            parse(vec![s("rmdir"), s("/runs")]).unwrap().1,
            Command::Rmdir { path: s("/runs") }
        );
        assert_eq!(
            parse(vec![s("rename"), s("/a"), s("/b")]).unwrap().1,
            Command::Rename {
                source: s("/a"),
                destination: s("/b")
            }
        );
        assert_eq!(
            parse(vec![s("rename-replace"), s("/stage"), s("/final")])
                .unwrap()
                .1,
            Command::RenameReplace {
                source: s("/stage"),
                destination: s("/final")
            }
        );
    }

    #[test]
    fn parse_metadata_checkpoint_archive_options_for_serve() {
        let (config, command) = parse(vec![s("serve")]).unwrap();
        assert_eq!(command, Command::Serve);
        assert_eq!(
            config.metadata_checkpoint_archive_prefix.as_deref(),
            Some(DEFAULT_METADATA_CHECKPOINT_ARCHIVE_PREFIX)
        );
        let (archive, command) = parse(vec![
            s("--metadata-checkpoint-archive-prefix"),
            s("/custom/checkpoints/"),
            s("serve"),
        ])
        .unwrap();
        assert_eq!(command, Command::Serve);
        assert_eq!(
            archive.metadata_checkpoint_archive_prefix.as_deref(),
            Some("custom/checkpoints")
        );
        let (disabled, command) =
            parse(vec![s("--no-metadata-checkpoint-archive"), s("serve")]).unwrap();
        assert_eq!(command, Command::Serve);
        assert_eq!(disabled.metadata_checkpoint_archive_prefix, None);
    }
}
