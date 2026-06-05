//! Minimal NoKV command line interface.

use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::time::Duration;

use nokvfs_client::{ArtifactMetadata, MetadataClient, MetadataClientOptions, NoKvFsClient};
use nokvfs_cluster::{FileMetadataRaftLogSync, NodeId};
use nokvfs_meta::{HistoryGcOptions, ObjectGcOptions};
use nokvfs_object::{ObjectKey, ObjectStoreConfig, S3ObjectStore, S3ObjectStoreOptions};
use nokvfs_server::{
    MetadataRaftPeerOptions, ServerOptions, DEFAULT_METADATA_CHECKPOINT_ARCHIVE_PREFIX,
    DEFAULT_SERVER_BIND,
};
use nokvfs_types::{FileType, MountId};
use sha2::{Digest, Sha256};

const DEFAULT_MODE_DIR: u32 = 0o755;
const DEFAULT_MODE_FILE: u32 = 0o644;
const DEFAULT_UID: u32 = 1000;
const DEFAULT_GID: u32 = 1000;
const DEFAULT_GC_LIMIT: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Config {
    meta: PathBuf,
    metadata_raft_node: NodeId,
    metadata_raft_voters: Vec<NodeId>,
    metadata_raft_peers: Vec<MetadataRaftPeerOptions>,
    metadata_raft_log_sync: FileMetadataRaftLogSync,
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
    metadata_read_endpoints: Vec<SocketAddr>,
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
        read_only: bool,
    },
    MountSnapshot {
        snapshot_id: u64,
        mountpoint: PathBuf,
    },
    Serve,
    Gc {
        limit: usize,
    },
    Snapshot {
        path: String,
    },
    CatSnapshot {
        snapshot_id: u64,
        path: String,
    },
    RetireSnapshot {
        snapshot_id: u64,
    },
    Help,
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

type Client = NoKvFsClient<S3ObjectStore>;

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
                        producer: "nokv-fs".to_owned(),
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
            read_only,
        } => {
            let metadata = MetadataClient::new(
                MetadataClientOptions::new(config.server_bind)
                    .with_read_endpoints(config.metadata_read_endpoints.clone()),
            );
            let objects = config.object.open().map_err(from_object)?;
            metadata
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            nokvfs_fuse::mount_client(
                metadata,
                objects,
                mountpoint,
                nokvfs_fuse::FuseOptions {
                    access: if read_only {
                        nokvfs_fuse::FuseAccessMode::ReadOnly
                    } else {
                        nokvfs_fuse::FuseAccessMode::ReadWrite
                    },
                    ..nokvfs_fuse::FuseOptions::default()
                },
            )
            .map_err(from_io)?;
        }
        Command::MountSnapshot {
            snapshot_id,
            mountpoint,
        } => {
            let metadata = MetadataClient::new(
                MetadataClientOptions::new(config.server_bind)
                    .with_read_endpoints(config.metadata_read_endpoints.clone()),
            );
            let objects = config.object.open().map_err(from_object)?;
            let snapshot = metadata
                .snapshot_pin(snapshot_id)
                .map_err(from_client)?
                .ok_or_else(|| CliError::Client(format!("snapshot {snapshot_id} not found")))?;
            nokvfs_fuse::mount_client(
                metadata,
                objects,
                mountpoint,
                nokvfs_fuse::FuseOptions {
                    fs_name: format!("nokv-fs-snapshot-{snapshot_id}"),
                    view: nokvfs_fuse::FuseView::Snapshot {
                        snapshot_id,
                        root: snapshot.root,
                    },
                    ..nokvfs_fuse::FuseOptions::default()
                },
            )
            .map_err(from_io)?;
        }
        Command::Serve => {
            nokvfs_server::run(ServerOptions {
                bind: config.server_bind,
                mount: config.mount,
                meta_path: config.meta,
                metadata_raft_node: config.metadata_raft_node,
                metadata_raft_voters: config.metadata_raft_voters,
                metadata_raft_peers: config.metadata_raft_peers,
                metadata_raft_log_sync: config.metadata_raft_log_sync,
                metadata_checkpoint_archive_prefix: config.metadata_checkpoint_archive_prefix,
                object: config.object,
                uid: config.uid,
                gid: config.gid,
                object_gc: ObjectGcOptions {
                    interval: config.object_gc_interval,
                    limit: config.object_gc_limit,
                    run_immediately: false,
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
        Command::Snapshot { path } => {
            let client = open_client(&config)?;
            let snapshot = client.metadata().snapshot(&path).map_err(from_client)?;
            println!(
                "snapshot path={} id={} root={} read_version={} created_version={}",
                path,
                snapshot.snapshot_id,
                snapshot.root.get(),
                snapshot.read_version,
                snapshot.created_version
            );
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
        Command::Help => {
            print_help(&mut io::stdout()).map_err(from_io)?;
        }
    }
    Ok(())
}

fn open_client(config: &Config) -> Result<Client, CliError> {
    let objects = config.object.open().map_err(from_object)?;
    let metadata = MetadataClient::new(
        MetadataClientOptions::new(config.server_bind)
            .with_read_endpoints(config.metadata_read_endpoints.clone()),
    );
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
    let mut meta = PathBuf::from(".nokv-fs/meta");
    let mut metadata_raft_node = NodeId::new(1).expect("default metadata raft node is non-zero");
    let mut metadata_raft_voters = Vec::new();
    let mut metadata_raft_peers = Vec::new();
    let mut metadata_raft_log_sync = FileMetadataRaftLogSync::Data;
    let mut metadata_checkpoint_archive_prefix =
        Some(DEFAULT_METADATA_CHECKPOINT_ARCHIVE_PREFIX.to_owned());
    let mut object_backend = ObjectBackendKind::RustFs;
    let mut s3 = S3ObjectStoreOptions::new("");
    let mut mount = MountId::new(1).expect("default mount id is non-zero");
    let mut uid = DEFAULT_UID;
    let mut gid = DEFAULT_GID;
    let mut object_gc_interval = ObjectGcOptions::default().interval;
    let mut object_gc_limit = ObjectGcOptions::default().limit;
    let mut history_gc_interval = HistoryGcOptions::default().interval;
    let mut history_gc_limit = HistoryGcOptions::default().limit;
    let mut server_bind = DEFAULT_SERVER_BIND;
    let mut metadata_read_endpoints = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--meta" => {
                index += 1;
                meta = PathBuf::from(value(&args, index, "--meta")?);
            }
            "--metadata-raft-node" => {
                index += 1;
                metadata_raft_node = parse_node_id(value(&args, index, "--metadata-raft-node")?)?;
            }
            "--metadata-raft-voters" => {
                index += 1;
                metadata_raft_voters =
                    parse_node_id_list(value(&args, index, "--metadata-raft-voters")?)?;
            }
            "--metadata-raft-peer" => {
                index += 1;
                metadata_raft_peers.push(parse_metadata_raft_peer(value(
                    &args,
                    index,
                    "--metadata-raft-peer",
                )?)?);
            }
            "--metadata-raft-log-sync" => {
                index += 1;
                metadata_raft_log_sync =
                    parse_metadata_raft_log_sync(value(&args, index, "--metadata-raft-log-sync")?)?;
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
            "--metadata-read-endpoint" => {
                index += 1;
                metadata_read_endpoints.push(parse_socket_addr(
                    value(&args, index, "--metadata-read-endpoint")?,
                    "metadata_read_endpoint",
                )?);
            }
            "--help" | "-h" => {
                return Ok((
                    Config {
                        meta,
                        metadata_raft_node,
                        metadata_raft_voters,
                        metadata_raft_peers,
                        metadata_raft_log_sync,
                        metadata_checkpoint_archive_prefix,
                        object: object_config(object_backend, s3),
                        mount,
                        uid,
                        gid,
                        object_gc_interval,
                        object_gc_limit,
                        history_gc_interval,
                        history_gc_limit,
                        server_bind,
                        metadata_read_endpoints,
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
            metadata_raft_node,
            metadata_raft_voters,
            metadata_raft_peers,
            metadata_raft_log_sync,
            metadata_checkpoint_archive_prefix,
            object: object_config(object_backend, s3),
            mount,
            uid,
            gid,
            object_gc_interval,
            object_gc_limit,
            history_gc_interval,
            history_gc_limit,
            server_bind,
            metadata_read_endpoints,
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

fn parse_metadata_raft_log_sync(raw: &str) -> Result<FileMetadataRaftLogSync, CliError> {
    match raw {
        "data" => Ok(FileMetadataRaftLogSync::Data),
        "none" => Ok(FileMetadataRaftLogSync::None),
        _ => Err(CliError::UnknownOption(format!(
            "--metadata-raft-log-sync {raw}"
        ))),
    }
}

fn parse_archive_prefix(raw: &str, option: &str) -> Result<String, CliError> {
    let prefix = raw.trim_matches('/');
    ObjectKey::new(prefix).map_err(|_| CliError::UnknownOption(format!("{option} {raw}")))?;
    Ok(prefix.to_owned())
}

fn parse_node_id(raw: &str) -> Result<NodeId, CliError> {
    let parsed = parse_u64(raw, "metadata_raft_node")?;
    NodeId::new(parsed).map_err(|_| CliError::InvalidNumber {
        field: "metadata_raft_node",
        value: raw.to_owned(),
    })
}

fn parse_node_id_list(raw: &str) -> Result<Vec<NodeId>, CliError> {
    if raw.is_empty() {
        return Err(CliError::InvalidNumber {
            field: "metadata_raft_nodes",
            value: raw.to_owned(),
        });
    }
    raw.split(',')
        .map(|part| {
            if part.is_empty() {
                return Err(CliError::InvalidNumber {
                    field: "metadata_raft_nodes",
                    value: raw.to_owned(),
                });
            }
            let parsed = parse_u64(part, "metadata_raft_nodes")?;
            NodeId::new(parsed).map_err(|_| CliError::InvalidNumber {
                field: "metadata_raft_nodes",
                value: raw.to_owned(),
            })
        })
        .collect()
}

fn parse_metadata_raft_peer(raw: &str) -> Result<MetadataRaftPeerOptions, CliError> {
    let Some((node, address)) = raw.split_once('=') else {
        return Err(CliError::UnknownOption(format!(
            "--metadata-raft-peer {raw}"
        )));
    };
    Ok(MetadataRaftPeerOptions {
        node: parse_node_id(node).map_err(|_| CliError::InvalidNumber {
            field: "metadata_raft_peer_node",
            value: node.to_owned(),
        })?,
        address: parse_socket_addr(address, "metadata_raft_peer")?,
    })
}

fn object_config(backend: ObjectBackendKind, mut s3: S3ObjectStoreOptions) -> ObjectStoreConfig {
    match backend {
        ObjectBackendKind::S3 => ObjectStoreConfig::s3(s3),
        ObjectBackendKind::RustFs => {
            apply_rustfs_defaults(&mut s3);
            ObjectStoreConfig::s3(s3)
        }
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
        "mount" => match args {
            [_, mountpoint] => Ok(Command::Mount {
                mountpoint: PathBuf::from(mountpoint),
                read_only: false,
            }),
            [_, flag, mountpoint] if flag == "--read-only" => Ok(Command::Mount {
                mountpoint: PathBuf::from(mountpoint),
                read_only: true,
            }),
            [_, flag, _] if flag.starts_with('-') => Err(CliError::UnknownOption(flag.clone())),
            _ if args.len() < 2 => Err(CliError::MissingArgument("mountpoint")),
            _ => Err(CliError::TooManyArguments),
        },
        "mount-snapshot" => {
            exact_args(args, 3)?;
            Ok(Command::MountSnapshot {
                snapshot_id: parse_u64(&args[1], "snapshot_id")?,
                mountpoint: PathBuf::from(&args[2]),
            })
        }
        "serve" => exact_args(args, 1).map(|()| Command::Serve),
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
        "help" => Ok(Command::Help),
        other => Err(CliError::UnknownCommand(other.to_owned())),
    }
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

fn file_type_label(file_type: FileType) -> &'static str {
    match file_type {
        FileType::File => "file",
        FileType::Directory => "dir",
        FileType::Symlink => "symlink",
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
  nokv-fs [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] init\n\
  nokv-fs [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] mkdir PATH\n\
  nokv-fs [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] put-artifact PATH SOURCE\n\
  nokv-fs [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] ls PATH\n\
  nokv-fs [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] cat PATH\n\
  nokv-fs [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rm PATH\n\
  nokv-fs [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rmdir PATH\n\
  nokv-fs [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rename SOURCE DESTINATION\n\
  nokv-fs [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rename-replace SOURCE DESTINATION\n\
  nokv-fs [--server-bind ADDR] [--metadata-read-endpoint ADDR] [--object-backend s3|rustfs] [--mount ID] mount [--read-only] MOUNTPOINT\n\
  nokv-fs [--server-bind ADDR] [--metadata-read-endpoint ADDR] [--object-backend s3|rustfs] [--mount ID] mount-snapshot SNAPSHOT_ID MOUNTPOINT\n\
  nokv-fs [--meta PATH] [--object-backend s3|rustfs] [--mount ID] serve\n\
  nokv-fs [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] gc [LIMIT]\n\
  nokv-fs [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] snapshot PATH\n\
  nokv-fs [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] cat-snapshot SNAPSHOT_ID PATH\n\
  nokv-fs [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] retire-snapshot SNAPSHOT_ID\n\
\n\
Object backends:\n\
  --object-backend s3|rustfs\n\
  --s3-bucket BUCKET                S3/RustFS bucket\n\
  --s3-endpoint URL                 S3/RustFS endpoint\n\
  --s3-region REGION                S3 region; RustFS commonly uses auto\n\
  --s3-access-key-id KEY\n\
  --s3-secret-access-key SECRET\n\
  --s3-root PREFIX\n\
  --object-gc-interval-ms MS       Background object GC interval for live mount\n\
  --object-gc-limit LIMIT          Max queued object records per GC iteration\n\
  --history-gc-interval-ms MS      Background metadata history GC interval for live mount\n\
  --history-gc-limit LIMIT         Max history records removed per GC iteration\n\
  --server-bind ADDR              Metadata service address for client commands and serve bind\n\
  --metadata-read-endpoint ADDR   Preferred metadata read endpoint; repeat for learners\n\
  --metadata-raft-node NODE       Local OpenRaft metadata node id\n\
  --metadata-raft-voters CSV      OpenRaft voter node ids, e.g. 1,2,3\n\
  --metadata-raft-peer NODE=ADDR  OpenRaft peer endpoint; repeat for remote voters\n\
  --metadata-raft-log-sync data|none\n\
                                  data fsyncs metadata Raft log records; none only flushes to the OS\n\
\n\
Defaults:\n\
  --meta .nokv-fs/meta\n\
  --object-backend rustfs\n\
  --s3-bucket nokv\n\
  --s3-endpoint http://127.0.0.1:9000\n\
  --object-gc-interval-ms 30000\n\
  --object-gc-limit 1024\n\
  --history-gc-interval-ms 30000\n\
  --history-gc-limit 1024\n\
  --server-bind 127.0.0.1:7777\n\
  --metadata-raft-node 1\n\
  --metadata-raft-voters <local node only>\n\
  --metadata-raft-peer <empty>\n\
  --metadata-raft-log-sync data\n\
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

    use nokvfs_client::NoKvFsClient;
    use nokvfs_object::{MemoryObjectStore, ObjectKey, ObjectStore};
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
        let server = nokvfs_server::Server::open(ServerOptions {
            bind,
            mount: MountId::new(1).unwrap(),
            meta_path: dir.path().join("meta"),
            metadata_raft_node: NodeId::new(1).unwrap(),
            metadata_raft_voters: Vec::new(),
            metadata_raft_peers: Vec::new(),
            metadata_raft_log_sync: FileMetadataRaftLogSync::Data,
            metadata_checkpoint_archive_prefix: None,
            object: fake_server_object_config(),
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
        assert_eq!(config.meta, PathBuf::from(".nokv-fs/meta"));
        let options = config.object.options();
        assert_eq!(options.bucket, "nokv");
        assert_eq!(options.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(config.mount.get(), 1);
        assert_eq!(config.metadata_raft_node.get(), 1);
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
        assert_eq!(config.metadata_raft_node.get(), 1);
        assert!(config.metadata_raft_voters.is_empty());
        assert!(config.metadata_raft_peers.is_empty());
        assert_eq!(
            config.server_bind,
            "127.0.0.1:17777".parse::<SocketAddr>().unwrap()
        );
        assert!(config.metadata_read_endpoints.is_empty());
        assert_eq!(command, Command::Mkdir { path: s("/runs") });
    }

    #[test]
    fn parse_metadata_read_endpoints() {
        let (config, command) = parse(vec![
            s("--server-bind"),
            s("127.0.0.1:17777"),
            s("--metadata-read-endpoint"),
            s("127.0.0.1:17778"),
            s("--metadata-read-endpoint"),
            s("127.0.0.1:17779"),
            s("ls"),
            s("/runs"),
        ])
        .unwrap();

        assert_eq!(command, Command::Ls { path: s("/runs") });
        assert_eq!(
            config.server_bind,
            "127.0.0.1:17777".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            config.metadata_read_endpoints,
            vec![
                "127.0.0.1:17778".parse::<SocketAddr>().unwrap(),
                "127.0.0.1:17779".parse::<SocketAddr>().unwrap()
            ]
        );
    }

    #[test]
    fn parse_rejects_zero_object_gc_interval() {
        assert!(matches!(
            parse(vec![
                s("--object-gc-interval-ms"),
                s("0"),
                s("mount"),
                s("/tmp/nokv-fs")
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
                s("/tmp/nokv-fs")
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
        let (_config, command) = parse(vec![s("mount"), s("/tmp/nokv-fs")]).unwrap();
        assert_eq!(
            command,
            Command::Mount {
                mountpoint: PathBuf::from("/tmp/nokv-fs"),
                read_only: false,
            }
        );
        let (_config, command) =
            parse(vec![s("mount"), s("--read-only"), s("/tmp/nokv-fs-ro")]).unwrap();
        assert_eq!(
            command,
            Command::Mount {
                mountpoint: PathBuf::from("/tmp/nokv-fs-ro"),
                read_only: true,
            }
        );
        let (_config, command) =
            parse(vec![s("mount-snapshot"), s("42"), s("/tmp/nokv-fs-ro")]).unwrap();
        assert_eq!(
            command,
            Command::MountSnapshot {
                snapshot_id: 42,
                mountpoint: PathBuf::from("/tmp/nokv-fs-ro")
            }
        );
        assert!(matches!(
            parse(vec![s("mount-snapshot"), s("bad"), s("/tmp/nokv-fs-ro")]),
            Err(CliError::InvalidNumber {
                field: "snapshot_id",
                ..
            })
        ));
    }

    #[test]
    fn parse_serve_command() {
        let (config, command) = parse(vec![s("serve")]).unwrap();
        assert_eq!(command, Command::Serve);
        assert_eq!(config.metadata_raft_node.get(), 1);
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
    fn parse_metadata_raft_options_for_serve() {
        let (config, command) = parse(vec![
            s("--metadata-raft-node"),
            s("4"),
            s("--metadata-raft-voters"),
            s("1,2,3"),
            s("--metadata-raft-peer"),
            s("2=127.0.0.1:7778"),
            s("--metadata-raft-peer"),
            s("3=127.0.0.1:7779"),
            s("--metadata-raft-log-sync"),
            s("none"),
            s("serve"),
        ])
        .unwrap();
        assert_eq!(command, Command::Serve);
        assert_eq!(config.metadata_raft_node.get(), 4);
        assert_eq!(
            config
                .metadata_raft_voters
                .iter()
                .map(|node| node.get())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            config
                .metadata_raft_peers
                .iter()
                .map(|peer| (peer.node.get(), peer.address.to_string()))
                .collect::<Vec<_>>(),
            vec![
                (2, "127.0.0.1:7778".to_owned()),
                (3, "127.0.0.1:7779".to_owned())
            ]
        );
        assert_eq!(config.metadata_raft_log_sync, FileMetadataRaftLogSync::None);
        assert_eq!(
            config.metadata_checkpoint_archive_prefix.as_deref(),
            Some(DEFAULT_METADATA_CHECKPOINT_ARCHIVE_PREFIX)
        );
        let (node_only, command) =
            parse(vec![s("--metadata-raft-node"), s("4"), s("serve")]).unwrap();
        assert_eq!(command, Command::Serve);
        assert_eq!(node_only.metadata_raft_node.get(), 4);
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
        assert!(matches!(
            parse(vec![s("--metadata-raft-log-sync"), s("invalid"), s("serve")]),
            Err(CliError::UnknownOption(option)) if option == "--metadata-raft-log-sync invalid"
        ));
        assert!(matches!(
            parse(vec![s("--metadata-raft-node"), s("0"), s("serve")]),
            Err(CliError::InvalidNumber {
                field: "metadata_raft_node",
                ..
            })
        ));
        assert!(matches!(
            parse(vec![s("--metadata-raft-voters"), s("1,,3"), s("serve")]),
            Err(CliError::InvalidNumber {
                field: "metadata_raft_nodes",
                ..
            })
        ));
        assert!(matches!(
            parse(vec![s("--metadata-raft-peer"), s("2"), s("serve")]),
            Err(CliError::UnknownOption(option)) if option == "--metadata-raft-peer 2"
        ));
        assert!(matches!(
            parse(vec![s("--metadata-raft-peer"), s("2=bad"), s("serve")]),
            Err(CliError::InvalidAddress {
                field: "metadata_raft_peer",
                ..
            })
        ));
    }
}
