use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;

use nokvfs_meta::{
    DentryWithAttr, MetadError, NamespaceCard, NamespaceCardKind, NamespaceFilterCapability,
    NamespaceFindField, NamespaceFindRequest, NamespaceFindResult, NamespaceGrepMatch,
    NamespaceGrepRequest, NamespaceGrepResult, NamespaceInclude, NamespaceIndexValue,
    NamespaceListPage, NamespacePredicate, NamespacePredicateOp, NamespacePredicateValue,
    NamespaceQueryCatalog, NamespaceReadFormat, NamespaceReadOptions, NamespaceReadPage,
    NamespaceRecordCount, NamespaceRecordType, NamespaceSchema, NamespaceSort,
    NamespaceSortDirection, NamespaceSortField, PreparedArtifact, RecordCountProvenance,
};
use nokvfs_object::ObjectReadBlock;
use nokvfs_protocol::{
    decode_request, encode_envelope, MetadataProtocolError, MetadataRpcEnvelope,
    MetadataRpcRequest, MetadataRpcResult, WireBodyReadPlan, WireDentryWithAttr, WireMetadataError,
    WireNamespaceCard, WireNamespaceCardKind, WireNamespaceFilterCapability,
    WireNamespaceFindField, WireNamespaceFindRequest, WireNamespaceFindResult,
    WireNamespaceGrepMatch, WireNamespaceGrepRequest, WireNamespaceGrepResult,
    WireNamespaceInclude, WireNamespaceIndexValue, WireNamespaceListPage, WireNamespacePredicate,
    WireNamespacePredicateOp, WireNamespacePredicateValue, WireNamespaceQueryCatalog,
    WireNamespaceReadFormat, WireNamespaceReadItem, WireNamespaceReadOptions,
    WireNamespaceReadPage, WireNamespaceRecordCount, WireNamespaceRecordType, WireNamespaceSchema,
    WireNamespaceSort, WireNamespaceSortDirection, WireNamespaceSortField, WireObjectReadBlock,
    WirePathMetadata, WirePreparedArtifact, WireRecordCountProvenance,
};
use nokvfs_types::{DentryName, InodeId, MountId};

use crate::server::{Server, ServerError};

pub(crate) const FRAMED_RPC_MAGIC: &[u8; 8] = b"NKVRPC3\n";
const FRAME_HEADER_BYTES: usize = 16;
const MAX_FRAMED_RPC_BYTES: usize = 16 * 1024 * 1024;
const MIN_FRAMED_RPC_WORKERS: usize = 4;
const MAX_FRAMED_RPC_WORKERS: usize = 64;
const FRAMED_RPC_QUEUE_PER_WORKER: usize = 256;

type RpcJob = Box<dyn FnOnce() + Send + 'static>;

static FRAMED_RPC_WORKERS: OnceLock<RpcWorkerPool> = OnceLock::new();

struct RpcWorkerPool {
    sender: mpsc::SyncSender<RpcJob>,
}

impl RpcWorkerPool {
    fn new(workers: usize, queue_capacity: usize) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<RpcJob>(queue_capacity.max(workers));
        let receiver = Arc::new(Mutex::new(receiver));
        for worker in 0..workers {
            let receiver = Arc::clone(&receiver);
            thread::Builder::new()
                .name(format!("nokvfs-rpc-{worker}"))
                .spawn(move || loop {
                    let job = {
                        let receiver = receiver.lock().expect("metadata rpc worker receiver");
                        receiver.recv()
                    };
                    match job {
                        Ok(job) => job(),
                        Err(_) => return,
                    }
                })
                .expect("spawn metadata rpc worker");
        }
        Self { sender }
    }

    fn submit(&self, job: RpcJob) -> Result<(), ServerError> {
        self.sender.send(job).map_err(|_| {
            ServerError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "metadata framed rpc worker pool stopped",
            ))
        })
    }
}

struct RpcFrame {
    request_id: u64,
    flags: u32,
    payload: Vec<u8>,
}

fn handle_binary_rpc(server: &Server, body: &[u8]) -> Result<Vec<u8>, ServerError> {
    let envelope = match decode_request(body) {
        Ok(request) => match execute(server, request) {
            Ok(result) => MetadataRpcEnvelope {
                ok: true,
                result: Some(result),
                error: None,
                error_kind: None,
            },
            Err(err) => err_envelope(err),
        },
        Err(err) => MetadataRpcEnvelope {
            ok: false,
            result: None,
            error: Some(format!("invalid metadata binary rpc request: {err}")),
            error_kind: Some(WireMetadataError::Protocol {
                message: err.to_string(),
            }),
        },
    };
    encode_envelope(&envelope).map_err(|err| {
        ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("metadata binary rpc response encode failed: {err}"),
        ))
    })
}

pub(crate) fn handle_framed_stream_after_magic(
    server: Arc<Server>,
    mut stream: TcpStream,
) -> Result<(), ServerError> {
    stream.set_read_timeout(None).map_err(ServerError::Io)?;
    stream.set_write_timeout(None).map_err(ServerError::Io)?;
    let writer = Arc::new(Mutex::new(stream.try_clone().map_err(ServerError::Io)?));

    loop {
        let Some(frame) = read_frame(&mut stream)? else {
            return Ok(());
        };
        let server = Arc::clone(&server);
        let writer = Arc::clone(&writer);
        framed_rpc_worker_pool().submit(Box::new(move || {
            let response = handle_binary_rpc(server.as_ref(), &frame.payload);
            let response = match response {
                Ok(response) => response,
                Err(err) => {
                    let envelope = MetadataRpcEnvelope {
                        ok: false,
                        result: None,
                        error: Some(err.to_string()),
                        error_kind: Some(wire_server_error(&err)),
                    };
                    match encode_envelope(&envelope) {
                        Ok(response) => response,
                        Err(err) => {
                            eprintln!("metadata framed rpc error encode failed: {err}");
                            return;
                        }
                    }
                }
            };
            let mut writer = writer.lock().expect("framed rpc writer");
            if let Err(err) = write_frame(&mut writer, frame.request_id, frame.flags, &response) {
                eprintln!("metadata framed rpc response write failed: {err}");
            }
        }))?;
    }
}

fn framed_rpc_worker_pool() -> &'static RpcWorkerPool {
    FRAMED_RPC_WORKERS.get_or_init(|| {
        let workers = default_framed_rpc_worker_count();
        RpcWorkerPool::new(workers, workers.saturating_mul(FRAMED_RPC_QUEUE_PER_WORKER))
    })
}

fn default_framed_rpc_worker_count() -> usize {
    thread::available_parallelism()
        .map(|parallelism| parallelism.get().saturating_mul(4))
        .unwrap_or(MIN_FRAMED_RPC_WORKERS)
        .clamp(MIN_FRAMED_RPC_WORKERS, MAX_FRAMED_RPC_WORKERS)
}

fn read_frame(stream: &mut TcpStream) -> Result<Option<RpcFrame>, ServerError> {
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    match stream.read_exact(&mut header) {
        Ok(()) => {}
        Err(err) if is_clean_framed_end(&err) => return Ok(None),
        Err(err) => return Err(ServerError::Io(err)),
    }
    let request_id = u64::from_be_bytes(header[0..8].try_into().expect("request id header"));
    let flags = u32::from_be_bytes(header[8..12].try_into().expect("flags header"));
    let len = u32::from_be_bytes(header[12..16].try_into().expect("length header")) as usize;
    if len > MAX_FRAMED_RPC_BYTES {
        return Err(ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata framed rpc request exceeds size limit",
        )));
    }
    let mut body = vec![0_u8; len];
    stream.read_exact(&mut body).map_err(ServerError::Io)?;
    Ok(Some(RpcFrame {
        request_id,
        flags,
        payload: body,
    }))
}

fn write_frame(
    stream: &mut TcpStream,
    request_id: u64,
    flags: u32,
    body: &[u8],
) -> Result<(), ServerError> {
    let len = u32::try_from(body.len()).map_err(|_| {
        ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata framed rpc response exceeds u32 length",
        ))
    })?;
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    header[0..8].copy_from_slice(&request_id.to_be_bytes());
    header[8..12].copy_from_slice(&flags.to_be_bytes());
    header[12..16].copy_from_slice(&len.to_be_bytes());
    stream
        .write_all(&header)
        .and_then(|_| stream.write_all(body))
        .map_err(ServerError::Io)
}

fn is_clean_framed_end(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::UnexpectedEof | io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

fn execute_batch(
    server: &Server,
    requests: Vec<MetadataRpcRequest>,
) -> Result<MetadataRpcResult, ServerError> {
    let mut results = Vec::with_capacity(requests.len());
    let mut iter = requests.into_iter().peekable();
    while let Some(request) = iter.next() {
        let Some((parent_path, name, mode, uid, gid)) = create_file_path_parts(&request) else {
            results.push(execute_envelope(server, request));
            continue;
        };
        let mut names = vec![name];
        while let Some(next) = iter.peek() {
            let Some((next_parent, next_name, next_mode, next_uid, next_gid)) =
                create_file_path_parts(next)
            else {
                break;
            };
            if next_parent != parent_path || next_mode != mode || next_uid != uid || next_gid != gid
            {
                break;
            }
            names.push(next_name);
            iter.next();
        }
        if names.len() == 1 {
            let path = child_path(&parent_path, &names.remove(0));
            results.push(execute_envelope(
                server,
                MetadataRpcRequest::CreateFilePath {
                    path,
                    mode,
                    uid,
                    gid,
                },
            ));
        } else {
            results.extend(create_files_in_dir_path_envelopes(
                server,
                &parent_path,
                names,
                mode,
                uid,
                gid,
            ));
        }
    }
    Ok(MetadataRpcResult::Batch { results })
}

fn execute_envelope(server: &Server, request: MetadataRpcRequest) -> MetadataRpcEnvelope {
    match execute(server, request) {
        Ok(result) => ok_envelope(result),
        Err(err) => err_envelope(err),
    }
}

fn ok_envelope(result: MetadataRpcResult) -> MetadataRpcEnvelope {
    MetadataRpcEnvelope {
        ok: true,
        result: Some(result),
        error: None,
        error_kind: None,
    }
}

fn err_envelope(err: ServerError) -> MetadataRpcEnvelope {
    let error_kind = wire_server_error(&err);
    MetadataRpcEnvelope {
        ok: false,
        result: None,
        error: Some(err.to_string()),
        error_kind: Some(error_kind),
    }
}

fn wire_server_error(err: &ServerError) -> WireMetadataError {
    match err {
        ServerError::Io(err) => WireMetadataError::Io {
            message: err.to_string(),
        },
        ServerError::Object(err) => WireMetadataError::Object {
            message: err.to_string(),
        },
        ServerError::Metadata(err) => wire_metad_error(err),
    }
}

fn wire_metad_error(err: &MetadError) -> WireMetadataError {
    match err {
        MetadError::Metadata(nokvfs_meta::MetadataError::PredicateFailed) => {
            WireMetadataError::PredicateFailed
        }
        MetadError::Metadata(err) => WireMetadataError::Metadata {
            message: err.to_string(),
        },
        MetadError::Object(err) => WireMetadataError::Object {
            message: err.to_string(),
        },
        MetadError::PublishArtifactFailed { source, .. } => wire_metad_error(source),
        MetadError::StaleBodyGeneration { expected, current } => {
            WireMetadataError::StaleBodyGeneration {
                expected: *expected,
                current: *current,
            }
        }
        MetadError::InvalidPath(message) => WireMetadataError::InvalidPath {
            message: message.clone(),
        },
        MetadError::InvalidQuery(message) => WireMetadataError::InvalidQuery {
            message: message.clone(),
        },
        MetadError::NotFound => WireMetadataError::NotFound,
        MetadError::NotFile => WireMetadataError::NotFile,
        MetadError::NotDirectory => WireMetadataError::NotDirectory,
        MetadError::MissingBodyDescriptor => WireMetadataError::MissingBodyDescriptor,
        other => WireMetadataError::Metadata {
            message: other.to_string(),
        },
    }
}

fn create_files_in_dir_path_envelopes(
    server: &Server,
    parent_path: &str,
    names: Vec<String>,
    mode: u32,
    uid: u32,
    gid: u32,
) -> Vec<MetadataRpcEnvelope> {
    let parsed = names
        .iter()
        .map(|name| dentry_name(name.clone()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(ServerError::Metadata);
    let coalesced = parsed.and_then(|names| {
        server
            .service()
            .create_files_in_dir_path(parent_path, names, mode, uid, gid)
            .map_err(ServerError::Metadata)
    });
    match coalesced {
        Ok(entries) => entries
            .iter()
            .map(|entry| {
                ok_envelope(MetadataRpcResult::Dentry {
                    entry: Some(Box::new(wire_dentry(entry))),
                })
            })
            .collect(),
        Err(_) => names
            .into_iter()
            .map(|name| {
                execute_envelope(
                    server,
                    MetadataRpcRequest::CreateFilePath {
                        path: child_path(parent_path, &name),
                        mode,
                        uid,
                        gid,
                    },
                )
            })
            .collect(),
    }
}

fn create_file_path_parts(request: &MetadataRpcRequest) -> Option<(String, String, u32, u32, u32)> {
    match request {
        MetadataRpcRequest::CreateFilePath {
            path,
            mode,
            uid,
            gid,
        } => {
            let (parent_path, name) = split_parent_path(path)?;
            Some((parent_path, name, *mode, *uid, *gid))
        }
        _ => None,
    }
}

fn split_parent_path(path: &str) -> Option<(String, String)> {
    if !path.starts_with('/') || path == "/" {
        return None;
    }
    let slash = path.rfind('/')?;
    let name = path.get(slash + 1..)?;
    if name.is_empty() {
        return None;
    }
    let parent = if slash == 0 { "/" } else { &path[..slash] };
    Some((parent.to_owned(), name.to_owned()))
}

fn child_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

fn execute(server: &Server, request: MetadataRpcRequest) -> Result<MetadataRpcResult, ServerError> {
    match request {
        MetadataRpcRequest::Batch { requests } => execute_batch(server, requests),
        MetadataRpcRequest::BootstrapRoot { mode, uid, gid } => {
            let attr = server.service().bootstrap_root(mode, uid, gid)?;
            Ok(MetadataRpcResult::InodeAttr {
                attr: Some(nokvfs_protocol::WireInodeAttr::from_inode_attr(&attr)),
            })
        }
        MetadataRpcRequest::GetAttr { inode } => {
            let attr = server.service().get_attr(inode_id(inode)?)?;
            Ok(MetadataRpcResult::InodeAttr {
                attr: attr
                    .as_ref()
                    .map(nokvfs_protocol::WireInodeAttr::from_inode_attr),
            })
        }
        MetadataRpcRequest::LookupPlus { parent, name } => {
            let entry = server
                .service()
                .lookup_plus(inode_id(parent)?, &dentry_name(name)?)?;
            Ok(MetadataRpcResult::Dentry {
                entry: entry.as_ref().map(|entry| Box::new(wire_dentry(entry))),
            })
        }
        MetadataRpcRequest::LookupPath { path } => {
            let entry = server.service().lookup_path(&path)?;
            Ok(MetadataRpcResult::Dentry {
                entry: entry.as_ref().map(|entry| Box::new(wire_dentry(entry))),
            })
        }
        MetadataRpcRequest::StatPath { path } => {
            let metadata = server.service().stat_path(&path)?;
            Ok(MetadataRpcResult::PathMetadata {
                metadata: metadata.as_ref().map(WirePathMetadata::from_path_metadata),
            })
        }
        MetadataRpcRequest::ReadDirPlus { parent } => {
            let entries = server.service().read_dir_plus(inode_id(parent)?)?;
            Ok(MetadataRpcResult::Dentries {
                entries: entries.iter().map(wire_dentry).collect(),
            })
        }
        MetadataRpcRequest::ReadDirPlusPath { path } => {
            let entries = server.service().read_dir_plus_path(&path)?;
            Ok(MetadataRpcResult::Dentries {
                entries: entries.iter().map(wire_dentry).collect(),
            })
        }
        MetadataRpcRequest::StatCard { path } => {
            let card = server.service().stat_card(&path)?;
            Ok(MetadataRpcResult::NamespaceCard {
                card: card
                    .as_ref()
                    .map(|card| Box::new(wire_namespace_card(card))),
            })
        }
        MetadataRpcRequest::ListPage {
            path,
            cursor,
            limit,
        } => {
            let limit = usize::try_from(limit).map_err(|_| {
                ServerError::Metadata(MetadError::InvalidQuery(
                    "namespace list limit exceeds platform limit".to_owned(),
                ))
            })?;
            let page = server
                .service()
                .list_page(&path, nokvfs_meta::NamespaceListOptions { cursor, limit })?;
            Ok(MetadataRpcResult::NamespaceListPage {
                page: Box::new(wire_namespace_list_page(&page)?),
            })
        }
        MetadataRpcRequest::FindPaths { request } => {
            let result = server
                .service()
                .find_paths(namespace_find_request(*request)?)?;
            Ok(MetadataRpcResult::NamespaceFindResult {
                result: Box::new(wire_namespace_find_result(&result)?),
            })
        }
        MetadataRpcRequest::GrepPaths { request } => {
            let result = server
                .service()
                .grep_paths(namespace_grep_request(*request)?)?;
            Ok(MetadataRpcResult::NamespaceGrepResult {
                result: Box::new(wire_namespace_grep_result(&result)?),
            })
        }
        MetadataRpcRequest::ReadPage { path, options } => {
            let page = server
                .service()
                .read_page(&path, namespace_read_options(*options)?)?;
            Ok(MetadataRpcResult::NamespaceReadPage {
                page: Box::new(wire_namespace_read_page(&page)?),
            })
        }
        MetadataRpcRequest::CreateDir {
            parent,
            name,
            mode,
            uid,
            gid,
        } => {
            let entry = server.service().create_dir(
                inode_id(parent)?,
                dentry_name(name)?,
                mode,
                uid,
                gid,
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::CreateDirPath {
            path,
            mode,
            uid,
            gid,
        } => {
            let entry = server.service().create_dir_path(&path, mode, uid, gid)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::CreateFile {
            parent,
            name,
            mode,
            uid,
            gid,
        } => {
            let entry = server.service().create_file(
                inode_id(parent)?,
                dentry_name(name)?,
                mode,
                uid,
                gid,
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::CreateFilePath {
            path,
            mode,
            uid,
            gid,
        } => {
            let entry = server.service().create_file_path(&path, mode, uid, gid)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::CreateFilesInDirPath {
            parent_path,
            names,
            mode,
            uid,
            gid,
        } => Ok(MetadataRpcResult::Batch {
            results: create_files_in_dir_path_envelopes(
                server,
                &parent_path,
                names,
                mode,
                uid,
                gid,
            ),
        }),
        MetadataRpcRequest::RemoveFile { parent, name } => {
            let entry = server
                .service()
                .remove_file(inode_id(parent)?, &dentry_name(name)?)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::RemoveFilePath { path } => {
            let entry = server.service().remove_file_path(&path)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::RemoveEmptyDir { parent, name } => {
            let entry = server
                .service()
                .remove_empty_dir(inode_id(parent)?, &dentry_name(name)?)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::RemoveEmptyDirPath { path } => {
            let entry = server.service().remove_empty_dir_path(&path)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::Rename {
            parent,
            name,
            new_parent,
            new_name,
        } => {
            let entry = server.service().rename(
                inode_id(parent)?,
                &dentry_name(name)?,
                inode_id(new_parent)?,
                dentry_name(new_name)?,
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::RenamePath {
            source,
            destination,
        } => {
            let entry = server.service().rename_path(&source, &destination)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::RenameReplace {
            parent,
            name,
            new_parent,
            new_name,
        } => {
            let result = server.service().rename_replace(
                inode_id(parent)?,
                &dentry_name(name)?,
                inode_id(new_parent)?,
                dentry_name(new_name)?,
            )?;
            Ok(MetadataRpcResult::RenameReplace {
                entry: Box::new(wire_dentry(&result.entry)),
                replaced: result
                    .replaced
                    .as_ref()
                    .map(|entry| Box::new(wire_dentry(entry))),
            })
        }
        MetadataRpcRequest::RenameReplacePath {
            source,
            destination,
        } => {
            let result = server
                .service()
                .rename_replace_path(&source, &destination)?;
            Ok(MetadataRpcResult::RenameReplace {
                entry: Box::new(wire_dentry(&result.entry)),
                replaced: result
                    .replaced
                    .as_ref()
                    .map(|entry| Box::new(wire_dentry(entry))),
            })
        }
        MetadataRpcRequest::SnapshotSubtree { root } => {
            let snapshot = server.service().snapshot_subtree(inode_id(root)?)?;
            Ok(MetadataRpcResult::Snapshot {
                snapshot: nokvfs_protocol::WireSnapshotPin::from_snapshot_pin(&snapshot),
            })
        }
        MetadataRpcRequest::SnapshotSubtreePath { path } => {
            let snapshot = server.service().snapshot_subtree_path(&path)?;
            Ok(MetadataRpcResult::Snapshot {
                snapshot: nokvfs_protocol::WireSnapshotPin::from_snapshot_pin(&snapshot),
            })
        }
        MetadataRpcRequest::StatPathAtSnapshot { snapshot_id, path } => {
            let metadata = server.service().stat_path_at_snapshot(snapshot_id, &path)?;
            Ok(MetadataRpcResult::PathMetadata {
                metadata: metadata.as_ref().map(WirePathMetadata::from_path_metadata),
            })
        }
        MetadataRpcRequest::ReadDirPlusPathAtSnapshot { snapshot_id, path } => {
            let entries = server
                .service()
                .read_dir_plus_path_at_snapshot(snapshot_id, &path)?;
            Ok(MetadataRpcResult::Dentries {
                entries: entries.iter().map(wire_dentry).collect(),
            })
        }
        MetadataRpcRequest::RetireSnapshot { snapshot_id } => {
            let retired = server.service().retire_snapshot(snapshot_id)?;
            Ok(MetadataRpcResult::RetiredSnapshot { retired })
        }
        MetadataRpcRequest::ReadBodyPlan {
            inode,
            generation,
            offset,
            len,
        } => {
            let len = usize::try_from(len).map_err(|_| {
                ServerError::Metadata(MetadError::Codec(
                    "body read length exceeds platform limit".to_owned(),
                ))
            })?;
            let plan =
                server
                    .service()
                    .read_file_plan(inode_id(inode)?, generation, offset, len)?;
            Ok(MetadataRpcResult::BodyReadPlan {
                plan: wire_body_read_plan(&plan),
            })
        }
        MetadataRpcRequest::ReadArtifactPathAtSnapshot { snapshot_id, path } => {
            let bytes = server
                .service()
                .read_artifact_path_at_snapshot(snapshot_id, &path)?;
            Ok(MetadataRpcResult::FileBytes { bytes })
        }
        MetadataRpcRequest::ReadFilePathAtSnapshot {
            snapshot_id,
            path,
            offset,
            len,
        } => {
            let len = usize::try_from(len).map_err(|_| {
                ServerError::Metadata(MetadError::Codec(
                    "snapshot read length exceeds platform limit".to_owned(),
                ))
            })?;
            let bytes =
                server
                    .service()
                    .read_file_path_at_snapshot(snapshot_id, &path, offset, len)?;
            Ok(MetadataRpcResult::FileBytes { bytes })
        }
        MetadataRpcRequest::PrepareArtifact {
            parent,
            name,
            replace,
        } => {
            let name = dentry_name(name)?;
            let prepared = if replace {
                server
                    .service()
                    .prepare_artifact_replace(inode_id(parent)?, name)?
            } else {
                server
                    .service()
                    .prepare_artifact_create(inode_id(parent)?, name)?
            };
            Ok(MetadataRpcResult::PreparedArtifact {
                prepared: wire_prepared_artifact(server.service().mount_id(), &prepared),
            })
        }
        MetadataRpcRequest::PrepareArtifactPath { path, replace } => {
            let prepared = if replace {
                server.service().prepare_artifact_replace_path(&path)?
            } else {
                server.service().prepare_artifact_create_path(&path)?
            };
            Ok(MetadataRpcResult::PreparedArtifact {
                prepared: wire_prepared_artifact(server.service().mount_id(), &prepared),
            })
        }
        MetadataRpcRequest::PublishPreparedArtifact {
            prepared,
            body,
            chunks,
            mode,
            uid,
            gid,
        } => {
            if prepared.mount != server.service().mount_id().get() {
                return Err(ServerError::Metadata(MetadError::Codec(
                    "prepared artifact mount does not match server mount".to_owned(),
                )));
            }
            let result = server.service().publish_prepared_artifact(
                prepared_artifact(prepared)?,
                (*body).into_body_descriptor(),
                chunks
                    .into_iter()
                    .map(|chunk| chunk.into_chunk_manifest().map_err(protocol_error))
                    .collect::<Result<Vec<_>, _>>()?,
                mode,
                uid,
                gid,
            )?;
            Ok(MetadataRpcResult::RenameReplace {
                entry: Box::new(wire_dentry(&result.entry)),
                replaced: result
                    .replaced
                    .as_ref()
                    .map(|entry| Box::new(wire_dentry(entry))),
            })
        }
    }
}

fn inode_id(raw: u64) -> Result<InodeId, MetadError> {
    InodeId::new(raw).map_err(Into::into)
}

fn dentry_name(name: String) -> Result<DentryName, MetadError> {
    DentryName::new(name.into_bytes()).map_err(|err| MetadError::Codec(err.to_string()))
}

fn wire_dentry(entry: &DentryWithAttr) -> WireDentryWithAttr {
    WireDentryWithAttr {
        dentry: nokvfs_protocol::WireDentryRecord::from_dentry_record(&entry.dentry),
        attr: nokvfs_protocol::WireInodeAttr::from_inode_attr(&entry.attr),
        body: entry
            .body
            .as_ref()
            .map(nokvfs_protocol::WireBodyDescriptor::from_body_descriptor),
    }
}

fn wire_prepared_artifact(mount: MountId, prepared: &PreparedArtifact) -> WirePreparedArtifact {
    WirePreparedArtifact {
        mount: mount.get(),
        parent: prepared.parent.get(),
        name: String::from_utf8(prepared.name.as_bytes().to_vec())
            .expect("metadata prepared artifact names are utf-8"),
        inode: prepared.inode.get(),
        generation: prepared.generation,
        mtime_ms: prepared.mtime_ms,
        ctime_ms: prepared.ctime_ms,
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
    }
}

fn prepared_artifact(prepared: WirePreparedArtifact) -> Result<PreparedArtifact, MetadError> {
    MountId::new(prepared.mount)?;
    Ok(PreparedArtifact {
        parent: inode_id(prepared.parent)?,
        name: dentry_name(prepared.name)?,
        inode: inode_id(prepared.inode)?,
        generation: prepared.generation,
        mtime_ms: prepared.mtime_ms,
        ctime_ms: prepared.ctime_ms,
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
    })
}

fn wire_body_read_plan(plan: &nokvfs_meta::BodyReadPlan) -> WireBodyReadPlan {
    WireBodyReadPlan {
        output_len: plan.output_len as u64,
        blocks: plan.blocks.iter().map(wire_object_read_block).collect(),
    }
}

fn wire_namespace_card(card: &NamespaceCard) -> WireNamespaceCard {
    WireNamespaceCard {
        path: card.path.clone(),
        name: card.name.clone(),
        kind: wire_namespace_card_kind(&card.kind),
        evidence: card.evidence.clone(),
        snapshot_id: card.snapshot_id,
        inode: card.inode.get(),
        generation: card.generation,
        size_bytes: card.size_bytes,
        entry_count: card.entry_count.map(|count| count as u64),
        record_count: card.record_count.as_ref().map(wire_namespace_record_count),
        schema: card.schema.as_ref().map(wire_namespace_schema),
        sample: card.sample.clone(),
        body: card
            .body
            .as_ref()
            .map(|body| nokvfs_protocol::WireBodyDescriptor {
                producer: body.producer.clone(),
                digest_uri: body.digest_uri.clone(),
                size: body.size,
                content_type: body.content_type.clone(),
                manifest_id: body.manifest_id.clone(),
                generation: body.generation,
                chunk_size: body.chunk_size,
                block_size: body.block_size,
            }),
        catalog: wire_namespace_query_catalog(&card.catalog),
        indexed_values: card
            .indexed_values
            .iter()
            .map(wire_namespace_index_value)
            .collect(),
    }
}

fn wire_namespace_card_kind(kind: &NamespaceCardKind) -> WireNamespaceCardKind {
    match kind {
        NamespaceCardKind::File => WireNamespaceCardKind::File,
        NamespaceCardKind::Directory => WireNamespaceCardKind::Directory,
        NamespaceCardKind::Symlink => WireNamespaceCardKind::Symlink,
    }
}

fn wire_namespace_record_count(count: &NamespaceRecordCount) -> WireNamespaceRecordCount {
    WireNamespaceRecordCount {
        count: count.count as u64,
        provenance: match count.provenance {
            RecordCountProvenance::LiveNamespace => WireRecordCountProvenance::LiveNamespace,
            RecordCountProvenance::StructuredBody => WireRecordCountProvenance::StructuredBody,
            RecordCountProvenance::MaterializedIndex => {
                WireRecordCountProvenance::MaterializedIndex
            }
            RecordCountProvenance::Approximate => WireRecordCountProvenance::Approximate,
        },
    }
}

fn wire_namespace_schema(schema: &NamespaceSchema) -> WireNamespaceSchema {
    WireNamespaceSchema {
        record_type: wire_namespace_record_type(&schema.record_type),
        fields: schema.fields.clone(),
    }
}

fn wire_namespace_record_type(record_type: &NamespaceRecordType) -> WireNamespaceRecordType {
    match record_type {
        NamespaceRecordType::DirectoryEntries => WireNamespaceRecordType::DirectoryEntries,
        NamespaceRecordType::JsonArray => WireNamespaceRecordType::JsonArray,
        NamespaceRecordType::JsonObject => WireNamespaceRecordType::JsonObject,
        NamespaceRecordType::YamlMapping => WireNamespaceRecordType::YamlMapping,
        NamespaceRecordType::TextLines => WireNamespaceRecordType::TextLines,
    }
}

fn wire_namespace_query_catalog(catalog: &NamespaceQueryCatalog) -> WireNamespaceQueryCatalog {
    WireNamespaceQueryCatalog {
        filterable: catalog
            .filterable
            .iter()
            .map(wire_namespace_filter_capability)
            .collect(),
        sortable: catalog
            .sortable
            .iter()
            .map(wire_namespace_sort_field)
            .collect(),
        facetable: catalog
            .facetable
            .iter()
            .map(wire_namespace_find_field)
            .collect(),
        projections: catalog
            .projections
            .iter()
            .map(wire_namespace_include)
            .collect(),
    }
}

fn wire_namespace_filter_capability(
    capability: &NamespaceFilterCapability,
) -> WireNamespaceFilterCapability {
    WireNamespaceFilterCapability {
        field: wire_namespace_find_field(&capability.field),
        operators: capability
            .operators
            .iter()
            .map(wire_namespace_predicate_op)
            .collect(),
    }
}

fn wire_namespace_include(include: &NamespaceInclude) -> WireNamespaceInclude {
    match include {
        NamespaceInclude::Body => WireNamespaceInclude::Body,
        NamespaceInclude::Schema => WireNamespaceInclude::Schema,
        NamespaceInclude::Sample => WireNamespaceInclude::Sample,
        NamespaceInclude::Catalog => WireNamespaceInclude::Catalog,
    }
}

fn wire_namespace_find_field(field: &NamespaceFindField) -> WireNamespaceFindField {
    WireNamespaceFindField {
        id: field.id.clone(),
    }
}

fn wire_namespace_predicate_op(op: &NamespacePredicateOp) -> WireNamespacePredicateOp {
    match op {
        NamespacePredicateOp::Eq => WireNamespacePredicateOp::Eq,
        NamespacePredicateOp::Prefix => WireNamespacePredicateOp::Prefix,
        NamespacePredicateOp::Suffix => WireNamespacePredicateOp::Suffix,
        NamespacePredicateOp::Contains => WireNamespacePredicateOp::Contains,
        NamespacePredicateOp::GreaterThan => WireNamespacePredicateOp::GreaterThan,
        NamespacePredicateOp::GreaterThanOrEqual => WireNamespacePredicateOp::GreaterThanOrEqual,
        NamespacePredicateOp::LessThan => WireNamespacePredicateOp::LessThan,
        NamespacePredicateOp::LessThanOrEqual => WireNamespacePredicateOp::LessThanOrEqual,
    }
}

fn wire_namespace_sort_field(field: &NamespaceSortField) -> WireNamespaceSortField {
    WireNamespaceSortField {
        id: field.id.clone(),
    }
}

fn wire_namespace_index_value(value: &NamespaceIndexValue) -> WireNamespaceIndexValue {
    WireNamespaceIndexValue {
        field: wire_namespace_find_field(&value.field),
        value: wire_namespace_predicate_value(&value.value),
    }
}

fn wire_namespace_predicate_value(value: &NamespacePredicateValue) -> WireNamespacePredicateValue {
    match value {
        NamespacePredicateValue::String(value) => {
            WireNamespacePredicateValue::String(value.clone())
        }
        NamespacePredicateValue::U64(value) => WireNamespacePredicateValue::U64(*value),
    }
}

fn wire_namespace_list_page(
    page: &NamespaceListPage,
) -> Result<WireNamespaceListPage, ServerError> {
    Ok(WireNamespaceListPage {
        path: page.path.clone(),
        evidence: page.evidence.clone(),
        snapshot_id: page.snapshot_id,
        entry_count: page.entry_count as u64,
        entries: page.entries.iter().map(wire_namespace_card).collect(),
        next_cursor: page.next_cursor.clone(),
        truncated: page.truncated,
    })
}

fn wire_namespace_find_result(
    result: &NamespaceFindResult,
) -> Result<WireNamespaceFindResult, ServerError> {
    Ok(WireNamespaceFindResult {
        path: result.path.clone(),
        evidence: result.evidence.clone(),
        snapshot_id: result.snapshot_id,
        match_count: result.match_count as u64,
        matches: result.matches.iter().map(wire_namespace_card).collect(),
        next_cursor: result.next_cursor.clone(),
        truncated: result.truncated,
        scanned_entries: result.scanned_entries as u64,
    })
}

fn wire_namespace_grep_result(
    result: &NamespaceGrepResult,
) -> Result<WireNamespaceGrepResult, ServerError> {
    Ok(WireNamespaceGrepResult {
        path: result.path.clone(),
        pattern: result.pattern.clone(),
        recursive: result.recursive,
        evidence: result.evidence.clone(),
        snapshot_id: result.snapshot_id,
        matches: result
            .matches
            .iter()
            .map(wire_namespace_grep_match)
            .collect(),
        files_scanned: result.files_scanned as u64,
        bytes_read: result.bytes_read as u64,
        next_cursor: result.next_cursor.clone(),
        truncated: result.truncated,
    })
}

fn wire_namespace_grep_match(match_: &NamespaceGrepMatch) -> WireNamespaceGrepMatch {
    WireNamespaceGrepMatch {
        path: match_.path.clone(),
        line_number: match_.line_number as u64,
        snippet: match_.snippet.clone(),
        evidence: match_.evidence.clone(),
        generation: match_.generation,
    }
}

fn wire_namespace_read_page(
    page: &NamespaceReadPage,
) -> Result<WireNamespaceReadPage, ServerError> {
    Ok(WireNamespaceReadPage {
        path: page.path.clone(),
        evidence: page.evidence.clone(),
        snapshot_id: page.snapshot_id,
        generation: page.generation,
        total_size_bytes: page.total_size_bytes,
        format: match page.format {
            NamespaceReadFormat::Structured => WireNamespaceReadFormat::Structured,
            NamespaceReadFormat::Bytes => WireNamespaceReadFormat::Bytes,
        },
        record_type: page.record_type.as_ref().map(wire_namespace_record_type),
        record_count: page.record_count.map(|count| count as u64),
        cursor: page.cursor.clone(),
        next_cursor: page.next_cursor.clone(),
        truncated: page.truncated,
        items: page
            .items
            .iter()
            .map(|item| WireNamespaceReadItem {
                index: item.index as u64,
                value_json: item.value_json.clone(),
                evidence: item.evidence.clone(),
            })
            .collect(),
        bytes: page.bytes.clone(),
    })
}

fn namespace_find_request(
    request: WireNamespaceFindRequest,
) -> Result<NamespaceFindRequest, ServerError> {
    Ok(NamespaceFindRequest {
        path: request.path,
        predicates: request
            .predicates
            .into_iter()
            .map(namespace_predicate)
            .collect::<Result<Vec<_>, _>>()?,
        sort: request
            .sort
            .into_iter()
            .map(namespace_sort)
            .collect::<Vec<_>>(),
        include: request
            .include
            .into_iter()
            .map(namespace_include)
            .collect::<Vec<_>>(),
        cursor: request.cursor,
        limit: usize::try_from(request.limit).map_err(|_| {
            ServerError::Metadata(MetadError::InvalidQuery(
                "namespace find limit exceeds platform limit".to_owned(),
            ))
        })?,
    })
}

fn namespace_grep_request(
    request: WireNamespaceGrepRequest,
) -> Result<NamespaceGrepRequest, ServerError> {
    Ok(NamespaceGrepRequest {
        path: request.path,
        pattern: request.pattern,
        recursive: request.recursive,
        cursor: request.cursor,
        limit: usize::try_from(request.limit).map_err(|_| {
            ServerError::Metadata(MetadError::InvalidQuery(
                "namespace grep limit exceeds platform limit".to_owned(),
            ))
        })?,
        max_files: request
            .max_files
            .map(|value| {
                usize::try_from(value).map_err(|_| {
                    ServerError::Metadata(MetadError::InvalidQuery(
                        "namespace grep max_files exceeds platform limit".to_owned(),
                    ))
                })
            })
            .transpose()?,
        max_bytes: request
            .max_bytes
            .map(|value| {
                usize::try_from(value).map_err(|_| {
                    ServerError::Metadata(MetadError::InvalidQuery(
                        "namespace grep max_bytes exceeds platform limit".to_owned(),
                    ))
                })
            })
            .transpose()?,
    })
}

fn namespace_include(include: WireNamespaceInclude) -> NamespaceInclude {
    match include {
        WireNamespaceInclude::Body => NamespaceInclude::Body,
        WireNamespaceInclude::Schema => NamespaceInclude::Schema,
        WireNamespaceInclude::Sample => NamespaceInclude::Sample,
        WireNamespaceInclude::Catalog => NamespaceInclude::Catalog,
    }
}

fn namespace_predicate(
    predicate: WireNamespacePredicate,
) -> Result<NamespacePredicate, ServerError> {
    Ok(NamespacePredicate {
        field: namespace_find_field(predicate.field),
        op: namespace_predicate_op(predicate.op),
        value: match predicate.value {
            WireNamespacePredicateValue::String(value) => NamespacePredicateValue::String(value),
            WireNamespacePredicateValue::U64(value) => NamespacePredicateValue::U64(value),
        },
    })
}

fn namespace_find_field(field: WireNamespaceFindField) -> NamespaceFindField {
    NamespaceFindField::new(field.id)
}

fn namespace_predicate_op(op: WireNamespacePredicateOp) -> NamespacePredicateOp {
    match op {
        WireNamespacePredicateOp::Eq => NamespacePredicateOp::Eq,
        WireNamespacePredicateOp::Prefix => NamespacePredicateOp::Prefix,
        WireNamespacePredicateOp::Suffix => NamespacePredicateOp::Suffix,
        WireNamespacePredicateOp::Contains => NamespacePredicateOp::Contains,
        WireNamespacePredicateOp::GreaterThan => NamespacePredicateOp::GreaterThan,
        WireNamespacePredicateOp::GreaterThanOrEqual => NamespacePredicateOp::GreaterThanOrEqual,
        WireNamespacePredicateOp::LessThan => NamespacePredicateOp::LessThan,
        WireNamespacePredicateOp::LessThanOrEqual => NamespacePredicateOp::LessThanOrEqual,
    }
}

fn namespace_sort(sort: WireNamespaceSort) -> NamespaceSort {
    NamespaceSort {
        field: NamespaceSortField::new(sort.field.id),
        direction: match sort.direction {
            WireNamespaceSortDirection::Asc => NamespaceSortDirection::Asc,
            WireNamespaceSortDirection::Desc => NamespaceSortDirection::Desc,
        },
    }
}

fn namespace_read_options(
    options: WireNamespaceReadOptions,
) -> Result<NamespaceReadOptions, ServerError> {
    Ok(NamespaceReadOptions {
        format: match options.format {
            WireNamespaceReadFormat::Structured => NamespaceReadFormat::Structured,
            WireNamespaceReadFormat::Bytes => NamespaceReadFormat::Bytes,
        },
        cursor: options.cursor,
        offset: options.offset,
        limit: usize::try_from(options.limit).map_err(|_| {
            ServerError::Metadata(MetadError::InvalidQuery(
                "namespace read limit exceeds platform limit".to_owned(),
            ))
        })?,
        expected_generation: options.expected_generation,
    })
}

fn wire_object_read_block(block: &ObjectReadBlock) -> WireObjectReadBlock {
    WireObjectReadBlock {
        object_key: block.object_key.clone(),
        object_offset: block.object_offset,
        len: block.len as u64,
        output_offset: block.output_offset as u64,
    }
}

fn protocol_error(err: MetadataProtocolError) -> MetadError {
    MetadError::Codec(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::test_server;
    use nokvfs_protocol::{
        decode_envelope, encode_request, WireBlockDescriptor, WireBodyDescriptor,
        WireChunkManifest, WireMetadataError,
    };

    fn request_envelope(server: &Server, request: MetadataRpcRequest) -> MetadataRpcEnvelope {
        let body = encode_request(&request).unwrap();
        let response = handle_binary_rpc(server, &body).unwrap();
        decode_envelope(&response).unwrap()
    }

    fn expect_dentry(envelope: MetadataRpcEnvelope) -> WireDentryWithAttr {
        assert!(envelope.ok, "unexpected error envelope: {envelope:?}");
        match envelope.result.unwrap() {
            MetadataRpcResult::Dentry { entry: Some(entry) } => *entry,
            other => panic!("unexpected dentry result: {other:?}"),
        }
    }

    #[test]
    fn rpc_creates_and_lists_directory() {
        let server = test_server();
        let created = expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::CreateDir {
                parent: 1,
                name: "runs".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        ));
        assert_eq!(created.dentry.name_hex, "72756e73");

        let envelope = request_envelope(&server, MetadataRpcRequest::ReadDirPlus { parent: 1 });
        let entries = match envelope.result.unwrap() {
            MetadataRpcResult::Dentries { entries } => entries,
            other => panic!("unexpected readdir result: {other:?}"),
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].dentry.name_hex, "72756e73");
    }

    #[test]
    fn rpc_path_ops_resolve_on_server_side() {
        let server = test_server();
        let dir = expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::CreateDirPath {
                path: "/runs".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        ));
        assert_eq!(dir.dentry.name_hex, "72756e73");
        let file = expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::CreateFilePath {
                path: "/runs/checkpoint.bin".to_owned(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        ));
        assert_eq!(file.dentry.name_hex, "636865636b706f696e742e62696e");

        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::ReadDirPlusPath {
                path: "/runs".to_owned(),
            },
        );
        let entries = match envelope.result.unwrap() {
            MetadataRpcResult::Dentries { entries } => entries,
            other => panic!("unexpected readdir result: {other:?}"),
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].dentry.name_hex, "636865636b706f696e742e62696e");
    }

    #[test]
    fn rpc_namespace_card_and_list_page_use_service_surface() {
        let server = test_server();
        request_envelope(
            &server,
            MetadataRpcRequest::CreateDirPath {
                path: "/runs".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        );
        request_envelope(
            &server,
            MetadataRpcRequest::CreateFilePath {
                path: "/runs/a.bin".to_owned(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        );
        request_envelope(
            &server,
            MetadataRpcRequest::CreateFilePath {
                path: "/runs/b.bin".to_owned(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        );

        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::StatCard {
                path: "/runs".to_owned(),
            },
        );
        let card = match envelope.result.unwrap() {
            MetadataRpcResult::NamespaceCard { card: Some(card) } => card,
            other => panic!("unexpected namespace card result: {other:?}"),
        };
        assert_eq!(card.path, "/runs");
        assert_eq!(card.entry_count, Some(2));
        assert_eq!(
            card.record_count.as_ref().unwrap().provenance,
            nokvfs_protocol::WireRecordCountProvenance::LiveNamespace
        );
        assert!(card
            .catalog
            .sortable
            .iter()
            .any(|field| field.id == "size_bytes"));
        assert!(card
            .catalog
            .projections
            .contains(&nokvfs_protocol::WireNamespaceInclude::Body));

        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::ListPage {
                path: "/runs".to_owned(),
                cursor: None,
                limit: 1,
            },
        );
        let page = match envelope.result.unwrap() {
            MetadataRpcResult::NamespaceListPage { page } => page,
            other => panic!("unexpected namespace list result: {other:?}"),
        };
        assert_eq!(page.entry_count, 2);
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.next_cursor, Some("1".to_owned()));
        assert!(page.truncated);
    }

    #[test]
    fn rpc_namespace_grep_routes_to_service_surface() {
        let server = test_server();
        request_envelope(
            &server,
            MetadataRpcRequest::CreateFilePath {
                path: "/empty.txt".to_owned(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        );

        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::GrepPaths {
                request: Box::new(nokvfs_protocol::WireNamespaceGrepRequest {
                    path: "/".to_owned(),
                    pattern: "needle".to_owned(),
                    recursive: true,
                    cursor: None,
                    limit: 10,
                    max_files: None,
                    max_bytes: None,
                }),
            },
        );
        let result = match envelope.result.unwrap() {
            MetadataRpcResult::NamespaceGrepResult { result } => result,
            other => panic!("unexpected namespace grep result: {other:?}"),
        };
        assert_eq!(result.path, "/");
        assert_eq!(result.pattern, "needle");
        assert!(result.matches.is_empty());
        assert!(!result.truncated);
    }

    #[test]
    fn rpc_batch_preserves_ordered_per_request_results() {
        let server = test_server();
        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::Batch {
                requests: vec![
                    MetadataRpcRequest::CreateDirPath {
                        path: "/runs".to_owned(),
                        mode: 0o755,
                        uid: 1000,
                        gid: 1000,
                    },
                    MetadataRpcRequest::CreateFilePath {
                        path: "/runs/a.bin".to_owned(),
                        mode: 0o644,
                        uid: 1000,
                        gid: 1000,
                    },
                    MetadataRpcRequest::CreateFilePath {
                        path: "/runs/a.bin".to_owned(),
                        mode: 0o644,
                        uid: 1000,
                        gid: 1000,
                    },
                ],
            },
        );
        let results = match envelope.result.unwrap() {
            MetadataRpcResult::Batch { results } => results,
            other => panic!("unexpected batch result: {other:?}"),
        };
        assert_eq!(results.len(), 3);
        assert!(results[0].ok);
        assert!(results[1].ok);
        assert!(!results[2].ok);
        assert!(results[2].error.is_some());
        assert_eq!(
            results[2].error_kind,
            Some(WireMetadataError::PredicateFailed)
        );
    }

    #[test]
    fn rpc_reports_predicate_errors_without_panicking() {
        let server = test_server();
        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::RemoveEmptyDir {
                parent: 1,
                name: "missing".to_owned(),
            },
        );
        assert!(!envelope.ok);
        assert!(envelope.error.is_some());
    }

    #[test]
    fn rpc_rejects_malformed_binary_request() {
        let server = test_server();
        let response = handle_binary_rpc(&server, b"not-msgpack").unwrap();
        let envelope = decode_envelope(&response).unwrap();
        assert!(!envelope.ok);
        assert!(matches!(
            envelope.error_kind,
            Some(WireMetadataError::Protocol { .. })
        ));
    }

    #[test]
    fn framed_rpc_worker_count_is_bounded() {
        let workers = default_framed_rpc_worker_count();
        assert!(workers >= MIN_FRAMED_RPC_WORKERS);
        assert!(workers <= MAX_FRAMED_RPC_WORKERS);
    }

    #[test]
    fn rpc_prepares_and_publishes_artifact_manifest() {
        let server = test_server();
        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::PrepareArtifact {
                parent: 1,
                name: "artifact.bin".to_owned(),
                replace: false,
            },
        );
        let prepared = match envelope.result.unwrap() {
            MetadataRpcResult::PreparedArtifact { prepared } => prepared,
            other => panic!("unexpected prepare result: {other:?}"),
        };
        let request = MetadataRpcRequest::PublishPreparedArtifact {
            body: Box::new(WireBodyDescriptor {
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:body".to_owned(),
                size: 4,
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "artifact.bin".to_owned(),
                generation: prepared.generation,
                chunk_size: 64 * 1024 * 1024,
                block_size: 4 * 1024 * 1024,
            }),
            chunks: vec![WireChunkManifest {
                chunk_index: 0,
                logical_offset: 0,
                len: 4,
                blocks: vec![WireBlockDescriptor {
                    object_key: format!("blocks/1/{}/{}", prepared.inode, prepared.generation),
                    logical_offset: 0,
                    object_offset: 0,
                    len: 4,
                    digest_uri: "sha256:block".to_owned(),
                }],
            }],
            prepared,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        };
        let envelope = request_envelope(&server, request);
        let result = match envelope.result.unwrap() {
            MetadataRpcResult::RenameReplace { entry, replaced } => (entry, replaced),
            other => panic!("unexpected publish result: {other:?}"),
        };
        assert_eq!(result.0.dentry.name_hex, "61727469666163742e62696e");
        assert!(result.1.is_none());
    }
}
