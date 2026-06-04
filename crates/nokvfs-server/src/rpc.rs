use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;

use nokvfs_meta::{DentryWithAttr, MetadError, PreparedArtifact};
use nokvfs_object::ObjectReadBlock;
use nokvfs_protocol::{
    decode_request, encode_envelope, MetadataProtocolError, MetadataRpcEnvelope,
    MetadataRpcRequest, MetadataRpcResult, WireBodyReadPlan, WireDentryWithAttr,
    WireObjectReadBlock, WirePreparedArtifact,
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

pub(crate) fn handle_rpc(server: &Server, body: &[u8]) -> String {
    let envelope = match serde_json::from_slice::<MetadataRpcRequest>(body) {
        Ok(request) => match execute(server, request) {
            Ok(result) => MetadataRpcEnvelope {
                ok: true,
                result: Some(result),
                error: None,
            },
            Err(err) => MetadataRpcEnvelope {
                ok: false,
                result: None,
                error: Some(err.to_string()),
            },
        },
        Err(err) => MetadataRpcEnvelope {
            ok: false,
            result: None,
            error: Some(format!("invalid metadata rpc request: {err}")),
        },
    };
    serde_json::to_string(&envelope).expect("metadata rpc envelope is serializable") + "\n"
}

fn handle_binary_rpc(server: &Server, body: &[u8]) -> Result<Vec<u8>, ServerError> {
    let envelope = match decode_request(body) {
        Ok(request) => match execute(server, request) {
            Ok(result) => MetadataRpcEnvelope {
                ok: true,
                result: Some(result),
                error: None,
            },
            Err(err) => MetadataRpcEnvelope {
                ok: false,
                result: None,
                error: Some(err.to_string()),
            },
        },
        Err(err) => MetadataRpcEnvelope {
            ok: false,
            result: None,
            error: Some(format!("invalid metadata binary rpc request: {err}")),
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
    }
}

fn err_envelope(err: ServerError) -> MetadataRpcEnvelope {
    MetadataRpcEnvelope {
        ok: false,
        result: None,
        error: Some(err.to_string()),
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
            .expect("remote prepared artifact names are utf-8"),
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
    use nokvfs_protocol::{WireBlockDescriptor, WireBodyDescriptor, WireChunkManifest};

    #[test]
    fn rpc_creates_and_lists_directory() {
        let server = test_server();
        let response = handle_rpc(
            &server,
            br#"{"op":"create_dir","parent":1,"name":"runs","mode":493,"uid":1000,"gid":1000}"#,
        );
        assert!(response.contains("\"ok\":true"));
        assert!(response.contains("\"name_hex\":\"72756e73\""));

        let response = handle_rpc(&server, br#"{"op":"read_dir_plus","parent":1}"#);
        assert!(response.contains("\"entries\""));
        assert!(response.contains("\"name_hex\":\"72756e73\""));
    }

    #[test]
    fn rpc_path_ops_resolve_on_server_side() {
        let server = test_server();
        let response = handle_rpc(
            &server,
            br#"{"op":"create_dir_path","path":"/runs","mode":493,"uid":1000,"gid":1000}"#,
        );
        assert!(response.contains("\"ok\":true"));
        let response = handle_rpc(
            &server,
            br#"{"op":"create_file_path","path":"/runs/checkpoint.bin","mode":420,"uid":1000,"gid":1000}"#,
        );
        assert!(response.contains("\"ok\":true"));
        assert!(response.contains("\"name_hex\":\"636865636b706f696e742e62696e\""));
        let response = handle_rpc(&server, br#"{"op":"read_dir_plus_path","path":"/runs"}"#);
        assert!(response.contains("\"entries\""));
        assert!(response.contains("\"name_hex\":\"636865636b706f696e742e62696e\""));
    }

    #[test]
    fn rpc_batch_preserves_ordered_per_request_results() {
        let server = test_server();
        let response = handle_rpc(
            &server,
            br#"{"op":"batch","requests":[{"op":"create_dir_path","path":"/runs","mode":493,"uid":1000,"gid":1000},{"op":"create_file_path","path":"/runs/a.bin","mode":420,"uid":1000,"gid":1000},{"op":"create_file_path","path":"/runs/a.bin","mode":420,"uid":1000,"gid":1000}]}"#,
        );
        let envelope: MetadataRpcEnvelope = serde_json::from_str(&response).unwrap();
        let results = match envelope.result.unwrap() {
            MetadataRpcResult::Batch { results } => results,
            other => panic!("unexpected batch result: {other:?}"),
        };
        assert_eq!(results.len(), 3);
        assert!(results[0].ok);
        assert!(results[1].ok);
        assert!(!results[2].ok);
        assert!(results[2].error.is_some());
    }

    #[test]
    fn rpc_reports_predicate_errors_without_panicking() {
        let server = test_server();
        let response = handle_rpc(
            &server,
            br#"{"op":"remove_empty_dir","parent":1,"name":"missing"}"#,
        );
        assert!(response.contains("\"ok\":false"));
        assert!(response.contains("\"error\""));
    }

    #[test]
    fn rpc_rejects_malformed_json() {
        let server = test_server();
        let response = handle_rpc(&server, b"not-json");
        assert!(response.contains("\"ok\":false"));
        assert!(response.contains("invalid metadata rpc request"));
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
        let response = handle_rpc(
            &server,
            br#"{"op":"prepare_artifact","parent":1,"name":"artifact.bin","replace":false}"#,
        );
        let envelope: MetadataRpcEnvelope = serde_json::from_str(&response).unwrap();
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
        let request = serde_json::to_vec(&request).unwrap();
        let response = handle_rpc(&server, &request);
        assert!(response.contains("\"ok\":true"));
        assert!(response.contains("\"name_hex\":\"61727469666163742e62696e\""));
        assert!(response.contains("\"type\":\"rename_replace\""));
    }
}
