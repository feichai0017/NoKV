use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use nokvfs_cluster::SharedLogError;
use nokvfs_meta::{CreateInDirPathBatch, DentryWithAttr, MetadError, PreparedArtifact};
use nokvfs_object::ObjectReadBlock;
use nokvfs_protocol::{
    decode_envelope, decode_name_cursor, decode_request, encode_envelope, encode_name_cursor,
    encode_request, MetadataProtocolError, MetadataRpcEnvelope, MetadataRpcRequest,
    MetadataRpcResult, WireBodyReadPlan, WireDentryWithAttr, WireMetadataBootstrapPlan,
    WireMetadataCheckpoint, WireMetadataCheckpointInstall, WireMetadataError, WireMetadataLogEntry,
    WireMetadataPosition, WireMetadataReceipt, WireObjectReadBlock, WirePathMetadata,
    WirePreparedArtifact,
};
use nokvfs_types::{DentryName, InodeId, MountId};

use crate::server::{Server, ServerError};

pub(crate) const FRAMED_RPC_MAGIC: &[u8; 8] = b"NKVRPC3\n";
const FRAME_HEADER_BYTES: usize = 16;
const MAX_FRAMED_RPC_BYTES: usize = 16 * 1024 * 1024;
const MAX_FRAMED_RPC_BATCH: usize = 64;
const MIN_FRAMED_RPC_WORKERS: usize = 4;
const MAX_FRAMED_RPC_WORKERS: usize = 64;
const FRAMED_RPC_QUEUE_PER_WORKER: usize = 256;
const OUTBOUND_FRAMED_RPC_TIMEOUT: Duration = Duration::from_secs(5);

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
                metadata_position: server
                    .metadata_log_applied_position()
                    .map(wire_log_position),
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
            metadata_position: None,
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
        let mut frames = vec![frame];
        drain_ready_frames(&mut stream, &mut frames)?;
        let server = Arc::clone(&server);
        let writer = Arc::clone(&writer);
        framed_rpc_worker_pool().submit(Box::new(move || {
            let responses = handle_binary_rpc_frames(server.as_ref(), frames);
            for (request_id, flags, response) in responses {
                let response = match response {
                    Ok(response) => response,
                    Err(err) => match encode_server_error(&err) {
                        Ok(response) => response,
                        Err(err) => {
                            eprintln!("metadata framed rpc error encode failed: {err}");
                            return;
                        }
                    },
                };
                let mut writer = writer.lock().expect("framed rpc writer");
                if let Err(err) = write_frame(&mut writer, request_id, flags, &response) {
                    eprintln!("metadata framed rpc response write failed: {err}");
                    return;
                }
            }
        }))?;
    }
}

pub(crate) fn call_framed_rpc(
    address: SocketAddr,
    request_id: u64,
    request: &MetadataRpcRequest,
) -> Result<MetadataRpcEnvelope, ServerError> {
    let mut stream = TcpStream::connect(address).map_err(ServerError::Io)?;
    stream
        .set_read_timeout(Some(OUTBOUND_FRAMED_RPC_TIMEOUT))
        .map_err(ServerError::Io)?;
    stream
        .set_write_timeout(Some(OUTBOUND_FRAMED_RPC_TIMEOUT))
        .map_err(ServerError::Io)?;
    stream
        .write_all(FRAMED_RPC_MAGIC)
        .map_err(ServerError::Io)?;
    let payload = encode_request(request).map_err(|err| {
        ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("metadata framed rpc request encode failed: {err}"),
        ))
    })?;
    write_frame(&mut stream, request_id, 0, &payload)?;
    let Some(frame) = read_frame(&mut stream)? else {
        return Err(ServerError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "metadata framed rpc peer closed before response",
        )));
    };
    if frame.request_id != request_id {
        return Err(ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "metadata framed rpc response id {} did not match request id {}",
                frame.request_id, request_id
            ),
        )));
    }
    decode_envelope(&frame.payload).map_err(|err| {
        ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("metadata framed rpc response decode failed: {err}"),
        ))
    })
}

pub(crate) fn call_append_metadata_log(
    address: SocketAddr,
    leader: nokvfs_cluster::NodeId,
    entry: &nokvfs_cluster::MetadataLogEntry,
) -> Result<(), ServerError> {
    let encoded =
        nokvfs_cluster::encode_metadata_log_entry(entry).map_err(ServerError::SharedLog)?;
    let envelope = call_framed_rpc(
        address,
        entry.position.index.get(),
        &MetadataRpcRequest::AppendMetadataLog {
            leader: leader.get(),
            entry: encoded,
        },
    )?;
    if !envelope.ok {
        return Err(ServerError::SharedLog(SharedLogError::Backend(
            envelope
                .error
                .unwrap_or_else(|| "metadata peer append failed".to_owned()),
        )));
    }
    match envelope.result {
        Some(MetadataRpcResult::MetadataLogAppend { position, .. }) => {
            if position.term == entry.position.term.get()
                && position.index == entry.position.index.get()
            {
                Ok(())
            } else {
                Err(ServerError::SharedLog(SharedLogError::Backend(format!(
                    "metadata peer appended {}:{}, expected {}:{}",
                    position.term,
                    position.index,
                    entry.position.term.get(),
                    entry.position.index.get()
                ))))
            }
        }
        Some(other) => Err(ServerError::SharedLog(SharedLogError::Backend(format!(
            "metadata peer append returned unexpected result: {other:?}"
        )))),
        None => Err(ServerError::SharedLog(SharedLogError::Backend(
            "metadata peer append response had no result".to_owned(),
        ))),
    }
}

pub(crate) fn call_install_metadata_checkpoint(
    address: SocketAddr,
    request: nokvfs_cluster::InstallCheckpointRequest,
) -> Result<nokvfs_cluster::InstallCheckpointResponse, ServerError> {
    let learner = request.plan.node;
    let replay_start = request.plan.replay_start;
    let envelope = call_framed_rpc(
        address,
        learner.get(),
        &MetadataRpcRequest::InstallMetadataCheckpoint {
            plan: wire_metadata_bootstrap_plan(&request),
        },
    )?;
    if !envelope.ok {
        return Err(ServerError::SharedLog(SharedLogError::Backend(
            envelope
                .error
                .unwrap_or_else(|| "metadata checkpoint install failed".to_owned()),
        )));
    }
    match envelope.result {
        Some(MetadataRpcResult::MetadataCheckpointInstall { install }) => {
            let installed_learner =
                nokvfs_cluster::NodeId::new(install.learner).map_err(ServerError::SharedLog)?;
            if installed_learner != learner {
                return Err(ServerError::SharedLog(SharedLogError::Backend(format!(
                    "metadata checkpoint install returned learner {}, expected {}",
                    installed_learner.get(),
                    learner.get()
                ))));
            }
            let installed_replay_start = nokvfs_cluster::LogIndex::new(install.replay_start_index)
                .map_err(ServerError::SharedLog)?;
            if installed_replay_start != replay_start {
                return Err(ServerError::SharedLog(SharedLogError::Backend(format!(
                    "metadata checkpoint install returned replay start {}, expected {}",
                    installed_replay_start.get(),
                    replay_start.get()
                ))));
            }
            Ok(nokvfs_cluster::InstallCheckpointResponse {
                learner,
                replay_start,
                replayed_index: nokvfs_cluster::LogIndex::new(install.replayed_index)
                    .map_err(ServerError::SharedLog)?,
            })
        }
        Some(other) => Err(ServerError::SharedLog(SharedLogError::Backend(format!(
            "metadata checkpoint install returned unexpected result: {other:?}"
        )))),
        None => Err(ServerError::SharedLog(SharedLogError::Backend(
            "metadata checkpoint install response had no result".to_owned(),
        ))),
    }
}

fn drain_ready_frames(
    stream: &mut TcpStream,
    frames: &mut Vec<RpcFrame>,
) -> Result<(), ServerError> {
    stream.set_nonblocking(true).map_err(ServerError::Io)?;
    let drain = drain_ready_frames_nonblocking(stream, frames);
    let restore = stream.set_nonblocking(false).map_err(ServerError::Io);
    match (drain, restore) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(err), _) => Err(err),
        (Ok(()), Err(err)) => Err(err),
    }
}

fn drain_ready_frames_nonblocking(
    stream: &mut TcpStream,
    frames: &mut Vec<RpcFrame>,
) -> Result<(), ServerError> {
    while frames.len() < MAX_FRAMED_RPC_BATCH {
        let Some(_len) = peek_ready_frame_len(stream)? else {
            return Ok(());
        };
        let Some(frame) = read_frame(stream)? else {
            return Ok(());
        };
        frames.push(frame);
    }
    Ok(())
}

fn peek_ready_frame_len(stream: &TcpStream) -> Result<Option<usize>, ServerError> {
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    let read = match stream.peek(&mut header) {
        Ok(read) => read,
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(None),
        Err(err) if err.kind() == io::ErrorKind::Interrupted => return Ok(None),
        Err(err) => return Err(ServerError::Io(err)),
    };
    if read < FRAME_HEADER_BYTES {
        return Ok(None);
    }
    let len = u32::from_be_bytes(header[12..16].try_into().expect("length header")) as usize;
    if len > MAX_FRAMED_RPC_BYTES {
        return Err(ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata framed rpc request exceeds size limit",
        )));
    }
    let total = FRAME_HEADER_BYTES + len;
    let mut frame = vec![0_u8; total];
    match stream.peek(&mut frame) {
        Ok(read) if read >= total => Ok(Some(total)),
        Ok(_) => Ok(None),
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(None),
        Err(err) if err.kind() == io::ErrorKind::Interrupted => Ok(None),
        Err(err) => Err(ServerError::Io(err)),
    }
}

fn handle_binary_rpc_frames(
    server: &Server,
    frames: Vec<RpcFrame>,
) -> Vec<(u64, u32, Result<Vec<u8>, ServerError>)> {
    if frames.len() <= 1 {
        return frames
            .into_iter()
            .map(|frame| handle_binary_rpc_frame(server, frame))
            .collect();
    }

    let mut requests = Vec::with_capacity(frames.len());
    for frame in &frames {
        match decode_request(&frame.payload) {
            Ok(request) => requests.push(request),
            Err(_) => {
                return frames
                    .into_iter()
                    .map(|frame| handle_binary_rpc_frame(server, frame))
                    .collect();
            }
        }
    }

    let Ok(MetadataRpcResult::Batch { results }) = execute_batch(server, requests) else {
        return frames
            .into_iter()
            .map(|frame| handle_binary_rpc_frame(server, frame))
            .collect();
    };
    if results.len() != frames.len() {
        return frames
            .into_iter()
            .map(|frame| handle_binary_rpc_frame(server, frame))
            .collect();
    }

    frames
        .into_iter()
        .zip(results)
        .map(|(frame, envelope)| {
            (
                frame.request_id,
                frame.flags,
                encode_envelope(&envelope).map_err(|err| {
                    ServerError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("metadata binary rpc response encode failed: {err}"),
                    ))
                }),
            )
        })
        .collect()
}

fn handle_binary_rpc_frame(
    server: &Server,
    frame: RpcFrame,
) -> (u64, u32, Result<Vec<u8>, ServerError>) {
    (
        frame.request_id,
        frame.flags,
        handle_binary_rpc(server, &frame.payload),
    )
}

fn encode_server_error(err: &ServerError) -> Result<Vec<u8>, ServerError> {
    let envelope = MetadataRpcEnvelope {
        ok: false,
        result: None,
        error: Some(err.to_string()),
        error_kind: Some(wire_server_error(err)),
        metadata_position: None,
    };
    encode_envelope(&envelope).map_err(|err| {
        ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("metadata binary rpc response encode failed: {err}"),
        ))
    })
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
        let Some(parts) = create_path_parts(&request) else {
            results.push(execute_envelope(server, request));
            continue;
        };

        let kind = parts.kind;
        let mut groups = Vec::new();
        let mut group = CreatePathGroup::from_parts(parts);
        while let Some(next) = iter.peek() {
            let Some(next_parts) = create_path_parts(next) else {
                break;
            };
            if next_parts.kind != kind {
                break;
            }
            iter.next();
            if group.can_absorb(&next_parts) {
                group.names.push(next_parts.name);
            } else {
                groups.push(group);
                group = CreatePathGroup::from_parts(next_parts);
            }
        }
        groups.push(group);
        if groups.len() == 1 {
            let group = groups.pop().expect("one create group");
            results.extend(create_path_batch_envelopes(
                server,
                kind,
                &group.parent_path,
                group.names,
                group.mode,
                group.uid,
                group.gid,
            ));
        } else {
            results.extend(create_path_group_envelopes(server, kind, groups));
        }
    }
    Ok(MetadataRpcResult::Batch { results })
}

fn execute_envelope(server: &Server, request: MetadataRpcRequest) -> MetadataRpcEnvelope {
    match execute(server, request) {
        Ok(result) => ok_envelope(server, result),
        Err(err) => err_envelope(err),
    }
}

fn ok_envelope(server: &Server, result: MetadataRpcResult) -> MetadataRpcEnvelope {
    MetadataRpcEnvelope {
        ok: true,
        result: Some(result),
        error: None,
        error_kind: None,
        metadata_position: server
            .metadata_log_applied_position()
            .map(wire_log_position),
    }
}

fn err_envelope(err: ServerError) -> MetadataRpcEnvelope {
    let error_kind = wire_server_error(&err);
    MetadataRpcEnvelope {
        ok: false,
        result: None,
        error: Some(err.to_string()),
        error_kind: Some(error_kind),
        metadata_position: None,
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
        ServerError::SharedLog(err) => wire_shared_log_error(err),
    }
}

fn wire_shared_log_error(err: &nokvfs_cluster::SharedLogError) -> WireMetadataError {
    match err {
        nokvfs_cluster::SharedLogError::ReadNotFresh { required, applied } => {
            WireMetadataError::ReadNotFresh {
                required: wire_log_position(*required),
                applied: applied.map(wire_log_position),
            }
        }
        other => WireMetadataError::Metadata {
            message: other.to_string(),
        },
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
        MetadError::NotFound => WireMetadataError::NotFound,
        MetadError::NotFile => WireMetadataError::NotFile,
        MetadError::NotDirectory => WireMetadataError::NotDirectory,
        MetadError::MissingBodyDescriptor => WireMetadataError::MissingBodyDescriptor,
        other => WireMetadataError::Metadata {
            message: other.to_string(),
        },
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CreatePathKind {
    Directory,
    File,
}

struct CreatePathParts {
    kind: CreatePathKind,
    parent_path: String,
    name: String,
    mode: u32,
    uid: u32,
    gid: u32,
}

struct CreatePathGroup {
    parent_path: String,
    names: Vec<String>,
    mode: u32,
    uid: u32,
    gid: u32,
}

impl CreatePathGroup {
    fn from_parts(parts: CreatePathParts) -> Self {
        Self {
            parent_path: parts.parent_path,
            names: vec![parts.name],
            mode: parts.mode,
            uid: parts.uid,
            gid: parts.gid,
        }
    }

    fn can_absorb(&self, parts: &CreatePathParts) -> bool {
        self.parent_path == parts.parent_path
            && self.mode == parts.mode
            && self.uid == parts.uid
            && self.gid == parts.gid
    }
}

fn create_path_batch_envelopes(
    server: &Server,
    kind: CreatePathKind,
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
        match kind {
            CreatePathKind::Directory => {
                server
                    .service()
                    .create_dirs_in_dir_path(parent_path, names, mode, uid, gid)
            }
            CreatePathKind::File => {
                server
                    .service()
                    .create_files_in_dir_path(parent_path, names, mode, uid, gid)
            }
        }
        .map_err(ServerError::Metadata)
    });
    match coalesced {
        Ok(entries) => entries
            .iter()
            .map(|entry| {
                ok_envelope(
                    server,
                    MetadataRpcResult::Dentry {
                        entry: Some(Box::new(wire_dentry(entry))),
                    },
                )
            })
            .collect(),
        Err(_) => names
            .into_iter()
            .map(|name| {
                execute_envelope(
                    server,
                    create_path_request(kind, parent_path, &name, mode, uid, gid),
                )
            })
            .collect(),
    }
}

fn create_path_group_envelopes(
    server: &Server,
    kind: CreatePathKind,
    groups: Vec<CreatePathGroup>,
) -> Vec<MetadataRpcEnvelope> {
    let parsed = groups
        .iter()
        .map(|group| {
            let names = group
                .names
                .iter()
                .map(|name| dentry_name(name.clone()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(ServerError::Metadata)?;
            Ok(CreateInDirPathBatch {
                parent_path: group.parent_path.clone(),
                names,
                mode: group.mode,
                uid: group.uid,
                gid: group.gid,
            })
        })
        .collect::<Result<Vec<_>, ServerError>>();

    let committed = parsed.map(|batches| {
        let results: Vec<Result<Vec<DentryWithAttr>, MetadError>> = match kind {
            CreatePathKind::Directory => server.service().create_dir_batches_in_dir_path(batches),
            CreatePathKind::File => server.service().create_file_batches_in_dir_path(batches),
        };
        results
    });

    match committed {
        Ok(group_results) => groups
            .into_iter()
            .zip(group_results)
            .flat_map(|(group, result)| match result {
                Ok(entries) => entries
                    .iter()
                    .map(|entry| {
                        ok_envelope(
                            server,
                            MetadataRpcResult::Dentry {
                                entry: Some(Box::new(wire_dentry(entry))),
                            },
                        )
                    })
                    .collect::<Vec<_>>(),
                Err(_) => create_path_batch_envelopes(
                    server,
                    kind,
                    &group.parent_path,
                    group.names,
                    group.mode,
                    group.uid,
                    group.gid,
                ),
            })
            .collect(),
        Err(_) => groups
            .into_iter()
            .flat_map(|group| {
                create_path_batch_envelopes(
                    server,
                    kind,
                    &group.parent_path,
                    group.names,
                    group.mode,
                    group.uid,
                    group.gid,
                )
            })
            .collect(),
    }
}

fn create_path_parts(request: &MetadataRpcRequest) -> Option<CreatePathParts> {
    match request {
        MetadataRpcRequest::CreateDirPath {
            path,
            mode,
            uid,
            gid,
        } => create_path_parts_from_path(CreatePathKind::Directory, path, *mode, *uid, *gid),
        MetadataRpcRequest::CreateFilePath {
            path,
            mode,
            uid,
            gid,
        } => create_path_parts_from_path(CreatePathKind::File, path, *mode, *uid, *gid),
        _ => None,
    }
}

fn create_path_parts_from_path(
    kind: CreatePathKind,
    path: &str,
    mode: u32,
    uid: u32,
    gid: u32,
) -> Option<CreatePathParts> {
    let (parent_path, name) = split_parent_path(path)?;
    Some(CreatePathParts {
        kind,
        parent_path,
        name,
        mode,
        uid,
        gid,
    })
}

fn create_path_request(
    kind: CreatePathKind,
    parent_path: &str,
    name: &str,
    mode: u32,
    uid: u32,
    gid: u32,
) -> MetadataRpcRequest {
    let path = child_path(parent_path, name);
    match kind {
        CreatePathKind::Directory => MetadataRpcRequest::CreateDirPath {
            path,
            mode,
            uid,
            gid,
        },
        CreatePathKind::File => MetadataRpcRequest::CreateFilePath {
            path,
            mode,
            uid,
            gid,
        },
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
        MetadataRpcRequest::RequireApplied { position, request } => {
            server.ensure_metadata_log_applied(log_position(position)?)?;
            execute(server, *request)
        }
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
        MetadataRpcRequest::ReadDirPlusPage {
            parent,
            after_name_hex,
            limit,
        } => {
            let after = after_name_hex
                .as_deref()
                .map(decode_name_cursor)
                .transpose()
                .map_err(protocol_error)?;
            let page =
                server
                    .service()
                    .read_dir_plus_page(inode_id(parent)?, after.as_ref(), limit)?;
            Ok(MetadataRpcResult::DentriesPage {
                entries: page.entries.iter().map(wire_dentry).collect(),
                next_name_hex: page.next_cursor.as_ref().map(encode_name_cursor),
            })
        }
        MetadataRpcRequest::ReadDirPlusPath { path } => {
            let entries = server.service().read_dir_plus_path(&path)?;
            Ok(MetadataRpcResult::Dentries {
                entries: entries.iter().map(wire_dentry).collect(),
            })
        }
        MetadataRpcRequest::ReadDirPlusPathPage {
            path,
            after_name_hex,
            limit,
        } => {
            let after = after_name_hex
                .as_deref()
                .map(decode_name_cursor)
                .transpose()
                .map_err(protocol_error)?;
            let page = server
                .service()
                .read_dir_plus_path_page(&path, after.as_ref(), limit)?;
            Ok(MetadataRpcResult::DentriesPage {
                entries: page.entries.iter().map(wire_dentry).collect(),
                next_name_hex: page.next_cursor.as_ref().map(encode_name_cursor),
            })
        }
        MetadataRpcRequest::ReadIndexedPathPage {
            path,
            after_name_hex,
            limit,
        } => {
            let after = after_name_hex
                .as_deref()
                .map(decode_name_cursor)
                .transpose()
                .map_err(protocol_error)?;
            let page = server
                .service()
                .list_indexed_path_page(&path, after.as_ref(), limit)?;
            Ok(MetadataRpcResult::DentriesPage {
                entries: page.entries.iter().map(wire_dentry).collect(),
                next_name_hex: page.next_cursor.as_ref().map(encode_name_cursor),
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
            results: create_path_batch_envelopes(
                server,
                CreatePathKind::File,
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
        MetadataRpcRequest::ReadPathPlan {
            path,
            offset,
            len,
            expected_generation,
        } => {
            let len = usize::try_from(len).map_err(|_| {
                ServerError::Metadata(MetadError::Codec(
                    "path read length exceeds platform limit".to_owned(),
                ))
            })?;
            let path_plan =
                server
                    .service()
                    .read_path_plan(&path, offset, len, expected_generation)?;
            Ok(MetadataRpcResult::PathReadPlan {
                metadata: WirePathMetadata::from_path_metadata(&path_plan.metadata),
                plan: wire_body_read_plan(&path_plan.plan),
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
        MetadataRpcRequest::ReadMetadataLog { start_index, limit } => {
            let start =
                nokvfs_cluster::LogIndex::new(start_index).map_err(ServerError::SharedLog)?;
            let (entries, committed) = server.read_metadata_log_tail(start, limit)?;
            Ok(MetadataRpcResult::MetadataLogEntries {
                entries: entries
                    .iter()
                    .map(wire_metadata_log_entry)
                    .collect::<Result<Vec<_>, _>>()?,
                committed: committed.map(wire_log_position),
            })
        }
        MetadataRpcRequest::AppendMetadataLog { leader, entry } => {
            let leader = nokvfs_cluster::NodeId::new(leader).map_err(ServerError::SharedLog)?;
            let entry = nokvfs_cluster::decode_metadata_log_entry(&entry)
                .map_err(ServerError::SharedLog)?;
            let request = nokvfs_cluster::AppendMetadataBatchRequest::new(leader, entry)
                .map_err(ServerError::SharedLog)?;
            let response = server.append_metadata_log_batch(request)?;
            Ok(MetadataRpcResult::MetadataLogAppend {
                position: wire_log_position(response.position),
                receipts: response
                    .receipts
                    .iter()
                    .map(wire_metadata_receipt)
                    .collect(),
            })
        }
        MetadataRpcRequest::ReadMetadataCheckpoint { mount } => {
            let mount = MountId::new(mount).map_err(|err| {
                ServerError::Metadata(MetadError::Codec(format!(
                    "invalid metadata checkpoint mount: {err}"
                )))
            })?;
            let checkpoint = server.latest_metadata_checkpoint(mount)?;
            Ok(MetadataRpcResult::MetadataCheckpoint {
                checkpoint: checkpoint.as_ref().map(wire_metadata_checkpoint),
            })
        }
        MetadataRpcRequest::PlanMetadataBootstrap {
            leader,
            learner,
            mount,
        } => {
            let leader = nokvfs_cluster::NodeId::new(leader).map_err(ServerError::SharedLog)?;
            let learner = nokvfs_cluster::NodeId::new(learner).map_err(ServerError::SharedLog)?;
            let mount = MountId::new(mount).map_err(|err| {
                ServerError::Metadata(MetadError::Codec(format!(
                    "invalid metadata bootstrap mount: {err}"
                )))
            })?;
            let plan = server.plan_metadata_bootstrap(leader, learner, mount)?;
            Ok(MetadataRpcResult::MetadataBootstrapPlan {
                plan: wire_metadata_bootstrap_plan(&plan),
            })
        }
        MetadataRpcRequest::InstallMetadataCheckpoint { plan } => {
            let request = metadata_checkpoint_install_request(plan)?;
            let install = server.install_metadata_checkpoint(request)?;
            Ok(MetadataRpcResult::MetadataCheckpointInstall {
                install: WireMetadataCheckpointInstall {
                    learner: install.learner.get(),
                    replay_start_index: install.replay_start.get(),
                    replayed_index: install.replayed_index.get(),
                },
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
        path: prepared.path.clone(),
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
        path: prepared.path,
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

fn wire_metadata_log_entry(
    entry: &nokvfs_cluster::MetadataLogEntry,
) -> Result<WireMetadataLogEntry, ServerError> {
    Ok(WireMetadataLogEntry {
        position: wire_log_position(entry.position),
        mount: entry.mount.get(),
        payload: nokvfs_cluster::encode_metadata_log_entry(entry)
            .map_err(ServerError::SharedLog)?,
    })
}

fn wire_metadata_receipt(receipt: &nokvfs_cluster::DurableReceipt) -> WireMetadataReceipt {
    WireMetadataReceipt {
        position: wire_log_position(receipt.position),
        mount: receipt.mount.get(),
        batch_position: receipt.batch_position,
        request_id: receipt.request_id.clone(),
        commit_version: receipt.commit_version.get(),
    }
}

fn wire_metadata_checkpoint(
    checkpoint: &nokvfs_cluster::CheckpointManifest,
) -> WireMetadataCheckpoint {
    WireMetadataCheckpoint {
        id: checkpoint.id.clone(),
        mount: checkpoint.mount.get(),
        durable_position: wire_log_position(checkpoint.frontier.durable_position),
        applied_position: wire_log_position(checkpoint.frontier.applied_position),
        min_retained_index: checkpoint.frontier.min_retained_index.get(),
        max_commit_version: checkpoint.frontier.max_commit_version.get(),
        artifact_uri: checkpoint.artifact.uri.clone(),
        artifact_digest: checkpoint.artifact.digest.clone(),
        artifact_size_bytes: checkpoint.artifact.size_bytes,
    }
}

fn wire_metadata_bootstrap_plan(
    request: &nokvfs_cluster::InstallCheckpointRequest,
) -> WireMetadataBootstrapPlan {
    WireMetadataBootstrapPlan {
        leader: request.leader.get(),
        learner: request.plan.node.get(),
        checkpoint: wire_metadata_checkpoint(&request.plan.checkpoint),
        replay_start_index: request.plan.replay_start.get(),
        replayed_index: request.plan.replayed_index.get(),
    }
}

fn metadata_checkpoint_install_request(
    plan: WireMetadataBootstrapPlan,
) -> Result<nokvfs_cluster::InstallCheckpointRequest, ServerError> {
    let leader = nokvfs_cluster::NodeId::new(plan.leader).map_err(ServerError::SharedLog)?;
    let learner = nokvfs_cluster::NodeId::new(plan.learner).map_err(ServerError::SharedLog)?;
    let checkpoint = metadata_checkpoint_manifest(plan.checkpoint)?;
    let replay_start =
        nokvfs_cluster::LogIndex::new(plan.replay_start_index).map_err(ServerError::SharedLog)?;
    let replayed_index =
        nokvfs_cluster::LogIndex::new(plan.replayed_index).map_err(ServerError::SharedLog)?;
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

fn metadata_checkpoint_manifest(
    checkpoint: WireMetadataCheckpoint,
) -> Result<nokvfs_cluster::CheckpointManifest, ServerError> {
    let mount = MountId::new(checkpoint.mount).map_err(|err| {
        ServerError::Metadata(MetadError::Codec(format!(
            "invalid metadata checkpoint mount: {err}"
        )))
    })?;
    let frontier = nokvfs_cluster::CheckpointFrontier {
        durable_position: log_position(checkpoint.durable_position)?,
        applied_position: log_position(checkpoint.applied_position)?,
        min_retained_index: nokvfs_cluster::LogIndex::new(checkpoint.min_retained_index)
            .map_err(ServerError::SharedLog)?,
        max_commit_version: nokvfs_meta::Version::new(checkpoint.max_commit_version)
            .map_err(|err| ServerError::Metadata(MetadError::Codec(err.to_string())))?,
    };
    let artifact = nokvfs_cluster::CheckpointArtifact::new(
        checkpoint.artifact_uri,
        checkpoint.artifact_digest,
        checkpoint.artifact_size_bytes,
    )
    .map_err(ServerError::SharedLog)?;
    nokvfs_cluster::CheckpointManifest::new(checkpoint.id, mount, frontier, artifact)
        .map_err(ServerError::SharedLog)
}

fn protocol_error(err: MetadataProtocolError) -> MetadError {
    MetadError::Codec(err.to_string())
}

fn wire_log_position(position: nokvfs_cluster::LogPosition) -> WireMetadataPosition {
    WireMetadataPosition {
        term: position.term.get(),
        index: position.index.get(),
    }
}

fn log_position(
    position: WireMetadataPosition,
) -> Result<nokvfs_cluster::LogPosition, ServerError> {
    Ok(nokvfs_cluster::LogPosition {
        term: nokvfs_cluster::LogTerm::new(position.term).map_err(ServerError::SharedLog)?,
        index: nokvfs_cluster::LogIndex::new(position.index).map_err(ServerError::SharedLog)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::{
        open_test_server_with_checkpoint_objects, publish_test_metadata_membership,
        test_checkpoint_objects, test_options, test_server,
    };
    use nokvfs_protocol::{
        decode_envelope, encode_request, WireBlockDescriptor, WireBodyDescriptor,
        WireChunkManifest, WireMetadataError,
    };
    use std::net::TcpListener;
    use std::path::Path;
    use tempfile::tempdir;

    fn request_envelope(server: &Server, request: MetadataRpcRequest) -> MetadataRpcEnvelope {
        let body = encode_request(&request).unwrap();
        let response = handle_binary_rpc(server, &body).unwrap();
        decode_envelope(&response).unwrap()
    }

    fn open_metadata_log_follower(root: &Path, metadata_log: &Path) -> Server {
        publish_test_metadata_membership(
            metadata_log,
            nokvfs_cluster::MetadataMembership::new(
                MountId::new(1).unwrap(),
                nokvfs_cluster::LogTerm::new(1).unwrap(),
                nokvfs_cluster::NodeId::new(1).unwrap(),
                [
                    nokvfs_cluster::NodeId::new(1).unwrap(),
                    nokvfs_cluster::NodeId::new(2).unwrap(),
                ],
                [],
            )
            .unwrap(),
        );
        let mut options = test_options(root, Some(metadata_log.to_path_buf()));
        options.metadata_log_node = nokvfs_cluster::NodeId::new(2).unwrap();
        Server::open(options).unwrap()
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
    fn rpc_read_path_plan_returns_metadata_and_object_plan() {
        let server = test_server();
        let prepared = server
            .service()
            .prepare_artifact_create_path("/artifact.bin")
            .unwrap();
        let published = server
            .service()
            .publish_prepared_artifact(
                prepared.clone(),
                nokvfs_types::BodyDescriptor {
                    producer: "rpc-test".to_owned(),
                    digest_uri: "sha256:test".to_owned(),
                    size: 12,
                    content_type: "application/octet-stream".to_owned(),
                    manifest_id: "artifact.bin".to_owned(),
                    generation: prepared.generation,
                    chunk_size: nokvfs_object::DEFAULT_CHUNK_SIZE,
                    block_size: nokvfs_object::DEFAULT_BLOCK_SIZE as u64,
                },
                vec![nokvfs_types::ChunkManifest {
                    chunk_index: 0,
                    logical_offset: 0,
                    len: 12,
                    blocks: vec![nokvfs_types::BlockDescriptor {
                        object_key: "blocks/demo".to_owned(),
                        logical_offset: 0,
                        object_offset: 0,
                        len: 12,
                        digest_uri: "sha256:test".to_owned(),
                    }],
                }],
                0o644,
                1000,
                1000,
            )
            .unwrap()
            .entry;

        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::ReadPathPlan {
                path: "/artifact.bin".to_owned(),
                offset: 6,
                len: 6,
                expected_generation: Some(published.attr.generation),
            },
        );
        assert!(envelope.ok, "unexpected read path plan error: {envelope:?}");
        let MetadataRpcResult::PathReadPlan { metadata, plan } = envelope.result.unwrap() else {
            panic!("unexpected read path plan result")
        };
        assert_eq!(metadata.attr.inode, published.attr.inode.get());
        assert_eq!(metadata.body.unwrap().digest_uri, "sha256:test");
        assert_eq!(plan.output_len, 6);
        assert_eq!(plan.blocks.len(), 1);
        assert_eq!(plan.blocks[0].object_offset, 6);
        assert_eq!(plan.blocks[0].len, 6);

        let stale = request_envelope(
            &server,
            MetadataRpcRequest::ReadPathPlan {
                path: "/artifact.bin".to_owned(),
                offset: 0,
                len: 1,
                expected_generation: Some(published.attr.generation - 1),
            },
        );
        assert!(!stale.ok);
        assert!(matches!(
            stale.error_kind,
            Some(WireMetadataError::StaleBodyGeneration { .. })
        ));
    }

    #[test]
    fn rpc_require_applied_enforces_shared_log_freshness() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let server = Server::open(test_options(dir.path(), Some(metadata_log))).unwrap();
        let created = request_envelope(
            &server,
            MetadataRpcRequest::CreateFilePath {
                path: "/model.bin".to_owned(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        );
        assert!(created.ok);
        let position = created
            .metadata_position
            .expect("logged metadata response carries applied position");

        let fresh = request_envelope(
            &server,
            MetadataRpcRequest::RequireApplied {
                position,
                request: Box::new(MetadataRpcRequest::StatPath {
                    path: "/model.bin".to_owned(),
                }),
            },
        );
        assert!(fresh.ok, "unexpected stale response: {fresh:?}");

        let stale = request_envelope(
            &server,
            MetadataRpcRequest::RequireApplied {
                position: WireMetadataPosition {
                    term: position.term,
                    index: position.index + 1,
                },
                request: Box::new(MetadataRpcRequest::StatPath {
                    path: "/model.bin".to_owned(),
                }),
            },
        );
        assert!(!stale.ok);
        assert!(matches!(
            stale.error_kind,
            Some(WireMetadataError::ReadNotFresh { required, applied: Some(applied) })
                if required.index == position.index + 1 && applied == position
        ));
    }

    #[test]
    fn rpc_reads_committed_metadata_log_tail() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let server = Server::open(test_options(dir.path(), Some(metadata_log))).unwrap();
        let created = request_envelope(
            &server,
            MetadataRpcRequest::CreateFilePath {
                path: "/model.bin".to_owned(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        );
        assert!(created.ok);

        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::ReadMetadataLog {
                start_index: 1,
                limit: 0,
            },
        );
        assert!(
            envelope.ok,
            "unexpected metadata log read error: {envelope:?}"
        );
        let (entries, committed) = match envelope.result.unwrap() {
            MetadataRpcResult::MetadataLogEntries { entries, committed } => (entries, committed),
            other => panic!("unexpected metadata log result: {other:?}"),
        };
        let committed = committed.expect("metadata log read reports committed frontier");
        assert!(committed.index >= 2);
        assert!(entries.len() >= 2);

        let decoded = entries
            .iter()
            .map(|entry| {
                let decoded = nokvfs_cluster::decode_metadata_log_entry(&entry.payload).unwrap();
                assert_eq!(entry.position, wire_log_position(decoded.position));
                assert_eq!(entry.mount, decoded.mount.get());
                decoded
            })
            .collect::<Vec<_>>();
        assert!(decoded
            .iter()
            .flat_map(|entry| entry.commands.iter())
            .any(|command| command.kind == nokvfs_meta::command::CommandKind::CreateFile));
    }

    #[test]
    fn rpc_reads_latest_metadata_checkpoint_after_gc_publishes_manifest() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let checkpoint_objects = test_checkpoint_objects();
        let server = open_test_server_with_checkpoint_objects(
            dir.path(),
            Some(metadata_log),
            &checkpoint_objects,
        );

        let before = request_envelope(
            &server,
            MetadataRpcRequest::ReadMetadataCheckpoint { mount: 1 },
        );
        assert!(before.ok, "unexpected checkpoint read error: {before:?}");
        assert!(matches!(
            before.result.unwrap(),
            MetadataRpcResult::MetadataCheckpoint { checkpoint: None }
        ));

        let created = request_envelope(
            &server,
            MetadataRpcRequest::CreateDirPath {
                path: "/runs".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        );
        assert!(created.ok);
        server.run_manual_gc(128).unwrap();

        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::ReadMetadataCheckpoint { mount: 1 },
        );
        assert!(
            envelope.ok,
            "unexpected checkpoint read error: {envelope:?}"
        );
        let checkpoint = match envelope.result.unwrap() {
            MetadataRpcResult::MetadataCheckpoint {
                checkpoint: Some(checkpoint),
            } => checkpoint,
            other => panic!("unexpected checkpoint result: {other:?}"),
        };
        assert_eq!(checkpoint.mount, 1);
        assert!(checkpoint.id.starts_with(b"mount-1-term-1-index-"));
        assert!(checkpoint.artifact_uri.starts_with(b"object:"));
        assert!(!checkpoint.artifact_digest.is_empty());
        assert!(checkpoint.artifact_size_bytes > 0);
        assert!(checkpoint.min_retained_index >= checkpoint.applied_position.index);
        assert!(checkpoint.max_commit_version >= 2);
    }

    #[test]
    fn rpc_rejects_metadata_checkpoint_read_when_catalog_is_disabled() {
        let server = test_server();
        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::ReadMetadataCheckpoint { mount: 1 },
        );

        assert!(!envelope.ok);
        assert!(matches!(
            envelope.error_kind,
            Some(WireMetadataError::Metadata { message })
                if message.contains("metadata checkpoint catalog is disabled")
        ));
    }

    #[test]
    fn rpc_plans_metadata_bootstrap_from_checkpoint_and_retained_tail() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let checkpoint_objects = test_checkpoint_objects();
        let server = open_test_server_with_checkpoint_objects(
            dir.path(),
            Some(metadata_log),
            &checkpoint_objects,
        );
        let created_before_checkpoint = request_envelope(
            &server,
            MetadataRpcRequest::CreateDirPath {
                path: "/runs".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        );
        assert!(created_before_checkpoint.ok);
        server.run_manual_gc(128).unwrap();

        let created_after_checkpoint = request_envelope(
            &server,
            MetadataRpcRequest::CreateDirPath {
                path: "/after-checkpoint".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        );
        assert!(created_after_checkpoint.ok);
        let after_position = created_after_checkpoint
            .metadata_position
            .expect("logged write should carry position");

        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::PlanMetadataBootstrap {
                leader: 1,
                learner: 4,
                mount: 1,
            },
        );
        assert!(envelope.ok, "unexpected bootstrap plan error: {envelope:?}");
        let plan = match envelope.result.unwrap() {
            MetadataRpcResult::MetadataBootstrapPlan { plan } => plan,
            other => panic!("unexpected bootstrap plan result: {other:?}"),
        };
        assert_eq!(plan.leader, 1);
        assert_eq!(plan.learner, 4);
        assert_eq!(plan.checkpoint.mount, 1);
        assert!(plan.checkpoint.id.starts_with(b"mount-1-term-1-index-"));
        assert_eq!(plan.replay_start_index, plan.checkpoint.min_retained_index);
        assert_eq!(plan.replayed_index, after_position.index);
        assert!(plan.replay_start_index <= plan.replayed_index);
    }

    #[test]
    fn rpc_installs_metadata_checkpoint_and_replays_retained_tail() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let checkpoint_objects = test_checkpoint_objects();
        let server = open_test_server_with_checkpoint_objects(
            dir.path(),
            Some(metadata_log),
            &checkpoint_objects,
        );
        let created_before_checkpoint = request_envelope(
            &server,
            MetadataRpcRequest::CreateDirPath {
                path: "/runs".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        );
        assert!(created_before_checkpoint.ok);
        server.run_manual_gc(128).unwrap();

        let created_after_checkpoint = request_envelope(
            &server,
            MetadataRpcRequest::CreateDirPath {
                path: "/after-checkpoint".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        );
        assert!(created_after_checkpoint.ok);

        let plan = match request_envelope(
            &server,
            MetadataRpcRequest::PlanMetadataBootstrap {
                leader: 1,
                learner: 1,
                mount: 1,
            },
        )
        .result
        .unwrap()
        {
            MetadataRpcResult::MetadataBootstrapPlan { plan } => plan,
            other => panic!("unexpected bootstrap plan result: {other:?}"),
        };
        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::InstallMetadataCheckpoint { plan },
        );
        assert!(
            envelope.ok,
            "unexpected checkpoint install error: {envelope:?}"
        );
        let install = match envelope.result.unwrap() {
            MetadataRpcResult::MetadataCheckpointInstall { install } => install,
            other => panic!("unexpected checkpoint install result: {other:?}"),
        };
        assert_eq!(install.learner, 1);
        assert!(install.replay_start_index <= install.replayed_index);

        assert!(matches!(
            request_envelope(
                &server,
                MetadataRpcRequest::LookupPath {
                    path: "/runs".to_owned(),
                },
            )
            .result
            .unwrap(),
            MetadataRpcResult::Dentry { entry: Some(_) }
        ));
        assert!(matches!(
            request_envelope(
                &server,
                MetadataRpcRequest::LookupPath {
                    path: "/after-checkpoint".to_owned(),
                },
            )
            .result
            .unwrap(),
            MetadataRpcResult::Dentry { entry: Some(_) }
        ));
    }

    #[test]
    fn rpc_rejects_metadata_bootstrap_plan_without_checkpoint() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let server = Server::open(test_options(dir.path(), Some(metadata_log))).unwrap();
        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::PlanMetadataBootstrap {
                leader: 1,
                learner: 4,
                mount: 1,
            },
        );

        assert!(!envelope.ok);
        assert!(matches!(
            envelope.error_kind,
            Some(WireMetadataError::Metadata { message })
                if message.contains("requires a checkpoint")
        ));
    }

    #[test]
    fn rpc_rejects_metadata_bootstrap_plan_from_unauthorized_leader() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let mut options = test_options(dir.path(), Some(metadata_log));
        options.metadata_log_node = nokvfs_cluster::NodeId::new(4).unwrap();
        options.metadata_log_leader = nokvfs_cluster::NodeId::new(4).unwrap();
        let server = Server::open(options).unwrap();
        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::PlanMetadataBootstrap {
                leader: 1,
                learner: 5,
                mount: 1,
            },
        );

        assert!(!envelope.ok);
        assert!(matches!(
            envelope.error_kind,
            Some(WireMetadataError::Metadata { message })
                if message.contains("leader 1 is not authorized")
                    && message.contains("expected leader 4")
        ));
    }

    #[test]
    fn rpc_appends_metadata_log_batch_and_replays_state() {
        let leader_dir = tempdir().unwrap();
        let leader_log = leader_dir.path().join("metadata.log");
        let leader = Server::open(test_options(leader_dir.path(), Some(leader_log))).unwrap();
        let entries = create_file_log_tail_entries(&leader, "/model.bin");

        let replica_dir = tempdir().unwrap();
        let replica_log = replica_dir.path().join("metadata.log");
        let replica = open_metadata_log_follower(replica_dir.path(), &replica_log);
        let envelope = append_metadata_log_entries(&replica, &entries);

        assert!(envelope.ok, "unexpected append error: {envelope:?}");
        let (position, receipts) = match envelope.result.unwrap() {
            MetadataRpcResult::MetadataLogAppend { position, receipts } => (position, receipts),
            other => panic!("unexpected append result: {other:?}"),
        };
        assert!(position.index >= 2);
        assert!(!receipts.is_empty());
        for (batch_position, receipt) in receipts.iter().enumerate() {
            assert_eq!(receipt.position, position);
            assert_eq!(receipt.mount, 1);
            assert_eq!(receipt.batch_position, batch_position);
            assert!(!receipt.request_id.is_empty());
        }

        let stat = request_envelope(
            &replica,
            MetadataRpcRequest::StatPath {
                path: "/model.bin".to_owned(),
            },
        );
        assert!(stat.ok, "replica did not apply appended log: {stat:?}");
        match stat.result.unwrap() {
            MetadataRpcResult::PathMetadata {
                metadata: Some(metadata),
            } => assert_eq!(metadata.attr.file_type, "file"),
            other => panic!("unexpected stat result after append: {other:?}"),
        }
    }

    #[test]
    fn rpc_rejects_metadata_log_append_with_zero_leader() {
        let leader_dir = tempdir().unwrap();
        let leader_log = leader_dir.path().join("metadata.log");
        let leader = Server::open(test_options(leader_dir.path(), Some(leader_log))).unwrap();
        let entry = create_file_log_tail_entries(&leader, "/model.bin")
            .into_iter()
            .next()
            .expect("leader log entry");

        let replica_dir = tempdir().unwrap();
        let replica_log = replica_dir.path().join("metadata.log");
        let replica = Server::open(test_options(replica_dir.path(), Some(replica_log))).unwrap();
        let envelope = request_envelope(
            &replica,
            MetadataRpcRequest::AppendMetadataLog { leader: 0, entry },
        );

        assert!(!envelope.ok);
        assert!(matches!(
            envelope.error_kind,
            Some(WireMetadataError::Metadata { message })
                if message.contains("cluster node id must be non-zero")
        ));
    }

    #[test]
    fn rpc_rejects_metadata_log_append_from_unauthorized_leader() {
        let leader_dir = tempdir().unwrap();
        let leader_log = leader_dir.path().join("metadata.log");
        let leader = Server::open(test_options(leader_dir.path(), Some(leader_log))).unwrap();
        let entry = create_file_log_tail_entries(&leader, "/model.bin")
            .into_iter()
            .next()
            .expect("leader log entry");

        let replica_dir = tempdir().unwrap();
        let replica_log = replica_dir.path().join("metadata.log");
        let mut options = test_options(replica_dir.path(), Some(replica_log));
        options.metadata_log_node = nokvfs_cluster::NodeId::new(4).unwrap();
        options.metadata_log_leader = nokvfs_cluster::NodeId::new(4).unwrap();
        let replica = Server::open(options).unwrap();
        let envelope = request_envelope(
            &replica,
            MetadataRpcRequest::AppendMetadataLog { leader: 1, entry },
        );

        assert!(!envelope.ok);
        assert!(matches!(
            envelope.error_kind,
            Some(WireMetadataError::Metadata { message })
                if message.contains("leader 1 is not authorized")
                    && message.contains("expected leader 4")
        ));
    }

    #[test]
    fn rpc_rejects_stale_metadata_log_append_term() {
        let leader_dir = tempdir().unwrap();
        let leader_log = leader_dir.path().join("metadata.log");
        let mut leader_options = test_options(leader_dir.path(), Some(leader_log));
        leader_options.metadata_log_term = nokvfs_cluster::LogTerm::new(3).unwrap();
        let leader = Server::open(leader_options).unwrap();
        let entries = create_file_log_tail_entries(&leader, "/model.bin");

        let replica_dir = tempdir().unwrap();
        let replica_log = replica_dir.path().join("metadata.log");
        let replica = open_metadata_log_follower(replica_dir.path(), &replica_log);
        let first = append_metadata_log_entries(&replica, &entries);
        assert!(first.ok, "unexpected first append error: {first:?}");

        let stale = request_envelope(
            &replica,
            MetadataRpcRequest::AppendMetadataLog {
                leader: 1,
                entry: encoded_metadata_log_entry(
                    2,
                    entries.len() as u64 + 1,
                    vec![create_file_command(b"b", 3)],
                ),
            },
        );
        assert!(!stale.ok);
        assert!(matches!(
            stale.error_kind,
            Some(WireMetadataError::Metadata { message })
                if message.contains("rejects stale term")
        ));
    }

    #[test]
    fn rpc_rejects_metadata_log_append_when_log_is_disabled() {
        let leader_dir = tempdir().unwrap();
        let leader_log = leader_dir.path().join("metadata.log");
        let leader = Server::open(test_options(leader_dir.path(), Some(leader_log))).unwrap();
        let entry = create_file_log_tail_entries(&leader, "/model.bin")
            .into_iter()
            .next()
            .expect("leader log entry");
        let server = test_server();
        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::AppendMetadataLog { leader: 1, entry },
        );

        assert!(!envelope.ok);
        assert!(matches!(
            envelope.error_kind,
            Some(WireMetadataError::Metadata { message })
                if message.contains("metadata log is disabled")
        ));
    }

    #[test]
    fn rpc_rejects_metadata_log_append_for_wrong_mount() {
        let replica_dir = tempdir().unwrap();
        let replica_log = replica_dir.path().join("metadata.log");
        let replica = open_metadata_log_follower(replica_dir.path(), &replica_log);
        let envelope = request_envelope(
            &replica,
            MetadataRpcRequest::AppendMetadataLog {
                leader: 1,
                entry: encoded_metadata_log_entry_for_mount(
                    1,
                    1,
                    99,
                    vec![create_file_command(b"a", 2)],
                ),
            },
        );

        assert!(!envelope.ok);
        assert!(matches!(
            envelope.error_kind,
            Some(WireMetadataError::Metadata { message })
                if message.contains("does not match server mount")
        ));
    }

    #[test]
    fn rpc_rejects_metadata_log_read_when_log_is_disabled() {
        let server = test_server();
        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::ReadMetadataLog {
                start_index: 1,
                limit: 1,
            },
        );

        assert!(!envelope.ok);
        assert!(matches!(
            envelope.error_kind,
            Some(WireMetadataError::Metadata { message })
                if message.contains("metadata log is disabled")
        ));
    }

    #[test]
    fn rpc_rejects_non_contiguous_metadata_log_append() {
        let replica_dir = tempdir().unwrap();
        let replica_log = replica_dir.path().join("metadata.log");
        let replica = open_metadata_log_follower(replica_dir.path(), &replica_log);
        let envelope = request_envelope(
            &replica,
            MetadataRpcRequest::AppendMetadataLog {
                leader: 1,
                entry: encoded_metadata_log_entry(1, 2, vec![create_file_command(b"a", 2)]),
            },
        );

        assert!(!envelope.ok);
        assert!(matches!(
            envelope.error_kind,
            Some(WireMetadataError::Metadata { message })
                if message.contains("expected append at index 1, got 2")
        ));
    }

    fn create_file_log_tail_entries(server: &Server, path: &str) -> Vec<Vec<u8>> {
        let created = request_envelope(
            server,
            MetadataRpcRequest::CreateFilePath {
                path: path.to_owned(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        );
        assert!(created.ok, "failed to create test file: {created:?}");
        let log = request_envelope(
            server,
            MetadataRpcRequest::ReadMetadataLog {
                start_index: 1,
                limit: 0,
            },
        );
        let entries = match log.result.unwrap() {
            MetadataRpcResult::MetadataLogEntries { entries, .. } => entries,
            other => panic!("unexpected metadata log result: {other:?}"),
        };
        assert!(entries.iter().any(|entry| {
            nokvfs_cluster::decode_metadata_log_entry(&entry.payload)
                .unwrap()
                .commands
                .iter()
                .any(|command| command.kind == nokvfs_meta::command::CommandKind::CreateFile)
        }));
        entries.into_iter().map(|entry| entry.payload).collect()
    }

    fn append_metadata_log_entries(server: &Server, entries: &[Vec<u8>]) -> MetadataRpcEnvelope {
        let mut last = None;
        for entry in entries {
            last = Some(request_envelope(
                server,
                MetadataRpcRequest::AppendMetadataLog {
                    leader: 1,
                    entry: entry.clone(),
                },
            ));
            if !last.as_ref().unwrap().ok {
                break;
            }
        }
        last.expect("at least one metadata log entry")
    }

    fn encoded_metadata_log_entry(
        term: u64,
        index: u64,
        commands: Vec<nokvfs_meta::command::MetadataCommand>,
    ) -> Vec<u8> {
        encoded_metadata_log_entry_for_mount(term, index, 1, commands)
    }

    fn encoded_metadata_log_entry_for_mount(
        term: u64,
        index: u64,
        mount: u64,
        commands: Vec<nokvfs_meta::command::MetadataCommand>,
    ) -> Vec<u8> {
        nokvfs_cluster::encode_metadata_log_entry(&nokvfs_cluster::MetadataLogEntry {
            position: nokvfs_cluster::LogPosition {
                term: nokvfs_cluster::LogTerm::new(term).unwrap(),
                index: nokvfs_cluster::LogIndex::new(index).unwrap(),
            },
            mount: MountId::new(mount).unwrap(),
            commands,
        })
        .unwrap()
    }

    fn create_file_command(
        request_id: &[u8],
        commit_version: u64,
    ) -> nokvfs_meta::command::MetadataCommand {
        nokvfs_meta::command::MetadataCommand {
            request_id: request_id.to_vec(),
            kind: nokvfs_meta::command::CommandKind::CreateFile,
            read_version: nokvfs_meta::Version::new(commit_version - 1).unwrap(),
            commit_version: nokvfs_meta::Version::new(commit_version).unwrap(),
            primary_family: nokvfs_types::RecordFamily::Dentry,
            primary_key: request_id.to_vec(),
            predicates: Vec::new(),
            mutations: vec![nokvfs_meta::command::Mutation {
                family: nokvfs_types::RecordFamily::Dentry,
                key: request_id.to_vec(),
                op: nokvfs_meta::command::MutationOp::Put,
                value: Some(nokvfs_meta::command::Value(b"value".to_vec())),
            }],
            watch: Vec::new(),
        }
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
    fn rpc_lists_directory_pages_with_name_cursor() {
        let server = test_server();
        expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::CreateDirPath {
                path: "/runs".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        ));
        for name in ["a.bin", "b.bin", "c.bin"] {
            expect_dentry(request_envelope(
                &server,
                MetadataRpcRequest::CreateFilePath {
                    path: format!("/runs/{name}"),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
            ));
        }

        let first = request_envelope(
            &server,
            MetadataRpcRequest::ReadDirPlusPathPage {
                path: "/runs".to_owned(),
                after_name_hex: None,
                limit: 2,
            },
        );
        let (entries, cursor) = match first.result.unwrap() {
            MetadataRpcResult::DentriesPage {
                entries,
                next_name_hex,
            } => (entries, next_name_hex),
            other => panic!("unexpected page result: {other:?}"),
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].dentry.name_hex, "612e62696e");
        assert_eq!(entries[1].dentry.name_hex, "622e62696e");
        assert_eq!(cursor.as_deref(), Some("622e62696e"));

        let second = request_envelope(
            &server,
            MetadataRpcRequest::ReadDirPlusPathPage {
                path: "/runs".to_owned(),
                after_name_hex: cursor,
                limit: 2,
            },
        );
        let (entries, cursor) = match second.result.unwrap() {
            MetadataRpcResult::DentriesPage {
                entries,
                next_name_hex,
            } => (entries, next_name_hex),
            other => panic!("unexpected page result: {other:?}"),
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].dentry.name_hex, "632e62696e");
        assert_eq!(cursor, None);
    }

    #[test]
    fn rpc_lists_indexed_path_pages_without_plain_namespace_entries() {
        let server = test_server();
        expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::CreateDirPath {
                path: "/runs".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        ));
        expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::CreateFilePath {
                path: "/runs/plain.txt".to_owned(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        ));
        let prepared = match request_envelope(
            &server,
            MetadataRpcRequest::PrepareArtifactPath {
                path: "/runs/metrics.json".to_owned(),
                replace: false,
            },
        )
        .result
        .unwrap()
        {
            MetadataRpcResult::PreparedArtifact { prepared } => prepared,
            other => panic!("unexpected prepare result: {other:?}"),
        };
        let published = request_envelope(
            &server,
            MetadataRpcRequest::PublishPreparedArtifact {
                body: Box::new(WireBodyDescriptor {
                    producer: "unit-test".to_owned(),
                    digest_uri: "sha256:metrics".to_owned(),
                    size: 2,
                    content_type: "application/json".to_owned(),
                    manifest_id: "metrics.json".to_owned(),
                    generation: prepared.generation,
                    chunk_size: 64 * 1024 * 1024,
                    block_size: 4 * 1024 * 1024,
                }),
                chunks: vec![WireChunkManifest {
                    chunk_index: 0,
                    logical_offset: 0,
                    len: 2,
                    blocks: vec![WireBlockDescriptor {
                        object_key: format!("blocks/1/{}/{}", prepared.inode, prepared.generation),
                        logical_offset: 0,
                        object_offset: 0,
                        len: 2,
                        digest_uri: "sha256:block".to_owned(),
                    }],
                }],
                prepared,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        );
        assert!(published.ok, "unexpected publish error: {published:?}");

        let page = request_envelope(
            &server,
            MetadataRpcRequest::ReadIndexedPathPage {
                path: "/runs".to_owned(),
                after_name_hex: None,
                limit: 100,
            },
        );
        let entries = match page.result.unwrap() {
            MetadataRpcResult::DentriesPage {
                entries,
                next_name_hex,
            } => {
                assert_eq!(next_name_hex, None);
                entries
            }
            other => panic!("unexpected indexed page result: {other:?}"),
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].dentry.name_hex, "6d6574726963732e6a736f6e");
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
    fn rpc_batch_coalesces_same_parent_create_dir_paths() {
        let server = test_server();
        expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::CreateDirPath {
                path: "/runs".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        ));
        let before = server.service().metadata_store_stats();

        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::Batch {
                requests: vec![
                    MetadataRpcRequest::CreateDirPath {
                        path: "/runs/a".to_owned(),
                        mode: 0o755,
                        uid: 1000,
                        gid: 1000,
                    },
                    MetadataRpcRequest::CreateDirPath {
                        path: "/runs/b".to_owned(),
                        mode: 0o755,
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
        let after = server.service().metadata_store_stats();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| result.ok));
        assert_eq!(after.commit_total - before.commit_total, 1);
        let listed = request_envelope(
            &server,
            MetadataRpcRequest::ReadDirPlusPath {
                path: "/runs".to_owned(),
            },
        );
        let entries = match listed.result.unwrap() {
            MetadataRpcResult::Dentries { entries } => entries,
            other => panic!("unexpected readdir result: {other:?}"),
        };
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn rpc_batch_coalesces_multi_parent_create_files_into_shared_log_entry() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let server = Server::open(test_options(dir.path(), Some(metadata_log))).unwrap();
        expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::CreateDirPath {
                path: "/runs".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        ));
        expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::CreateDirPath {
                path: "/runs/a".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        ));
        expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::CreateDirPath {
                path: "/runs/b".to_owned(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        ));

        let envelope = request_envelope(
            &server,
            MetadataRpcRequest::Batch {
                requests: vec![
                    MetadataRpcRequest::CreateFilePath {
                        path: "/runs/a/one.bin".to_owned(),
                        mode: 0o644,
                        uid: 1000,
                        gid: 1000,
                    },
                    MetadataRpcRequest::CreateFilePath {
                        path: "/runs/b/two.bin".to_owned(),
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
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| result.ok));
        assert!(server.stats_json().contains("\"max_commands_per_entry\":2"));
    }

    #[test]
    fn framed_rpc_coalesces_pipelined_create_frames_into_shared_log_entry() {
        let dir = tempdir().unwrap();
        let metadata_log = dir.path().join("metadata.log");
        let server = Arc::new(Server::open(test_options(dir.path(), Some(metadata_log))).unwrap());
        server
            .service()
            .create_dir_path("/runs", 0o755, 1000, 1000)
            .unwrap();
        server
            .service()
            .create_dir_path("/runs/a", 0o755, 1000, 1000)
            .unwrap();
        server
            .service()
            .create_dir_path("/runs/b", 0o755, 1000, 1000)
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).unwrap();
        client.write_all(FRAMED_RPC_MAGIC).unwrap();
        write_frame(
            &mut client,
            1,
            0,
            &encode_request(&MetadataRpcRequest::CreateFilePath {
                path: "/runs/a/one.bin".to_owned(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap(),
        )
        .unwrap();
        write_frame(
            &mut client,
            2,
            0,
            &encode_request(&MetadataRpcRequest::CreateFilePath {
                path: "/runs/b/two.bin".to_owned(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap(),
        )
        .unwrap();
        client.shutdown(std::net::Shutdown::Write).unwrap();

        let server_thread = {
            let server = Arc::clone(&server);
            thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                crate::http::handle_stream(server, stream).unwrap();
            })
        };

        let first = read_frame(&mut client).unwrap().unwrap();
        let second = read_frame(&mut client).unwrap().unwrap();
        assert_eq!(first.request_id, 1);
        assert_eq!(second.request_id, 2);
        assert!(decode_envelope(&first.payload).unwrap().ok);
        assert!(decode_envelope(&second.payload).unwrap().ok);
        server_thread.join().unwrap();

        assert!(server.stats_json().contains("\"max_commands_per_entry\":2"));
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
