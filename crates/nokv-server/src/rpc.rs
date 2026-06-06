use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc, Arc, Mutex,
};
use std::thread;
use std::time::Duration;

use nokv_cluster::{MetadataRaftError, MetadataRaftRpcClient, NodeId};
use nokv_meta::{
    CreateInDirPathBatch, DentryWithAttr, MetadError, PreparedArtifact,
    PublishArtifactStagedSession, UpdateAttr, XattrSetMode,
};
use nokv_object::{
    ObjectKey, ObjectReadBlock, StagedObject, StagedObjectSet, StoredBlock, StoredChunk,
};
use nokv_protocol::{
    decode_advisory_lock_kind, decode_envelope, decode_file_type, decode_name_cursor,
    decode_request, decode_xattr_name, encode_envelope, encode_name_cursor, encode_request,
    encode_xattr_name, MetadataProtocolError, MetadataRpcEnvelope, MetadataRpcRequest,
    MetadataRpcResult, WireAdvisoryLock, WireBodyReadPlan, WireDentryWithAttr, WireMetadataError,
    WireMetadataPosition, WireMetadataRaftAppendEntriesRequest,
    WireMetadataRaftAppendEntriesResponse, WireMetadataRaftInstallSnapshotRequest,
    WireMetadataRaftInstallSnapshotResponse, WireMetadataRaftVoteRequest,
    WireMetadataRaftVoteResponse, WireObjectReadBlock, WirePathMetadata, WirePreparedArtifact,
    WireStagedObjectSet, WireUpdateAttr, WireXattrSetMode,
};
use nokv_types::{AdvisoryLockRequest, DentryName, InodeId, MountId, SpecialNodeSpec};

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

pub(crate) struct RpcWorkerPool {
    sender: mpsc::SyncSender<RpcJob>,
}

pub(crate) struct FramedRpcClient {
    address: SocketAddr,
    next_request_id: AtomicU64,
    stream: Mutex<Option<TcpStream>>,
}

#[derive(Clone, Default)]
pub(crate) struct MetadataRaftFramedRpcClient {
    peers: Arc<Mutex<BTreeMap<SocketAddr, Arc<FramedRpcClient>>>>,
}

impl RpcWorkerPool {
    pub(crate) fn new(workers: usize, queue_capacity: usize) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<RpcJob>(queue_capacity.max(workers));
        let receiver = Arc::new(Mutex::new(receiver));
        for worker in 0..workers {
            let receiver = Arc::clone(&receiver);
            thread::Builder::new()
                .name(format!("nokv-rpc-{worker}"))
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

    pub(crate) fn submit(&self, job: RpcJob) -> Result<(), ServerError> {
        self.sender.send(job).map_err(|_| {
            ServerError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "metadata framed rpc worker pool stopped",
            ))
        })
    }
}

impl FramedRpcClient {
    pub(crate) fn new(address: SocketAddr) -> Self {
        Self {
            address,
            next_request_id: AtomicU64::new(1),
            stream: Mutex::new(None),
        }
    }

    pub(crate) fn call(
        &self,
        request: &MetadataRpcRequest,
    ) -> Result<MetadataRpcEnvelope, ServerError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let mut stream = self
            .stream
            .lock()
            .map_err(|_| ServerError::Io(io::Error::other("metadata peer rpc mutex poisoned")))?;
        if stream.is_none() {
            *stream = Some(open_framed_rpc_stream(self.address)?);
        }
        let result = call_framed_rpc_on_stream(
            stream.as_mut().expect("framed rpc stream was initialized"),
            request_id,
            request,
        );
        if result.is_err() {
            *stream = None;
        }
        result
    }
}

impl MetadataRaftFramedRpcClient {
    fn call_peer(
        &self,
        address: &str,
        request: &MetadataRpcRequest,
    ) -> Result<MetadataRpcResult, MetadataRaftError> {
        let address = address.parse::<SocketAddr>().map_err(|err| {
            MetadataRaftError::Backend(format!("metadata raft peer address {address:?}: {err}"))
        })?;
        let client = {
            let mut peers = self.peers.lock().map_err(|_| {
                MetadataRaftError::Backend("metadata raft peer client cache poisoned".to_owned())
            })?;
            Arc::clone(
                peers
                    .entry(address)
                    .or_insert_with(|| Arc::new(FramedRpcClient::new(address))),
            )
        };
        let envelope = client
            .call(request)
            .map_err(|err| MetadataRaftError::Backend(format!("metadata raft peer rpc: {err}")))?;
        if !envelope.ok {
            return Err(MetadataRaftError::Backend(format!(
                "metadata raft peer rejected rpc: {}",
                envelope.error.unwrap_or_else(|| "unknown error".to_owned())
            )));
        }
        envelope.result.ok_or_else(|| {
            MetadataRaftError::Backend("metadata raft peer returned no result".to_owned())
        })
    }
}

impl MetadataRaftRpcClient for MetadataRaftFramedRpcClient {
    fn vote_metadata_raft(
        &self,
        _target: u64,
        address: &str,
        request: WireMetadataRaftVoteRequest,
    ) -> Result<WireMetadataRaftVoteResponse, MetadataRaftError> {
        match self.call_peer(address, &MetadataRpcRequest::MetadataRaftVote { request })? {
            MetadataRpcResult::MetadataRaftVote { response } => Ok(response),
            other => Err(MetadataRaftError::Backend(format!(
                "metadata raft vote returned unexpected result: {other:?}"
            ))),
        }
    }

    fn append_metadata_raft_entries(
        &self,
        _target: u64,
        address: &str,
        request: WireMetadataRaftAppendEntriesRequest,
    ) -> Result<WireMetadataRaftAppendEntriesResponse, MetadataRaftError> {
        match self.call_peer(
            address,
            &MetadataRpcRequest::MetadataRaftAppendEntries { request },
        )? {
            MetadataRpcResult::MetadataRaftAppendEntries { response } => Ok(response),
            other => Err(MetadataRaftError::Backend(format!(
                "metadata raft append entries returned unexpected result: {other:?}"
            ))),
        }
    }

    fn install_metadata_raft_snapshot(
        &self,
        _target: u64,
        address: &str,
        request: WireMetadataRaftInstallSnapshotRequest,
    ) -> Result<WireMetadataRaftInstallSnapshotResponse, MetadataRaftError> {
        match self.call_peer(
            address,
            &MetadataRpcRequest::MetadataRaftInstallSnapshot { request },
        )? {
            MetadataRpcResult::MetadataRaftInstallSnapshot { response } => Ok(response),
            other => Err(MetadataRaftError::Backend(format!(
                "metadata raft install snapshot returned unexpected result: {other:?}"
            ))),
        }
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
                    .metadata_raft_applied_position()
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
    stream.set_nodelay(true).map_err(ServerError::Io)?;
    stream.set_read_timeout(None).map_err(ServerError::Io)?;
    stream.set_write_timeout(None).map_err(ServerError::Io)?;
    let writer = Arc::new(Mutex::new(stream.try_clone().map_err(ServerError::Io)?));

    loop {
        let Some(frame) = read_frame(&mut stream)? else {
            return Ok(());
        };
        let mut frames = vec![frame];
        drain_ready_frames(&mut stream, &mut frames)?;
        let task_server = Arc::clone(&server);
        let writer = Arc::clone(&writer);
        server.framed_rpc_workers().submit(Box::new(move || {
            let responses = handle_binary_rpc_frames(task_server.as_ref(), frames);
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

fn open_framed_rpc_stream(address: SocketAddr) -> Result<TcpStream, ServerError> {
    let mut stream = TcpStream::connect(address).map_err(ServerError::Io)?;
    stream.set_nodelay(true).map_err(ServerError::Io)?;
    stream
        .set_read_timeout(Some(OUTBOUND_FRAMED_RPC_TIMEOUT))
        .map_err(ServerError::Io)?;
    stream
        .set_write_timeout(Some(OUTBOUND_FRAMED_RPC_TIMEOUT))
        .map_err(ServerError::Io)?;
    stream
        .write_all(FRAMED_RPC_MAGIC)
        .map_err(ServerError::Io)?;
    Ok(stream)
}

fn call_framed_rpc_on_stream(
    stream: &mut TcpStream,
    request_id: u64,
    request: &MetadataRpcRequest,
) -> Result<MetadataRpcEnvelope, ServerError> {
    let payload = encode_request(request).map_err(|err| {
        ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("metadata framed rpc request encode failed: {err}"),
        ))
    })?;
    write_frame(stream, request_id, 0, &payload)?;
    let Some(frame) = read_frame(stream)? else {
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

pub(crate) fn default_framed_rpc_worker_count() -> usize {
    thread::available_parallelism()
        .map(|parallelism| parallelism.get().saturating_mul(4))
        .unwrap_or(MIN_FRAMED_RPC_WORKERS)
        .clamp(MIN_FRAMED_RPC_WORKERS, MAX_FRAMED_RPC_WORKERS)
}

pub(crate) fn default_framed_rpc_queue_capacity() -> usize {
    default_framed_rpc_worker_count().saturating_mul(FRAMED_RPC_QUEUE_PER_WORKER)
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
            let Some(parts) = remove_path_parts(&request) else {
                results.push(execute_envelope(server, request));
                continue;
            };
            let mut group = RemovePathGroup::from_parts(parts);
            while let Some(next) = iter.peek() {
                let Some(next_parts) = remove_path_parts(next) else {
                    break;
                };
                if !group.can_absorb(&next_parts) {
                    break;
                }
                iter.next();
                group.names.push(next_parts.name);
            }
            results.extend(remove_path_batch_envelopes(
                server,
                group.kind,
                &group.parent_path,
                group.names,
            ));
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
            )?);
        } else {
            results.extend(create_path_group_envelopes(server, kind, groups)?);
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
            .metadata_raft_applied_position()
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
        ServerError::MetadataRaft(err) => wire_metadata_raft_error(err),
    }
}

fn wire_metadata_raft_error(err: &nokv_cluster::MetadataRaftError) -> WireMetadataError {
    match err {
        nokv_cluster::MetadataRaftError::ReadNotFresh { required, applied } => {
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
        MetadError::Metadata(nokv_meta::MetadataError::ReadNotFresh {
            required_term,
            required_index,
            applied_term,
            applied_index,
        }) => WireMetadataError::ReadNotFresh {
            required: WireMetadataPosition {
                term: *required_term,
                index: *required_index,
            },
            applied: match (*applied_term, *applied_index) {
                (Some(term), Some(index)) => Some(WireMetadataPosition { term, index }),
                _ => None,
            },
        },
        MetadError::Metadata(nokv_meta::MetadataError::ForwardToLeader { leader_id, address }) => {
            WireMetadataError::ForwardToLeader {
                leader_id: *leader_id,
                address: address.clone(),
            }
        }
        MetadError::Metadata(nokv_meta::MetadataError::PredicateFailed) => {
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
        MetadError::LockConflict(lock) => WireMetadataError::LockConflict {
            lock: WireAdvisoryLock::from_advisory_lock(lock),
        },
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum RemovePathKind {
    File,
    EmptyDir,
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

struct RemovePathParts {
    kind: RemovePathKind,
    parent_path: String,
    name: String,
}

struct RemovePathGroup {
    kind: RemovePathKind,
    parent_path: String,
    names: Vec<String>,
}

impl RemovePathGroup {
    fn from_parts(parts: RemovePathParts) -> Self {
        Self {
            kind: parts.kind,
            parent_path: parts.parent_path,
            names: vec![parts.name],
        }
    }

    fn can_absorb(&self, parts: &RemovePathParts) -> bool {
        self.kind == parts.kind
            && self.parent_path == parts.parent_path
            && !self.names.contains(&parts.name)
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
) -> Result<Vec<MetadataRpcEnvelope>, ServerError> {
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
        Ok(entries) => Ok(entries
            .iter()
            .map(|entry| {
                ok_envelope(
                    server,
                    MetadataRpcResult::Dentry {
                        entry: Some(Box::new(wire_dentry(entry))),
                    },
                )
            })
            .collect()),
        Err(err) if server_error_is_forward_to_leader(&err) => Err(err),
        Err(_) => Ok(names
            .into_iter()
            .map(|name| {
                execute_envelope(
                    server,
                    create_path_request(kind, parent_path, &name, mode, uid, gid),
                )
            })
            .collect()),
    }
}

fn server_error_is_forward_to_leader(err: &ServerError) -> bool {
    matches!(
        err,
        ServerError::Metadata(MetadError::Metadata(
            nokv_meta::MetadataError::ForwardToLeader { .. }
        ))
    )
}

fn remove_path_batch_envelopes(
    server: &Server,
    kind: RemovePathKind,
    parent_path: &str,
    names: Vec<String>,
) -> Vec<MetadataRpcEnvelope> {
    let parsed = names
        .iter()
        .map(|name| dentry_name(name.clone()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(ServerError::Metadata);
    let committed = parsed.and_then(|names| {
        match kind {
            RemovePathKind::File => server
                .service()
                .remove_files_in_dir_path(parent_path, names),
            RemovePathKind::EmptyDir => server
                .service()
                .remove_empty_dirs_in_dir_path(parent_path, names),
        }
        .map_err(ServerError::Metadata)
    });
    match committed {
        Ok(results) => results
            .into_iter()
            .map(|result| match result {
                Ok(entry) => ok_envelope(
                    server,
                    MetadataRpcResult::Dentry {
                        entry: Some(Box::new(wire_dentry(&entry))),
                    },
                ),
                Err(err) => err_envelope(ServerError::Metadata(err)),
            })
            .collect(),
        Err(_) => names
            .into_iter()
            .map(|name| execute_envelope(server, remove_path_request(kind, parent_path, &name)))
            .collect(),
    }
}

fn create_path_group_envelopes(
    server: &Server,
    kind: CreatePathKind,
    groups: Vec<CreatePathGroup>,
) -> Result<Vec<MetadataRpcEnvelope>, ServerError> {
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

    let mut out = Vec::new();
    match committed {
        Ok(group_results) => {
            for (group, result) in groups.into_iter().zip(group_results) {
                match result {
                    Ok(entries) => out.extend(entries.iter().map(|entry| {
                        ok_envelope(
                            server,
                            MetadataRpcResult::Dentry {
                                entry: Some(Box::new(wire_dentry(entry))),
                            },
                        )
                    })),
                    Err(err) => {
                        let err = ServerError::Metadata(err);
                        if server_error_is_forward_to_leader(&err) {
                            return Err(err);
                        }
                        out.extend(create_path_batch_envelopes(
                            server,
                            kind,
                            &group.parent_path,
                            group.names,
                            group.mode,
                            group.uid,
                            group.gid,
                        )?);
                    }
                }
            }
        }
        Err(_) => {
            for group in groups {
                out.extend(create_path_batch_envelopes(
                    server,
                    kind,
                    &group.parent_path,
                    group.names,
                    group.mode,
                    group.uid,
                    group.gid,
                )?);
            }
        }
    }
    Ok(out)
}

fn remove_path_parts(request: &MetadataRpcRequest) -> Option<RemovePathParts> {
    match request {
        MetadataRpcRequest::RemoveFilePath { path } => {
            let (parent_path, name) = split_parent_path(path)?;
            Some(RemovePathParts {
                kind: RemovePathKind::File,
                parent_path,
                name,
            })
        }
        MetadataRpcRequest::RemoveEmptyDirPath { path } => {
            let (parent_path, name) = split_parent_path(path)?;
            Some(RemovePathParts {
                kind: RemovePathKind::EmptyDir,
                parent_path,
                name,
            })
        }
        _ => None,
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

fn remove_path_request(kind: RemovePathKind, parent_path: &str, name: &str) -> MetadataRpcRequest {
    let path = child_path(parent_path, name);
    match kind {
        RemovePathKind::File => MetadataRpcRequest::RemoveFilePath { path },
        RemovePathKind::EmptyDir => MetadataRpcRequest::RemoveEmptyDirPath { path },
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
    if refreshes_metadata_view(&request) {
        server.refresh_metadata_view()?;
    }
    match request {
        MetadataRpcRequest::Batch { requests } => execute_batch(server, requests),
        MetadataRpcRequest::RequireApplied { position, request } => {
            server.ensure_metadata_raft_applied(log_position(position)?)?;
            server.refresh_metadata_view()?;
            execute(server, *request)
        }
        MetadataRpcRequest::BootstrapRoot { mode, uid, gid } => {
            let attr = server.service().bootstrap_root(mode, uid, gid)?;
            Ok(MetadataRpcResult::InodeAttr {
                attr: Some(nokv_protocol::WireInodeAttr::from_inode_attr(&attr)),
            })
        }
        MetadataRpcRequest::GetAttr { inode } => {
            let attr = server.service().get_attr(inode_id(inode)?)?;
            Ok(MetadataRpcResult::InodeAttr {
                attr: attr
                    .as_ref()
                    .map(nokv_protocol::WireInodeAttr::from_inode_attr),
            })
        }
        MetadataRpcRequest::GetAttrAtSnapshot { snapshot_id, inode } => {
            let attr = server
                .service()
                .get_attr_at_snapshot(snapshot_id, inode_id(inode)?)?;
            Ok(MetadataRpcResult::InodeAttr {
                attr: attr
                    .as_ref()
                    .map(nokv_protocol::WireInodeAttr::from_inode_attr),
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
        MetadataRpcRequest::LookupPlusAtSnapshot {
            snapshot_id,
            parent,
            name,
        } => {
            let entry = server.service().lookup_plus_at_snapshot(
                snapshot_id,
                inode_id(parent)?,
                &dentry_name(name)?,
            )?;
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
        MetadataRpcRequest::ReadDirPlusAtSnapshot {
            snapshot_id,
            parent,
        } => {
            let entries = server
                .service()
                .read_dir_plus_at_snapshot(snapshot_id, inode_id(parent)?)?;
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
        MetadataRpcRequest::CreateSymlink {
            parent,
            name,
            target,
            mode,
            uid,
            gid,
        } => {
            let entry = server.service().create_symlink(
                inode_id(parent)?,
                dentry_name(name)?,
                target,
                mode,
                uid,
                gid,
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::CreateSpecialNode {
            parent,
            name,
            file_type,
            mode,
            rdev,
            uid,
            gid,
        } => {
            let file_type = decode_file_type(&file_type).map_err(protocol_error)?;
            let entry = server.service().create_special_node(
                inode_id(parent)?,
                dentry_name(name)?,
                SpecialNodeSpec {
                    file_type,
                    mode,
                    rdev,
                    uid,
                    gid,
                },
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::UpdateAttrs {
            parent,
            name,
            changes,
        } => {
            let entry = server.service().update_attrs(
                inode_id(parent)?,
                &dentry_name(name)?,
                update_attr(changes),
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::UpdateRootAttrs { changes } => {
            let attr = server.service().update_root_attrs(update_attr(changes))?;
            Ok(MetadataRpcResult::InodeAttr {
                attr: Some(nokv_protocol::WireInodeAttr::from_inode_attr(&attr)),
            })
        }
        MetadataRpcRequest::SetXattr {
            inode,
            name_hex,
            value,
            mode,
        } => {
            let name = decode_xattr_name(&name_hex).map_err(protocol_error)?;
            server
                .service()
                .set_xattr(inode_id(inode)?, &name, value, xattr_set_mode(mode))?;
            Ok(MetadataRpcResult::Unit)
        }
        MetadataRpcRequest::GetXattr { inode, name_hex } => {
            let name = decode_xattr_name(&name_hex).map_err(protocol_error)?;
            let value = server.service().get_xattr(inode_id(inode)?, &name)?;
            Ok(MetadataRpcResult::XattrValue { value })
        }
        MetadataRpcRequest::ListXattr { inode } => {
            let names = server.service().list_xattr(inode_id(inode)?)?;
            Ok(MetadataRpcResult::XattrNames {
                names_hex: names.iter().map(|name| encode_xattr_name(name)).collect(),
            })
        }
        MetadataRpcRequest::RemoveXattr { inode, name_hex } => {
            let name = decode_xattr_name(&name_hex).map_err(protocol_error)?;
            server.service().remove_xattr(inode_id(inode)?, &name)?;
            Ok(MetadataRpcResult::Unit)
        }
        MetadataRpcRequest::GetAdvisoryLock {
            inode,
            owner,
            start,
            end,
            kind,
            pid,
        } => {
            let lock = server.service().get_advisory_lock(AdvisoryLockRequest {
                inode: inode_id(inode)?,
                owner,
                start,
                end,
                kind: decode_advisory_lock_kind(&kind).map_err(protocol_error)?,
                pid,
                wait: false,
            })?;
            Ok(MetadataRpcResult::AdvisoryLock {
                lock: lock.as_ref().map(WireAdvisoryLock::from_advisory_lock),
            })
        }
        MetadataRpcRequest::SetAdvisoryLock {
            inode,
            owner,
            start,
            end,
            kind,
            pid,
            wait,
        } => {
            server.service().set_advisory_lock(AdvisoryLockRequest {
                inode: inode_id(inode)?,
                owner,
                start,
                end,
                kind: decode_advisory_lock_kind(&kind).map_err(protocol_error)?,
                pid,
                wait,
            })?;
            Ok(MetadataRpcResult::Unit)
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
            )?,
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
        MetadataRpcRequest::Link {
            inode,
            new_parent,
            new_name,
        } => {
            let entry = server.service().link(
                inode_id(inode)?,
                inode_id(new_parent)?,
                dentry_name(new_name)?,
            )?;
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
                snapshot: nokv_protocol::WireSnapshotPin::from_snapshot_pin(&snapshot),
            })
        }
        MetadataRpcRequest::SnapshotPin { snapshot_id } => {
            let snapshot = server.service().snapshot_pin(snapshot_id)?;
            Ok(MetadataRpcResult::SnapshotPin {
                snapshot: snapshot
                    .as_ref()
                    .map(nokv_protocol::WireSnapshotPin::from_snapshot_pin),
            })
        }
        MetadataRpcRequest::SnapshotSubtreePath { path } => {
            let snapshot = server.service().snapshot_subtree_path(&path)?;
            Ok(MetadataRpcResult::Snapshot {
                snapshot: nokv_protocol::WireSnapshotPin::from_snapshot_pin(&snapshot),
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
        MetadataRpcRequest::ReadFileAtSnapshot {
            snapshot_id,
            inode,
            offset,
            len,
        } => {
            let len = usize::try_from(len).map_err(|_| {
                ServerError::Metadata(MetadError::Codec(
                    "snapshot read length exceeds platform limit".to_owned(),
                ))
            })?;
            let bytes = server.service().read_file_at_snapshot(
                snapshot_id,
                inode_id(inode)?,
                offset,
                len,
            )?;
            Ok(MetadataRpcResult::FileBytes { bytes })
        }
        MetadataRpcRequest::ReadSymlink { inode } => {
            let bytes = server.service().read_symlink(inode_id(inode)?)?;
            Ok(MetadataRpcResult::FileBytes { bytes })
        }
        MetadataRpcRequest::ReadSymlinkAtSnapshot { snapshot_id, inode } => {
            let bytes = server
                .service()
                .read_symlink_at_snapshot(snapshot_id, inode_id(inode)?)?;
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
        MetadataRpcRequest::PublishPreparedArtifactStagedSession {
            prepared,
            producer,
            digest_uri,
            content_type,
            manifest_id,
            size,
            chunks,
            staged,
            mode,
            uid,
            gid,
        } => {
            if prepared.mount != server.service().mount_id().get() {
                return Err(ServerError::Metadata(MetadError::Codec(
                    "prepared artifact mount does not match server mount".to_owned(),
                )));
            }
            let prepared = prepared_artifact(prepared)?;
            let result = server.service().publish_prepared_artifact_staged_session(
                prepared.clone(),
                PublishArtifactStagedSession {
                    parent: prepared.parent,
                    name: prepared.name,
                    producer,
                    digest_uri,
                    content_type,
                    manifest_id,
                    size,
                    chunks: chunks
                        .into_iter()
                        .map(stored_chunk)
                        .collect::<Result<Vec<_>, _>>()?,
                    staged: staged_object_set(staged)?,
                    mode,
                    uid,
                    gid,
                },
            )?;
            Ok(MetadataRpcResult::RenameReplace {
                entry: Box::new(wire_dentry(&result.entry)),
                replaced: result
                    .replaced
                    .as_ref()
                    .map(|entry| Box::new(wire_dentry(entry))),
            })
        }
        MetadataRpcRequest::MetadataRaftAddLearner {
            node,
            address,
            blocking,
        } => {
            let node = NodeId::new(node).map_err(ServerError::MetadataRaft)?;
            let position = server.add_metadata_raft_learner(node, address, blocking)?;
            Ok(MetadataRpcResult::MetadataPosition {
                position: wire_log_position(position),
            })
        }
        MetadataRpcRequest::MetadataRaftVote { request } => {
            let response = server
                .service()
                .metadata_store()
                .handle_metadata_raft_vote(request)
                .map_err(MetadError::from)?;
            Ok(MetadataRpcResult::MetadataRaftVote { response })
        }
        MetadataRpcRequest::MetadataRaftAppendEntries { request } => {
            let response = server
                .service()
                .metadata_store()
                .handle_metadata_raft_append_entries(request)
                .map_err(MetadError::from)?;
            Ok(MetadataRpcResult::MetadataRaftAppendEntries { response })
        }
        MetadataRpcRequest::MetadataRaftInstallSnapshot { request } => {
            let response = server
                .service()
                .metadata_store()
                .handle_metadata_raft_install_snapshot(request)
                .map_err(MetadError::from)?;
            Ok(MetadataRpcResult::MetadataRaftInstallSnapshot { response })
        }
    }
}

fn refreshes_metadata_view(request: &MetadataRpcRequest) -> bool {
    match request {
        MetadataRpcRequest::Batch { requests } => requests.iter().any(refreshes_metadata_view),
        MetadataRpcRequest::RequireApplied { request, .. } => refreshes_metadata_view(request),
        MetadataRpcRequest::GetAttr { .. }
        | MetadataRpcRequest::GetAttrAtSnapshot { .. }
        | MetadataRpcRequest::LookupPlus { .. }
        | MetadataRpcRequest::LookupPlusAtSnapshot { .. }
        | MetadataRpcRequest::LookupPath { .. }
        | MetadataRpcRequest::StatPath { .. }
        | MetadataRpcRequest::ReadDirPlus { .. }
        | MetadataRpcRequest::ReadDirPlusPage { .. }
        | MetadataRpcRequest::ReadDirPlusAtSnapshot { .. }
        | MetadataRpcRequest::ReadDirPlusPath { .. }
        | MetadataRpcRequest::ReadDirPlusPathPage { .. }
        | MetadataRpcRequest::ReadIndexedPathPage { .. }
        | MetadataRpcRequest::StatPathAtSnapshot { .. }
        | MetadataRpcRequest::ReadDirPlusPathAtSnapshot { .. }
        | MetadataRpcRequest::ReadFileAtSnapshot { .. }
        | MetadataRpcRequest::ReadFilePathAtSnapshot { .. }
        | MetadataRpcRequest::ReadSymlink { .. }
        | MetadataRpcRequest::ReadSymlinkAtSnapshot { .. }
        | MetadataRpcRequest::GetXattr { .. }
        | MetadataRpcRequest::ListXattr { .. }
        | MetadataRpcRequest::ReadBodyPlan { .. }
        | MetadataRpcRequest::ReadPathPlan { .. }
        | MetadataRpcRequest::ReadArtifactPathAtSnapshot { .. }
        | MetadataRpcRequest::SnapshotPin { .. } => true,
        MetadataRpcRequest::BootstrapRoot { .. }
        | MetadataRpcRequest::CreateDir { .. }
        | MetadataRpcRequest::CreateDirPath { .. }
        | MetadataRpcRequest::CreateFile { .. }
        | MetadataRpcRequest::CreateSymlink { .. }
        | MetadataRpcRequest::CreateSpecialNode { .. }
        | MetadataRpcRequest::UpdateAttrs { .. }
        | MetadataRpcRequest::UpdateRootAttrs { .. }
        | MetadataRpcRequest::SetXattr { .. }
        | MetadataRpcRequest::GetAdvisoryLock { .. }
        | MetadataRpcRequest::SetAdvisoryLock { .. }
        | MetadataRpcRequest::RemoveXattr { .. }
        | MetadataRpcRequest::CreateFilePath { .. }
        | MetadataRpcRequest::CreateFilesInDirPath { .. }
        | MetadataRpcRequest::RemoveFile { .. }
        | MetadataRpcRequest::RemoveFilePath { .. }
        | MetadataRpcRequest::RemoveEmptyDir { .. }
        | MetadataRpcRequest::RemoveEmptyDirPath { .. }
        | MetadataRpcRequest::Link { .. }
        | MetadataRpcRequest::Rename { .. }
        | MetadataRpcRequest::RenamePath { .. }
        | MetadataRpcRequest::RenameReplace { .. }
        | MetadataRpcRequest::RenameReplacePath { .. }
        | MetadataRpcRequest::SnapshotSubtree { .. }
        | MetadataRpcRequest::SnapshotSubtreePath { .. }
        | MetadataRpcRequest::RetireSnapshot { .. }
        | MetadataRpcRequest::PrepareArtifact { .. }
        | MetadataRpcRequest::PrepareArtifactPath { .. }
        | MetadataRpcRequest::PublishPreparedArtifact { .. }
        | MetadataRpcRequest::PublishPreparedArtifactStagedSession { .. }
        | MetadataRpcRequest::MetadataRaftAddLearner { .. }
        | MetadataRpcRequest::MetadataRaftVote { .. }
        | MetadataRpcRequest::MetadataRaftAppendEntries { .. }
        | MetadataRpcRequest::MetadataRaftInstallSnapshot { .. } => false,
    }
}

fn inode_id(raw: u64) -> Result<InodeId, MetadError> {
    InodeId::new(raw).map_err(Into::into)
}

fn dentry_name(name: String) -> Result<DentryName, MetadError> {
    DentryName::new(name.into_bytes()).map_err(|err| MetadError::Codec(err.to_string()))
}

fn update_attr(wire: WireUpdateAttr) -> UpdateAttr {
    UpdateAttr {
        mode: wire.mode,
        uid: wire.uid,
        gid: wire.gid,
        size: wire.size,
        mtime_ms: wire.mtime_ms,
        ctime_ms: wire.ctime_ms,
    }
}

fn xattr_set_mode(wire: WireXattrSetMode) -> XattrSetMode {
    match wire {
        WireXattrSetMode::Any => XattrSetMode::Any,
        WireXattrSetMode::Create => XattrSetMode::Create,
        WireXattrSetMode::Replace => XattrSetMode::Replace,
    }
}

fn wire_dentry(entry: &DentryWithAttr) -> WireDentryWithAttr {
    WireDentryWithAttr {
        dentry: nokv_protocol::WireDentryRecord::from_dentry_record(&entry.dentry),
        attr: nokv_protocol::WireInodeAttr::from_inode_attr(&entry.attr),
        body: entry
            .body
            .as_ref()
            .map(nokv_protocol::WireBodyDescriptor::from_body_descriptor),
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

fn stored_chunk(chunk: nokv_protocol::WireChunkManifest) -> Result<StoredChunk, MetadError> {
    Ok(StoredChunk {
        chunk_index: chunk.chunk_index,
        logical_offset: chunk.logical_offset,
        len: chunk.len,
        blocks: chunk
            .slices
            .into_iter()
            .flat_map(|slice| slice.blocks.into_iter())
            .map(|block| {
                Ok(StoredBlock {
                    object_key: block.object_key,
                    logical_offset: block.logical_offset,
                    object_offset: block.object_offset,
                    len: block.len,
                    digest_uri: block.digest_uri,
                })
            })
            .collect::<Result<Vec<_>, MetadError>>()?,
    })
}

fn staged_object_set(staged: WireStagedObjectSet) -> Result<StagedObjectSet, MetadError> {
    staged
        .objects
        .into_iter()
        .map(|object| {
            Ok(StagedObject {
                key: ObjectKey::new(object.key)?,
                size: object.size,
            })
        })
        .collect::<Result<Vec<_>, MetadError>>()
        .map(StagedObjectSet::new)
}

fn wire_body_read_plan(plan: &nokv_meta::BodyReadPlan) -> WireBodyReadPlan {
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

fn wire_log_position(position: nokv_cluster::LogPosition) -> WireMetadataPosition {
    WireMetadataPosition {
        term: position.term.get(),
        index: position.index.get(),
    }
}

fn log_position(position: WireMetadataPosition) -> Result<nokv_cluster::LogPosition, ServerError> {
    Ok(nokv_cluster::LogPosition {
        term: nokv_cluster::LogTerm::new(position.term).map_err(ServerError::MetadataRaft)?,
        index: nokv_cluster::LogIndex::new(position.index).map_err(ServerError::MetadataRaft)?,
    })
}

#[cfg(test)]
#[path = "rpc_tests.rs"]
mod tests;
