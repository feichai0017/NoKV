use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::{collections::HashMap, io};

use nokvfs_meta::{DentryWithAttr, ObjectTransferStats, RenameReplaceResult};
use nokvfs_object::{
    delete_staged_objects, put_chunked_object, read_object_blocks, ChunkWriteOptions,
    MemoryBlockCache, ObjectError, ObjectReadBlock, ObjectStore, StagedObjectSet,
    DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
};
use nokvfs_protocol::{
    decode_envelope, encode_request, MetadataProtocolError, MetadataRpcEnvelope,
    MetadataRpcRequest, MetadataRpcResult, WireBodyDescriptor, WireBodyReadPlan, WireChunkManifest,
    WireDentryWithAttr, WireMetadataError, WireObjectReadBlock, WirePathMetadata,
    WirePreparedArtifact,
};
use nokvfs_types::{
    parse_absolute_path, BlockDescriptor, BodyDescriptor, ChunkManifest, DentryName, FileType,
    InodeId, PathMetadata, SnapshotPin,
};

use crate::{ArtifactMetadata, ClientError, NamespaceRead};

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RPC_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_BATCH_RPC_REQUESTS: usize = 512;
const FRAMED_RPC_MAGIC: &[u8; 8] = b"NKVRPC3\n";
const FRAME_HEADER_BYTES: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoteMetadataClientOptions {
    pub address: SocketAddr,
    pub timeout: Duration,
}

pub struct RemoteMetadataClient {
    options: RemoteMetadataClientOptions,
    next_request_id: AtomicU64,
    connection: Mutex<Option<Arc<PipelinedConnection>>>,
}

struct PipelinedConnection {
    writer: Mutex<TcpStream>,
    pending: Arc<Mutex<HashMap<u64, mpsc::Sender<PendingFrame>>>>,
}

enum PendingFrame {
    Payload(Vec<u8>),
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteBodyReadPlan {
    pub output_len: usize,
    pub blocks: Vec<ObjectReadBlock>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemotePreparedArtifact {
    pub mount: u64,
    pub parent: InodeId,
    pub name: DentryName,
    pub inode: InodeId,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
    pub replace: bool,
    pub dentry_version: Option<u64>,
    pub old_generation: Option<u64>,
}

pub struct RemoteNoKvFsClient<O> {
    metadata: RemoteMetadataClient,
    objects: O,
    block_cache: MemoryBlockCache,
    block_cache_enabled: bool,
    object_puts: AtomicU64,
    object_gets: AtomicU64,
    cache_hits: AtomicU64,
    manifest_chunks: AtomicU64,
    manifest_blocks: AtomicU64,
}

impl RemoteMetadataClientOptions {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            timeout: DEFAULT_RPC_TIMEOUT,
        }
    }
}

impl<O> RemoteNoKvFsClient<O>
where
    O: ObjectStore,
{
    pub fn new(metadata: RemoteMetadataClient, objects: O) -> Self {
        Self {
            metadata,
            objects,
            block_cache: MemoryBlockCache::default(),
            block_cache_enabled: true,
            object_puts: AtomicU64::new(0),
            object_gets: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            manifest_chunks: AtomicU64::new(0),
            manifest_blocks: AtomicU64::new(0),
        }
    }

    pub fn connect(address: SocketAddr, objects: O) -> Self {
        Self::new(RemoteMetadataClient::connect(address), objects)
    }

    pub fn metadata(&self) -> &RemoteMetadataClient {
        &self.metadata
    }

    pub fn set_block_cache_enabled(&mut self, enabled: bool) {
        self.block_cache_enabled = enabled;
    }

    pub fn block_cache_enabled(&self) -> bool {
        self.block_cache_enabled
    }

    pub fn object_stats(&self) -> ObjectTransferStats {
        ObjectTransferStats {
            object_puts: self.object_puts.load(Ordering::Relaxed),
            object_gets: self.object_gets.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            manifest_chunks: self.manifest_chunks.load(Ordering::Relaxed),
            manifest_blocks: self.manifest_blocks.load(Ordering::Relaxed),
        }
    }

    pub fn cat(&self, path: &str) -> Result<Vec<u8>, ClientError> {
        let entry = self
            .metadata
            .lookup(path)?
            .ok_or_else(|| ClientError::NotFound(path.to_owned()))?;
        self.read_entry(path, &entry, 0, file_len(entry.attr.size)?)
    }

    pub fn cat_snapshot(&self, snapshot_id: u64, path: &str) -> Result<Vec<u8>, ClientError> {
        self.metadata.read_artifact_at_snapshot(snapshot_id, path)
    }

    pub fn read_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        self.metadata
            .read_file_path_at_snapshot(snapshot_id, path, offset, len)
    }

    pub fn read(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, ClientError> {
        let entry = self
            .metadata
            .lookup(path)?
            .ok_or_else(|| ClientError::NotFound(path.to_owned()))?;
        self.read_entry(path, &entry, offset, len)
    }

    pub fn read_path(
        &self,
        path: &str,
        offset: u64,
        len: usize,
        expected_generation: Option<u64>,
    ) -> Result<NamespaceRead, ClientError> {
        let metadata = self
            .metadata
            .stat_path(path)?
            .ok_or_else(|| ClientError::NotFound(path.to_owned()))?;
        if metadata.attr.file_type != FileType::File {
            return Err(ClientError::Metadata(nokvfs_meta::MetadError::NotFile));
        }
        if let Some(expected) = expected_generation {
            if metadata.attr.generation != expected {
                return Err(ClientError::Metadata(
                    nokvfs_meta::MetadError::StaleBodyGeneration {
                        expected,
                        current: metadata.attr.generation,
                    },
                ));
            }
        }
        let bytes = self.read_path_metadata(path, &metadata, offset, len)?;
        Ok(NamespaceRead { metadata, bytes })
    }

    fn read_entry(
        &self,
        path: &str,
        entry: &DentryWithAttr,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        if entry.attr.file_type != FileType::File {
            return Err(ClientError::Metadata(nokvfs_meta::MetadError::NotFile));
        }
        if len == 0 || offset >= entry.attr.size {
            return Ok(Vec::new());
        }
        let body = entry.body.as_ref().ok_or_else(|| {
            ClientError::Protocol(format!("file {path} is missing body descriptor"))
        })?;
        let len = bounded_read_len(entry.attr.size - offset, len)?;
        let plan = self
            .metadata
            .read_body_plan(entry.attr.inode, body.generation, offset, len)?;
        let cache = if self.block_cache_enabled {
            Some(&self.block_cache)
        } else {
            None
        };
        let outcome = read_object_blocks(&self.objects, cache, plan.output_len, &plan.blocks)
            .map_err(ClientError::Object)?;
        self.object_gets
            .fetch_add(outcome.object_gets as u64, Ordering::Relaxed);
        self.cache_hits
            .fetch_add(outcome.cache_hits as u64, Ordering::Relaxed);
        Ok(outcome.bytes)
    }

    fn read_path_metadata(
        &self,
        path: &str,
        metadata: &PathMetadata,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        if len == 0 || offset >= metadata.attr.size {
            return Ok(Vec::new());
        }
        let body = metadata.body.as_ref().ok_or_else(|| {
            ClientError::Protocol(format!("file {path} is missing body descriptor"))
        })?;
        let len = bounded_read_len(metadata.attr.size - offset, len)?;
        let plan =
            self.metadata
                .read_body_plan(metadata.attr.inode, body.generation, offset, len)?;
        let cache = if self.block_cache_enabled {
            Some(&self.block_cache)
        } else {
            None
        };
        let outcome = read_object_blocks(&self.objects, cache, plan.output_len, &plan.blocks)
            .map_err(ClientError::Object)?;
        self.object_gets
            .fetch_add(outcome.object_gets as u64, Ordering::Relaxed);
        self.cache_hits
            .fetch_add(outcome.cache_hits as u64, Ordering::Relaxed);
        Ok(outcome.bytes)
    }

    pub fn put_artifact(
        &self,
        path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, ClientError> {
        let prepared = self.metadata.prepare_artifact_path(path, false)?;
        let mode = metadata.mode;
        let uid = metadata.uid;
        let gid = metadata.gid;
        let (body, chunks, staged) = self.stage_artifact_body(&prepared, &bytes, metadata)?;
        match self
            .metadata
            .publish_prepared_artifact(prepared, body, chunks, mode, uid, gid)
        {
            Ok(result) => Ok(result.entry),
            Err(err) => {
                delete_staged_objects(&self.objects, &staged).map_err(ClientError::Object)?;
                Err(err)
            }
        }
    }

    pub fn put_artifact_replace(
        &self,
        path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<RenameReplaceResult, ClientError> {
        let prepared = self.metadata.prepare_artifact_path(path, true)?;
        let mode = metadata.mode;
        let uid = metadata.uid;
        let gid = metadata.gid;
        let (body, chunks, staged) = self.stage_artifact_body(&prepared, &bytes, metadata)?;
        match self
            .metadata
            .publish_prepared_artifact(prepared, body, chunks, mode, uid, gid)
        {
            Ok(result) => Ok(result),
            Err(err) => {
                delete_staged_objects(&self.objects, &staged).map_err(ClientError::Object)?;
                Err(err)
            }
        }
    }

    fn stage_artifact_body(
        &self,
        prepared: &RemotePreparedArtifact,
        bytes: &[u8],
        metadata: ArtifactMetadata,
    ) -> Result<(BodyDescriptor, Vec<ChunkManifest>, StagedObjectSet), ClientError> {
        let written = match put_chunked_object(
            &self.objects,
            bytes,
            ChunkWriteOptions {
                manifest_id: metadata.manifest_id,
                mount: prepared.mount,
                inode: prepared.inode.get(),
                generation: prepared.generation,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
        ) {
            Ok(written) => written,
            Err(err) => {
                cleanup_staged_write_error(&self.objects, &err)?;
                return Err(ClientError::Object(err));
            }
        };
        self.object_puts
            .fetch_add(written.object_puts as u64, Ordering::Relaxed);
        self.manifest_chunks
            .fetch_add(written.chunks.len() as u64, Ordering::Relaxed);
        self.manifest_blocks.fetch_add(
            written
                .chunks
                .iter()
                .map(|chunk| chunk.blocks.len() as u64)
                .sum::<u64>(),
            Ordering::Relaxed,
        );
        let staged = written.staged_objects()?;
        let chunks = written
            .chunks
            .iter()
            .map(|chunk| ChunkManifest {
                chunk_index: chunk.chunk_index,
                logical_offset: chunk.logical_offset,
                len: chunk.len,
                blocks: chunk
                    .blocks
                    .iter()
                    .map(|block| BlockDescriptor {
                        object_key: block.object_key.clone(),
                        logical_offset: block.logical_offset,
                        object_offset: block.object_offset,
                        len: block.len,
                        digest_uri: block.digest_uri.clone(),
                    })
                    .collect(),
            })
            .collect();
        Ok((
            BodyDescriptor {
                producer: metadata.producer,
                digest_uri: metadata.digest_uri,
                size: written.size,
                content_type: metadata.content_type,
                manifest_id: written.manifest_id,
                generation: prepared.generation,
                chunk_size: written.chunk_size,
                block_size: written.block_size,
            },
            chunks,
            staged,
        ))
    }
}

fn cleanup_staged_write_error<O: ObjectStore>(
    objects: &O,
    err: &ObjectError,
) -> Result<(), ClientError> {
    if let ObjectError::StagedWriteFailed { staged, .. } = err {
        delete_staged_objects(objects, staged).map_err(ClientError::Object)?;
    }
    Ok(())
}

impl RemoteMetadataClient {
    pub fn new(options: RemoteMetadataClientOptions) -> Self {
        Self {
            options,
            next_request_id: AtomicU64::new(1),
            connection: Mutex::new(None),
        }
    }

    pub fn connect(address: SocketAddr) -> Self {
        Self::new(RemoteMetadataClientOptions::new(address))
    }

    pub fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::BootstrapRoot { mode, uid, gid })? {
            MetadataRpcResult::InodeAttr { .. } => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn mkdir(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateDirPath {
            path: path.to_owned(),
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_file(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateFilePath {
            path: path.to_owned(),
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_files(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut entries = Vec::with_capacity(paths.len());
        for chunk in paths.chunks(MAX_BATCH_RPC_REQUESTS) {
            let request = create_files_request(chunk, mode, uid, gid)?;
            let results: Vec<Result<MetadataRpcResult, ClientError>> = match self.call(request)? {
                MetadataRpcResult::Batch { results } => {
                    results.into_iter().map(envelope_result).collect()
                }
                other => return Err(unexpected_result(other)),
            };
            for result in results {
                entries.push(result.and_then(dentry_result));
            }
        }
        Ok(entries)
    }

    pub fn lookup(&self, path: &str) -> Result<Option<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::LookupPath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry } => {
                entry.map(|entry| wire_dentry(*entry)).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn stat_path(&self, path: &str) -> Result<Option<PathMetadata>, ClientError> {
        match self.call(MetadataRpcRequest::StatPath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::PathMetadata { metadata } => {
                metadata.map(wire_path_metadata).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn list(&self, path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::ReadDirPlusPath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentries { entries } => {
                entries.into_iter().map(wire_dentry).collect()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn stat_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Option<PathMetadata>, ClientError> {
        match self.call(MetadataRpcRequest::StatPathAtSnapshot {
            snapshot_id,
            path: path.to_owned(),
        })? {
            MetadataRpcResult::PathMetadata { metadata } => {
                metadata.map(wire_path_metadata).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn list_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Vec<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::ReadDirPlusPathAtSnapshot {
            snapshot_id,
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentries { entries } => {
                entries.into_iter().map(wire_dentry).collect()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove(&self, path: &str) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RemoveFilePath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rmdir(&self, path: &str) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RemoveEmptyDirPath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rename(&self, source: &str, destination: &str) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RenamePath {
            source: source.to_owned(),
            destination: destination.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rename_replace(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<RenameReplaceResult, ClientError> {
        match self.call(MetadataRpcRequest::RenameReplacePath {
            source: source.to_owned(),
            destination: destination.to_owned(),
        })? {
            MetadataRpcResult::RenameReplace { entry, replaced } => Ok(RenameReplaceResult {
                entry: wire_dentry(*entry)?,
                replaced: replaced.map(|entry| wire_dentry(*entry)).transpose()?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn snapshot(&self, path: &str) -> Result<SnapshotPin, ClientError> {
        match self.call(MetadataRpcRequest::SnapshotSubtreePath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Snapshot { snapshot } => wire_snapshot(snapshot),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn retire_snapshot(&self, snapshot_id: u64) -> Result<bool, ClientError> {
        match self.call(MetadataRpcRequest::RetireSnapshot { snapshot_id })? {
            MetadataRpcResult::RetiredSnapshot { retired } => Ok(retired),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_body_plan(
        &self,
        inode: InodeId,
        generation: u64,
        offset: u64,
        len: usize,
    ) -> Result<RemoteBodyReadPlan, ClientError> {
        let len = u64::try_from(len)
            .map_err(|_| ClientError::Protocol("body read length exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::ReadBodyPlan {
            inode: inode.get(),
            generation,
            offset,
            len,
        })? {
            MetadataRpcResult::BodyReadPlan { plan } => wire_body_read_plan(plan),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_artifact_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Vec<u8>, ClientError> {
        match self.call(MetadataRpcRequest::ReadArtifactPathAtSnapshot {
            snapshot_id,
            path: path.to_owned(),
        })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_file_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        let len = u64::try_from(len)
            .map_err(|_| ClientError::Protocol("snapshot read length exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::ReadFilePathAtSnapshot {
            snapshot_id,
            path: path.to_owned(),
            offset,
            len,
        })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn prepare_artifact(
        &self,
        parent: InodeId,
        name: DentryName,
        replace: bool,
    ) -> Result<RemotePreparedArtifact, ClientError> {
        match self.call(MetadataRpcRequest::PrepareArtifact {
            parent: parent.get(),
            name: rpc_name(&name)?,
            replace,
        })? {
            MetadataRpcResult::PreparedArtifact { prepared } => wire_prepared_artifact(prepared),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn prepare_artifact_path(
        &self,
        path: &str,
        replace: bool,
    ) -> Result<RemotePreparedArtifact, ClientError> {
        match self.call(MetadataRpcRequest::PrepareArtifactPath {
            path: path.to_owned(),
            replace,
        })? {
            MetadataRpcResult::PreparedArtifact { prepared } => wire_prepared_artifact(prepared),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn publish_prepared_artifact(
        &self,
        prepared: RemotePreparedArtifact,
        body: BodyDescriptor,
        chunks: Vec<ChunkManifest>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<RenameReplaceResult, ClientError> {
        match self.call(MetadataRpcRequest::PublishPreparedArtifact {
            prepared: prepared_artifact_to_wire(&prepared)?,
            body: Box::new(body_to_wire(&body)),
            chunks: chunks.iter().map(chunk_to_wire).collect(),
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::RenameReplace { entry, replaced } => Ok(RenameReplaceResult {
                entry: wire_dentry(*entry)?,
                replaced: replaced.map(|entry| wire_dentry(*entry)).transpose()?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    fn call(&self, request: MetadataRpcRequest) -> Result<MetadataRpcResult, ClientError> {
        let body =
            encode_request(&request).map_err(|err| ClientError::Protocol(err.to_string()))?;
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let connection = self.connection()?;
        let body = connection.call(request_id, &body, self.options.timeout)?;
        let envelope =
            decode_envelope(&body).map_err(|err| ClientError::Protocol(err.to_string()))?;
        envelope_result(envelope)
    }

    fn connection(&self) -> Result<Arc<PipelinedConnection>, ClientError> {
        let mut guard = self.connection.lock().expect("metadata rpc connection");
        if let Some(connection) = &*guard {
            return Ok(Arc::clone(connection));
        }
        let connection = Arc::new(PipelinedConnection::connect(self.options.address)?);
        *guard = Some(Arc::clone(&connection));
        Ok(connection)
    }
}

fn create_files_request(
    paths: &[String],
    mode: u32,
    uid: u32,
    gid: u32,
) -> Result<MetadataRpcRequest, ClientError> {
    let mut parent_path = None;
    let mut names = Vec::with_capacity(paths.len());
    for path in paths {
        let (parent, name) = rpc_parent_and_name(path)?;
        if parent_path
            .as_deref()
            .is_some_and(|existing| existing != parent)
        {
            let requests = paths
                .iter()
                .map(|path| MetadataRpcRequest::CreateFilePath {
                    path: path.clone(),
                    mode,
                    uid,
                    gid,
                })
                .collect();
            return Ok(MetadataRpcRequest::Batch { requests });
        }
        parent_path = Some(parent);
        names.push(name);
    }
    Ok(MetadataRpcRequest::CreateFilesInDirPath {
        parent_path: parent_path.unwrap_or_else(|| "/".to_owned()),
        names,
        mode,
        uid,
        gid,
    })
}

fn rpc_parent_and_name(path: &str) -> Result<(String, String), ClientError> {
    let mut components = parse_absolute_path(path)?;
    let name = components.pop().ok_or(ClientError::RootHasNoParent)?;
    let name = rpc_name(&name)?;
    let mut parent = String::from("/");
    for (index, component) in components.iter().enumerate() {
        if index > 0 {
            parent.push('/');
        }
        parent.push_str(&rpc_name(component)?);
    }
    Ok((parent, name))
}

impl PipelinedConnection {
    fn connect(address: SocketAddr) -> Result<Self, ClientError> {
        let mut stream =
            TcpStream::connect(address).map_err(|err| ClientError::Io(err.to_string()))?;
        stream
            .write_all(FRAMED_RPC_MAGIC)
            .map_err(|err| ClientError::Io(err.to_string()))?;
        let reader = stream
            .try_clone()
            .map_err(|err| ClientError::Io(err.to_string()))?;
        let connection = Self {
            writer: Mutex::new(stream),
            pending: Arc::new(Mutex::new(HashMap::new())),
        };
        connection.spawn_reader(reader);
        Ok(connection)
    }

    fn spawn_reader(&self, mut reader: TcpStream) {
        let pending = Arc::clone(&self.pending);
        thread::spawn(move || loop {
            match read_frame(&mut reader) {
                Ok((request_id, _flags, payload)) => {
                    let waiter = pending
                        .lock()
                        .expect("metadata rpc pending")
                        .remove(&request_id);
                    if let Some(waiter) = waiter {
                        let _ = waiter.send(PendingFrame::Payload(payload));
                    }
                }
                Err(err) => {
                    let mut pending = pending.lock().expect("metadata rpc pending");
                    let waiters = pending
                        .drain()
                        .map(|(_, waiter)| waiter)
                        .collect::<Vec<_>>();
                    drop(pending);
                    for waiter in waiters {
                        let _ = waiter.send(PendingFrame::Failed(err.to_string()));
                    }
                    break;
                }
            }
        });
    }

    fn call(
        &self,
        request_id: u64,
        body: &[u8],
        timeout: Duration,
    ) -> Result<Vec<u8>, ClientError> {
        let (tx, rx) = mpsc::channel();
        self.pending
            .lock()
            .expect("metadata rpc pending")
            .insert(request_id, tx);
        let write_result = {
            let mut writer = self.writer.lock().expect("metadata rpc writer");
            write_frame(&mut writer, request_id, 0, body)
        };
        if let Err(err) = write_result {
            self.pending
                .lock()
                .expect("metadata rpc pending")
                .remove(&request_id);
            return Err(err);
        }
        match rx.recv_timeout(timeout) {
            Ok(PendingFrame::Payload(payload)) => Ok(payload),
            Ok(PendingFrame::Failed(err)) => Err(ClientError::Io(err)),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.pending
                    .lock()
                    .expect("metadata rpc pending")
                    .remove(&request_id);
                Err(ClientError::Io(
                    "metadata rpc response timed out".to_owned(),
                ))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(ClientError::Io("metadata rpc connection closed".to_owned()))
            }
        }
    }
}

fn write_frame(
    stream: &mut TcpStream,
    request_id: u64,
    flags: u32,
    body: &[u8],
) -> Result<(), ClientError> {
    let len = u32::try_from(body.len())
        .map_err(|_| ClientError::Protocol("metadata rpc request exceeds u32".to_owned()))?;
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    header[0..8].copy_from_slice(&request_id.to_be_bytes());
    header[8..12].copy_from_slice(&flags.to_be_bytes());
    header[12..16].copy_from_slice(&len.to_be_bytes());
    stream
        .write_all(&header)
        .and_then(|_| stream.write_all(body))
        .map_err(|err| ClientError::Io(err.to_string()))
}

fn read_frame(stream: &mut TcpStream) -> Result<(u64, u32, Vec<u8>), ClientError> {
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    stream.read_exact(&mut header).map_err(rpc_read_error)?;
    let request_id = u64::from_be_bytes(header[0..8].try_into().expect("request id header"));
    let flags = u32::from_be_bytes(header[8..12].try_into().expect("flags header"));
    let len = u32::from_be_bytes(header[12..16].try_into().expect("length header")) as usize;
    if len > MAX_RPC_RESPONSE_BYTES {
        return Err(ClientError::Protocol(
            "metadata rpc response exceeds size limit".to_owned(),
        ));
    }
    let mut body = vec![0_u8; len];
    stream
        .read_exact(&mut body)
        .map_err(|err| ClientError::Io(err.to_string()))?;
    Ok((request_id, flags, body))
}

fn rpc_read_error(err: io::Error) -> ClientError {
    ClientError::Io(err.to_string())
}

fn rpc_name(name: &DentryName) -> Result<String, ClientError> {
    String::from_utf8(name.as_bytes().to_vec())
        .map_err(|_| ClientError::InvalidName("remote rpc requires utf-8 names".to_owned()))
}

fn wire_dentry(entry: WireDentryWithAttr) -> Result<DentryWithAttr, ClientError> {
    Ok(DentryWithAttr {
        dentry: entry.dentry.into_dentry_record().map_err(protocol_error)?,
        attr: entry.attr.into_inode_attr().map_err(protocol_error)?,
        body: entry.body.map(|body| body.into_body_descriptor()),
    })
}

fn wire_path_metadata(metadata: WirePathMetadata) -> Result<PathMetadata, ClientError> {
    metadata.into_path_metadata().map_err(protocol_error)
}

fn wire_prepared_artifact(
    prepared: WirePreparedArtifact,
) -> Result<RemotePreparedArtifact, ClientError> {
    Ok(RemotePreparedArtifact {
        mount: prepared.mount,
        parent: inode_id(prepared.parent)?,
        name: DentryName::new(prepared.name.into_bytes())
            .map_err(|err| ClientError::InvalidName(err.to_string()))?,
        inode: inode_id(prepared.inode)?,
        generation: prepared.generation,
        mtime_ms: prepared.mtime_ms,
        ctime_ms: prepared.ctime_ms,
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
    })
}

fn prepared_artifact_to_wire(
    prepared: &RemotePreparedArtifact,
) -> Result<WirePreparedArtifact, ClientError> {
    Ok(WirePreparedArtifact {
        mount: prepared.mount,
        parent: prepared.parent.get(),
        name: rpc_name(&prepared.name)?,
        inode: prepared.inode.get(),
        generation: prepared.generation,
        mtime_ms: prepared.mtime_ms,
        ctime_ms: prepared.ctime_ms,
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
    })
}

fn body_to_wire(body: &BodyDescriptor) -> WireBodyDescriptor {
    WireBodyDescriptor::from_body_descriptor(body)
}

fn chunk_to_wire(chunk: &ChunkManifest) -> WireChunkManifest {
    WireChunkManifest::from_chunk_manifest(chunk)
}

fn wire_body_read_plan(plan: WireBodyReadPlan) -> Result<RemoteBodyReadPlan, ClientError> {
    Ok(RemoteBodyReadPlan {
        output_len: usize::try_from(plan.output_len).map_err(|_| {
            ClientError::Protocol("body read plan output length exceeds platform limit".to_owned())
        })?,
        blocks: plan
            .blocks
            .into_iter()
            .map(wire_object_read_block)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn wire_object_read_block(block: WireObjectReadBlock) -> Result<ObjectReadBlock, ClientError> {
    Ok(ObjectReadBlock {
        object_key: block.object_key,
        object_offset: block.object_offset,
        len: usize::try_from(block.len).map_err(|_| {
            ClientError::Protocol("body read block length exceeds platform limit".to_owned())
        })?,
        output_offset: usize::try_from(block.output_offset).map_err(|_| {
            ClientError::Protocol("body read block offset exceeds platform limit".to_owned())
        })?,
    })
}

fn wire_snapshot(snapshot: nokvfs_protocol::WireSnapshotPin) -> Result<SnapshotPin, ClientError> {
    snapshot.into_snapshot_pin().map_err(protocol_error)
}

fn file_len(size: u64) -> Result<usize, ClientError> {
    usize::try_from(size)
        .map_err(|_| ClientError::Protocol("file size exceeds platform limit".to_owned()))
}

fn bounded_read_len(available: u64, requested: usize) -> Result<usize, ClientError> {
    let requested_u64 = u64::try_from(requested).unwrap_or(u64::MAX);
    if requested_u64 <= available {
        return Ok(requested);
    }
    file_len(available)
}

fn inode_id(raw: u64) -> Result<InodeId, ClientError> {
    InodeId::new(raw).map_err(|err| ClientError::Protocol(err.to_string()))
}

fn protocol_error(err: MetadataProtocolError) -> ClientError {
    ClientError::Protocol(err.to_string())
}

fn dentry_result(result: MetadataRpcResult) -> Result<DentryWithAttr, ClientError> {
    match result {
        MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
        other => Err(unexpected_result(other)),
    }
}

fn envelope_result(envelope: MetadataRpcEnvelope) -> Result<MetadataRpcResult, ClientError> {
    if !envelope.ok {
        let message = envelope
            .error
            .unwrap_or_else(|| "unknown remote error".to_owned());
        let Some(error) = envelope.error_kind else {
            return Err(ClientError::Protocol(format!(
                "metadata rpc error is missing typed error_kind: {message}"
            )));
        };
        return Err(client_error_from_wire_error(error));
    }
    envelope
        .result
        .ok_or_else(|| ClientError::Protocol("metadata rpc response missing result".to_owned()))
}

fn client_error_from_wire_error(error: WireMetadataError) -> ClientError {
    match error {
        WireMetadataError::NotFound => ClientError::Metadata(nokvfs_meta::MetadError::NotFound),
        WireMetadataError::NotFile => ClientError::Metadata(nokvfs_meta::MetadError::NotFile),
        WireMetadataError::NotDirectory => {
            ClientError::Metadata(nokvfs_meta::MetadError::NotDirectory)
        }
        WireMetadataError::MissingBodyDescriptor => {
            ClientError::Metadata(nokvfs_meta::MetadError::MissingBodyDescriptor)
        }
        WireMetadataError::PredicateFailed => ClientError::Metadata(
            nokvfs_meta::MetadError::Metadata(nokvfs_meta::MetadataError::PredicateFailed),
        ),
        WireMetadataError::StaleBodyGeneration { expected, current } => {
            ClientError::Metadata(nokvfs_meta::MetadError::StaleBodyGeneration {
                expected,
                current,
            })
        }
        WireMetadataError::InvalidPath { message } => {
            ClientError::Metadata(nokvfs_meta::MetadError::InvalidPath(message))
        }
        WireMetadataError::Metadata { message } => ClientError::Metadata(
            nokvfs_meta::MetadError::Metadata(nokvfs_meta::MetadataError::Backend(message)),
        ),
        WireMetadataError::Object { message } => {
            ClientError::Object(nokvfs_object::ObjectError::Backend(message))
        }
        WireMetadataError::Io { message } => ClientError::Io(message),
        WireMetadataError::Protocol { message } => ClientError::Protocol(message),
    }
}

fn unexpected_result(result: MetadataRpcResult) -> ClientError {
    ClientError::Protocol(format!("unexpected metadata rpc result {result:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokvfs_object::{MemoryObjectStore, ObjectKey};
    use nokvfs_protocol::{decode_request, encode_envelope, WireDentryRecord, WireInodeAttr};
    use std::net::TcpListener;
    use std::thread;

    fn serve_one(body: &'static str) -> SocketAddr {
        serve_many(vec![response_body(body)])
    }

    fn serve_many(bodies: Vec<Vec<u8>>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
            stream.read_exact(&mut magic).unwrap();
            assert_eq!(&magic, FRAMED_RPC_MAGIC);
            for body in bodies {
                let (request_id, flags, request) = read_frame(&mut stream).unwrap();
                decode_request(&request).expect("framed request is binary metadata rpc");
                write_frame(&mut stream, request_id, flags, &body).unwrap();
            }
        });
        addr
    }

    fn response_body(json: &str) -> Vec<u8> {
        let envelope: MetadataRpcEnvelope = serde_json::from_str(json).unwrap();
        encode_envelope(&envelope).unwrap()
    }

    fn dentry_response(parent: u64, name: &str, inode: u64, generation: u64) -> Vec<u8> {
        let envelope = MetadataRpcEnvelope {
            ok: true,
            result: Some(MetadataRpcResult::Dentry {
                entry: Some(Box::new(WireDentryWithAttr {
                    dentry: WireDentryRecord {
                        parent,
                        name_hex: name
                            .as_bytes()
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>(),
                        child: inode,
                        child_type: "file".to_owned(),
                        attr_generation: generation,
                    },
                    attr: WireInodeAttr {
                        inode,
                        file_type: "file".to_owned(),
                        mode: 0o644,
                        uid: 1000,
                        gid: 1000,
                        size: 0,
                        generation,
                        mtime_ms: generation,
                        ctime_ms: generation,
                    },
                    body: None,
                })),
            }),
            error: None,
            error_kind: None,
        };
        encode_envelope(&envelope).unwrap()
    }

    fn dentry_response_for_request(request: &MetadataRpcRequest) -> Vec<u8> {
        let MetadataRpcRequest::CreateFilePath { path, .. } = request else {
            panic!("unexpected pipelined request: {request:?}");
        };
        let name = path.rsplit('/').next().expect("path has a final component");
        let inode = match name {
            "a.bin" => 40,
            "b.bin" => 41,
            other => panic!("unexpected file name: {other}"),
        };
        dentry_response(2, name, inode, inode)
    }

    fn artifact_metadata(manifest_id: &str) -> ArtifactMetadata {
        ArtifactMetadata {
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:demo".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: manifest_id.to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        }
    }

    #[test]
    fn remote_mkdir_sends_metadata_rpc() {
        let addr = serve_one(
            r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_hex":"72756e73","child":2,"child_type":"directory","attr_generation":1},"attr":{"inode":2,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"size":0,"generation":1,"mtime_ms":1,"ctime_ms":1},"body":null}}}"#,
        );
        let client = RemoteMetadataClient::connect(addr);
        let entry = client.mkdir("/runs", 0o755, 1000, 1000).unwrap();
        assert_eq!(entry.attr.inode.get(), 2);
        assert_eq!(entry.dentry.name.as_bytes(), b"runs");
    }

    #[test]
    fn remote_create_file_uses_single_path_rpc_for_nested_parent() {
        let addr = serve_one(
            r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"636865636b706f696e742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}}"#,
        );
        let client = RemoteMetadataClient::connect(addr);
        let entry = client
            .create_file("/runs/checkpoint.bin", 0o644, 1000, 1000)
            .unwrap();
        assert_eq!(entry.attr.inode.get(), 42);
        assert_eq!(entry.dentry.name.as_bytes(), b"checkpoint.bin");
    }

    #[test]
    fn remote_create_files_uses_single_coalesced_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
            stream.read_exact(&mut magic).unwrap();
            assert_eq!(&magic, FRAMED_RPC_MAGIC);
            let (request_id, flags, request) = read_frame(&mut stream).unwrap();
            let request = decode_request(&request).unwrap();
            match request {
                MetadataRpcRequest::CreateFilesInDirPath {
                    parent_path, names, ..
                } => {
                    assert_eq!(parent_path, "/runs");
                    assert_eq!(names, vec!["a.bin".to_owned(), "b.bin".to_owned()]);
                }
                other => panic!("unexpected request: {other:?}"),
            }
            let response = response_body(
                r#"{"ok":true,"result":{"type":"batch","results":[{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"612e62696e","child":40,"child_type":"file","attr_generation":7},"attr":{"inode":40,"file_type":"file","mode":420,"uid":1000,"gid":1000,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}},{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"622e62696e","child":41,"child_type":"file","attr_generation":8},"attr":{"inode":41,"file_type":"file","mode":420,"uid":1000,"gid":1000,"size":0,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":null}}}]}}"#,
            );
            write_frame(&mut stream, request_id, flags, &response).unwrap();
        });
        let client = RemoteMetadataClient::connect(addr);
        let paths = vec!["/runs/a.bin".to_owned(), "/runs/b.bin".to_owned()];
        let entries = client.create_files(&paths, 0o644, 1000, 1000).unwrap();
        let entries = entries.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(entries[0].attr.inode.get(), 40);
        assert_eq!(entries[1].attr.inode.get(), 41);
    }

    #[test]
    fn remote_framed_rpc_accepts_out_of_order_responses() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
            stream.read_exact(&mut magic).unwrap();
            assert_eq!(&magic, FRAMED_RPC_MAGIC);

            let first = read_frame(&mut stream).unwrap();
            let second = read_frame(&mut stream).unwrap();
            let first_request = decode_request(&first.2).unwrap();
            let second_request = decode_request(&second.2).unwrap();
            let first_response = dentry_response_for_request(&first_request);
            let second_response = dentry_response_for_request(&second_request);

            write_frame(&mut stream, second.0, second.1, &second_response).unwrap();
            write_frame(&mut stream, first.0, first.1, &first_response).unwrap();
        });

        let client = Arc::new(RemoteMetadataClient::connect(addr));
        let first = {
            let client = Arc::clone(&client);
            thread::spawn(move || client.create_file("/runs/a.bin", 0o644, 1000, 1000))
        };
        let second = {
            let client = Arc::clone(&client);
            thread::spawn(move || client.create_file("/runs/b.bin", 0o644, 1000, 1000))
        };

        let first = first.join().unwrap().unwrap();
        let second = second.join().unwrap().unwrap();
        assert_eq!(first.dentry.name.as_bytes(), b"a.bin");
        assert_eq!(second.dentry.name.as_bytes(), b"b.bin");
    }

    #[test]
    fn remote_error_without_error_kind_is_protocol_error() {
        let addr = serve_one(r#"{"ok":false,"error":"metadata command predicate failed"}"#);
        let client = RemoteMetadataClient::connect(addr);
        let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
        assert!(
            matches!(
                err,
                ClientError::Protocol(ref err)
                    if err.contains("missing typed error_kind")
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn remote_typed_error_maps_predicate_failed_to_metadata_error() {
        let addr = serve_one(
            r#"{"ok":false,"error":"metadata command predicate failed","error_kind":{"type":"predicate_failed"}}"#,
        );
        let client = RemoteMetadataClient::connect(addr);
        let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
        assert!(matches!(
            err,
            ClientError::Metadata(nokvfs_meta::MetadError::Metadata(
                nokvfs_meta::MetadataError::PredicateFailed
            ))
        ));
    }

    #[test]
    fn remote_typed_error_maps_stale_generation_to_metadata_error() {
        let addr = serve_one(
            r#"{"ok":false,"error":"body generation 7 is stale; current generation is 8","error_kind":{"type":"stale_body_generation","expected":7,"current":8}}"#,
        );
        let client = RemoteMetadataClient::connect(addr);
        let err = client
            .read_body_plan(InodeId::new(42).unwrap(), 7, 0, 1)
            .unwrap_err();
        assert!(matches!(
            err,
            ClientError::Metadata(nokvfs_meta::MetadError::StaleBodyGeneration {
                expected: 7,
                current: 8
            })
        ));
    }

    #[test]
    fn remote_typed_error_maps_backend_metadata_error() {
        let addr = serve_one(
            r#"{"ok":false,"error":"metadata backend unavailable","error_kind":{"type":"metadata","message":"metadata backend unavailable"}}"#,
        );
        let client = RemoteMetadataClient::connect(addr);
        let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
        assert!(matches!(
            err,
            ClientError::Metadata(nokvfs_meta::MetadError::Metadata(
                nokvfs_meta::MetadataError::Backend(ref message)
            )) if message == "metadata backend unavailable"
        ));
    }

    #[test]
    fn remote_typed_error_maps_backend_object_error() {
        let addr = serve_one(
            r#"{"ok":false,"error":"object backend unavailable","error_kind":{"type":"object","message":"object backend unavailable"}}"#,
        );
        let client = RemoteMetadataClient::connect(addr);
        let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
        assert!(matches!(
            err,
            ClientError::Object(nokvfs_object::ObjectError::Backend(ref message))
                if message == "object backend unavailable"
        ));
    }

    #[test]
    fn remote_snapshot_cat_uses_snapshot_file_rpc() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
            stream.read_exact(&mut magic).unwrap();
            assert_eq!(&magic, FRAMED_RPC_MAGIC);
            let (request_id, flags, request) = read_frame(&mut stream).unwrap();
            let request = decode_request(&request).unwrap();
            assert!(matches!(
                request,
                MetadataRpcRequest::ReadArtifactPathAtSnapshot { snapshot_id, path }
                    if snapshot_id == 9 && path == "/runs/checkpoint"
            ));
            let response = response_body(
                r#"{"ok":true,"result":{"type":"file_bytes","bytes":[111,108,100]}}"#,
            );
            write_frame(&mut stream, request_id, flags, &response).unwrap();
        });
        let client = RemoteNoKvFsClient::connect(addr, MemoryObjectStore::new());
        assert_eq!(client.cat_snapshot(9, "/runs/checkpoint").unwrap(), b"old");
    }

    #[test]
    fn remote_snapshot_namespace_methods_use_snapshot_rooted_rpcs() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
            stream.read_exact(&mut magic).unwrap();
            assert_eq!(&magic, FRAMED_RPC_MAGIC);

            let (request_id, flags, request) = read_frame(&mut stream).unwrap();
            let request = decode_request(&request).unwrap();
            assert!(matches!(
                request,
                MetadataRpcRequest::StatPathAtSnapshot { snapshot_id, path }
                    if snapshot_id == 9 && path == "/"
            ));
            write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"path_metadata","metadata":{"attr":{"inode":2,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"size":0,"generation":2,"mtime_ms":2,"ctime_ms":2},"body":null}}}"#,
                ),
            )
            .unwrap();

            let (request_id, flags, request) = read_frame(&mut stream).unwrap();
            let request = decode_request(&request).unwrap();
            assert!(matches!(
                request,
                MetadataRpcRequest::ReadDirPlusPathAtSnapshot { snapshot_id, path }
                    if snapshot_id == 9 && path == "/"
            ));
            write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"dentries","entries":[{"dentry":{"parent":2,"name_hex":"6e6573746564","child":3,"child_type":"directory","attr_generation":3},"attr":{"inode":3,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"size":0,"generation":3,"mtime_ms":3,"ctime_ms":3},"body":null}]}}"#,
                ),
            )
            .unwrap();

            let (request_id, flags, request) = read_frame(&mut stream).unwrap();
            let request = decode_request(&request).unwrap();
            assert!(matches!(
                request,
                MetadataRpcRequest::ReadFilePathAtSnapshot {
                    snapshot_id,
                    path,
                    offset,
                    len
                } if snapshot_id == 9 && path == "/nested/model.bin" && offset == 7 && len == 3
            ));
            write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"file_bytes","bytes":[111,108,100]}}"#,
                ),
            )
            .unwrap();
        });

        let client = RemoteNoKvFsClient::connect(addr, MemoryObjectStore::new());
        let root = client
            .metadata()
            .stat_path_at_snapshot(9, "/")
            .unwrap()
            .unwrap();
        assert_eq!(root.attr.inode.get(), 2);
        let entries = client.metadata().list_path_at_snapshot(9, "/").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].dentry.name.as_bytes(), b"nested");
        assert_eq!(
            client.read_snapshot(9, "/nested/model.bin", 7, 3).unwrap(),
            b"old"
        );
    }

    #[test]
    fn remote_metadata_stat_path_uses_path_metadata_rpc() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
            stream.read_exact(&mut magic).unwrap();
            assert_eq!(&magic, FRAMED_RPC_MAGIC);
            let (request_id, flags, request) = read_frame(&mut stream).unwrap();
            let request = decode_request(&request).unwrap();
            assert!(matches!(
                request,
                MetadataRpcRequest::StatPath { path } if path == "/artifact.bin"
            ));
            write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"path_metadata","metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"size":12,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":12,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"chunk_size":67108864,"block_size":4194304}}}}"#,
                ),
            )
            .unwrap();
        });

        let client = RemoteMetadataClient::connect(addr);
        let metadata = client.stat_path("/artifact.bin").unwrap().unwrap();
        assert_eq!(metadata.attr.inode.get(), 42);
        assert_eq!(metadata.body.unwrap().digest_uri, "sha256:demo");
    }

    #[test]
    fn remote_file_client_read_path_returns_metadata_and_checks_expected_generation() {
        let store = MemoryObjectStore::new();
        store
            .put(&ObjectKey::new("blocks/demo").unwrap(), b"hello remote")
            .unwrap();
        let addr = serve_many(vec![
            response_body(
                r#"{"ok":true,"result":{"type":"path_metadata","metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"size":12,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":12,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"chunk_size":67108864,"block_size":4194304}}}}"#,
            ),
            response_body(
                r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","object_offset":6,"len":6,"output_offset":0}]}}}"#,
            ),
        ]);
        let client = RemoteNoKvFsClient::connect(addr, store);
        let read = client.read_path("/artifact.bin", 6, 6, Some(7)).unwrap();
        assert_eq!(read.bytes, b"remote");
        assert_eq!(read.metadata.attr.generation, 7);
        assert_eq!(read.metadata.body.unwrap().digest_uri, "sha256:demo");

        let addr = serve_one(
            r#"{"ok":true,"result":{"type":"path_metadata","metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"size":12,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":{"producer":"unit-test","digest_uri":"sha256:new","size":12,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":8,"chunk_size":67108864,"block_size":4194304}}}}"#,
        );
        let client = RemoteNoKvFsClient::connect(addr, MemoryObjectStore::new());
        let err = client
            .read_path("/artifact.bin", 0, 6, Some(7))
            .unwrap_err();
        assert!(matches!(
            err,
            ClientError::Metadata(nokvfs_meta::MetadError::StaleBodyGeneration {
                expected: 7,
                current: 8
            })
        ));
    }

    #[test]
    fn remote_file_client_reads_body_from_object_store() {
        let store = MemoryObjectStore::new();
        store
            .put(&ObjectKey::new("blocks/demo").unwrap(), b"hello remote")
            .unwrap();
        let addr = serve_many(vec![
            response_body(
                r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_hex":"61727469666163742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"size":12,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":12,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"chunk_size":67108864,"block_size":4194304}}}}"#,
            ),
            response_body(
                r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","object_offset":6,"len":6,"output_offset":0}]}}}"#,
            ),
        ]);
        let client = RemoteNoKvFsClient::connect(addr, store);
        let bytes = client.read("/artifact.bin", 6, 6).unwrap();
        assert_eq!(bytes, b"remote");
    }

    #[test]
    fn remote_file_client_uploads_blocks_then_publishes_metadata() {
        let store = MemoryObjectStore::new();
        let addr = serve_many(vec![
            response_body(
                r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":false,"dentry_version":null,"old_generation":null}}}"#,
            ),
            response_body(
                r#"{"ok":true,"result":{"type":"rename_replace","entry":{"dentry":{"parent":1,"name_hex":"61727469666163742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"size":11,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":11,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"chunk_size":67108864,"block_size":4194304}},"replaced":null}}"#,
            ),
        ]);
        let client = RemoteNoKvFsClient::connect(addr, store.clone());
        let entry = client
            .put_artifact(
                "/artifact.bin",
                b"hello world".to_vec(),
                artifact_metadata("artifact.bin"),
            )
            .unwrap();
        assert_eq!(entry.attr.inode.get(), 42);
        assert!(
            store
                .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
                .unwrap()
                .is_some(),
            "remote publish should upload object block before metadata commit"
        );
    }

    #[test]
    fn remote_file_client_cleans_staged_blocks_after_publish_failure() {
        let store = MemoryObjectStore::new();
        let addr = serve_many(vec![
            response_body(
                r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":false,"dentry_version":null,"old_generation":null}}}"#,
            ),
            response_body(
                r#"{"ok":false,"error":"metadata command predicate failed","error_kind":{"type":"predicate_failed"}}"#,
            ),
        ]);
        let client = RemoteNoKvFsClient::connect(addr, store.clone());
        let err = client
            .put_artifact(
                "/artifact.bin",
                b"hello world".to_vec(),
                artifact_metadata("artifact.bin"),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ClientError::Metadata(nokvfs_meta::MetadError::Metadata(
                nokvfs_meta::MetadataError::PredicateFailed
            ))
        ));
        assert!(
            store
                .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
                .unwrap()
                .is_none(),
            "failed metadata publish should clean staged object block"
        );
    }
}
