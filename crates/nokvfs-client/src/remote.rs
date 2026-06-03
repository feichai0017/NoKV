use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use nokvfs_meta::{DentryWithAttr, ObjectTransferStats, RenameReplaceResult};
use nokvfs_object::{
    delete_staged_objects, put_chunked_object, read_object_blocks, ChunkWriteOptions,
    MemoryBlockCache, ObjectError, ObjectReadBlock, ObjectStore, StagedObjectSet,
    DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
};
use nokvfs_protocol::{
    MetadataRpcEnvelope, MetadataRpcRequest, MetadataRpcResult, WireBlockDescriptor,
    WireBodyDescriptor, WireBodyReadPlan, WireChunkManifest, WireDentryRecord, WireDentryWithAttr,
    WireInodeAttr, WireObjectReadBlock, WirePreparedArtifact, WireSnapshotPin,
};
use nokvfs_types::{
    BlockDescriptor, BodyDescriptor, ChunkManifest, DentryName, DentryRecord, FileType, InodeAttr,
    InodeId, SnapshotPin,
};

use crate::{display_name, parse_absolute_path, ArtifactMetadata, ClientError};

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RPC_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const FRAMED_RPC_MAGIC: &[u8; 8] = b"NKVRPC1\n";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoteMetadataClientOptions {
    pub address: SocketAddr,
    pub timeout: Duration,
}

pub struct RemoteMetadataClient {
    options: RemoteMetadataClientOptions,
    connections: Mutex<Vec<TcpStream>>,
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

    pub fn read(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, ClientError> {
        let entry = self
            .metadata
            .lookup(path)?
            .ok_or_else(|| ClientError::NotFound(path.to_owned()))?;
        self.read_entry(path, &entry, offset, len)
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
            connections: Mutex::new(Vec::new()),
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

    pub fn remove(&self, path: &str) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(path)?;
        match self.call(MetadataRpcRequest::RemoveFile {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rmdir(&self, path: &str) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(path)?;
        match self.call(MetadataRpcRequest::RemoveEmptyDir {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rename(&self, source: &str, destination: &str) -> Result<DentryWithAttr, ClientError> {
        let (parent, name) = self.resolve_parent(source)?;
        let (new_parent, new_name) = self.resolve_parent(destination)?;
        match self.call(MetadataRpcRequest::Rename {
            parent: parent.get(),
            name: rpc_name(&name)?,
            new_parent: new_parent.get(),
            new_name: rpc_name(&new_name)?,
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
        let (parent, name) = self.resolve_parent(source)?;
        let (new_parent, new_name) = self.resolve_parent(destination)?;
        match self.call(MetadataRpcRequest::RenameReplace {
            parent: parent.get(),
            name: rpc_name(&name)?,
            new_parent: new_parent.get(),
            new_name: rpc_name(&new_name)?,
        })? {
            MetadataRpcResult::RenameReplace { entry, replaced } => Ok(RenameReplaceResult {
                entry: wire_dentry(*entry)?,
                replaced: replaced.map(|entry| wire_dentry(*entry)).transpose()?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn snapshot(&self, path: &str) -> Result<SnapshotPin, ClientError> {
        let root = self.resolve_directory(path)?;
        match self.call(MetadataRpcRequest::SnapshotSubtree { root: root.get() })? {
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
            body: body_to_wire(&body),
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

    fn resolve_parent(&self, path: &str) -> Result<(InodeId, DentryName), ClientError> {
        let mut components = parse_absolute_path(path)?;
        let name = components.pop().ok_or(ClientError::RootHasNoParent)?;
        let parent = self.resolve_components_as_directory(&components)?;
        Ok((parent, name))
    }

    fn resolve_directory(&self, path: &str) -> Result<InodeId, ClientError> {
        let components = parse_absolute_path(path)?;
        self.resolve_components_as_directory(&components)
    }

    fn resolve_components_as_directory(
        &self,
        components: &[DentryName],
    ) -> Result<InodeId, ClientError> {
        let mut current = InodeId::root();
        for name in components {
            let label = display_name(name);
            let entry = match self.call(MetadataRpcRequest::LookupPlus {
                parent: current.get(),
                name: rpc_name(name)?,
            })? {
                MetadataRpcResult::Dentry { entry } => {
                    entry.map(|entry| wire_dentry(*entry)).transpose()?
                }
                other => return Err(unexpected_result(other)),
            }
            .ok_or_else(|| ClientError::NotFound(label.clone()))?;
            if entry.attr.file_type != FileType::Directory {
                return Err(ClientError::NotDirectory(label));
            }
            current = entry.attr.inode;
        }
        Ok(current)
    }

    fn call(&self, request: MetadataRpcRequest) -> Result<MetadataRpcResult, ClientError> {
        let body =
            serde_json::to_vec(&request).map_err(|err| ClientError::Protocol(err.to_string()))?;
        let mut stream = self.take_connection()?;
        write_frame(&mut stream, &body)?;
        let body = read_frame(&mut stream)?;
        self.return_connection(stream);
        let envelope: MetadataRpcEnvelope =
            serde_json::from_slice(&body).map_err(|err| ClientError::Protocol(err.to_string()))?;
        if !envelope.ok {
            return Err(ClientError::Remote(
                envelope
                    .error
                    .unwrap_or_else(|| "unknown remote error".to_owned()),
            ));
        }
        envelope
            .result
            .ok_or_else(|| ClientError::Protocol("metadata rpc response missing result".to_owned()))
    }

    fn take_connection(&self) -> Result<TcpStream, ClientError> {
        if let Some(stream) = self.connections.lock().expect("connection pool").pop() {
            return Ok(stream);
        }
        let mut stream = TcpStream::connect(self.options.address)
            .map_err(|err| ClientError::Io(err.to_string()))?;
        stream
            .set_read_timeout(Some(self.options.timeout))
            .map_err(|err| ClientError::Io(err.to_string()))?;
        stream
            .set_write_timeout(Some(self.options.timeout))
            .map_err(|err| ClientError::Io(err.to_string()))?;
        stream
            .write_all(FRAMED_RPC_MAGIC)
            .map_err(|err| ClientError::Io(err.to_string()))?;
        Ok(stream)
    }

    fn return_connection(&self, stream: TcpStream) {
        self.connections
            .lock()
            .expect("connection pool")
            .push(stream);
    }
}

fn write_frame(stream: &mut TcpStream, body: &[u8]) -> Result<(), ClientError> {
    let len = u32::try_from(body.len())
        .map_err(|_| ClientError::Protocol("metadata rpc request exceeds u32".to_owned()))?;
    stream
        .write_all(&len.to_be_bytes())
        .and_then(|_| stream.write_all(body))
        .map_err(|err| ClientError::Io(err.to_string()))
}

fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, ClientError> {
    let mut len = [0_u8; 4];
    stream
        .read_exact(&mut len)
        .map_err(|err| ClientError::Io(err.to_string()))?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_RPC_RESPONSE_BYTES {
        return Err(ClientError::Protocol(
            "metadata rpc response exceeds size limit".to_owned(),
        ));
    }
    let mut body = vec![0_u8; len];
    stream
        .read_exact(&mut body)
        .map_err(|err| ClientError::Io(err.to_string()))?;
    Ok(body)
}

fn rpc_name(name: &DentryName) -> Result<String, ClientError> {
    String::from_utf8(name.as_bytes().to_vec())
        .map_err(|_| ClientError::InvalidName("remote rpc requires utf-8 names".to_owned()))
}

fn wire_dentry(entry: WireDentryWithAttr) -> Result<DentryWithAttr, ClientError> {
    Ok(DentryWithAttr {
        dentry: wire_dentry_record(entry.dentry)?,
        attr: wire_inode_attr(entry.attr)?,
        body: entry.body.map(wire_body).transpose()?,
    })
}

fn wire_dentry_record(record: WireDentryRecord) -> Result<DentryRecord, ClientError> {
    Ok(DentryRecord {
        parent: inode_id(record.parent)?,
        name: DentryName::new(hex_decode(&record.name_hex)?)
            .map_err(|err| ClientError::InvalidName(err.to_string()))?,
        child: inode_id(record.child)?,
        child_type: wire_file_type(&record.child_type)?,
        attr_generation: record.attr_generation,
    })
}

fn wire_inode_attr(attr: WireInodeAttr) -> Result<InodeAttr, ClientError> {
    Ok(InodeAttr {
        inode: inode_id(attr.inode)?,
        file_type: wire_file_type(&attr.file_type)?,
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        size: attr.size,
        generation: attr.generation,
        mtime_ms: attr.mtime_ms,
        ctime_ms: attr.ctime_ms,
    })
}

fn wire_body(body: WireBodyDescriptor) -> Result<BodyDescriptor, ClientError> {
    Ok(BodyDescriptor {
        producer: body.producer,
        digest_uri: body.digest_uri,
        size: body.size,
        content_type: body.content_type,
        manifest_id: body.manifest_id,
        generation: body.generation,
        chunk_size: body.chunk_size,
        block_size: body.block_size,
    })
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
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
    })
}

fn body_to_wire(body: &BodyDescriptor) -> WireBodyDescriptor {
    WireBodyDescriptor {
        producer: body.producer.clone(),
        digest_uri: body.digest_uri.clone(),
        size: body.size,
        content_type: body.content_type.clone(),
        manifest_id: body.manifest_id.clone(),
        generation: body.generation,
        chunk_size: body.chunk_size,
        block_size: body.block_size,
    }
}

fn chunk_to_wire(chunk: &ChunkManifest) -> WireChunkManifest {
    WireChunkManifest {
        chunk_index: chunk.chunk_index,
        logical_offset: chunk.logical_offset,
        len: chunk.len,
        blocks: chunk.blocks.iter().map(block_to_wire).collect(),
    }
}

fn block_to_wire(block: &BlockDescriptor) -> WireBlockDescriptor {
    WireBlockDescriptor {
        object_key: block.object_key.clone(),
        logical_offset: block.logical_offset,
        object_offset: block.object_offset,
        len: block.len,
        digest_uri: block.digest_uri.clone(),
    }
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

fn wire_snapshot(snapshot: WireSnapshotPin) -> Result<SnapshotPin, ClientError> {
    Ok(SnapshotPin {
        snapshot_id: snapshot.snapshot_id,
        root: inode_id(snapshot.root)?,
        read_version: snapshot.read_version,
        created_version: snapshot.created_version,
    })
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

fn wire_file_type(raw: &str) -> Result<FileType, ClientError> {
    match raw {
        "file" => Ok(FileType::File),
        "directory" => Ok(FileType::Directory),
        "symlink" => Ok(FileType::Symlink),
        other => Err(ClientError::Protocol(format!("unknown file type {other}"))),
    }
}

fn hex_decode(raw: &str) -> Result<Vec<u8>, ClientError> {
    if !raw.len().is_multiple_of(2) {
        return Err(ClientError::Protocol(
            "hex string has odd length".to_owned(),
        ));
    }
    let mut out = Vec::with_capacity(raw.len() / 2);
    for pair in raw.as_bytes().chunks_exact(2) {
        let high = hex_digit(pair[0])?;
        let low = hex_digit(pair[1])?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_digit(byte: u8) -> Result<u8, ClientError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(ClientError::Protocol("invalid hex digit".to_owned())),
    }
}

fn unexpected_result(result: MetadataRpcResult) -> ClientError {
    ClientError::Protocol(format!("unexpected metadata rpc result {result:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokvfs_object::{MemoryObjectStore, ObjectKey};
    use std::net::TcpListener;
    use std::thread;

    fn serve_one(body: &'static str) -> SocketAddr {
        serve_many(vec![body])
    }

    fn serve_many(bodies: Vec<&'static str>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
            stream.read_exact(&mut magic).unwrap();
            assert_eq!(&magic, FRAMED_RPC_MAGIC);
            for body in bodies {
                let request = read_frame(&mut stream).unwrap();
                let request = String::from_utf8_lossy(&request);
                assert!(request.contains(r#""op":"#));
                write_frame(&mut stream, body.as_bytes()).unwrap();
            }
        });
        addr
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
            r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_utf8":"runs","name_hex":"72756e73","child":2,"child_type":"directory","attr_generation":1},"attr":{"inode":2,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"size":0,"generation":1,"mtime_ms":1,"ctime_ms":1},"body":null}}}"#,
        );
        let client = RemoteMetadataClient::connect(addr);
        let entry = client.mkdir("/runs", 0o755, 1000, 1000).unwrap();
        assert_eq!(entry.attr.inode.get(), 2);
        assert_eq!(entry.dentry.name.as_bytes(), b"runs");
    }

    #[test]
    fn remote_create_file_uses_single_path_rpc_for_nested_parent() {
        let addr = serve_one(
            r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_utf8":"checkpoint.bin","name_hex":"636865636b706f696e742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}}"#,
        );
        let client = RemoteMetadataClient::connect(addr);
        let entry = client
            .create_file("/runs/checkpoint.bin", 0o644, 1000, 1000)
            .unwrap();
        assert_eq!(entry.attr.inode.get(), 42);
        assert_eq!(entry.dentry.name.as_bytes(), b"checkpoint.bin");
    }

    #[test]
    fn remote_error_maps_to_client_error() {
        let addr = serve_one(r#"{"ok":false,"error":"metadata command predicate failed"}"#);
        let client = RemoteMetadataClient::connect(addr);
        let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
        assert!(
            matches!(
                err,
                ClientError::Remote(ref err) if err.contains("predicate failed")
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn remote_file_client_reads_body_from_object_store() {
        let store = MemoryObjectStore::new();
        store
            .put(&ObjectKey::new("blocks/demo").unwrap(), b"hello remote")
            .unwrap();
        let addr = serve_many(vec![
            r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_utf8":"artifact.bin","name_hex":"61727469666163742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"size":12,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":12,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"chunk_size":67108864,"block_size":4194304}}}}"#,
            r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","object_offset":6,"len":6,"output_offset":0}]}}}"#,
        ]);
        let client = RemoteNoKvFsClient::connect(addr, store);
        let bytes = client.read("/artifact.bin", 6, 6).unwrap();
        assert_eq!(bytes, b"remote");
    }

    #[test]
    fn remote_file_client_uploads_blocks_then_publishes_metadata() {
        let store = MemoryObjectStore::new();
        let addr = serve_many(vec![
            r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"replace":false,"dentry_version":null,"old_generation":null}}}"#,
            r#"{"ok":true,"result":{"type":"rename_replace","entry":{"dentry":{"parent":1,"name_utf8":"artifact.bin","name_hex":"61727469666163742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"size":11,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":11,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"chunk_size":67108864,"block_size":4194304}},"replaced":null}}"#,
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
            r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"replace":false,"dentry_version":null,"old_generation":null}}}"#,
            r#"{"ok":false,"error":"metadata command predicate failed"}"#,
        ]);
        let client = RemoteNoKvFsClient::connect(addr, store.clone());
        let err = client
            .put_artifact(
                "/artifact.bin",
                b"hello world".to_vec(),
                artifact_metadata("artifact.bin"),
            )
            .unwrap_err();
        assert!(
            matches!(err, ClientError::Remote(ref err) if err.contains("predicate failed")),
            "unexpected error: {err:?}"
        );
        assert!(
            store
                .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
                .unwrap()
                .is_none(),
            "failed metadata publish should clean staged object block"
        );
    }
}
