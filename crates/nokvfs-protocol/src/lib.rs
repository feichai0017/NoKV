//! Wire protocol types for NoKV metadata RPC.
//!
//! This crate owns storage-neutral request and response DTOs shared by the
//! server and service clients. It does not execute metadata semantics, know Holt
//! layout, own object-store behavior, or implement path resolution.

use std::fmt;

use nokvfs_types::{
    BlockDescriptor, BodyDescriptor, ChunkManifest, DentryName, DentryRecord, FileType, InodeAttr,
    InodeId, PathMetadata, SnapshotPin,
};
use serde::{Deserialize, Serialize};

const BINARY_CODEC_LIMIT: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MetadataRpcRequest {
    Batch {
        requests: Vec<MetadataRpcRequest>,
    },
    RequireApplied {
        position: WireMetadataPosition,
        request: Box<MetadataRpcRequest>,
    },
    BootstrapRoot {
        mode: u32,
        uid: u32,
        gid: u32,
    },
    GetAttr {
        inode: u64,
    },
    LookupPlus {
        parent: u64,
        name: String,
    },
    LookupPath {
        path: String,
    },
    StatPath {
        path: String,
    },
    ReadDirPlus {
        parent: u64,
    },
    ReadDirPlusPage {
        parent: u64,
        after_name_hex: Option<String>,
        limit: usize,
    },
    ReadDirPlusPath {
        path: String,
    },
    ReadDirPlusPathPage {
        path: String,
        after_name_hex: Option<String>,
        limit: usize,
    },
    CreateDir {
        parent: u64,
        name: String,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    CreateDirPath {
        path: String,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    CreateFile {
        parent: u64,
        name: String,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    CreateFilePath {
        path: String,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    CreateFilesInDirPath {
        parent_path: String,
        names: Vec<String>,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    RemoveFile {
        parent: u64,
        name: String,
    },
    RemoveFilePath {
        path: String,
    },
    RemoveEmptyDir {
        parent: u64,
        name: String,
    },
    RemoveEmptyDirPath {
        path: String,
    },
    Rename {
        parent: u64,
        name: String,
        new_parent: u64,
        new_name: String,
    },
    RenamePath {
        source: String,
        destination: String,
    },
    RenameReplace {
        parent: u64,
        name: String,
        new_parent: u64,
        new_name: String,
    },
    RenameReplacePath {
        source: String,
        destination: String,
    },
    SnapshotSubtree {
        root: u64,
    },
    SnapshotSubtreePath {
        path: String,
    },
    StatPathAtSnapshot {
        snapshot_id: u64,
        path: String,
    },
    ReadDirPlusPathAtSnapshot {
        snapshot_id: u64,
        path: String,
    },
    ReadFilePathAtSnapshot {
        snapshot_id: u64,
        path: String,
        offset: u64,
        len: u64,
    },
    RetireSnapshot {
        snapshot_id: u64,
    },
    ReadBodyPlan {
        inode: u64,
        generation: u64,
        offset: u64,
        len: u64,
    },
    ReadArtifactPathAtSnapshot {
        snapshot_id: u64,
        path: String,
    },
    PrepareArtifact {
        parent: u64,
        name: String,
        replace: bool,
    },
    PrepareArtifactPath {
        path: String,
        replace: bool,
    },
    PublishPreparedArtifact {
        prepared: WirePreparedArtifact,
        body: Box<WireBodyDescriptor>,
        chunks: Vec<WireChunkManifest>,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    ReadMetadataLog {
        start_index: u64,
        limit: usize,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetadataRpcEnvelope {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<MetadataRpcResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<WireMetadataError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_position: Option<WireMetadataPosition>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WireMetadataError {
    NotFound,
    NotFile,
    NotDirectory,
    MissingBodyDescriptor,
    PredicateFailed,
    ReadNotFresh {
        required: WireMetadataPosition,
        applied: Option<WireMetadataPosition>,
    },
    StaleBodyGeneration {
        expected: u64,
        current: u64,
    },
    InvalidPath {
        message: String,
    },
    Metadata {
        message: String,
    },
    Object {
        message: String,
    },
    Io {
        message: String,
    },
    Protocol {
        message: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct WireMetadataPosition {
    pub term: u64,
    pub index: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MetadataRpcResult {
    Batch {
        results: Vec<MetadataRpcEnvelope>,
    },
    InodeAttr {
        attr: Option<WireInodeAttr>,
    },
    Dentry {
        entry: Option<Box<WireDentryWithAttr>>,
    },
    Dentries {
        entries: Vec<WireDentryWithAttr>,
    },
    DentriesPage {
        entries: Vec<WireDentryWithAttr>,
        next_name_hex: Option<String>,
    },
    PathMetadata {
        metadata: Option<WirePathMetadata>,
    },
    RenameReplace {
        entry: Box<WireDentryWithAttr>,
        replaced: Option<Box<WireDentryWithAttr>>,
    },
    Snapshot {
        snapshot: WireSnapshotPin,
    },
    RetiredSnapshot {
        retired: bool,
    },
    BodyReadPlan {
        plan: WireBodyReadPlan,
    },
    FileBytes {
        bytes: Vec<u8>,
    },
    PreparedArtifact {
        prepared: WirePreparedArtifact,
    },
    MetadataLogEntries {
        entries: Vec<WireMetadataLogEntry>,
        committed: Option<WireMetadataPosition>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireDentryWithAttr {
    pub dentry: WireDentryRecord,
    pub attr: WireInodeAttr,
    pub body: Option<WireBodyDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WirePathMetadata {
    pub attr: WireInodeAttr,
    pub body: Option<WireBodyDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireDentryRecord {
    pub parent: u64,
    pub name_hex: String,
    pub child: u64,
    pub child_type: String,
    pub attr_generation: u64,
}

pub fn encode_name_cursor(name: &DentryName) -> String {
    hex_encode(name.as_bytes())
}

pub fn decode_name_cursor(raw: &str) -> Result<DentryName, MetadataProtocolError> {
    DentryName::new(hex_decode(raw)?).map_err(|err| MetadataProtocolError::new(err.to_string()))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireInodeAttr {
    pub inode: u64,
    pub file_type: String,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireBodyDescriptor {
    pub producer: String,
    pub digest_uri: String,
    pub size: u64,
    pub content_type: String,
    pub manifest_id: String,
    pub generation: u64,
    pub chunk_size: u64,
    pub block_size: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WirePreparedArtifact {
    pub mount: u64,
    pub parent: u64,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub inode: u64,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
    pub replace: bool,
    pub dentry_version: Option<u64>,
    pub old_generation: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireChunkManifest {
    pub chunk_index: u64,
    pub logical_offset: u64,
    pub len: u64,
    pub blocks: Vec<WireBlockDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireBlockDescriptor {
    pub object_key: String,
    pub logical_offset: u64,
    pub object_offset: u64,
    pub len: u64,
    pub digest_uri: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireBodyReadPlan {
    pub output_len: u64,
    pub blocks: Vec<WireObjectReadBlock>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireObjectReadBlock {
    pub object_key: String,
    pub object_offset: u64,
    pub len: u64,
    pub output_offset: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireSnapshotPin {
    pub snapshot_id: u64,
    pub root: u64,
    pub read_version: u64,
    pub created_version: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireMetadataLogEntry {
    pub position: WireMetadataPosition,
    pub mount: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataProtocolError(String);

impl MetadataProtocolError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl WireDentryRecord {
    pub fn from_dentry_record(record: &DentryRecord) -> Self {
        Self {
            parent: record.parent.get(),
            name_hex: hex_encode(record.name.as_bytes()),
            child: record.child.get(),
            child_type: file_type_label(record.child_type).to_owned(),
            attr_generation: record.attr_generation,
        }
    }

    pub fn into_dentry_record(self) -> Result<DentryRecord, MetadataProtocolError> {
        Ok(DentryRecord {
            parent: inode_id(self.parent)?,
            name: DentryName::new(hex_decode(&self.name_hex)?)
                .map_err(|err| MetadataProtocolError::new(err.to_string()))?,
            child: inode_id(self.child)?,
            child_type: parse_file_type(&self.child_type)?,
            attr_generation: self.attr_generation,
        })
    }
}

impl WirePathMetadata {
    pub fn from_path_metadata(metadata: &PathMetadata) -> Self {
        Self {
            attr: WireInodeAttr::from_inode_attr(&metadata.attr),
            body: metadata
                .body
                .as_ref()
                .map(WireBodyDescriptor::from_body_descriptor),
        }
    }

    pub fn into_path_metadata(self) -> Result<PathMetadata, MetadataProtocolError> {
        Ok(PathMetadata {
            attr: self.attr.into_inode_attr()?,
            body: self.body.map(WireBodyDescriptor::into_body_descriptor),
        })
    }
}

impl WireInodeAttr {
    pub fn from_inode_attr(attr: &InodeAttr) -> Self {
        Self {
            inode: attr.inode.get(),
            file_type: file_type_label(attr.file_type).to_owned(),
            mode: attr.mode,
            uid: attr.uid,
            gid: attr.gid,
            size: attr.size,
            generation: attr.generation,
            mtime_ms: attr.mtime_ms,
            ctime_ms: attr.ctime_ms,
        }
    }

    pub fn into_inode_attr(self) -> Result<InodeAttr, MetadataProtocolError> {
        Ok(InodeAttr {
            inode: inode_id(self.inode)?,
            file_type: parse_file_type(&self.file_type)?,
            mode: self.mode,
            uid: self.uid,
            gid: self.gid,
            size: self.size,
            generation: self.generation,
            mtime_ms: self.mtime_ms,
            ctime_ms: self.ctime_ms,
        })
    }
}

impl WireBodyDescriptor {
    pub fn from_body_descriptor(body: &BodyDescriptor) -> Self {
        Self {
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

    pub fn into_body_descriptor(self) -> BodyDescriptor {
        BodyDescriptor {
            producer: self.producer,
            digest_uri: self.digest_uri,
            size: self.size,
            content_type: self.content_type,
            manifest_id: self.manifest_id,
            generation: self.generation,
            chunk_size: self.chunk_size,
            block_size: self.block_size,
        }
    }
}

impl WireChunkManifest {
    pub fn from_chunk_manifest(chunk: &ChunkManifest) -> Self {
        Self {
            chunk_index: chunk.chunk_index,
            logical_offset: chunk.logical_offset,
            len: chunk.len,
            blocks: chunk
                .blocks
                .iter()
                .map(WireBlockDescriptor::from_block_descriptor)
                .collect(),
        }
    }

    pub fn into_chunk_manifest(self) -> Result<ChunkManifest, MetadataProtocolError> {
        Ok(ChunkManifest {
            chunk_index: self.chunk_index,
            logical_offset: self.logical_offset,
            len: self.len,
            blocks: self
                .blocks
                .into_iter()
                .map(WireBlockDescriptor::into_block_descriptor)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl WireBlockDescriptor {
    pub fn from_block_descriptor(block: &BlockDescriptor) -> Self {
        Self {
            object_key: block.object_key.clone(),
            logical_offset: block.logical_offset,
            object_offset: block.object_offset,
            len: block.len,
            digest_uri: block.digest_uri.clone(),
        }
    }

    pub fn into_block_descriptor(self) -> Result<BlockDescriptor, MetadataProtocolError> {
        if self.object_key.is_empty() {
            return Err(MetadataProtocolError::new(
                "block descriptor object key is empty",
            ));
        }
        Ok(BlockDescriptor {
            object_key: self.object_key,
            logical_offset: self.logical_offset,
            object_offset: self.object_offset,
            len: self.len,
            digest_uri: self.digest_uri,
        })
    }
}

impl WireSnapshotPin {
    pub fn from_snapshot_pin(snapshot: &SnapshotPin) -> Self {
        Self {
            snapshot_id: snapshot.snapshot_id,
            root: snapshot.root.get(),
            read_version: snapshot.read_version,
            created_version: snapshot.created_version,
        }
    }

    pub fn into_snapshot_pin(self) -> Result<SnapshotPin, MetadataProtocolError> {
        Ok(SnapshotPin {
            snapshot_id: self.snapshot_id,
            root: inode_id(self.root)?,
            read_version: self.read_version,
            created_version: self.created_version,
        })
    }
}

pub fn encode_request(request: &MetadataRpcRequest) -> Result<Vec<u8>, MetadataProtocolError> {
    serialize(request)
}

pub fn decode_request(body: &[u8]) -> Result<MetadataRpcRequest, MetadataProtocolError> {
    deserialize(body)
}

pub fn encode_envelope(envelope: &MetadataRpcEnvelope) -> Result<Vec<u8>, MetadataProtocolError> {
    serialize(envelope)
}

pub fn decode_envelope(body: &[u8]) -> Result<MetadataRpcEnvelope, MetadataProtocolError> {
    deserialize(body)
}

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, MetadataProtocolError> {
    let mut out = Vec::new();
    value
        .serialize(&mut rmp_serde::Serializer::new(&mut out).with_struct_map())
        .map_err(|err| MetadataProtocolError(err.to_string()))?;
    Ok(out)
}

fn deserialize<'a, T: Deserialize<'a>>(body: &'a [u8]) -> Result<T, MetadataProtocolError> {
    if body.len() as u64 > BINARY_CODEC_LIMIT {
        return Err(MetadataProtocolError(format!(
            "metadata binary rpc body exceeds {BINARY_CODEC_LIMIT} bytes"
        )));
    }
    rmp_serde::from_slice(body).map_err(|err| MetadataProtocolError(err.to_string()))
}

fn inode_id(raw: u64) -> Result<InodeId, MetadataProtocolError> {
    InodeId::new(raw).map_err(|err| MetadataProtocolError::new(err.to_string()))
}

fn file_type_label(file_type: FileType) -> &'static str {
    match file_type {
        FileType::File => "file",
        FileType::Directory => "directory",
        FileType::Symlink => "symlink",
    }
}

fn parse_file_type(raw: &str) -> Result<FileType, MetadataProtocolError> {
    match raw {
        "file" => Ok(FileType::File),
        "directory" => Ok(FileType::Directory),
        "symlink" => Ok(FileType::Symlink),
        other => Err(MetadataProtocolError::new(format!(
            "unknown file type {other}"
        ))),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(raw: &str) -> Result<Vec<u8>, MetadataProtocolError> {
    if !raw.len().is_multiple_of(2) {
        return Err(MetadataProtocolError::new("hex string has odd length"));
    }
    let mut out = Vec::with_capacity(raw.len() / 2);
    for pair in raw.as_bytes().chunks_exact(2) {
        let high = hex_digit(pair[0])?;
        let low = hex_digit(pair[1])?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_digit(byte: u8) -> Result<u8, MetadataProtocolError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(MetadataProtocolError::new("invalid hex digit")),
    }
}

impl fmt::Display for MetadataProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for MetadataProtocolError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_codec_round_trips_metadata_request() {
        let request = MetadataRpcRequest::CreateFilePath {
            path: "/runs/a.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        };
        let encoded = encode_request(&request).unwrap();
        assert!(encoded.len() < 64);
        assert_eq!(decode_request(&encoded).unwrap(), request);
    }

    #[test]
    fn binary_codec_round_trips_metadata_envelope() {
        let envelope = MetadataRpcEnvelope {
            ok: true,
            result: Some(MetadataRpcResult::InodeAttr {
                attr: Some(WireInodeAttr {
                    inode: 7,
                    file_type: "file".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    size: 16,
                    generation: 2,
                    mtime_ms: 2,
                    ctime_ms: 2,
                }),
            }),
            error: None,
            error_kind: None,
            metadata_position: Some(WireMetadataPosition { term: 1, index: 7 }),
        };
        let encoded = encode_envelope(&envelope).unwrap();
        assert_eq!(decode_envelope(&encoded).unwrap(), envelope);
    }

    #[test]
    fn binary_codec_round_trips_metadata_log_entries() {
        let request = MetadataRpcRequest::ReadMetadataLog {
            start_index: 7,
            limit: 32,
        };
        assert_eq!(
            decode_request(&encode_request(&request).unwrap()).unwrap(),
            request
        );

        let envelope = MetadataRpcEnvelope {
            ok: true,
            result: Some(MetadataRpcResult::MetadataLogEntries {
                entries: vec![WireMetadataLogEntry {
                    position: WireMetadataPosition { term: 2, index: 7 },
                    mount: 1,
                    payload: b"entry-payload".to_vec(),
                }],
                committed: Some(WireMetadataPosition { term: 2, index: 8 }),
            }),
            error: None,
            error_kind: None,
            metadata_position: Some(WireMetadataPosition { term: 2, index: 8 }),
        };
        assert_eq!(
            decode_envelope(&encode_envelope(&envelope).unwrap()).unwrap(),
            envelope
        );
    }
}
