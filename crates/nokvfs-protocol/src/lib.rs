//! Wire protocol types for NoKV-FS metadata RPC.
//!
//! This crate owns storage-neutral request and response DTOs shared by the
//! server and remote clients. It does not execute metadata semantics, know Holt
//! layout, own object-store behavior, or implement path resolution.

use std::fmt;

use serde::{Deserialize, Serialize};

const BINARY_CODEC_LIMIT: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MetadataRpcRequest {
    Batch {
        requests: Vec<MetadataRpcRequest>,
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
    ReadDirPlus {
        parent: u64,
    },
    ReadDirPlusPath {
        path: String,
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
    RemoveEmptyDir {
        parent: u64,
        name: String,
    },
    Rename {
        parent: u64,
        name: String,
        new_parent: u64,
        new_name: String,
    },
    RenameReplace {
        parent: u64,
        name: String,
        new_parent: u64,
        new_name: String,
    },
    SnapshotSubtree {
        root: u64,
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
        body: WireBodyDescriptor,
        chunks: Vec<WireChunkManifest>,
        mode: u32,
        uid: u32,
        gid: u32,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetadataRpcEnvelope {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<MetadataRpcResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireDentryWithAttr {
    pub dentry: WireDentryRecord,
    pub attr: WireInodeAttr,
    pub body: Option<WireBodyDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireDentryRecord {
    pub parent: u64,
    pub name_utf8: Option<String>,
    pub name_hex: String,
    pub child: u64,
    pub child_type: String,
    pub attr_generation: u64,
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
    pub inode: u64,
    pub generation: u64,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataProtocolError(String);

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
        };
        let encoded = encode_envelope(&envelope).unwrap();
        assert_eq!(decode_envelope(&encoded).unwrap(), envelope);
    }
}
