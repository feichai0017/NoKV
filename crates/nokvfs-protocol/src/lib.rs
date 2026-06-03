//! Wire protocol types for NoKV-FS metadata RPC.
//!
//! This crate owns storage-neutral request and response DTOs shared by the
//! server and remote clients. It does not execute metadata semantics, know Holt
//! layout, own object-store behavior, or implement path resolution.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MetadataRpcRequest {
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
    ReadDirPlus {
        parent: u64,
    },
    CreateDir {
        parent: u64,
        name: String,
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
pub struct WireSnapshotPin {
    pub snapshot_id: u64,
    pub root: u64,
    pub read_version: u64,
    pub created_version: u64,
}
