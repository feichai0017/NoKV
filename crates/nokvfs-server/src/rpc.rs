use serde::{Deserialize, Serialize};

use nokvfs_meta::{DentryWithAttr, MetadError};
use nokvfs_types::{
    BodyDescriptor, DentryName, DentryRecord, FileType, InodeAttr, InodeId, SnapshotPin,
};

use crate::server::{Server, ServerError};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(crate) enum MetadataRpcRequest {
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

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct MetadataRpcEnvelope {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<MetadataRpcResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum MetadataRpcResult {
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

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct WireDentryWithAttr {
    pub dentry: WireDentryRecord,
    pub attr: WireInodeAttr,
    pub body: Option<WireBodyDescriptor>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct WireDentryRecord {
    pub parent: u64,
    pub name_utf8: Option<String>,
    pub name_hex: String,
    pub child: u64,
    pub child_type: &'static str,
    pub attr_generation: u64,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct WireInodeAttr {
    pub inode: u64,
    pub file_type: &'static str,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct WireBodyDescriptor {
    pub producer: String,
    pub digest_uri: String,
    pub size: u64,
    pub content_type: String,
    pub manifest_id: String,
    pub generation: u64,
    pub chunk_size: u64,
    pub block_size: u64,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct WireSnapshotPin {
    pub snapshot_id: u64,
    pub root: u64,
    pub read_version: u64,
    pub created_version: u64,
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

fn execute(server: &Server, request: MetadataRpcRequest) -> Result<MetadataRpcResult, ServerError> {
    match request {
        MetadataRpcRequest::BootstrapRoot { mode, uid, gid } => {
            let attr = server.service().bootstrap_root(mode, uid, gid)?;
            Ok(MetadataRpcResult::InodeAttr {
                attr: Some((&attr).into()),
            })
        }
        MetadataRpcRequest::GetAttr { inode } => {
            let attr = server.service().get_attr(inode_id(inode)?)?;
            Ok(MetadataRpcResult::InodeAttr {
                attr: attr.as_ref().map(Into::into),
            })
        }
        MetadataRpcRequest::LookupPlus { parent, name } => {
            let entry = server
                .service()
                .lookup_plus(inode_id(parent)?, &dentry_name(name)?)?;
            Ok(MetadataRpcResult::Dentry {
                entry: entry.as_ref().map(|entry| Box::new(entry.into())),
            })
        }
        MetadataRpcRequest::ReadDirPlus { parent } => {
            let entries = server.service().read_dir_plus(inode_id(parent)?)?;
            Ok(MetadataRpcResult::Dentries {
                entries: entries.iter().map(Into::into).collect(),
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
                entry: Some(Box::new((&entry).into())),
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
                entry: Some(Box::new((&entry).into())),
            })
        }
        MetadataRpcRequest::RemoveFile { parent, name } => {
            let entry = server
                .service()
                .remove_file(inode_id(parent)?, &dentry_name(name)?)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new((&entry).into())),
            })
        }
        MetadataRpcRequest::RemoveEmptyDir { parent, name } => {
            let entry = server
                .service()
                .remove_empty_dir(inode_id(parent)?, &dentry_name(name)?)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new((&entry).into())),
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
                entry: Some(Box::new((&entry).into())),
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
                entry: Box::new((&result.entry).into()),
                replaced: result.replaced.as_ref().map(|entry| Box::new(entry.into())),
            })
        }
        MetadataRpcRequest::SnapshotSubtree { root } => {
            let snapshot = server.service().snapshot_subtree(inode_id(root)?)?;
            Ok(MetadataRpcResult::Snapshot {
                snapshot: (&snapshot).into(),
            })
        }
        MetadataRpcRequest::RetireSnapshot { snapshot_id } => {
            let retired = server.service().retire_snapshot(snapshot_id)?;
            Ok(MetadataRpcResult::RetiredSnapshot { retired })
        }
    }
}

fn inode_id(raw: u64) -> Result<InodeId, MetadError> {
    InodeId::new(raw).map_err(Into::into)
}

fn dentry_name(name: String) -> Result<DentryName, MetadError> {
    DentryName::new(name.into_bytes()).map_err(|err| MetadError::Codec(err.to_string()))
}

impl From<&DentryWithAttr> for WireDentryWithAttr {
    fn from(entry: &DentryWithAttr) -> Self {
        Self {
            dentry: (&entry.dentry).into(),
            attr: (&entry.attr).into(),
            body: entry.body.as_ref().map(Into::into),
        }
    }
}

impl From<&DentryRecord> for WireDentryRecord {
    fn from(record: &DentryRecord) -> Self {
        Self {
            parent: record.parent.get(),
            name_utf8: String::from_utf8(record.name.as_bytes().to_vec()).ok(),
            name_hex: hex_encode(record.name.as_bytes()),
            child: record.child.get(),
            child_type: file_type_label(record.child_type),
            attr_generation: record.attr_generation,
        }
    }
}

impl From<&InodeAttr> for WireInodeAttr {
    fn from(attr: &InodeAttr) -> Self {
        Self {
            inode: attr.inode.get(),
            file_type: file_type_label(attr.file_type),
            mode: attr.mode,
            uid: attr.uid,
            gid: attr.gid,
            size: attr.size,
            generation: attr.generation,
            mtime_ms: attr.mtime_ms,
            ctime_ms: attr.ctime_ms,
        }
    }
}

impl From<&BodyDescriptor> for WireBodyDescriptor {
    fn from(body: &BodyDescriptor) -> Self {
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
}

impl From<&SnapshotPin> for WireSnapshotPin {
    fn from(snapshot: &SnapshotPin) -> Self {
        Self {
            snapshot_id: snapshot.snapshot_id,
            root: snapshot.root.get(),
            read_version: snapshot.read_version,
            created_version: snapshot.created_version,
        }
    }
}

fn file_type_label(file_type: FileType) -> &'static str {
    match file_type {
        FileType::File => "file",
        FileType::Directory => "directory",
        FileType::Symlink => "symlink",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::test_server;

    #[test]
    fn rpc_creates_and_lists_directory() {
        let server = test_server();
        let response = handle_rpc(
            &server,
            br#"{"op":"create_dir","parent":1,"name":"runs","mode":493,"uid":1000,"gid":1000}"#,
        );
        assert!(response.contains("\"ok\":true"));
        assert!(response.contains("\"name_utf8\":\"runs\""));

        let response = handle_rpc(&server, br#"{"op":"read_dir_plus","parent":1}"#);
        assert!(response.contains("\"entries\""));
        assert!(response.contains("\"name_utf8\":\"runs\""));
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
}
