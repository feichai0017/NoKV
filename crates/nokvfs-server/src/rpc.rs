use nokvfs_meta::{DentryWithAttr, MetadError, PreparedArtifact};
use nokvfs_object::ObjectReadBlock;
use nokvfs_protocol::{
    MetadataRpcEnvelope, MetadataRpcRequest, MetadataRpcResult, WireBlockDescriptor,
    WireBodyDescriptor, WireBodyReadPlan, WireChunkManifest, WireDentryRecord, WireDentryWithAttr,
    WireInodeAttr, WireObjectReadBlock, WirePreparedArtifact, WireSnapshotPin,
};
use nokvfs_types::{
    BlockDescriptor, BodyDescriptor, ChunkManifest, DentryName, DentryRecord, FileType, InodeAttr,
    InodeId, MountId, SnapshotPin,
};

use crate::server::{Server, ServerError};

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
                attr: Some(wire_inode_attr(&attr)),
            })
        }
        MetadataRpcRequest::GetAttr { inode } => {
            let attr = server.service().get_attr(inode_id(inode)?)?;
            Ok(MetadataRpcResult::InodeAttr {
                attr: attr.as_ref().map(wire_inode_attr),
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
        MetadataRpcRequest::RemoveFile { parent, name } => {
            let entry = server
                .service()
                .remove_file(inode_id(parent)?, &dentry_name(name)?)?;
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
        MetadataRpcRequest::SnapshotSubtree { root } => {
            let snapshot = server.service().snapshot_subtree(inode_id(root)?)?;
            Ok(MetadataRpcResult::Snapshot {
                snapshot: wire_snapshot(&snapshot),
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
                body_descriptor(body),
                chunks
                    .into_iter()
                    .map(chunk_manifest)
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
        dentry: wire_dentry_record(&entry.dentry),
        attr: wire_inode_attr(&entry.attr),
        body: entry.body.as_ref().map(wire_body),
    }
}

fn wire_dentry_record(record: &DentryRecord) -> WireDentryRecord {
    WireDentryRecord {
        parent: record.parent.get(),
        name_utf8: String::from_utf8(record.name.as_bytes().to_vec()).ok(),
        name_hex: hex_encode(record.name.as_bytes()),
        child: record.child.get(),
        child_type: file_type_label(record.child_type).to_owned(),
        attr_generation: record.attr_generation,
    }
}

fn wire_inode_attr(attr: &InodeAttr) -> WireInodeAttr {
    WireInodeAttr {
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

fn wire_body(body: &BodyDescriptor) -> WireBodyDescriptor {
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

fn wire_prepared_artifact(mount: MountId, prepared: &PreparedArtifact) -> WirePreparedArtifact {
    WirePreparedArtifact {
        mount: mount.get(),
        parent: prepared.parent.get(),
        name: String::from_utf8(prepared.name.as_bytes().to_vec())
            .expect("remote prepared artifact names are utf-8"),
        inode: prepared.inode.get(),
        generation: prepared.generation,
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
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
    })
}

fn body_descriptor(body: WireBodyDescriptor) -> BodyDescriptor {
    BodyDescriptor {
        producer: body.producer,
        digest_uri: body.digest_uri,
        size: body.size,
        content_type: body.content_type,
        manifest_id: body.manifest_id,
        generation: body.generation,
        chunk_size: body.chunk_size,
        block_size: body.block_size,
    }
}

fn chunk_manifest(chunk: WireChunkManifest) -> Result<ChunkManifest, MetadError> {
    Ok(ChunkManifest {
        chunk_index: chunk.chunk_index,
        logical_offset: chunk.logical_offset,
        len: chunk.len,
        blocks: chunk
            .blocks
            .into_iter()
            .map(block_descriptor)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn block_descriptor(block: WireBlockDescriptor) -> Result<BlockDescriptor, MetadError> {
    if block.object_key.is_empty() {
        return Err(MetadError::Codec(
            "block descriptor object key is empty".to_owned(),
        ));
    }
    Ok(BlockDescriptor {
        object_key: block.object_key,
        logical_offset: block.logical_offset,
        object_offset: block.object_offset,
        len: block.len,
        digest_uri: block.digest_uri,
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

fn wire_snapshot(snapshot: &SnapshotPin) -> WireSnapshotPin {
    WireSnapshotPin {
        snapshot_id: snapshot.snapshot_id,
        root: snapshot.root.get(),
        read_version: snapshot.read_version,
        created_version: snapshot.created_version,
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
        assert!(response.contains("\"name_utf8\":\"checkpoint.bin\""));
        let response = handle_rpc(&server, br#"{"op":"read_dir_plus_path","path":"/runs"}"#);
        assert!(response.contains("\"entries\""));
        assert!(response.contains("\"name_utf8\":\"checkpoint.bin\""));
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
            body: WireBodyDescriptor {
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:body".to_owned(),
                size: 4,
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "artifact.bin".to_owned(),
                generation: prepared.generation,
                chunk_size: 64 * 1024 * 1024,
                block_size: 4 * 1024 * 1024,
            },
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
        assert!(response.contains("\"name_utf8\":\"artifact.bin\""));
        assert!(response.contains("\"type\":\"rename_replace\""));
    }
}
