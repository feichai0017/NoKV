use nokv_meta::{DentryWithAttr, MetadError, PreparedArtifact, UpdateAttr, XattrSetMode};
use nokv_object::{
    ObjectKey, ObjectReadBlock, StagedObject, StagedObjectSet, StoredBlock, StoredChunk,
};
use nokv_protocol::{
    MetadataProtocolError, MetadataRpcEnvelope, MetadataRpcResult, WireAdvisoryLock,
    WireBodyReadPlan, WireDentryWithAttr, WireMetadataError, WireMetadataPosition,
    WireObjectReadBlock, WirePreparedArtifact, WireStagedObjectSet, WireUpdateAttr,
    WireXattrSetMode,
};
use nokv_types::{DentryName, InodeId, MountId};

use crate::server::{Server, ServerError};

pub(super) fn ok_envelope(server: &Server, result: MetadataRpcResult) -> MetadataRpcEnvelope {
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

pub(super) fn err_envelope(err: ServerError) -> MetadataRpcEnvelope {
    let error_kind = wire_server_error(&err);
    MetadataRpcEnvelope {
        ok: false,
        result: None,
        error: Some(err.to_string()),
        error_kind: Some(error_kind),
        metadata_position: None,
    }
}

pub(super) fn wire_server_error(err: &ServerError) -> WireMetadataError {
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

pub(super) fn inode_id(raw: u64) -> Result<InodeId, MetadError> {
    InodeId::new(raw).map_err(Into::into)
}

pub(super) fn dentry_name(name: String) -> Result<DentryName, MetadError> {
    DentryName::new(name.into_bytes()).map_err(|err| MetadError::Codec(err.to_string()))
}

pub(super) fn update_attr(wire: WireUpdateAttr) -> UpdateAttr {
    UpdateAttr {
        mode: wire.mode,
        uid: wire.uid,
        gid: wire.gid,
        size: wire.size,
        mtime_ms: wire.mtime_ms,
        ctime_ms: wire.ctime_ms,
    }
}

pub(super) fn xattr_set_mode(wire: WireXattrSetMode) -> XattrSetMode {
    match wire {
        WireXattrSetMode::Any => XattrSetMode::Any,
        WireXattrSetMode::Create => XattrSetMode::Create,
        WireXattrSetMode::Replace => XattrSetMode::Replace,
    }
}

pub(super) fn wire_dentry(entry: &DentryWithAttr) -> WireDentryWithAttr {
    WireDentryWithAttr {
        dentry: nokv_protocol::WireDentryRecord::from_dentry_record(&entry.dentry),
        attr: nokv_protocol::WireInodeAttr::from_inode_attr(&entry.attr),
        body: entry
            .body
            .as_ref()
            .map(nokv_protocol::WireBodyDescriptor::from_body_descriptor),
    }
}

pub(super) fn wire_prepared_artifact(
    mount: MountId,
    prepared: &PreparedArtifact,
) -> WirePreparedArtifact {
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

pub(super) fn prepared_artifact(
    prepared: WirePreparedArtifact,
) -> Result<PreparedArtifact, MetadError> {
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

pub(super) fn stored_chunk(
    chunk: nokv_protocol::WireChunkManifest,
) -> Result<StoredChunk, MetadError> {
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

pub(super) fn staged_object_set(
    staged: WireStagedObjectSet,
) -> Result<StagedObjectSet, MetadError> {
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

pub(super) fn wire_body_read_plan(plan: &nokv_meta::BodyReadPlan) -> WireBodyReadPlan {
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

pub(super) fn protocol_error(err: MetadataProtocolError) -> MetadError {
    MetadError::Codec(err.to_string())
}

pub(super) fn wire_log_position(position: nokv_cluster::LogPosition) -> WireMetadataPosition {
    WireMetadataPosition {
        term: position.term.get(),
        index: position.index.get(),
    }
}

pub(super) fn log_position(
    position: WireMetadataPosition,
) -> Result<nokv_cluster::LogPosition, ServerError> {
    Ok(nokv_cluster::LogPosition {
        term: nokv_cluster::LogTerm::new(position.term).map_err(ServerError::MetadataRaft)?,
        index: nokv_cluster::LogIndex::new(position.index).map_err(ServerError::MetadataRaft)?,
    })
}
