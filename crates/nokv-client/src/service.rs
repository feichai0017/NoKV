use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use nokv_meta::{
    DentryWithAttr, PublishArtifactStagedSession, RenameReplaceResult, UpdateAttr, XattrSetMode,
};
use nokv_object::ObjectReadPlan;
use nokv_protocol::{
    decode_envelope, decode_name_cursor, decode_xattr_name, encode_advisory_lock_kind,
    encode_file_type, encode_name_cursor, encode_request, encode_xattr_name, MetadataRpcRequest,
    MetadataRpcResult, WireMetadataPosition,
};
use nokv_types::{
    AdvisoryLock, AdvisoryLockRequest, BodyDescriptor, ChunkManifest, DentryName, InodeAttr,
    InodeId, PathMetadata, SnapshotPin, SpecialNodeSpec,
};

use crate::ClientError;

use crate::framed::PipelinedConnection;
#[cfg(test)]
use crate::framed::{read_frame, write_frame, FRAMED_RPC_MAGIC};
use crate::wire::*;

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);
const READ_NOT_FRESH_RETRY_INTERVAL: Duration = Duration::from_millis(25);
const MAX_BATCH_RPC_REQUESTS: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataClientOptions {
    pub address: SocketAddr,
    pub read_endpoints: Vec<SocketAddr>,
    pub timeout: Duration,
}

pub struct MetadataClient {
    options: MetadataClientOptions,
    next_request_id: AtomicU64,
    connections: Mutex<HashMap<SocketAddr, Arc<PipelinedConnection>>>,
    observed_position: Mutex<Option<WireMetadataPosition>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientPreparedArtifact {
    pub mount: u64,
    pub parent: InodeId,
    pub name: DentryName,
    pub path: Option<String>,
    pub inode: InodeId,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
    pub replace: bool,
    pub dentry_version: Option<u64>,
    pub old_generation: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientReadDirPlusPage {
    pub entries: Vec<DentryWithAttr>,
    pub next_cursor: Option<DentryName>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClientMetadataPosition {
    pub term: u64,
    pub index: u64,
}

const DEFAULT_LIST_PAGE_SIZE: usize = 1024;

impl MetadataClientOptions {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            read_endpoints: Vec::new(),
            timeout: DEFAULT_RPC_TIMEOUT,
        }
    }

    pub fn with_read_endpoints(mut self, endpoints: Vec<SocketAddr>) -> Self {
        self.read_endpoints = endpoints;
        self
    }
}

impl MetadataClient {
    pub fn new(options: MetadataClientOptions) -> Self {
        Self {
            options,
            next_request_id: AtomicU64::new(1),
            connections: Mutex::new(HashMap::new()),
            observed_position: Mutex::new(None),
        }
    }

    pub fn connect(address: SocketAddr) -> Self {
        Self::new(MetadataClientOptions::new(address))
    }

    pub fn observed_metadata_position(&self) -> Option<ClientMetadataPosition> {
        self.observed_position
            .lock()
            .expect("metadata observed position")
            .map(wire_metadata_position)
    }

    pub fn observe_metadata_position(&self, position: ClientMetadataPosition) {
        self.record_observed_position(metadata_position_to_wire(position));
    }

    pub fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::BootstrapRoot { mode, uid, gid })? {
            MetadataRpcResult::InodeAttr { .. } => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn add_metadata_raft_learner(
        &self,
        node: u64,
        address: SocketAddr,
        blocking: bool,
    ) -> Result<ClientMetadataPosition, ClientError> {
        match self.call(MetadataRpcRequest::MetadataRaftAddLearner {
            node,
            address: address.to_string(),
            blocking,
        })? {
            MetadataRpcResult::MetadataPosition { position } => {
                Ok(wire_metadata_position(position))
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn get_attr(&self, inode: InodeId) -> Result<Option<InodeAttr>, ClientError> {
        match self.call(MetadataRpcRequest::GetAttr { inode: inode.get() })? {
            MetadataRpcResult::InodeAttr { attr } => attr
                .map(|attr| attr.into_inode_attr().map_err(protocol_error))
                .transpose(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn get_attr_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> Result<Option<InodeAttr>, ClientError> {
        match self.call(MetadataRpcRequest::GetAttrAtSnapshot {
            snapshot_id,
            inode: inode.get(),
        })? {
            MetadataRpcResult::InodeAttr { attr } => attr
                .map(|attr| attr.into_inode_attr().map_err(protocol_error))
                .transpose(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn lookup_plus(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<Option<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::LookupPlus {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry } => {
                entry.map(|entry| wire_dentry(*entry)).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn lookup_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
        name: DentryName,
    ) -> Result<Option<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::LookupPlusAtSnapshot {
            snapshot_id,
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry } => {
                entry.map(|entry| wire_dentry(*entry)).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_dir_plus_page(
        &self,
        parent: InodeId,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ClientReadDirPlusPage, ClientError> {
        match self.call(MetadataRpcRequest::ReadDirPlusPage {
            parent: parent.get(),
            after_name_hex: after.map(encode_name_cursor),
            limit,
        })? {
            MetadataRpcResult::DentriesPage {
                entries,
                next_name_hex,
            } => Ok(ClientReadDirPlusPage {
                entries: entries
                    .into_iter()
                    .map(wire_dentry)
                    .collect::<Result<Vec<_>, _>>()?,
                next_cursor: next_name_hex
                    .as_deref()
                    .map(decode_name_cursor)
                    .transpose()
                    .map_err(|err| ClientError::Protocol(err.to_string()))?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_dir_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
    ) -> Result<Vec<DentryWithAttr>, ClientError> {
        match self.call(MetadataRpcRequest::ReadDirPlusAtSnapshot {
            snapshot_id,
            parent: parent.get(),
        })? {
            MetadataRpcResult::Dentries { entries } => {
                entries.into_iter().map(wire_dentry).collect()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateDir {
            parent: parent.get(),
            name: rpc_name(&name)?,
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_file_in_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateFile {
            parent: parent.get(),
            name: rpc_name(&name)?,
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_symlink(
        &self,
        parent: InodeId,
        name: DentryName,
        target: Vec<u8>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateSymlink {
            parent: parent.get(),
            name: rpc_name(&name)?,
            target,
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn create_special_node(
        &self,
        parent: InodeId,
        name: DentryName,
        spec: SpecialNodeSpec,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::CreateSpecialNode {
            parent: parent.get(),
            name: rpc_name(&name)?,
            file_type: encode_file_type(spec.file_type).to_owned(),
            mode: spec.mode,
            rdev: spec.rdev,
            uid: spec.uid,
            gid: spec.gid,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn update_attrs(
        &self,
        parent: InodeId,
        name: DentryName,
        changes: UpdateAttr,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::UpdateAttrs {
            parent: parent.get(),
            name: rpc_name(&name)?,
            changes: update_attr_to_wire(changes),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn update_root_attrs(&self, changes: UpdateAttr) -> Result<InodeAttr, ClientError> {
        match self.call(MetadataRpcRequest::UpdateRootAttrs {
            changes: update_attr_to_wire(changes),
        })? {
            MetadataRpcResult::InodeAttr { attr: Some(attr) } => {
                attr.into_inode_attr().map_err(protocol_error)
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn set_xattr(
        &self,
        inode: InodeId,
        name: &[u8],
        value: Vec<u8>,
        mode: XattrSetMode,
    ) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::SetXattr {
            inode: inode.get(),
            name_hex: encode_xattr_name(name),
            value,
            mode: xattr_set_mode_to_wire(mode),
        })? {
            MetadataRpcResult::Unit => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn get_xattr(&self, inode: InodeId, name: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        match self.call(MetadataRpcRequest::GetXattr {
            inode: inode.get(),
            name_hex: encode_xattr_name(name),
        })? {
            MetadataRpcResult::XattrValue { value } => Ok(value),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn list_xattr(&self, inode: InodeId) -> Result<Vec<Vec<u8>>, ClientError> {
        match self.call(MetadataRpcRequest::ListXattr { inode: inode.get() })? {
            MetadataRpcResult::XattrNames { names_hex } => names_hex
                .iter()
                .map(|name| decode_xattr_name(name).map_err(protocol_error))
                .collect(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove_xattr(&self, inode: InodeId, name: &[u8]) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::RemoveXattr {
            inode: inode.get(),
            name_hex: encode_xattr_name(name),
        })? {
            MetadataRpcResult::Unit => Ok(()),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn get_advisory_lock(
        &self,
        request: AdvisoryLockRequest,
    ) -> Result<Option<AdvisoryLock>, ClientError> {
        match self.call(MetadataRpcRequest::GetAdvisoryLock {
            inode: request.inode.get(),
            owner: request.owner,
            start: request.start,
            end: request.end,
            kind: encode_advisory_lock_kind(request.kind).to_owned(),
            pid: request.pid,
        })? {
            MetadataRpcResult::AdvisoryLock { lock } => lock.map(wire_advisory_lock).transpose(),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn set_advisory_lock(&self, request: AdvisoryLockRequest) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::SetAdvisoryLock {
            inode: request.inode.get(),
            owner: request.owner,
            start: request.start,
            end: request.end,
            kind: encode_advisory_lock_kind(request.kind).to_owned(),
            pid: request.pid,
            wait: request.wait,
        })? {
            MetadataRpcResult::Unit => Ok(()),
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

    pub fn mkdirs(
        &self,
        paths: &[String],
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut entries = Vec::with_capacity(paths.len());
        for chunk in paths.chunks(MAX_BATCH_RPC_REQUESTS) {
            let requests = chunk
                .iter()
                .map(|path| MetadataRpcRequest::CreateDirPath {
                    path: path.clone(),
                    mode,
                    uid,
                    gid,
                })
                .collect();
            let results: Vec<Result<MetadataRpcResult, ClientError>> =
                match self.call(MetadataRpcRequest::Batch { requests })? {
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
        let mut entries = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.list_page(path, cursor.as_ref(), DEFAULT_LIST_PAGE_SIZE)?;
            let page_empty = page.entries.is_empty();
            entries.extend(page.entries);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if page_empty || cursor.as_ref() == Some(&next_cursor) {
                return Err(ClientError::Protocol(
                    "metadata list page cursor did not advance".to_owned(),
                ));
            }
            cursor = Some(next_cursor);
        }
        Ok(entries)
    }

    pub fn list_indexed(&self, path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        let mut entries = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.list_indexed_page(path, cursor.as_ref(), DEFAULT_LIST_PAGE_SIZE)?;
            let page_empty = page.entries.is_empty();
            entries.extend(page.entries);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if page_empty || cursor.as_ref() == Some(&next_cursor) {
                return Err(ClientError::Protocol(
                    "indexed metadata list page cursor did not advance".to_owned(),
                ));
            }
            cursor = Some(next_cursor);
        }
        Ok(entries)
    }

    pub fn list_page(
        &self,
        path: &str,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ClientReadDirPlusPage, ClientError> {
        match self.call(MetadataRpcRequest::ReadDirPlusPathPage {
            path: path.to_owned(),
            after_name_hex: after.map(encode_name_cursor),
            limit,
        })? {
            MetadataRpcResult::DentriesPage {
                entries,
                next_name_hex,
            } => Ok(ClientReadDirPlusPage {
                entries: entries
                    .into_iter()
                    .map(wire_dentry)
                    .collect::<Result<Vec<_>, _>>()?,
                next_cursor: next_name_hex
                    .as_deref()
                    .map(decode_name_cursor)
                    .transpose()
                    .map_err(|err| ClientError::Protocol(err.to_string()))?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn list_indexed_page(
        &self,
        path: &str,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ClientReadDirPlusPage, ClientError> {
        match self.call(MetadataRpcRequest::ReadIndexedPathPage {
            path: path.to_owned(),
            after_name_hex: after.map(encode_name_cursor),
            limit,
        })? {
            MetadataRpcResult::DentriesPage {
                entries,
                next_name_hex,
            } => Ok(ClientReadDirPlusPage {
                entries: entries
                    .into_iter()
                    .map(wire_dentry)
                    .collect::<Result<Vec<_>, _>>()?,
                next_cursor: next_name_hex
                    .as_deref()
                    .map(decode_name_cursor)
                    .transpose()
                    .map_err(|err| ClientError::Protocol(err.to_string()))?,
            }),
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

    pub fn remove_file(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RemoveFile {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove_many(
        &self,
        paths: &[String],
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut entries = Vec::with_capacity(paths.len());
        for chunk in paths.chunks(MAX_BATCH_RPC_REQUESTS) {
            let requests = chunk
                .iter()
                .map(|path| MetadataRpcRequest::RemoveFilePath { path: path.clone() })
                .collect();
            let results: Vec<Result<MetadataRpcResult, ClientError>> =
                match self.call(MetadataRpcRequest::Batch { requests })? {
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

    pub fn rmdir(&self, path: &str) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RemoveEmptyDirPath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn remove_empty_dir(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RemoveEmptyDir {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn link(
        &self,
        inode: InodeId,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::Link {
            inode: inode.get(),
            new_parent: new_parent.get(),
            new_name: rpc_name(&new_name)?,
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rmdir_many(
        &self,
        paths: &[String],
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut entries = Vec::with_capacity(paths.len());
        for chunk in paths.chunks(MAX_BATCH_RPC_REQUESTS) {
            let requests = chunk
                .iter()
                .map(|path| MetadataRpcRequest::RemoveEmptyDirPath { path: path.clone() })
                .collect();
            let results: Vec<Result<MetadataRpcResult, ClientError>> =
                match self.call(MetadataRpcRequest::Batch { requests })? {
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

    pub fn rename(&self, source: &str, destination: &str) -> Result<DentryWithAttr, ClientError> {
        match self.call(MetadataRpcRequest::RenamePath {
            source: source.to_owned(),
            destination: destination.to_owned(),
        })? {
            MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rename_in_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<DentryWithAttr, ClientError> {
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

    pub fn rename_replace_in_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<RenameReplaceResult, ClientError> {
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
        match self.call(MetadataRpcRequest::SnapshotSubtreePath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Snapshot { snapshot } => wire_snapshot(snapshot),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn snapshot_subtree(&self, root: InodeId) -> Result<SnapshotPin, ClientError> {
        match self.call(MetadataRpcRequest::SnapshotSubtree { root: root.get() })? {
            MetadataRpcResult::Snapshot { snapshot } => wire_snapshot(snapshot),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn snapshot_pin(&self, snapshot_id: u64) -> Result<Option<SnapshotPin>, ClientError> {
        match self.call(MetadataRpcRequest::SnapshotPin { snapshot_id })? {
            MetadataRpcResult::SnapshotPin { snapshot } => snapshot.map(wire_snapshot).transpose(),
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
    ) -> Result<ObjectReadPlan, ClientError> {
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

    pub fn read_path_plan(
        &self,
        path: &str,
        offset: u64,
        len: usize,
        expected_generation: Option<u64>,
    ) -> Result<(PathMetadata, ObjectReadPlan), ClientError> {
        let len = u64::try_from(len)
            .map_err(|_| ClientError::Protocol("path read length exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::ReadPathPlan {
            path: path.to_owned(),
            offset,
            len,
            expected_generation,
        })? {
            MetadataRpcResult::PathReadPlan { metadata, plan } => {
                Ok((wire_path_metadata(metadata)?, wire_body_read_plan(plan)?))
            }
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

    pub fn read_file_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, ClientError> {
        let len = u64::try_from(len)
            .map_err(|_| ClientError::Protocol("snapshot read length exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::ReadFileAtSnapshot {
            snapshot_id,
            inode: inode.get(),
            offset,
            len,
        })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_symlink(&self, inode: InodeId) -> Result<Vec<u8>, ClientError> {
        match self.call(MetadataRpcRequest::ReadSymlink { inode: inode.get() })? {
            MetadataRpcResult::FileBytes { bytes } => Ok(bytes),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_symlink_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> Result<Vec<u8>, ClientError> {
        match self.call(MetadataRpcRequest::ReadSymlinkAtSnapshot {
            snapshot_id,
            inode: inode.get(),
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
    ) -> Result<ClientPreparedArtifact, ClientError> {
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
    ) -> Result<ClientPreparedArtifact, ClientError> {
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
        prepared: ClientPreparedArtifact,
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

    pub fn publish_prepared_artifact_staged_session(
        &self,
        prepared: ClientPreparedArtifact,
        request: PublishArtifactStagedSession,
    ) -> Result<RenameReplaceResult, ClientError> {
        match self.call(MetadataRpcRequest::PublishPreparedArtifactStagedSession {
            prepared: prepared_artifact_to_wire(&prepared)?,
            producer: request.producer,
            digest_uri: request.digest_uri,
            content_type: request.content_type,
            manifest_id: request.manifest_id,
            size: request.size,
            chunks: request.chunks.iter().map(stored_chunk_to_wire).collect(),
            staged: staged_object_set_to_wire(&request.staged),
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
        })? {
            MetadataRpcResult::RenameReplace { entry, replaced } => Ok(RenameReplaceResult {
                entry: wire_dentry(*entry)?,
                replaced: replaced.map(|entry| wire_dentry(*entry)).transpose()?,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    fn call(&self, request: MetadataRpcRequest) -> Result<MetadataRpcResult, ClientError> {
        let mut endpoints = self.request_endpoints(&request);
        if endpoints.is_empty() {
            return Err(ClientError::Protocol(
                "metadata request had no target endpoint".to_owned(),
            ));
        }
        let started = Instant::now();
        let mut fallback_error = None;

        loop {
            let mut saw_stale_read = false;
            let mut saw_forward_to_leader = false;
            let mut index = 0;
            while index < endpoints.len() {
                let address = endpoints[index];
                index += 1;
                let request = self.request_with_observed_position(request.clone());
                match self.call_at(address, &request) {
                    Ok(result) => return Ok(result),
                    Err(err @ ClientError::ReadNotFresh { .. }) => {
                        saw_stale_read = true;
                        fallback_error = Some(err);
                    }
                    Err(
                        err @ ClientError::ForwardToLeader {
                            address: Some(leader),
                            ..
                        },
                    ) => {
                        saw_forward_to_leader = true;
                        fallback_error = Some(err);
                        if !endpoints.contains(&leader) {
                            endpoints.insert(index, leader);
                        }
                    }
                    Err(err @ ClientError::ForwardToLeader { address: None, .. }) => {
                        saw_forward_to_leader = true;
                        fallback_error = Some(err);
                    }
                    Err(err @ ClientError::Io(_)) => {
                        self.drop_connection(address);
                        fallback_error = Some(err);
                    }
                    Err(err) => return Err(err),
                }
            }

            if !saw_stale_read && !saw_forward_to_leader {
                break;
            }
            let Some(delay) = read_not_fresh_retry_delay(started, self.options.timeout) else {
                break;
            };
            thread::sleep(delay);
        }

        Err(fallback_error.unwrap_or_else(|| {
            ClientError::Protocol("metadata request had no target endpoint".to_owned())
        }))
    }

    fn call_at(
        &self,
        address: SocketAddr,
        request: &MetadataRpcRequest,
    ) -> Result<MetadataRpcResult, ClientError> {
        let body = encode_request(request).map_err(|err| ClientError::Protocol(err.to_string()))?;
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let connection = self.connection(address)?;
        let body = connection.call(request_id, &body, self.options.timeout)?;
        let envelope =
            decode_envelope(&body).map_err(|err| ClientError::Protocol(err.to_string()))?;
        if envelope.ok {
            if let Some(position) = envelope.metadata_position {
                self.record_observed_position(position);
            }
        }
        envelope_result(envelope)
    }

    fn request_endpoints(&self, request: &MetadataRpcRequest) -> Vec<SocketAddr> {
        if !request_uses_read_endpoints(request) {
            return vec![self.options.address];
        }
        let mut endpoints = Vec::with_capacity(self.options.read_endpoints.len() + 1);
        for endpoint in &self.options.read_endpoints {
            if !endpoints.contains(endpoint) {
                endpoints.push(*endpoint);
            }
        }
        if !endpoints.contains(&self.options.address) {
            endpoints.push(self.options.address);
        }
        endpoints
    }

    fn request_with_observed_position(&self, request: MetadataRpcRequest) -> MetadataRpcRequest {
        if !request_requires_observed_position(&request) {
            return request;
        }
        let Some(position) = *self
            .observed_position
            .lock()
            .expect("metadata observed position")
        else {
            return request;
        };
        MetadataRpcRequest::RequireApplied {
            position,
            request: Box::new(request),
        }
    }

    fn record_observed_position(&self, position: WireMetadataPosition) {
        let mut observed = self
            .observed_position
            .lock()
            .expect("metadata observed position");
        if observed.map(|existing| position > existing).unwrap_or(true) {
            *observed = Some(position);
        }
    }

    fn connection(&self, address: SocketAddr) -> Result<Arc<PipelinedConnection>, ClientError> {
        let mut guard = self.connections.lock().expect("metadata rpc connections");
        if let Some(connection) = guard.get(&address) {
            return Ok(Arc::clone(connection));
        }
        let connection = Arc::new(PipelinedConnection::connect(address)?);
        guard.insert(address, Arc::clone(&connection));
        Ok(connection)
    }

    fn drop_connection(&self, address: SocketAddr) {
        self.connections
            .lock()
            .expect("metadata rpc connections")
            .remove(&address);
    }
}

fn request_uses_read_endpoints(request: &MetadataRpcRequest) -> bool {
    match request {
        MetadataRpcRequest::Batch { requests } => {
            !requests.is_empty() && requests.iter().all(request_uses_read_endpoints)
        }
        MetadataRpcRequest::RequireApplied { request, .. } => request_uses_read_endpoints(request),
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
        | MetadataRpcRequest::ReadFileAtSnapshot { .. }
        | MetadataRpcRequest::ReadFilePathAtSnapshot { .. }
        | MetadataRpcRequest::ReadSymlink { .. }
        | MetadataRpcRequest::ReadSymlinkAtSnapshot { .. }
        | MetadataRpcRequest::ReadBodyPlan { .. }
        | MetadataRpcRequest::ReadPathPlan { .. }
        | MetadataRpcRequest::SnapshotPin { .. } => true,
        _ => false,
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

fn request_requires_observed_position(request: &MetadataRpcRequest) -> bool {
    match request {
        MetadataRpcRequest::Batch { requests } => {
            requests.iter().any(request_requires_observed_position)
        }
        MetadataRpcRequest::RequireApplied { .. }
        | MetadataRpcRequest::BootstrapRoot { .. }
        | MetadataRpcRequest::MetadataRaftAddLearner { .. }
        | MetadataRpcRequest::MetadataRaftVote { .. }
        | MetadataRpcRequest::MetadataRaftAppendEntries { .. }
        | MetadataRpcRequest::MetadataRaftInstallSnapshot { .. } => false,
        _ => true,
    }
}

fn read_not_fresh_retry_delay(started: Instant, timeout: Duration) -> Option<Duration> {
    let elapsed = started.elapsed();
    if elapsed >= timeout {
        return None;
    }
    Some((timeout - elapsed).min(READ_NOT_FRESH_RETRY_INTERVAL))
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
