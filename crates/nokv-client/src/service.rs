use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nokv_meta::{
    DentryWithAttr, NamespaceAggregateGroup, NamespaceAggregateMeasure, NamespaceAggregateOp,
    NamespaceAggregateOutputMeasure, NamespaceAggregateRequest, NamespaceAggregateResult,
    NamespaceAggregateSample, NamespaceAggregateSort, NamespaceAggregateValue,
    NamespaceBodyDescriptor, NamespaceCard, NamespaceCardKind, NamespaceFacetSummary,
    NamespaceFacetValue, NamespaceFieldSource, NamespaceFieldSourceKind, NamespaceFieldValue,
    NamespaceFilterCapability, NamespaceFindField, NamespaceFindRequest, NamespaceFindResult,
    NamespaceGrepMatch, NamespaceGrepRequest, NamespaceGrepResult, NamespaceInclude,
    NamespaceIndexValue, NamespaceListOptions, NamespaceListPage, NamespacePredicate,
    NamespacePredicateOp, NamespacePredicateValue, NamespaceQueryCatalog, NamespaceReadFormat,
    NamespaceReadItem, NamespaceReadOptions, NamespaceReadPage, NamespaceRecordCount,
    NamespaceRecordType, NamespaceSchema, NamespaceSort, NamespaceSortDirection,
    NamespaceSortField, PublishArtifactStagedSession, RecordCountProvenance, RenameReplaceResult,
    SubtreeDelta, UpdateAttr, XattrSetMode,
};
use nokv_object::ObjectReadPlan;
use nokv_protocol::{
    decode_envelope, decode_name_cursor, decode_xattr_name, encode_advisory_lock_kind,
    encode_file_type, encode_name_cursor, encode_request, encode_xattr_name, MetadataRpcRequest,
    MetadataRpcResult, WireNamespaceAggregateGroup, WireNamespaceAggregateMeasure,
    WireNamespaceAggregateOp, WireNamespaceAggregateOutputMeasure, WireNamespaceAggregateRequest,
    WireNamespaceAggregateResult, WireNamespaceAggregateSample, WireNamespaceAggregateSort,
    WireNamespaceAggregateValue, WireNamespaceCard, WireNamespaceCardKind,
    WireNamespaceFacetSummary, WireNamespaceFacetValue, WireNamespaceFieldSource,
    WireNamespaceFieldSourceKind, WireNamespaceFieldValue, WireNamespaceFilterCapability,
    WireNamespaceFindField, WireNamespaceFindRequest, WireNamespaceFindResult,
    WireNamespaceGrepMatch, WireNamespaceGrepRequest, WireNamespaceGrepResult,
    WireNamespaceInclude, WireNamespaceIndexValue, WireNamespaceListPage, WireNamespacePredicate,
    WireNamespacePredicateOp, WireNamespacePredicateValue, WireNamespaceQueryCatalog,
    WireNamespaceReadFormat, WireNamespaceReadItem, WireNamespaceReadOptions,
    WireNamespaceReadPage, WireNamespaceRecordCount, WireNamespaceRecordType, WireNamespaceSchema,
    WireNamespaceSort, WireNamespaceSortDirection, WireNamespaceSortField,
    WireRecordCountProvenance,
};
use nokv_types::{
    AdvisoryLock, AdvisoryLockRequest, BodyDescriptor, ChunkManifest, DentryName, InodeAttr,
    InodeId, PathMetadata, ReadLease, SnapshotPin, SpecialNodeSpec,
};

use crate::ClientError;

use crate::framed::PipelinedConnection;
#[cfg(test)]
use crate::framed::{read_frame, write_frame, FRAMED_RPC_MAGIC};
use crate::wire::*;

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_BATCH_RPC_REQUESTS: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataClientOptions {
    pub address: SocketAddr,
    pub timeout: Duration,
}

pub struct MetadataClient {
    options: MetadataClientOptions,
    next_request_id: AtomicU64,
    connections: Mutex<HashMap<SocketAddr, Arc<PipelinedConnection>>>,
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
pub struct ClientCreatedPreparedArtifact {
    pub entry: DentryWithAttr,
    pub prepared: ClientPreparedArtifact,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientReadDirPlusPage {
    pub entries: Vec<DentryWithAttr>,
    pub next_cursor: Option<DentryName>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathLayoutOpen {
    pub metadata: PathMetadata,
    pub lease: ReadLease,
    pub plan: ObjectReadPlan,
}

/// Result of a copy-on-write subtree clone: the fork's namespace root inode and the
/// retained snapshot pin that protects the shared base blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloneOutcome {
    pub root: InodeId,
    pub snapshot_id: u64,
}

/// Result of pinning a subtree snapshot: the durable `snapshot_id` to pass to a
/// later rollback and the read version the snapshot captured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotOutcome {
    pub snapshot_id: u64,
    pub read_version: u64,
}

const DEFAULT_LIST_PAGE_SIZE: usize = 1024;

impl MetadataClientOptions {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            timeout: DEFAULT_RPC_TIMEOUT,
        }
    }
}

impl MetadataClient {
    pub fn new(options: MetadataClientOptions) -> Self {
        Self {
            options,
            next_request_id: AtomicU64::new(1),
            connections: Mutex::new(HashMap::new()),
        }
    }

    pub fn connect(address: SocketAddr) -> Self {
        Self::new(MetadataClientOptions::new(address))
    }

    pub fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::BootstrapRoot { mode, uid, gid })? {
            MetadataRpcResult::InodeAttr { .. } => Ok(()),
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

    pub fn current_dentry_version(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<Option<u64>, ClientError> {
        match self.call(MetadataRpcRequest::CurrentDentryVersion {
            parent: parent.get(),
            name: rpc_name(&name)?,
        })? {
            MetadataRpcResult::DentryVersion { version } => Ok(version),
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

    pub fn create_file_prepared_in_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<ClientCreatedPreparedArtifact, ClientError> {
        match self.call(MetadataRpcRequest::CreateFilePrepared {
            parent: parent.get(),
            name: rpc_name(&name)?,
            mode,
            uid,
            gid,
        })? {
            MetadataRpcResult::CreatedPreparedArtifact { entry, prepared } => {
                Ok(ClientCreatedPreparedArtifact {
                    entry: wire_dentry(*entry)?,
                    prepared: wire_prepared_artifact(prepared)?,
                })
            }
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

    pub fn stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, ClientError> {
        match self.call(MetadataRpcRequest::StatCard {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::NamespaceCard { card } => {
                card.map(|card| namespace_card(*card)).transpose()
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn namespace_list_page(
        &self,
        path: &str,
        options: NamespaceListOptions,
    ) -> Result<NamespaceListPage, ClientError> {
        let limit = u64::try_from(options.limit)
            .map_err(|_| ClientError::Protocol("namespace list limit exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::ListPage {
            path: path.to_owned(),
            cursor: options.cursor,
            limit,
        })? {
            MetadataRpcResult::NamespaceListPage { page } => namespace_list_page(*page),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn find_paths(
        &self,
        request: NamespaceFindRequest,
    ) -> Result<NamespaceFindResult, ClientError> {
        match self.call(MetadataRpcRequest::FindPaths {
            request: Box::new(wire_namespace_find_request(&request)?),
        })? {
            MetadataRpcResult::NamespaceFindResult { result } => namespace_find_result(*result),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn aggregate_paths(
        &self,
        request: NamespaceAggregateRequest,
    ) -> Result<NamespaceAggregateResult, ClientError> {
        match self.call(MetadataRpcRequest::AggregatePaths {
            request: Box::new(wire_namespace_aggregate_request(&request)?),
        })? {
            MetadataRpcResult::NamespaceAggregateResult { result } => {
                namespace_aggregate_result(*result)
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn grep_paths(
        &self,
        request: NamespaceGrepRequest,
    ) -> Result<NamespaceGrepResult, ClientError> {
        match self.call(MetadataRpcRequest::GrepPaths {
            request: Box::new(wire_namespace_grep_request(&request)?),
        })? {
            MetadataRpcResult::NamespaceGrepResult { result } => namespace_grep_result(*result),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn read_page(
        &self,
        path: &str,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, ClientError> {
        match self.call(MetadataRpcRequest::ReadPage {
            path: path.to_owned(),
            options: Box::new(wire_namespace_read_options(&options)?),
        })? {
            MetadataRpcResult::NamespaceReadPage { page } => namespace_read_page(*page),
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

    pub fn clone_subtree_path(&self, src: &str, dst: &str) -> Result<CloneOutcome, ClientError> {
        match self.call(MetadataRpcRequest::CloneSubtreePath {
            src_path: src.to_owned(),
            dst_path: dst.to_owned(),
        })? {
            MetadataRpcResult::CloneSubtree { root, snapshot_id } => Ok(CloneOutcome {
                root: inode_id(root)?,
                snapshot_id,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn diff_subtrees(&self, a: &str, b: &str) -> Result<Vec<SubtreeDelta>, ClientError> {
        match self.call(MetadataRpcRequest::DiffSubtrees {
            a_path: a.to_owned(),
            b_path: b.to_owned(),
        })? {
            MetadataRpcResult::SubtreeDeltas { deltas } => {
                Ok(deltas.into_iter().map(subtree_delta).collect())
            }
            other => Err(unexpected_result(other)),
        }
    }

    pub fn snapshot_subtree_path(&self, path: &str) -> Result<SnapshotOutcome, ClientError> {
        match self.call(MetadataRpcRequest::SnapshotSubtreePath {
            path: path.to_owned(),
        })? {
            MetadataRpcResult::Snapshot { snapshot } => Ok(SnapshotOutcome {
                snapshot_id: snapshot.snapshot_id,
                read_version: snapshot.read_version,
            }),
            other => Err(unexpected_result(other)),
        }
    }

    pub fn rollback_subtree_path(&self, target: &str, snapshot_id: u64) -> Result<(), ClientError> {
        match self.call(MetadataRpcRequest::RollbackSubtreePath {
            target_path: target.to_owned(),
            snapshot_id,
        })? {
            MetadataRpcResult::Unit => Ok(()),
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

    pub fn renew_snapshot(&self, snapshot_id: u64, lease_ms: u64) -> Result<bool, ClientError> {
        match self.call(MetadataRpcRequest::RenewSnapshot {
            snapshot_id,
            lease_ms,
        })? {
            MetadataRpcResult::RenewedSnapshot { renewed } => Ok(renewed),
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

    pub fn open_path_read_plan(
        &self,
        path: &str,
        offset: u64,
        len: usize,
        expected_generation: Option<u64>,
    ) -> Result<PathLayoutOpen, ClientError> {
        let len = u64::try_from(len)
            .map_err(|_| ClientError::Protocol("path read length exceeds u64".to_owned()))?;
        match self.call(MetadataRpcRequest::OpenPathReadPlan {
            path: path.to_owned(),
            offset,
            len,
            expected_generation,
        })? {
            MetadataRpcResult::OpenPathReadPlan {
                metadata,
                lease,
                plan,
            } => Ok(PathLayoutOpen {
                metadata: wire_path_metadata(metadata)?,
                lease: wire_read_lease(lease)?,
                plan: wire_body_read_plan(plan)?,
            }),
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
            chunks: request.chunks.iter().map(chunk_to_wire).collect(),
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
        let address = self.options.address;
        let body =
            encode_request(&request).map_err(|err| ClientError::Protocol(err.to_string()))?;
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let connection = self.connection(address)?;
        let body = match connection.call(request_id, &body, self.options.timeout) {
            Ok(body) => body,
            Err(err @ ClientError::Io(_)) => {
                self.drop_connection(address);
                return Err(err);
            }
            Err(err) => return Err(err),
        };
        let envelope =
            decode_envelope(&body).map_err(|err| ClientError::Protocol(err.to_string()))?;
        envelope_result(envelope)
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

fn namespace_card(card: WireNamespaceCard) -> Result<NamespaceCard, ClientError> {
    Ok(NamespaceCard {
        path: card.path,
        name: card.name,
        kind: match card.kind {
            WireNamespaceCardKind::File => NamespaceCardKind::File,
            WireNamespaceCardKind::Directory => NamespaceCardKind::Directory,
            WireNamespaceCardKind::Symlink => NamespaceCardKind::Symlink,
            WireNamespaceCardKind::Special => NamespaceCardKind::Special,
        },
        evidence: card.evidence,
        snapshot_id: card.snapshot_id,
        inode: inode_id(card.inode)?,
        generation: card.generation,
        size_bytes: card.size_bytes,
        entry_count: card
            .entry_count
            .map(usize::try_from)
            .transpose()
            .map_err(|_| ClientError::Protocol("entry_count exceeds platform limit".to_owned()))?,
        record_count: card.record_count.map(namespace_record_count).transpose()?,
        schema: card.schema.map(namespace_schema),
        sample: card.sample,
        body: card.body.map(|body| {
            let body = body.into_body_descriptor();
            NamespaceBodyDescriptor {
                producer: body.producer,
                digest_uri: body.digest_uri,
                size: body.size,
                content_type: body.content_type,
                manifest_id: body.manifest_id,
                generation: body.generation,
                chunk_size: body.chunk_size,
                block_size: body.block_size,
            }
        }),
        catalog: namespace_query_catalog(card.catalog)?,
        indexed_values: card
            .indexed_values
            .into_iter()
            .map(namespace_index_value)
            .collect(),
    })
}

fn namespace_record_count(
    count: WireNamespaceRecordCount,
) -> Result<NamespaceRecordCount, ClientError> {
    Ok(NamespaceRecordCount {
        count: usize::try_from(count.count)
            .map_err(|_| ClientError::Protocol("record_count exceeds platform limit".to_owned()))?,
        provenance: match count.provenance {
            WireRecordCountProvenance::LiveNamespace => RecordCountProvenance::LiveNamespace,
            WireRecordCountProvenance::StructuredBody => RecordCountProvenance::StructuredBody,
            WireRecordCountProvenance::MaterializedIndex => {
                RecordCountProvenance::MaterializedIndex
            }
            WireRecordCountProvenance::Approximate => RecordCountProvenance::Approximate,
        },
    })
}

fn namespace_schema(schema: WireNamespaceSchema) -> NamespaceSchema {
    NamespaceSchema {
        record_type: namespace_record_type(schema.record_type),
        fields: schema.fields,
    }
}

fn namespace_record_type(record_type: WireNamespaceRecordType) -> NamespaceRecordType {
    match record_type {
        WireNamespaceRecordType::DirectoryEntries => NamespaceRecordType::DirectoryEntries,
        WireNamespaceRecordType::JsonArray => NamespaceRecordType::JsonArray,
        WireNamespaceRecordType::JsonObject => NamespaceRecordType::JsonObject,
        WireNamespaceRecordType::YamlMapping => NamespaceRecordType::YamlMapping,
        WireNamespaceRecordType::TextLines => NamespaceRecordType::TextLines,
    }
}

fn namespace_query_catalog(
    catalog: WireNamespaceQueryCatalog,
) -> Result<NamespaceQueryCatalog, ClientError> {
    Ok(NamespaceQueryCatalog {
        filterable: catalog
            .filterable
            .into_iter()
            .map(namespace_filter_capability)
            .collect(),
        sortable: catalog
            .sortable
            .into_iter()
            .map(namespace_sort_field)
            .collect(),
        facetable: catalog
            .facetable
            .into_iter()
            .map(namespace_find_field)
            .collect(),
        facets: catalog
            .facets
            .into_iter()
            .map(namespace_facet_summary)
            .collect::<Result<Vec<_>, _>>()?,
        projections: catalog
            .projections
            .into_iter()
            .map(namespace_include)
            .collect(),
    })
}

fn namespace_facet_summary(
    facet: WireNamespaceFacetSummary,
) -> Result<NamespaceFacetSummary, ClientError> {
    Ok(NamespaceFacetSummary {
        field: namespace_find_field(facet.field),
        values: facet
            .values
            .into_iter()
            .map(namespace_facet_value)
            .collect::<Result<Vec<_>, _>>()?,
        distinct_count: usize::try_from(facet.distinct_count).map_err(|_| {
            ClientError::Protocol("facet distinct_count exceeds platform limit".to_owned())
        })?,
        truncated: facet.truncated,
    })
}

fn namespace_facet_value(
    value: WireNamespaceFacetValue,
) -> Result<NamespaceFacetValue, ClientError> {
    Ok(NamespaceFacetValue {
        value: namespace_predicate_value(value.value),
        count: usize::try_from(value.count)
            .map_err(|_| ClientError::Protocol("facet count exceeds platform limit".to_owned()))?,
    })
}

fn namespace_filter_capability(
    capability: WireNamespaceFilterCapability,
) -> NamespaceFilterCapability {
    NamespaceFilterCapability {
        field: namespace_find_field(capability.field),
        operators: capability
            .operators
            .into_iter()
            .map(namespace_predicate_op)
            .collect(),
    }
}

fn namespace_include(include: WireNamespaceInclude) -> NamespaceInclude {
    match include {
        WireNamespaceInclude::Body => NamespaceInclude::Body,
        WireNamespaceInclude::Schema => NamespaceInclude::Schema,
        WireNamespaceInclude::Sample => NamespaceInclude::Sample,
        WireNamespaceInclude::Catalog => NamespaceInclude::Catalog,
    }
}

fn namespace_find_field(field: WireNamespaceFindField) -> NamespaceFindField {
    NamespaceFindField::new(field.id)
}

fn namespace_predicate_op(op: WireNamespacePredicateOp) -> NamespacePredicateOp {
    match op {
        WireNamespacePredicateOp::Eq => NamespacePredicateOp::Eq,
        WireNamespacePredicateOp::NotEqual => NamespacePredicateOp::NotEqual,
        WireNamespacePredicateOp::In => NamespacePredicateOp::In,
        WireNamespacePredicateOp::Prefix => NamespacePredicateOp::Prefix,
        WireNamespacePredicateOp::Suffix => NamespacePredicateOp::Suffix,
        WireNamespacePredicateOp::Contains => NamespacePredicateOp::Contains,
        WireNamespacePredicateOp::GreaterThan => NamespacePredicateOp::GreaterThan,
        WireNamespacePredicateOp::GreaterThanOrEqual => NamespacePredicateOp::GreaterThanOrEqual,
        WireNamespacePredicateOp::LessThan => NamespacePredicateOp::LessThan,
        WireNamespacePredicateOp::LessThanOrEqual => NamespacePredicateOp::LessThanOrEqual,
        WireNamespacePredicateOp::Exists => NamespacePredicateOp::Exists,
        WireNamespacePredicateOp::NotExists => NamespacePredicateOp::NotExists,
    }
}

fn namespace_predicate(predicate: WireNamespacePredicate) -> NamespacePredicate {
    NamespacePredicate {
        field: namespace_find_field(predicate.field),
        op: namespace_predicate_op(predicate.op),
        value: predicate.value.map(namespace_predicate_value),
    }
}

fn namespace_sort_field(field: WireNamespaceSortField) -> NamespaceSortField {
    NamespaceSortField::new(field.id)
}

fn namespace_index_value(value: WireNamespaceIndexValue) -> NamespaceIndexValue {
    NamespaceIndexValue {
        field: namespace_find_field(value.field),
        value: namespace_predicate_value(value.value),
    }
}

fn namespace_predicate_value(value: WireNamespacePredicateValue) -> NamespacePredicateValue {
    match value {
        WireNamespacePredicateValue::String(value) => NamespacePredicateValue::String(value),
        WireNamespacePredicateValue::U64(value) => NamespacePredicateValue::U64(value),
        WireNamespacePredicateValue::F64(value) => NamespacePredicateValue::F64(value),
        WireNamespacePredicateValue::List(values) => NamespacePredicateValue::List(
            values.into_iter().map(namespace_predicate_value).collect(),
        ),
    }
}

fn namespace_list_page(page: WireNamespaceListPage) -> Result<NamespaceListPage, ClientError> {
    Ok(NamespaceListPage {
        path: page.path,
        evidence: page.evidence,
        snapshot_id: page.snapshot_id,
        entry_count: usize::try_from(page.entry_count)
            .map_err(|_| ClientError::Protocol("entry_count exceeds platform limit".to_owned()))?,
        entries: page
            .entries
            .into_iter()
            .map(namespace_card)
            .collect::<Result<Vec<_>, _>>()?,
        next_cursor: page.next_cursor,
        truncated: page.truncated,
    })
}

fn namespace_find_result(
    result: WireNamespaceFindResult,
) -> Result<NamespaceFindResult, ClientError> {
    Ok(NamespaceFindResult {
        path: result.path,
        evidence: result.evidence,
        snapshot_id: result.snapshot_id,
        match_count: usize::try_from(result.match_count)
            .map_err(|_| ClientError::Protocol("match_count exceeds platform limit".to_owned()))?,
        matches: result
            .matches
            .into_iter()
            .map(namespace_card)
            .collect::<Result<Vec<_>, _>>()?,
        facets: result
            .facets
            .into_iter()
            .map(namespace_facet_summary)
            .collect::<Result<Vec<_>, _>>()?,
        next_cursor: result.next_cursor,
        truncated: result.truncated,
        scanned_entries: usize::try_from(result.scanned_entries).map_err(|_| {
            ClientError::Protocol("scanned_entries exceeds platform limit".to_owned())
        })?,
    })
}

fn wire_namespace_grep_request(
    request: &NamespaceGrepRequest,
) -> Result<WireNamespaceGrepRequest, ClientError> {
    Ok(WireNamespaceGrepRequest {
        path: request.path.clone(),
        pattern: request.pattern.clone(),
        recursive: request.recursive,
        cursor: request.cursor.clone(),
        limit: u64::try_from(request.limit)
            .map_err(|_| ClientError::Protocol("namespace grep limit exceeds u64".to_owned()))?,
        max_files: request
            .max_files
            .map(|value| {
                u64::try_from(value).map_err(|_| {
                    ClientError::Protocol("namespace grep max_files exceeds u64".to_owned())
                })
            })
            .transpose()?,
        max_bytes: request
            .max_bytes
            .map(|value| {
                u64::try_from(value).map_err(|_| {
                    ClientError::Protocol("namespace grep max_bytes exceeds u64".to_owned())
                })
            })
            .transpose()?,
    })
}

fn namespace_grep_result(
    result: WireNamespaceGrepResult,
) -> Result<NamespaceGrepResult, ClientError> {
    Ok(NamespaceGrepResult {
        path: result.path,
        pattern: result.pattern,
        recursive: result.recursive,
        evidence: result.evidence,
        snapshot_id: result.snapshot_id,
        matches: result
            .matches
            .into_iter()
            .map(namespace_grep_match)
            .collect::<Result<Vec<_>, _>>()?,
        files_scanned: usize::try_from(result.files_scanned).map_err(|_| {
            ClientError::Protocol("grep files_scanned exceeds platform limit".to_owned())
        })?,
        bytes_read: usize::try_from(result.bytes_read).map_err(|_| {
            ClientError::Protocol("grep bytes_read exceeds platform limit".to_owned())
        })?,
        next_cursor: result.next_cursor,
        truncated: result.truncated,
    })
}

fn namespace_grep_match(match_: WireNamespaceGrepMatch) -> Result<NamespaceGrepMatch, ClientError> {
    Ok(NamespaceGrepMatch {
        path: match_.path,
        line_number: usize::try_from(match_.line_number).map_err(|_| {
            ClientError::Protocol("grep line_number exceeds platform limit".to_owned())
        })?,
        snippet: match_.snippet,
        evidence: match_.evidence,
        generation: match_.generation,
    })
}

fn namespace_aggregate_result(
    result: WireNamespaceAggregateResult,
) -> Result<NamespaceAggregateResult, ClientError> {
    Ok(NamespaceAggregateResult {
        path: result.path,
        evidence: result.evidence,
        snapshot_id: result.snapshot_id,
        predicates: result
            .predicates
            .into_iter()
            .map(namespace_predicate)
            .collect(),
        input_match_count: usize::try_from(result.input_match_count).map_err(|_| {
            ClientError::Protocol("aggregate input_match_count exceeds platform limit".to_owned())
        })?,
        row_count: usize::try_from(result.row_count).map_err(|_| {
            ClientError::Protocol("aggregate row_count exceeds platform limit".to_owned())
        })?,
        group_count: usize::try_from(result.group_count).map_err(|_| {
            ClientError::Protocol("aggregate group_count exceeds platform limit".to_owned())
        })?,
        groups: result
            .groups
            .into_iter()
            .map(namespace_aggregate_group)
            .collect::<Result<Vec<_>, _>>()?,
        truncated: result.truncated,
        scanned_entries: usize::try_from(result.scanned_entries).map_err(|_| {
            ClientError::Protocol("aggregate scanned_entries exceeds platform limit".to_owned())
        })?,
    })
}

fn namespace_aggregate_group(
    group: WireNamespaceAggregateGroup,
) -> Result<NamespaceAggregateGroup, ClientError> {
    Ok(NamespaceAggregateGroup {
        key: group.key.into_iter().map(namespace_field_value).collect(),
        measures: group
            .measures
            .into_iter()
            .map(namespace_aggregate_output_measure)
            .collect(),
        evidence: group.evidence,
        sample_matches: group
            .sample_matches
            .into_iter()
            .map(namespace_aggregate_sample)
            .collect(),
    })
}

fn namespace_field_value(value: WireNamespaceFieldValue) -> NamespaceFieldValue {
    NamespaceFieldValue {
        field: namespace_find_field(value.field),
        value: namespace_predicate_value(value.value),
        source: namespace_field_source(value.source),
    }
}

fn namespace_field_source(source: WireNamespaceFieldSource) -> NamespaceFieldSource {
    NamespaceFieldSource {
        evidence: source.evidence,
        source_path: source.source_path,
        source_kind: match source.source_kind {
            WireNamespaceFieldSourceKind::Namespace => NamespaceFieldSourceKind::Namespace,
            WireNamespaceFieldSourceKind::MaterializedIndex => {
                NamespaceFieldSourceKind::MaterializedIndex
            }
        },
    }
}

fn namespace_aggregate_output_measure(
    measure: WireNamespaceAggregateOutputMeasure,
) -> NamespaceAggregateOutputMeasure {
    NamespaceAggregateOutputMeasure {
        name: measure.name,
        op: namespace_aggregate_op(measure.op),
        field: measure.field.map(namespace_find_field),
        value: namespace_aggregate_value(measure.value),
    }
}

fn namespace_aggregate_sample(sample: WireNamespaceAggregateSample) -> NamespaceAggregateSample {
    NamespaceAggregateSample {
        path: sample.path,
        evidence: sample.evidence,
        generation: sample.generation,
    }
}

fn namespace_aggregate_value(value: WireNamespaceAggregateValue) -> NamespaceAggregateValue {
    match value {
        WireNamespaceAggregateValue::U64(value) => NamespaceAggregateValue::U64(value),
        WireNamespaceAggregateValue::F64(value) => NamespaceAggregateValue::F64(value),
        WireNamespaceAggregateValue::Null => NamespaceAggregateValue::Null,
    }
}

fn namespace_aggregate_op(op: WireNamespaceAggregateOp) -> NamespaceAggregateOp {
    match op {
        WireNamespaceAggregateOp::Count => NamespaceAggregateOp::Count,
        WireNamespaceAggregateOp::Sum => NamespaceAggregateOp::Sum,
        WireNamespaceAggregateOp::Avg => NamespaceAggregateOp::Avg,
        WireNamespaceAggregateOp::Min => NamespaceAggregateOp::Min,
        WireNamespaceAggregateOp::Max => NamespaceAggregateOp::Max,
    }
}

fn namespace_read_page(page: WireNamespaceReadPage) -> Result<NamespaceReadPage, ClientError> {
    Ok(NamespaceReadPage {
        path: page.path,
        evidence: page.evidence,
        snapshot_id: page.snapshot_id,
        generation: page.generation,
        total_size_bytes: page.total_size_bytes,
        format: match page.format {
            WireNamespaceReadFormat::Structured => NamespaceReadFormat::Structured,
            WireNamespaceReadFormat::Bytes => NamespaceReadFormat::Bytes,
        },
        record_type: page.record_type.map(namespace_record_type),
        record_count: page
            .record_count
            .map(usize::try_from)
            .transpose()
            .map_err(|_| ClientError::Protocol("record_count exceeds platform limit".to_owned()))?,
        cursor: page.cursor,
        next_cursor: page.next_cursor,
        truncated: page.truncated,
        items: page
            .items
            .into_iter()
            .map(namespace_read_item)
            .collect::<Result<Vec<_>, _>>()?,
        bytes: page.bytes,
    })
}

fn namespace_read_item(item: WireNamespaceReadItem) -> Result<NamespaceReadItem, ClientError> {
    Ok(NamespaceReadItem {
        index: usize::try_from(item.index)
            .map_err(|_| ClientError::Protocol("item index exceeds platform limit".to_owned()))?,
        value_json: item.value_json,
        evidence: item.evidence,
    })
}

fn wire_namespace_find_request(
    request: &NamespaceFindRequest,
) -> Result<WireNamespaceFindRequest, ClientError> {
    Ok(WireNamespaceFindRequest {
        path: request.path.clone(),
        predicates: request
            .predicates
            .iter()
            .map(wire_namespace_predicate)
            .collect(),
        sort: request.sort.iter().map(wire_namespace_sort).collect(),
        include: request.include.iter().map(wire_namespace_include).collect(),
        facets: request
            .facets
            .iter()
            .map(|field| WireNamespaceFindField {
                id: field.id.clone(),
            })
            .collect(),
        cursor: request.cursor.clone(),
        limit: u64::try_from(request.limit)
            .map_err(|_| ClientError::Protocol("namespace find limit exceeds u64".to_owned()))?,
    })
}

fn wire_namespace_aggregate_request(
    request: &NamespaceAggregateRequest,
) -> Result<WireNamespaceAggregateRequest, ClientError> {
    Ok(WireNamespaceAggregateRequest {
        path: request.path.clone(),
        predicates: request
            .predicates
            .iter()
            .map(wire_namespace_predicate)
            .collect(),
        group_by: request
            .group_by
            .iter()
            .map(|field| WireNamespaceFindField {
                id: field.id.clone(),
            })
            .collect(),
        measures: request
            .measures
            .iter()
            .map(wire_namespace_aggregate_measure)
            .collect(),
        sort: request
            .sort
            .iter()
            .map(wire_namespace_aggregate_sort)
            .collect(),
        limit: u64::try_from(request.limit).map_err(|_| {
            ClientError::Protocol("namespace aggregate limit exceeds u64".to_owned())
        })?,
    })
}

fn wire_namespace_aggregate_measure(
    measure: &NamespaceAggregateMeasure,
) -> WireNamespaceAggregateMeasure {
    WireNamespaceAggregateMeasure {
        name: measure.name.clone(),
        op: wire_namespace_aggregate_op(&measure.op),
        field: measure.field.as_ref().map(|field| WireNamespaceFindField {
            id: field.id.clone(),
        }),
    }
}

fn wire_namespace_aggregate_sort(sort: &NamespaceAggregateSort) -> WireNamespaceAggregateSort {
    WireNamespaceAggregateSort {
        field: sort.field.clone(),
        direction: wire_namespace_sort_direction(&sort.direction),
    }
}

fn wire_namespace_aggregate_op(op: &NamespaceAggregateOp) -> WireNamespaceAggregateOp {
    match op {
        NamespaceAggregateOp::Count => WireNamespaceAggregateOp::Count,
        NamespaceAggregateOp::Sum => WireNamespaceAggregateOp::Sum,
        NamespaceAggregateOp::Avg => WireNamespaceAggregateOp::Avg,
        NamespaceAggregateOp::Min => WireNamespaceAggregateOp::Min,
        NamespaceAggregateOp::Max => WireNamespaceAggregateOp::Max,
    }
}

fn wire_namespace_include(include: &NamespaceInclude) -> WireNamespaceInclude {
    match include {
        NamespaceInclude::Body => WireNamespaceInclude::Body,
        NamespaceInclude::Schema => WireNamespaceInclude::Schema,
        NamespaceInclude::Sample => WireNamespaceInclude::Sample,
        NamespaceInclude::Catalog => WireNamespaceInclude::Catalog,
    }
}

fn wire_namespace_predicate(predicate: &NamespacePredicate) -> WireNamespacePredicate {
    WireNamespacePredicate {
        field: WireNamespaceFindField {
            id: predicate.field.id.clone(),
        },
        op: wire_namespace_predicate_op(&predicate.op),
        value: predicate.value.as_ref().map(wire_namespace_predicate_value),
    }
}

fn wire_namespace_predicate_op(op: &NamespacePredicateOp) -> WireNamespacePredicateOp {
    match op {
        NamespacePredicateOp::Eq => WireNamespacePredicateOp::Eq,
        NamespacePredicateOp::NotEqual => WireNamespacePredicateOp::NotEqual,
        NamespacePredicateOp::In => WireNamespacePredicateOp::In,
        NamespacePredicateOp::Prefix => WireNamespacePredicateOp::Prefix,
        NamespacePredicateOp::Suffix => WireNamespacePredicateOp::Suffix,
        NamespacePredicateOp::Contains => WireNamespacePredicateOp::Contains,
        NamespacePredicateOp::GreaterThan => WireNamespacePredicateOp::GreaterThan,
        NamespacePredicateOp::GreaterThanOrEqual => WireNamespacePredicateOp::GreaterThanOrEqual,
        NamespacePredicateOp::LessThan => WireNamespacePredicateOp::LessThan,
        NamespacePredicateOp::LessThanOrEqual => WireNamespacePredicateOp::LessThanOrEqual,
        NamespacePredicateOp::Exists => WireNamespacePredicateOp::Exists,
        NamespacePredicateOp::NotExists => WireNamespacePredicateOp::NotExists,
    }
}

fn wire_namespace_predicate_value(value: &NamespacePredicateValue) -> WireNamespacePredicateValue {
    match value {
        NamespacePredicateValue::String(value) => {
            WireNamespacePredicateValue::String(value.clone())
        }
        NamespacePredicateValue::U64(value) => WireNamespacePredicateValue::U64(*value),
        NamespacePredicateValue::F64(value) => WireNamespacePredicateValue::F64(*value),
        NamespacePredicateValue::List(values) => WireNamespacePredicateValue::List(
            values.iter().map(wire_namespace_predicate_value).collect(),
        ),
    }
}

fn wire_namespace_sort(sort: &NamespaceSort) -> WireNamespaceSort {
    WireNamespaceSort {
        field: WireNamespaceSortField {
            id: sort.field.id.clone(),
        },
        direction: wire_namespace_sort_direction(&sort.direction),
    }
}

fn wire_namespace_sort_direction(direction: &NamespaceSortDirection) -> WireNamespaceSortDirection {
    match direction {
        NamespaceSortDirection::Asc => WireNamespaceSortDirection::Asc,
        NamespaceSortDirection::Desc => WireNamespaceSortDirection::Desc,
    }
}

fn wire_namespace_read_options(
    options: &NamespaceReadOptions,
) -> Result<WireNamespaceReadOptions, ClientError> {
    Ok(WireNamespaceReadOptions {
        format: match options.format {
            NamespaceReadFormat::Structured => WireNamespaceReadFormat::Structured,
            NamespaceReadFormat::Bytes => WireNamespaceReadFormat::Bytes,
        },
        cursor: options.cursor.clone(),
        offset: options.offset,
        limit: u64::try_from(options.limit)
            .map_err(|_| ClientError::Protocol("namespace read limit exceeds u64".to_owned()))?,
        expected_generation: options.expected_generation,
    })
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
