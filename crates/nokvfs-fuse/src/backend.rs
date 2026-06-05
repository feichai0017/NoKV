use std::fmt;

use nokvfs_client::{ClientError, ClientPreparedArtifact, MetadataClient};
use nokvfs_meta::{
    DentryWithAttr, MetadError, PublishArtifactRange, PublishArtifactStagedSession,
    ReadDirPlusPage, RenameReplaceResult, UpdateAttr, XattrSetMode,
};
use nokvfs_object::{
    delete_staged_objects, put_chunked_ranges_with_block_index_base, read_object_blocks,
    ChunkWriteOptions, ChunkWriteRange, ChunkedWrite, MemoryBlockCache, ObjectError,
    ObjectReadBlock, ObjectStore, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE,
};
use nokvfs_types::{
    BlockDescriptor, BodyDescriptor, ChunkManifest, DentryName, InodeAttr, InodeId, WatchCursor,
    WatchRecord,
};

pub(crate) type FuseBackendResult<T> = Result<T, FuseBackendError>;

#[derive(Debug)]
pub(crate) enum FuseBackendError {
    Metadata(MetadError),
    Client(ClientError),
    Object(ObjectError),
    Unsupported(&'static str),
}

pub(crate) trait FuseBackend: Send + Sync + 'static {
    type Prepared: Clone + Send + Sync + 'static;

    fn prepared_generation(&self, prepared: &Self::Prepared) -> u64;
    fn watch_subtree(&self, scope: InodeId) -> FuseBackendResult<Option<WatchCursor>>;
    fn replay_watch(
        &self,
        scope: InodeId,
        cursor: WatchCursor,
        limit: usize,
    ) -> FuseBackendResult<Vec<WatchRecord>>;
    fn get_attr(&self, inode: InodeId) -> FuseBackendResult<Option<InodeAttr>>;
    fn get_attr_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> FuseBackendResult<Option<InodeAttr>>;
    fn lookup_plus(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<Option<DentryWithAttr>>;
    fn lookup_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<Option<DentryWithAttr>>;
    fn read_dir_plus_page(
        &self,
        inode: InodeId,
        after: Option<&DentryName>,
        limit: usize,
    ) -> FuseBackendResult<ReadDirPlusPage>;
    fn read_dir_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> FuseBackendResult<Vec<DentryWithAttr>>;
    fn rename(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> FuseBackendResult<DentryWithAttr>;
    fn rename_replace(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> FuseBackendResult<RenameReplaceResult>;
    fn read_file(&self, inode: InodeId, offset: u64, len: usize) -> FuseBackendResult<Vec<u8>>;
    fn read_file_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> FuseBackendResult<Vec<u8>>;
    fn read_symlink(&self, inode: InodeId) -> FuseBackendResult<Vec<u8>>;
    fn read_symlink_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> FuseBackendResult<Vec<u8>>;
    fn update_root_attrs(&self, changes: UpdateAttr) -> FuseBackendResult<InodeAttr>;
    fn update_attrs(
        &self,
        parent: InodeId,
        name: &DentryName,
        changes: UpdateAttr,
    ) -> FuseBackendResult<DentryWithAttr>;
    fn set_xattr(
        &self,
        inode: InodeId,
        name: &[u8],
        value: Vec<u8>,
        mode: XattrSetMode,
    ) -> FuseBackendResult<()>;
    fn get_xattr(&self, inode: InodeId, name: &[u8]) -> FuseBackendResult<Option<Vec<u8>>>;
    fn list_xattr(&self, inode: InodeId) -> FuseBackendResult<Vec<Vec<u8>>>;
    fn remove_xattr(&self, inode: InodeId, name: &[u8]) -> FuseBackendResult<()>;
    fn create_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<DentryWithAttr>;
    fn create_file(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<DentryWithAttr>;
    fn create_symlink(
        &self,
        parent: InodeId,
        name: DentryName,
        target: Vec<u8>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<DentryWithAttr>;
    fn remove_file(&self, parent: InodeId, name: &DentryName) -> FuseBackendResult<DentryWithAttr>;
    fn remove_empty_dir(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<DentryWithAttr>;
    fn prepare_artifact_replace(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> FuseBackendResult<Self::Prepared>;
    fn stage_prepared_artifact_ranges(
        &self,
        prepared: &Self::Prepared,
        manifest_id: &str,
        ranges: &[PublishArtifactRange],
        block_index_base: u64,
    ) -> FuseBackendResult<ChunkedWrite>;
    fn cleanup_staged_objects(
        &self,
        staged: &nokvfs_object::StagedObjectSet,
    ) -> FuseBackendResult<()>;
    fn read_session_object_blocks(
        &self,
        output_len: usize,
        blocks: &[ObjectReadBlock],
    ) -> FuseBackendResult<Vec<u8>>;
    fn publish_prepared_artifact_staged_session(
        &self,
        prepared: Self::Prepared,
        request: PublishArtifactStagedSession,
    ) -> FuseBackendResult<RenameReplaceResult>;
}

pub(crate) struct ClientFuseBackend<O> {
    metadata: MetadataClient,
    objects: O,
    block_cache: MemoryBlockCache,
}

impl<O> ClientFuseBackend<O> {
    pub(crate) fn new(metadata: MetadataClient, objects: O) -> Self {
        Self {
            metadata,
            objects,
            block_cache: MemoryBlockCache::default(),
        }
    }
}

impl From<MetadError> for FuseBackendError {
    fn from(err: MetadError) -> Self {
        Self::Metadata(err)
    }
}

impl From<ClientError> for FuseBackendError {
    fn from(err: ClientError) -> Self {
        Self::Client(err)
    }
}

impl From<ObjectError> for FuseBackendError {
    fn from(err: ObjectError) -> Self {
        Self::Object(err)
    }
}

impl fmt::Display for FuseBackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metadata(err) => write!(f, "{err}"),
            Self::Client(err) => write!(f, "{err}"),
            Self::Object(err) => write!(f, "{err}"),
            Self::Unsupported(feature) => write!(f, "{feature} is not supported"),
        }
    }
}

impl std::error::Error for FuseBackendError {}

impl<O> FuseBackend for ClientFuseBackend<O>
where
    O: ObjectStore + Send + Sync + 'static,
{
    type Prepared = ClientPreparedArtifact;

    fn prepared_generation(&self, prepared: &Self::Prepared) -> u64 {
        prepared.generation
    }

    fn watch_subtree(&self, _scope: InodeId) -> FuseBackendResult<Option<WatchCursor>> {
        Ok(None)
    }

    fn replay_watch(
        &self,
        _scope: InodeId,
        _cursor: WatchCursor,
        _limit: usize,
    ) -> FuseBackendResult<Vec<WatchRecord>> {
        Ok(Vec::new())
    }

    fn get_attr(&self, inode: InodeId) -> FuseBackendResult<Option<InodeAttr>> {
        self.metadata.get_attr(inode).map_err(Into::into)
    }

    fn get_attr_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> FuseBackendResult<Option<InodeAttr>> {
        self.metadata
            .get_attr_at_snapshot(snapshot_id, inode)
            .map_err(Into::into)
    }

    fn lookup_plus(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<Option<DentryWithAttr>> {
        self.metadata
            .lookup_plus(parent, name.clone())
            .map_err(Into::into)
    }

    fn lookup_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<Option<DentryWithAttr>> {
        self.metadata
            .lookup_plus_at_snapshot(snapshot_id, parent, name.clone())
            .map_err(Into::into)
    }

    fn read_dir_plus_page(
        &self,
        inode: InodeId,
        after: Option<&DentryName>,
        limit: usize,
    ) -> FuseBackendResult<ReadDirPlusPage> {
        let page = self
            .metadata
            .read_dir_plus_page(inode, after, limit)
            .map_err(FuseBackendError::from)?;
        Ok(ReadDirPlusPage {
            entries: page.entries,
            next_cursor: page.next_cursor,
        })
    }

    fn read_dir_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> FuseBackendResult<Vec<DentryWithAttr>> {
        self.metadata
            .read_dir_plus_at_snapshot(snapshot_id, inode)
            .map_err(Into::into)
    }

    fn rename(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .rename_in_dir(parent, name.clone(), new_parent, new_name)
            .map_err(Into::into)
    }

    fn rename_replace(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> FuseBackendResult<RenameReplaceResult> {
        self.metadata
            .rename_replace_in_dir(parent, name.clone(), new_parent, new_name)
            .map_err(Into::into)
    }

    fn read_file(&self, inode: InodeId, offset: u64, len: usize) -> FuseBackendResult<Vec<u8>> {
        let Some(attr) = self
            .metadata
            .get_attr(inode)
            .map_err(FuseBackendError::from)?
        else {
            return Err(FuseBackendError::Metadata(MetadError::NotFound));
        };
        let plan = self
            .metadata
            .read_body_plan(inode, attr.generation, offset, len)
            .map_err(FuseBackendError::from)?;
        let read = read_object_blocks(
            &self.objects,
            Some(&self.block_cache),
            plan.output_len,
            &plan.blocks,
        )?;
        Ok(read.bytes)
    }

    fn read_file_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> FuseBackendResult<Vec<u8>> {
        self.metadata
            .read_file_at_snapshot(snapshot_id, inode, offset, len)
            .map_err(Into::into)
    }

    fn read_symlink(&self, inode: InodeId) -> FuseBackendResult<Vec<u8>> {
        self.metadata.read_symlink(inode).map_err(Into::into)
    }

    fn read_symlink_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> FuseBackendResult<Vec<u8>> {
        self.metadata
            .read_symlink_at_snapshot(snapshot_id, inode)
            .map_err(Into::into)
    }

    fn update_root_attrs(&self, changes: UpdateAttr) -> FuseBackendResult<InodeAttr> {
        self.metadata.update_root_attrs(changes).map_err(Into::into)
    }

    fn update_attrs(
        &self,
        parent: InodeId,
        name: &DentryName,
        changes: UpdateAttr,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .update_attrs(parent, name.clone(), changes)
            .map_err(Into::into)
    }

    fn set_xattr(
        &self,
        _inode: InodeId,
        _name: &[u8],
        _value: Vec<u8>,
        _mode: XattrSetMode,
    ) -> FuseBackendResult<()> {
        Err(FuseBackendError::Unsupported("remote FUSE xattr"))
    }

    fn get_xattr(&self, _inode: InodeId, _name: &[u8]) -> FuseBackendResult<Option<Vec<u8>>> {
        Err(FuseBackendError::Unsupported("remote FUSE xattr"))
    }

    fn list_xattr(&self, _inode: InodeId) -> FuseBackendResult<Vec<Vec<u8>>> {
        Err(FuseBackendError::Unsupported("remote FUSE xattr"))
    }

    fn remove_xattr(&self, _inode: InodeId, _name: &[u8]) -> FuseBackendResult<()> {
        Err(FuseBackendError::Unsupported("remote FUSE xattr"))
    }

    fn create_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .create_dir(parent, name, mode, uid, gid)
            .map_err(Into::into)
    }

    fn create_file(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .create_file_in_dir(parent, name, mode, uid, gid)
            .map_err(Into::into)
    }

    fn create_symlink(
        &self,
        parent: InodeId,
        name: DentryName,
        target: Vec<u8>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .create_symlink(parent, name, target, mode, uid, gid)
            .map_err(Into::into)
    }

    fn remove_file(&self, parent: InodeId, name: &DentryName) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .remove_file(parent, name.clone())
            .map_err(Into::into)
    }

    fn remove_empty_dir(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> FuseBackendResult<DentryWithAttr> {
        self.metadata
            .remove_empty_dir(parent, name.clone())
            .map_err(Into::into)
    }

    fn prepare_artifact_replace(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> FuseBackendResult<Self::Prepared> {
        self.metadata
            .prepare_artifact(parent, name, true)
            .map_err(Into::into)
    }

    fn stage_prepared_artifact_ranges(
        &self,
        prepared: &Self::Prepared,
        manifest_id: &str,
        ranges: &[PublishArtifactRange],
        block_index_base: u64,
    ) -> FuseBackendResult<ChunkedWrite> {
        let dirty_ranges = ranges
            .iter()
            .filter(|range| !range.bytes.is_empty())
            .map(|range| ChunkWriteRange {
                logical_offset: range.offset,
                bytes: range.bytes.clone(),
            })
            .collect::<Vec<_>>();
        put_chunked_ranges_with_block_index_base(
            &self.objects,
            &dirty_ranges,
            ChunkWriteOptions {
                manifest_id: manifest_id.to_owned(),
                mount: prepared.mount,
                inode: prepared.inode.get(),
                generation: prepared.generation,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
            block_index_base,
        )
        .map_err(Into::into)
    }

    fn cleanup_staged_objects(
        &self,
        staged: &nokvfs_object::StagedObjectSet,
    ) -> FuseBackendResult<()> {
        delete_staged_objects(&self.objects, staged)
            .map(|_| ())
            .map_err(Into::into)
    }

    fn read_session_object_blocks(
        &self,
        output_len: usize,
        blocks: &[ObjectReadBlock],
    ) -> FuseBackendResult<Vec<u8>> {
        let read = read_object_blocks(&self.objects, Some(&self.block_cache), output_len, blocks)?;
        Ok(read.bytes)
    }

    fn publish_prepared_artifact_staged_session(
        &self,
        prepared: Self::Prepared,
        request: PublishArtifactStagedSession,
    ) -> FuseBackendResult<RenameReplaceResult> {
        if prepared.parent != request.parent || prepared.name != request.name {
            let _ = delete_staged_objects(&self.objects, &request.staged);
            return Err(FuseBackendError::Metadata(
                MetadError::InvalidPreparedArtifact(
                    "prepared artifact target does not match staged publish session".to_owned(),
                ),
            ));
        }
        let body = BodyDescriptor {
            producer: request.producer,
            digest_uri: request.digest_uri,
            size: request.size,
            content_type: request.content_type,
            manifest_id: request.manifest_id,
            generation: prepared.generation,
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size: DEFAULT_BLOCK_SIZE as u64,
        };
        self.metadata
            .publish_prepared_artifact(
                prepared,
                body,
                request.chunks.into_iter().map(chunk_manifest).collect(),
                request.mode,
                request.uid,
                request.gid,
            )
            .map_err(|err| {
                let _ = delete_staged_objects(&self.objects, &request.staged);
                FuseBackendError::from(err)
            })
    }
}

fn chunk_manifest(chunk: nokvfs_object::StoredChunk) -> ChunkManifest {
    ChunkManifest {
        chunk_index: chunk.chunk_index,
        logical_offset: chunk.logical_offset,
        len: chunk.len,
        blocks: chunk
            .blocks
            .into_iter()
            .map(|block| BlockDescriptor {
                object_key: block.object_key,
                logical_offset: block.logical_offset,
                object_offset: block.object_offset,
                len: block.len,
                digest_uri: block.digest_uri,
            })
            .collect(),
    }
}
