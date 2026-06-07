use super::*;

struct PreparedCreateBatch {
    entries: Vec<DentryWithAttr>,
    command: MetadataCommand,
}

struct PreparedRemoveFile {
    entry: DentryWithAttr,
    command: MetadataCommand,
}

struct PreparedRemoveEmptyDir {
    entry: DentryWithAttr,
    command: MetadataCommand,
}

#[derive(Clone, Debug)]
struct LinkedDentryProjection {
    key: Vec<u8>,
    projection: DentryProjection,
    version: Version,
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn bootstrap_root(&self, mode: u32, uid: u32, gid: u32) -> Result<InodeAttr, MetadError> {
        let version = self.next_version()?;
        let root = directory_attr(InodeId::root(), mode, uid, gid, version.get());
        let command = MetadataCommand {
            request_id: request_id(b"bootstrap-root", self.mount, InodeId::root(), version),
            kind: CommandKind::CreateDir,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Inode,
            primary_key: inode_key(self.mount, InodeId::root()),
            predicates: vec![PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, InodeId::root()),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, InodeId::root()),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&root))),
            }],
            watch: Vec::new(),
        };
        match self.commit_metadata(command) {
            Ok(_) | Err(MetadError::Metadata(MetadataError::PredicateFailed)) => Ok(root),
            Err(err) => Err(err),
        }
    }

    pub fn create_dir(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let attr = directory_attr(inode, mode, uid, gid, version.get());
        let projection = projection(parent, name, attr, None);
        self.commit_create_projection(CommandKind::CreateDir, &projection, version)?;
        Ok(projection.into())
    }

    pub fn create_file(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let now_ms = current_time_ms();
        let attr = InodeAttr {
            inode,
            file_type: FileType::File,
            mode,
            uid,
            gid,
            rdev: 0,
            nlink: FileType::File.initial_link_count(),
            size: 0,
            generation: version.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
        };
        let projection = projection(parent, name, attr, None);
        self.commit_create_projection(CommandKind::CreateFile, &projection, version)?;
        Ok(projection.into())
    }

    pub fn create_file_prepared(
        &self,
        parent: InodeId,
        name: DentryName,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<CreatedPreparedArtifact, MetadError> {
        let entry = self.create_file(parent, name.clone(), mode, uid, gid)?;
        let prepared = self.prepare_artifact_replace(parent, name)?;
        Ok(CreatedPreparedArtifact { entry, prepared })
    }

    pub fn create_symlink(
        &self,
        parent: InodeId,
        name: DentryName,
        target: Vec<u8>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        if target.is_empty() || target.contains(&0) {
            return Err(MetadError::InvalidPath(
                "symlink target must be non-empty and must not contain NUL".to_owned(),
            ));
        }
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let digest_uri = body_digest_uri(&target);
        let request = PublishArtifact {
            parent,
            name: name.clone(),
            producer: "nokv-symlink".to_owned(),
            digest_uri,
            content_type: "text/plain; charset=utf-8".to_owned(),
            manifest_id: format!("symlink/{}/{}", parent.get(), inode.get()),
            bytes: target,
            mode,
            uid,
            gid,
        };
        let StagedArtifactBody {
            body,
            chunks,
            old_chunks: _,
            staged,
        } = self.stage_artifact_body(&request, inode, version)?;
        let now_ms = current_time_ms();
        let attr = InodeAttr {
            inode,
            file_type: FileType::Symlink,
            mode,
            uid,
            gid,
            rdev: 0,
            nlink: FileType::Symlink.initial_link_count(),
            size: body.size,
            generation: version.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
        };
        let projection = projection(parent, name, attr, Some(body));
        if let Err(err) = self.commit_create_projection_with_chunks(
            CommandKind::CreateSymlink,
            &projection,
            &chunks,
            version,
        ) {
            return Err(MetadError::PublishArtifactFailed {
                source: Box::new(err),
                staged,
            });
        }
        Ok(projection.into())
    }

    pub fn create_special_node(
        &self,
        parent: InodeId,
        name: DentryName,
        spec: SpecialNodeSpec,
    ) -> Result<DentryWithAttr, MetadError> {
        if !spec.file_type.is_special_node() {
            return Err(MetadError::InvalidPath(format!(
                "file type {:?} is not a special node",
                spec.file_type
            )));
        }
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let now_ms = current_time_ms();
        let attr = InodeAttr {
            inode,
            file_type: spec.file_type,
            mode: spec.mode,
            uid: spec.uid,
            gid: spec.gid,
            rdev: spec.rdev,
            nlink: spec.file_type.initial_link_count(),
            size: 0,
            generation: version.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
        };
        let projection = projection(parent, name, attr, None);
        self.commit_create_projection(CommandKind::CreateSpecialNode, &projection, version)?;
        Ok(projection.into())
    }

    pub fn link(
        &self,
        inode: InodeId,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        let version = self.next_version()?;
        let read_version = predecessor(version)?;
        let source_inode_key = inode_key(self.mount, inode);
        let Some(inode_item) = self.metadata.get_versioned(
            RecordFamily::Inode,
            &source_inode_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Err(MetadError::NotFound);
        };
        let mut attr = decode_inode_attr(&inode_item.value.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?;
        if attr.file_type == FileType::Directory {
            return Err(MetadError::NotFile);
        }
        let Some(parent_attr) = self.get_attr_at_version_for_purpose(
            new_parent,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Err(MetadError::NotFound);
        };
        if parent_attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        let linked = self.linked_dentry_projections_for_inode(inode, read_version)?;
        let Some(first_link) = linked.first() else {
            return Err(MetadError::NotFound);
        };
        attr.nlink = attr
            .nlink
            .checked_add(1)
            .ok_or_else(|| MetadError::InvalidPath("inode link count overflow".to_owned()))?;
        attr.generation = version.get();
        attr.ctime_ms = current_time_ms();
        let new_projection = projection(
            new_parent,
            new_name,
            attr.clone(),
            first_link.projection.body.clone(),
        );
        let destination_key = dentry_key(self.mount, new_parent, &new_projection.dentry.name);

        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::Inode,
                key: source_inode_key.clone(),
                predicate: Predicate::VersionEquals(inode_item.version),
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, new_parent),
                predicate: Predicate::Exists,
            },
            PredicateRef {
                family: RecordFamily::Dentry,
                key: destination_key.clone(),
                predicate: Predicate::NotExists,
            },
        ];
        let mut mutations = vec![Mutation {
            family: RecordFamily::Inode,
            key: source_inode_key,
            op: MutationOp::Put,
            value: Some(Value(encode_inode_attr(&attr))),
        }];
        for linked in &linked {
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: linked.key.clone(),
                predicate: Predicate::VersionEquals(linked.version),
            });
            let mut projection = linked.projection.clone();
            projection.attr = attr.clone();
            projection.dentry.attr_generation = attr.generation;
            mutations.push(put_projection_mutation(
                RecordFamily::Dentry,
                linked.key.clone(),
                &projection,
            ));
        }
        mutations.push(put_projection_mutation(
            RecordFamily::Dentry,
            destination_key.clone(),
            &new_projection,
        ));
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"link", self.mount, inode, version),
            kind: CommandKind::Link,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: destination_key,
            predicates,
            mutations,
            watch: self
                .watch_projection(
                    new_parent,
                    WatchEvent {
                        kind: WatchEventKind::Create,
                        parent: Some(new_parent),
                        name: Some(new_projection.dentry.name.clone()),
                        inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        })?;
        Ok(new_projection.into())
    }

    pub fn update_attrs(
        &self,
        parent: InodeId,
        name: &DentryName,
        changes: UpdateAttr,
    ) -> Result<DentryWithAttr, MetadError> {
        let (entry, dentry_version) = self
            .lookup_plus_for_write_plan(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if changes.is_empty() {
            return Ok(entry);
        }
        let version = self.next_version()?;
        let mut attr = entry.attr.clone();
        if let Some(mode) = changes.mode {
            attr.mode = mode;
        }
        if let Some(uid) = changes.uid {
            attr.uid = uid;
        }
        if let Some(gid) = changes.gid {
            attr.gid = gid;
        }
        if let Some(mtime_ms) = changes.mtime_ms {
            attr.mtime_ms = mtime_ms;
        }
        if changes.size.is_some() && changes.mtime_ms.is_none() {
            attr.mtime_ms = current_time_ms();
        }
        attr.ctime_ms = changes.ctime_ms.unwrap_or_else(current_time_ms);
        // `attr.generation` is the file's content generation and the key under
        // which the body summary / chunk manifests are stored (reads resolve the
        // body via `attr.generation`). Only advance it when the body actually
        // changes (a size change re-stages the body below). An attribute-only
        // update (chmod/chown/utimes) must leave it equal to the existing
        // `body.generation`; bumping it would point the dentry at a generation
        // that has no body summary, surfacing as MissingBodyDescriptor on read.
        if changes.size.is_some() {
            attr.generation = version.get();
        }

        let mut body = entry.body.clone();
        let mut chunks = Vec::new();
        let mut old_chunks = Vec::new();
        let mut old_generation = None;
        if let Some(size) = changes.size {
            if attr.file_type == FileType::Directory {
                return Err(MetadError::NotFile);
            }
            let read_version = self.read_version()?;
            old_generation = body.as_ref().map(|body| body.generation);
            let digest_uri =
                self.resized_body_digest_uri(entry.attr.inode, body.as_ref(), size, read_version)?;
            old_chunks = body
                .as_ref()
                .map(|body| {
                    self.chunk_manifests_for_body_at_version(
                        entry.attr.inode,
                        body,
                        read_version,
                        ReadPurpose::WritePlanLocal,
                    )
                })
                .transpose()?
                .unwrap_or_default();
            chunks = merge_session_chunks(size, old_chunks.clone(), Vec::new())?;
            body = Some(BodyDescriptor {
                producer: body
                    .as_ref()
                    .map(|body| body.producer.clone())
                    .unwrap_or_else(|| "nokv-metadata".to_owned()),
                digest_uri,
                size,
                content_type: body
                    .as_ref()
                    .map(|body| body.content_type.clone())
                    .unwrap_or_else(|| "application/octet-stream".to_owned()),
                manifest_id: body
                    .as_ref()
                    .map(|body| body.manifest_id.clone())
                    .unwrap_or_else(|| format!("metadata/{}/{}", parent.get(), attr.inode.get())),
                generation: version.get(),
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE as u64,
            });
            attr.size = size;
        }

        let projection = projection(parent, name.clone(), attr, body);
        self.commit_replace_projection_with_chunks(ReplaceProjectionCommit {
            kind: CommandKind::UpdateAttr,
            projection: &projection,
            chunks: &chunks,
            old_chunks: &old_chunks,
            dentry_version,
            old_generation,
            version,
            path_index: None,
        })?;
        Ok(projection.into())
    }

    pub fn update_root_attrs(&self, changes: UpdateAttr) -> Result<InodeAttr, MetadError> {
        let key = inode_key(self.mount, InodeId::root());
        let Some(item) = self.metadata.get_versioned(
            RecordFamily::Inode,
            &key,
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Err(MetadError::NotFound);
        };
        let mut attr =
            decode_inode_attr(&item.value.0).map_err(|err| MetadError::Codec(err.to_string()))?;
        if changes.is_empty() {
            return Ok(attr);
        }
        if changes.size.is_some() {
            return Err(MetadError::NotFile);
        }
        let version = self.next_version()?;
        if let Some(mode) = changes.mode {
            attr.mode = mode;
        }
        if let Some(uid) = changes.uid {
            attr.uid = uid;
        }
        if let Some(gid) = changes.gid {
            attr.gid = gid;
        }
        if let Some(mtime_ms) = changes.mtime_ms {
            attr.mtime_ms = mtime_ms;
        }
        attr.ctime_ms = changes.ctime_ms.unwrap_or_else(current_time_ms);
        attr.generation = version.get();

        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"update-root-attr", self.mount, InodeId::root(), version),
            kind: CommandKind::UpdateAttr,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Inode,
            primary_key: key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Inode,
                key: key.clone(),
                predicate: Predicate::VersionEquals(item.version),
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Inode,
                key,
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&attr))),
            }],
            watch: Vec::new(),
        })?;
        Ok(attr)
    }

    pub fn create_dir_path(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        let components = parse_absolute_path(path)?;
        let Some((name, parent_components)) = components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let parent = self.resolve_components_as_directory(parent_components)?;
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let attr = directory_attr(inode, mode, uid, gid, version.get());
        let projection = projection(parent, name.clone(), attr, None);
        self.commit_create_projection(CommandKind::CreateDir, &projection, version)?;
        Ok(projection.into())
    }

    pub fn create_file_path(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        let components = parse_absolute_path(path)?;
        let Some((name, parent_components)) = components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let parent = self.resolve_components_as_directory(parent_components)?;
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let now_ms = current_time_ms();
        let attr = InodeAttr {
            inode,
            file_type: FileType::File,
            mode,
            uid,
            gid,
            rdev: 0,
            nlink: FileType::File.initial_link_count(),
            size: 0,
            generation: version.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
        };
        let projection = projection(parent, name.clone(), attr, None);
        self.commit_create_projection(CommandKind::CreateFile, &projection, version)?;
        Ok(projection.into())
    }

    pub fn create_files_in_dir_path(
        &self,
        parent_path: &str,
        names: Vec<DentryName>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let parent_components = parse_absolute_path(parent_path)?;
        let parent = self.resolve_components_as_directory(&parent_components)?;
        self.create_files_in_dir_with_parent_components(parent, names, mode, uid, gid)
    }

    pub fn create_dirs_in_dir_path(
        &self,
        parent_path: &str,
        names: Vec<DentryName>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let parent_components = parse_absolute_path(parent_path)?;
        let parent = self.resolve_components_as_directory(&parent_components)?;
        if names.is_empty() {
            return Ok(Vec::new());
        }
        ensure_unique_names(&names)?;
        let version = self.next_version()?;
        let inodes = self.next_inodes(names.len())?;
        let projections = names
            .into_iter()
            .zip(inodes)
            .map(|(name, inode)| {
                projection(
                    parent,
                    name,
                    directory_attr(inode, mode, uid, gid, version.get()),
                    None,
                )
            })
            .collect::<Vec<_>>();
        self.commit_create_projections_with_path_indexes(
            CommandKind::CreateDir,
            &projections,
            version,
            None,
        )?;
        self.record_create_dirs_batch(projections.len());
        Ok(projections.into_iter().map(Into::into).collect())
    }

    pub fn create_file_batches_in_dir_path(
        &self,
        batches: Vec<CreateInDirPathBatch>,
    ) -> Vec<Result<Vec<DentryWithAttr>, MetadError>> {
        self.create_batches_in_dir_path(CommandKind::CreateFiles, batches)
    }

    pub fn create_dir_batches_in_dir_path(
        &self,
        batches: Vec<CreateInDirPathBatch>,
    ) -> Vec<Result<Vec<DentryWithAttr>, MetadError>> {
        self.create_batches_in_dir_path(CommandKind::CreateDir, batches)
    }

    fn create_batches_in_dir_path(
        &self,
        kind: CommandKind,
        batches: Vec<CreateInDirPathBatch>,
    ) -> Vec<Result<Vec<DentryWithAttr>, MetadError>> {
        let mut results = Vec::with_capacity(batches.len());
        results.resize_with(batches.len(), || None);
        let mut prepared = Vec::new();
        for (index, batch) in batches.into_iter().enumerate() {
            if batch.names.is_empty() {
                results[index] = Some(Ok(Vec::new()));
                continue;
            }
            match self.prepare_create_batch_in_dir_path(kind, batch) {
                Ok(batch) => prepared.push((index, batch)),
                Err(err) => results[index] = Some(Err(err)),
            }
        }

        let commands = prepared
            .iter()
            .map(|(_, batch)| batch.command.clone())
            .collect::<Vec<_>>();
        let committed = self.metadata.commit_independent_batch(&commands);
        for ((index, batch), result) in prepared.into_iter().zip(committed) {
            match result {
                Ok(_) => {
                    self.record_create_batch(kind, batch.entries.len());
                    results[index] = Some(Ok(batch.entries));
                }
                Err(err) => results[index] = Some(Err(err.into())),
            }
        }

        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(
                        MetadataError::Backend("batched create result was not recorded".to_owned())
                            .into(),
                    )
                })
            })
            .collect()
    }

    fn prepare_create_batch_in_dir_path(
        &self,
        kind: CommandKind,
        batch: CreateInDirPathBatch,
    ) -> Result<PreparedCreateBatch, MetadError> {
        ensure_unique_names(&batch.names)?;
        let parent_components = parse_absolute_path(&batch.parent_path)?;
        let parent = self.resolve_components_as_directory(&parent_components)?;
        let version = self.next_version()?;
        let inodes = self.next_inodes(batch.names.len())?;
        let now_ms = current_time_ms();
        let projections = batch
            .names
            .into_iter()
            .zip(inodes)
            .map(|(name, inode)| {
                let attr = match kind {
                    CommandKind::CreateDir => {
                        directory_attr(inode, batch.mode, batch.uid, batch.gid, version.get())
                    }
                    CommandKind::CreateFiles => InodeAttr {
                        inode,
                        file_type: FileType::File,
                        mode: batch.mode,
                        uid: batch.uid,
                        gid: batch.gid,
                        rdev: 0,
                        nlink: FileType::File.initial_link_count(),
                        size: 0,
                        generation: version.get(),
                        mtime_ms: now_ms,
                        ctime_ms: now_ms,
                    },
                    _ => unreachable!("create batch only supports files and directories"),
                };
                projection(parent, name, attr, None)
            })
            .collect::<Vec<_>>();
        let command = self.create_projections_command(kind, &projections, version, None)?;
        Ok(PreparedCreateBatch {
            entries: projections.into_iter().map(Into::into).collect(),
            command,
        })
    }

    fn record_create_batch(&self, kind: CommandKind, entries: usize) {
        match kind {
            CommandKind::CreateDir => self.record_create_dirs_batch(entries),
            CommandKind::CreateFiles => self.record_create_files_batch(entries),
            _ => {}
        }
    }

    pub fn create_files_in_dir(
        &self,
        parent: InodeId,
        names: Vec<DentryName>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        self.create_files_in_dir_with_parent_components(parent, names, mode, uid, gid)
    }

    fn create_files_in_dir_with_parent_components(
        &self,
        parent: InodeId,
        names: Vec<DentryName>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        ensure_unique_names(&names)?;
        let version = self.next_version()?;
        let inodes = self.next_inodes(names.len())?;
        let now_ms = current_time_ms();
        let projections = names
            .into_iter()
            .zip(inodes)
            .map(|(name, inode)| {
                let attr = InodeAttr {
                    inode,
                    file_type: FileType::File,
                    mode,
                    uid,
                    gid,
                    rdev: 0,
                    nlink: FileType::File.initial_link_count(),
                    size: 0,
                    generation: version.get(),
                    mtime_ms: now_ms,
                    ctime_ms: now_ms,
                };
                projection(parent, name, attr, None)
            })
            .collect::<Vec<_>>();
        self.commit_create_projections_with_path_indexes(
            CommandKind::CreateFiles,
            &projections,
            version,
            None,
        )?;
        self.record_create_files_batch(projections.len());
        Ok(projections.into_iter().map(Into::into).collect())
    }

    fn record_create_files_batch(&self, entries: usize) {
        if entries <= 1 {
            return;
        }
        self.create_files_batch_total
            .fetch_add(1, Ordering::Relaxed);
        self.create_files_entry_total
            .fetch_add(entries as u64, Ordering::Relaxed);
    }

    fn record_create_dirs_batch(&self, entries: usize) {
        if entries <= 1 {
            return;
        }
        self.create_dirs_batch_total.fetch_add(1, Ordering::Relaxed);
        self.create_dirs_entry_total
            .fetch_add(entries as u64, Ordering::Relaxed);
    }

    pub fn remove_file(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        self.remove_file_inner(parent, name, None)
    }

    fn remove_file_inner(
        &self,
        parent: InodeId,
        name: &DentryName,
        path_components: Option<&[DentryName]>,
    ) -> Result<DentryWithAttr, MetadError> {
        let version = self.next_version()?;
        let prepared = self.prepare_remove_file(parent, name, path_components, version)?;
        self.commit_metadata(prepared.command)?;
        Ok(prepared.entry)
    }

    fn prepare_remove_file(
        &self,
        parent: InodeId,
        name: &DentryName,
        path_components: Option<&[DentryName]>,
        version: Version,
    ) -> Result<PreparedRemoveFile, MetadError> {
        let (entry, dentry_version) = self
            .lookup_plus_for_write_plan(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type == FileType::Directory {
            return Err(MetadError::NotFile);
        }
        let inode_key = inode_key(self.mount, entry.attr.inode);
        let Some(inode_item) = self.metadata.get_versioned(
            RecordFamily::Inode,
            &inode_key,
            predecessor(version)?,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Err(MetadError::NotFound);
        };
        let mut canonical_attr = decode_inode_attr(&inode_item.value.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?;
        let key = dentry_key(self.mount, parent, name);
        let mut mutations = vec![delete_mutation(RecordFamily::Dentry, key.clone())];
        if let Some(path_index) =
            self.live_path_index_key_for_entry(path_components, parent, name, &entry, version)?
        {
            mutations.push(delete_mutation(RecordFamily::PathIndex, path_index));
        }
        if canonical_attr.nlink == 0 {
            return Err(MetadError::InvalidPath(
                "inode link count is already zero".to_owned(),
            ));
        }
        let final_link = canonical_attr.nlink == 1;
        let linked = if final_link {
            mutations.push(delete_mutation(RecordFamily::Inode, inode_key.clone()));
            Vec::new()
        } else {
            let linked =
                self.linked_dentry_projections_for_inode(entry.attr.inode, predecessor(version)?)?;
            canonical_attr.nlink -= 1;
            canonical_attr.generation = version.get();
            canonical_attr.ctime_ms = current_time_ms();
            mutations.push(Mutation {
                family: RecordFamily::Inode,
                key: inode_key.clone(),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&canonical_attr))),
            });
            for linked in &linked {
                if linked.key == key {
                    continue;
                }
                let mut projection = linked.projection.clone();
                projection.attr = canonical_attr.clone();
                projection.dentry.attr_generation = canonical_attr.generation;
                mutations.push(put_projection_mutation(
                    RecordFamily::Dentry,
                    linked.key.clone(),
                    &projection,
                ));
            }
            linked
        };
        if final_link {
            if let Some(body) = &entry.body {
                mutations.extend(self.chunk_manifest_delete_and_gc_mutations(
                    entry.attr.inode,
                    body.generation,
                    version,
                    &HashSet::new(),
                )?);
            }
        }
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::Dentry,
                key: key.clone(),
                predicate: Predicate::VersionEquals(dentry_version),
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key,
                predicate: Predicate::VersionEquals(inode_item.version),
            },
        ];
        for linked in linked {
            if linked.key == dentry_key(self.mount, parent, name) {
                continue;
            }
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: linked.key,
                predicate: Predicate::VersionEquals(linked.version),
            });
        }
        let command = MetadataCommand {
            request_id: request_id(b"remove-file", self.mount, entry.attr.inode, version),
            kind: CommandKind::RemoveFile,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: key.clone(),
            predicates,
            mutations,
            watch: self
                .watch_projection(
                    parent,
                    WatchEvent {
                        kind: WatchEventKind::Remove,
                        parent: Some(parent),
                        name: Some(name.clone()),
                        inode: entry.attr.inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        };
        Ok(PreparedRemoveFile { entry, command })
    }

    pub fn remove_file_path(&self, path: &str) -> Result<DentryWithAttr, MetadError> {
        let components = parse_absolute_path(path)?;
        let Some((name, parent_components)) = components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let parent = self.resolve_components_as_directory(parent_components)?;
        self.remove_file_inner(parent, name, Some(&components))
    }

    pub fn remove_files_in_dir_path(
        &self,
        parent_path: &str,
        names: Vec<DentryName>,
    ) -> Result<Vec<Result<DentryWithAttr, MetadError>>, MetadError> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        ensure_unique_names(&names)?;
        let parent_components = parse_absolute_path(parent_path)?;
        let parent = self.resolve_components_as_directory(&parent_components)?;
        let mut results = Vec::with_capacity(names.len());
        results.resize_with(names.len(), || None);
        let mut prepared = Vec::new();
        for (index, name) in names.into_iter().enumerate() {
            let version = self.next_version()?;
            let mut path_components = parent_components.clone();
            path_components.push(name.clone());
            match self.prepare_remove_file(parent, &name, Some(&path_components), version) {
                Ok(remove) => prepared.push((index, remove)),
                Err(err) => results[index] = Some(Err(err)),
            }
        }

        let commands = prepared
            .iter()
            .map(|(_, remove)| remove.command.clone())
            .collect::<Vec<_>>();
        let committed = self.metadata.commit_independent_batch(&commands);
        for ((index, remove), result) in prepared.into_iter().zip(committed) {
            results[index] = Some(result.map(|_| remove.entry).map_err(Into::into));
        }

        Ok(results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(
                        MetadataError::Backend("batched remove result was not recorded".to_owned())
                            .into(),
                    )
                })
            })
            .collect())
    }

    pub fn remove_empty_dir(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        self.remove_empty_dir_inner(parent, name, None)
    }

    fn remove_empty_dir_inner(
        &self,
        parent: InodeId,
        name: &DentryName,
        path_components: Option<&[DentryName]>,
    ) -> Result<DentryWithAttr, MetadError> {
        let version = self.next_version()?;
        let prepared = self.prepare_remove_empty_dir(parent, name, path_components, version)?;
        map_remove_empty_dir_commit(self.metadata.commit_metadata(prepared.command))?;
        Ok(prepared.entry)
    }

    fn prepare_remove_empty_dir(
        &self,
        parent: InodeId,
        name: &DentryName,
        path_components: Option<&[DentryName]>,
        version: Version,
    ) -> Result<PreparedRemoveEmptyDir, MetadError> {
        let (entry, dentry_version) = self
            .lookup_plus_for_write_plan(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        if entry.attr.inode == InodeId::root() {
            return Err(MetadError::CannotRemoveRoot);
        }
        let source_key = dentry_key(self.mount, parent, name);
        let child_prefix = dentry_prefix(self.mount, entry.attr.inode);
        let mut mutations = vec![
            delete_mutation(RecordFamily::Dentry, source_key.clone()),
            delete_mutation(RecordFamily::Inode, inode_key(self.mount, entry.attr.inode)),
        ];
        if let Some(path_index) =
            self.live_path_index_key_for_entry(path_components, parent, name, &entry, version)?
        {
            mutations.push(delete_mutation(RecordFamily::PathIndex, path_index));
        }
        let command = MetadataCommand {
            request_id: request_id(b"remove-empty-dir", self.mount, entry.attr.inode, version),
            kind: CommandKind::RemoveEmptyDir,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: source_key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: source_key.clone(),
                    predicate: Predicate::VersionEquals(dentry_version),
                },
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: child_prefix,
                    predicate: Predicate::PrefixEmpty,
                },
            ],
            mutations,
            watch: self
                .watch_projection(
                    parent,
                    WatchEvent {
                        kind: WatchEventKind::Remove,
                        parent: Some(parent),
                        name: Some(name.clone()),
                        inode: entry.attr.inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        };
        Ok(PreparedRemoveEmptyDir { entry, command })
    }

    pub fn remove_empty_dir_path(&self, path: &str) -> Result<DentryWithAttr, MetadError> {
        let components = parse_absolute_path(path)?;
        let Some((name, parent_components)) = components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let parent = self.resolve_components_as_directory(parent_components)?;
        self.remove_empty_dir_inner(parent, name, Some(&components))
    }

    pub fn remove_empty_dirs_in_dir_path(
        &self,
        parent_path: &str,
        names: Vec<DentryName>,
    ) -> Result<Vec<Result<DentryWithAttr, MetadError>>, MetadError> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        ensure_unique_names(&names)?;
        let parent_components = parse_absolute_path(parent_path)?;
        let parent = self.resolve_components_as_directory(&parent_components)?;
        let mut results = Vec::with_capacity(names.len());
        results.resize_with(names.len(), || None);
        let mut prepared = Vec::new();
        for (index, name) in names.into_iter().enumerate() {
            let version = self.next_version()?;
            let mut path_components = parent_components.clone();
            path_components.push(name.clone());
            match self.prepare_remove_empty_dir(parent, &name, Some(&path_components), version) {
                Ok(remove) => prepared.push((index, remove)),
                Err(err) => results[index] = Some(Err(err)),
            }
        }

        let commands = prepared
            .iter()
            .map(|(_, remove)| remove.command.clone())
            .collect::<Vec<_>>();
        let committed = self.metadata.commit_independent_batch(&commands);
        for ((index, remove), result) in prepared.into_iter().zip(committed) {
            results[index] = Some(map_remove_empty_dir_commit(result).map(|_| remove.entry));
        }

        Ok(results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(
                        MetadataError::Backend("batched rmdir result was not recorded".to_owned())
                            .into(),
                    )
                })
            })
            .collect())
    }

    pub fn rename(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        self.rename_inner(parent, name, new_parent, new_name, false, None)
            .map(|outcome| outcome.entry)
    }

    pub fn rename_path(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<DentryWithAttr, MetadError> {
        let source_components = parse_absolute_path(source)?;
        let destination_components = parse_absolute_path(destination)?;
        let Some((name, parent_components)) = source_components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let Some((new_name, new_parent_components)) = destination_components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let parent = self.resolve_components_as_directory(parent_components)?;
        let new_parent = self.resolve_components_as_directory(new_parent_components)?;
        self.rename_inner(
            parent,
            name,
            new_parent,
            new_name.clone(),
            false,
            Some((&source_components, &destination_components)),
        )
        .map(|outcome| outcome.entry)
    }

    pub fn rename_replace(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<RenameReplaceResult, MetadError> {
        self.rename_inner(parent, name, new_parent, new_name, true, None)
    }

    pub fn rename_replace_path(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<RenameReplaceResult, MetadError> {
        let source_components = parse_absolute_path(source)?;
        let destination_components = parse_absolute_path(destination)?;
        let Some((name, parent_components)) = source_components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let Some((new_name, new_parent_components)) = destination_components.split_last() else {
            return Err(MetadError::InvalidPath("root has no parent".to_owned()));
        };
        let parent = self.resolve_components_as_directory(parent_components)?;
        let new_parent = self.resolve_components_as_directory(new_parent_components)?;
        self.rename_inner(
            parent,
            name,
            new_parent,
            new_name.clone(),
            true,
            Some((&source_components, &destination_components)),
        )
    }

    pub(super) fn rename_inner(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
        replace: bool,
        path_index: Option<(&[DentryName], &[DentryName])>,
    ) -> Result<RenameReplaceResult, MetadError> {
        let (source, source_version) = self
            .lookup_plus_for_write_plan(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if parent == new_parent && *name == new_name {
            return Ok(RenameReplaceResult {
                entry: source,
                replaced: None,
            });
        }
        let destination = self.lookup_plus_for_write_plan(new_parent, &new_name)?;
        if !replace && destination.is_some() {
            return Err(MetadataError::PredicateFailed.into());
        }
        if replace {
            if source.attr.file_type == FileType::Directory {
                return Err(MetadError::NotFile);
            }
            if let Some((entry, _)) = &destination {
                if entry.attr.file_type == FileType::Directory {
                    return Err(MetadError::NotFile);
                }
            }
        }

        let version = self.next_version()?;
        let source_key = dentry_key(self.mount, parent, name);
        let destination_key = dentry_key(self.mount, new_parent, &new_name);
        let projection = projection(
            new_parent,
            new_name,
            source.attr.clone(),
            source.body.clone(),
        );
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, new_parent),
                predicate: Predicate::Exists,
            },
            PredicateRef {
                family: RecordFamily::Dentry,
                key: source_key.clone(),
                predicate: Predicate::VersionEquals(source_version),
            },
        ];
        let replaced = if let Some((entry, destination_version)) = destination {
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: destination_key.clone(),
                predicate: Predicate::VersionEquals(destination_version),
            });
            Some(entry)
        } else {
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: destination_key.clone(),
                predicate: Predicate::NotExists,
            });
            None
        };

        let mut mutations = vec![
            delete_mutation(RecordFamily::Dentry, source_key),
            Mutation {
                family: RecordFamily::Dentry,
                key: destination_key.clone(),
                op: MutationOp::Put,
                value: Some(Value(encode_dentry_projection(&projection))),
            },
        ];
        if let Some(source_path) = self.live_path_index_key_for_entry(
            path_index.map(|(source, _)| source),
            parent,
            name,
            &source,
            version,
        )? {
            let destination_path = path_index
                .map(|(_, destination)| path_index_key(self.mount, destination))
                .ok_or_else(|| {
                    MetadataError::Backend(
                        "live source path index requires destination path context".to_owned(),
                    )
                })?;
            mutations.push(delete_mutation(RecordFamily::PathIndex, source_path));
            mutations.push(put_projection_mutation(
                RecordFamily::PathIndex,
                destination_path,
                &projection,
            ));
        }
        if let Some(replaced) = &replaced {
            let replaced_inode_key = inode_key(self.mount, replaced.attr.inode);
            let Some(replaced_inode_item) = self.metadata.get_versioned(
                RecordFamily::Inode,
                &replaced_inode_key,
                predecessor(version)?,
                ReadPurpose::WritePlanLocal,
            )?
            else {
                return Err(MetadError::NotFound);
            };
            let mut replaced_attr = decode_inode_attr(&replaced_inode_item.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            if replaced_attr.nlink == 0 {
                return Err(MetadError::InvalidPath(
                    "replaced inode link count is already zero".to_owned(),
                ));
            }
            predicates.push(PredicateRef {
                family: RecordFamily::Inode,
                key: replaced_inode_key.clone(),
                predicate: Predicate::VersionEquals(replaced_inode_item.version),
            });
            if replaced_attr.nlink == 1 {
                mutations.push(delete_mutation(RecordFamily::Inode, replaced_inode_key));
                if let Some(body) = &replaced.body {
                    mutations.extend(self.chunk_manifest_delete_and_gc_mutations(
                        replaced.attr.inode,
                        body.generation,
                        version,
                        &HashSet::new(),
                    )?);
                }
            } else {
                replaced_attr.nlink -= 1;
                replaced_attr.generation = version.get();
                replaced_attr.ctime_ms = current_time_ms();
                mutations.push(Mutation {
                    family: RecordFamily::Inode,
                    key: replaced_inode_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_inode_attr(&replaced_attr))),
                });
                for linked in self.linked_dentry_projections_for_inode(
                    replaced.attr.inode,
                    predecessor(version)?,
                )? {
                    if linked.key == destination_key {
                        continue;
                    }
                    predicates.push(PredicateRef {
                        family: RecordFamily::Dentry,
                        key: linked.key.clone(),
                        predicate: Predicate::VersionEquals(linked.version),
                    });
                    let mut projection = linked.projection;
                    projection.attr = replaced_attr.clone();
                    projection.dentry.attr_generation = replaced_attr.generation;
                    mutations.push(put_projection_mutation(
                        RecordFamily::Dentry,
                        linked.key,
                        &projection,
                    ));
                }
            }
        }
        let mut watch = Vec::new();
        if let Some(replaced) = &replaced {
            if let Some(event) = self.watch_projection(
                new_parent,
                WatchEvent {
                    kind: WatchEventKind::Remove,
                    parent: Some(new_parent),
                    name: Some(projection.dentry.name.clone()),
                    inode: replaced.attr.inode,
                    version: version.get(),
                },
            ) {
                watch.push(event);
            }
        }
        if let Some(event) = self.watch_projection(
            parent,
            WatchEvent {
                kind: WatchEventKind::Remove,
                parent: Some(parent),
                name: Some(name.clone()),
                inode: source.attr.inode,
                version: version.get(),
            },
        ) {
            watch.push(event);
        }
        if let Some(event) = self.watch_projection(
            new_parent,
            WatchEvent {
                kind: WatchEventKind::Rename,
                parent: Some(new_parent),
                name: Some(projection.dentry.name.clone()),
                inode: source.attr.inode,
                version: version.get(),
            },
        ) {
            watch.push(event);
        }

        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                if replace {
                    b"rename-replace"
                } else {
                    b"rename"
                },
                self.mount,
                source.attr.inode,
                version,
            ),
            kind: if replace {
                CommandKind::RenameReplace
            } else {
                CommandKind::Rename
            },
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: destination_key,
            predicates,
            mutations,
            watch,
        })?;
        Ok(RenameReplaceResult {
            entry: projection.into(),
            replaced,
        })
    }

    fn live_path_index_key_for_entry(
        &self,
        components: Option<&[DentryName]>,
        parent: InodeId,
        name: &DentryName,
        entry: &DentryWithAttr,
        version: Version,
    ) -> Result<Option<Vec<u8>>, MetadError> {
        let Some(components) = components else {
            return Ok(None);
        };
        if entry.body.is_none() {
            return Ok(None);
        }
        let key = path_index_key(self.mount, components);
        let Some(item) = self.metadata.get_versioned(
            RecordFamily::PathIndex,
            &key,
            predecessor(version)?,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(None);
        };
        let indexed = crate::layout::decode_dentry_projection(&item.value.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?;
        let matches_canonical = indexed.attr.inode == entry.attr.inode
            && indexed.dentry.parent == parent
            && indexed.dentry.name == *name;
        Ok(matches_canonical.then_some(key))
    }

    fn linked_dentry_projections_for_inode(
        &self,
        inode: InodeId,
        version: Version,
    ) -> Result<Vec<LinkedDentryProjection>, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: dentry_mount_prefix(self.mount),
            start_after: None,
            version,
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        let mut linked = Vec::new();
        for row in rows {
            let projection = decode_dentry_projection(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            if projection.attr.inode == inode {
                linked.push(LinkedDentryProjection {
                    key: row.key,
                    projection,
                    version: row.version,
                });
            }
        }
        Ok(linked)
    }

    pub(super) fn commit_create_projection(
        &self,
        kind: CommandKind,
        projection: &DentryProjection,
        version: Version,
    ) -> Result<(), MetadError> {
        self.commit_create_projection_with_chunks(kind, projection, &[], version)
    }

    pub(super) fn commit_create_projections_with_path_indexes(
        &self,
        kind: CommandKind,
        projections: &[DentryProjection],
        version: Version,
        path_indexes: Option<&[Vec<u8>]>,
    ) -> Result<(), MetadError> {
        let command = self.create_projections_command(kind, projections, version, path_indexes)?;
        self.commit_metadata(command)?;
        Ok(())
    }

    fn create_projections_command(
        &self,
        kind: CommandKind,
        projections: &[DentryProjection],
        version: Version,
        path_indexes: Option<&[Vec<u8>]>,
    ) -> Result<MetadataCommand, MetadError> {
        let Some(first) = projections.first() else {
            return Err(MetadError::InvalidPath(
                "batched create requires at least one projection".to_owned(),
            ));
        };
        if let Some(path_indexes) = path_indexes {
            if path_indexes.len() != projections.len() {
                return Err(MetadError::InvalidPath(
                    "batched path index count does not match projections".to_owned(),
                ));
            }
        }
        let parent = first.dentry.parent;
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::Inode,
            key: inode_key(self.mount, parent),
            predicate: Predicate::Exists,
        }];
        let mut mutations =
            Vec::with_capacity(projections.len() * if path_indexes.is_some() { 3 } else { 2 });
        let mut watch = Vec::with_capacity(projections.len());
        for (index, projection) in projections.iter().enumerate() {
            if projection.dentry.parent != parent {
                return Err(MetadError::InvalidPath(
                    "batched create requires one parent".to_owned(),
                ));
            }
            let inode = projection.attr.inode;
            let dentry = dentry_key(
                self.mount,
                projection.dentry.parent,
                &projection.dentry.name,
            );
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                predicate: Predicate::NotExists,
            });
            mutations.push(Mutation {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, inode),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&projection.attr))),
            });
            mutations.push(put_projection_mutation(
                RecordFamily::Dentry,
                dentry,
                projection,
            ));
            if let Some(path_indexes) = path_indexes {
                mutations.push(put_projection_mutation(
                    RecordFamily::PathIndex,
                    path_indexes[index].clone(),
                    projection,
                ));
            }
            if let Some(event) = self.watch_projection(
                projection.dentry.parent,
                WatchEvent {
                    kind: create_watch_kind(kind),
                    parent: Some(projection.dentry.parent),
                    name: Some(projection.dentry.name.clone()),
                    inode,
                    version: version.get(),
                },
            ) {
                watch.push(event);
            }
        }
        Ok(MetadataCommand {
            request_id: request_id(kind_name(kind), self.mount, parent, version),
            kind,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry_prefix(self.mount, parent),
            predicates,
            mutations,
            watch,
        })
    }

    pub(super) fn commit_create_projection_with_chunks(
        &self,
        kind: CommandKind,
        projection: &DentryProjection,
        chunks: &[ChunkManifest],
        version: Version,
    ) -> Result<(), MetadError> {
        self.commit_create_projection_with_chunks_and_path_index(
            kind, projection, chunks, version, None,
        )
    }

    pub(super) fn commit_create_projection_with_chunks_and_path_index(
        &self,
        kind: CommandKind,
        projection: &DentryProjection,
        chunks: &[ChunkManifest],
        version: Version,
        path_index: Option<Vec<u8>>,
    ) -> Result<(), MetadError> {
        let inode = projection.attr.inode;
        let dentry = dentry_key(
            self.mount,
            projection.dentry.parent,
            &projection.dentry.name,
        );
        let mut mutations = vec![
            Mutation {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, inode),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&projection.attr))),
            },
            put_projection_mutation(RecordFamily::Dentry, dentry.clone(), projection),
        ];
        if let Some(path_index) = path_index {
            mutations.push(put_projection_mutation(
                RecordFamily::PathIndex,
                path_index,
                projection,
            ));
        }
        if let Some(body) = &projection.body {
            mutations.push(Mutation {
                family: RecordFamily::ChunkManifest,
                key: chunk_manifest_key(
                    self.mount,
                    inode,
                    body.generation,
                    BODY_SUMMARY_CHUNK_INDEX,
                ),
                op: MutationOp::Put,
                value: Some(Value(encode_body_descriptor(body))),
            });
            for chunk in chunks {
                mutations.push(Mutation {
                    family: RecordFamily::ChunkManifest,
                    key: chunk_manifest_key(self.mount, inode, body.generation, chunk.chunk_index),
                    op: MutationOp::Put,
                    value: Some(Value(encode_chunk_manifest(chunk))),
                });
            }
        }
        self.commit_metadata(MetadataCommand {
            request_id: request_id(kind_name(kind), self.mount, inode, version),
            kind,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, projection.dentry.parent),
                    predicate: Predicate::Exists,
                },
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: dentry,
                    predicate: Predicate::NotExists,
                },
            ],
            mutations,
            watch: self
                .watch_projection(
                    projection.dentry.parent,
                    WatchEvent {
                        kind: create_watch_kind(kind),
                        parent: Some(projection.dentry.parent),
                        name: Some(projection.dentry.name.clone()),
                        inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        })?;
        Ok(())
    }

    pub(super) fn commit_replace_projection_with_chunks(
        &self,
        commit: ReplaceProjectionCommit<'_>,
    ) -> Result<(), MetadError> {
        let ReplaceProjectionCommit {
            kind,
            projection,
            chunks,
            old_chunks,
            dentry_version,
            old_generation,
            version,
            path_index,
        } = commit;
        let inode = projection.attr.inode;
        let dentry = dentry_key(
            self.mount,
            projection.dentry.parent,
            &projection.dentry.name,
        );
        let read_version = predecessor(version)?;
        let linked = if projection.attr.nlink <= 1 {
            vec![LinkedDentryProjection {
                key: dentry.clone(),
                projection: projection.clone(),
                version: dentry_version,
            }]
        } else {
            self.linked_dentry_projections_for_inode(inode, read_version)?
        };
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                predicate: Predicate::VersionEquals(dentry_version),
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, inode),
                predicate: Predicate::Exists,
            },
        ];
        let mut mutations = vec![Mutation {
            family: RecordFamily::Inode,
            key: inode_key(self.mount, inode),
            op: MutationOp::Put,
            value: Some(Value(encode_inode_attr(&projection.attr))),
        }];
        let mut primary_projection_updated = false;
        for linked in linked {
            if linked.key != dentry {
                predicates.push(PredicateRef {
                    family: RecordFamily::Dentry,
                    key: linked.key.clone(),
                    predicate: Predicate::VersionEquals(linked.version),
                });
            } else {
                primary_projection_updated = true;
            }
            let mut updated = linked.projection;
            updated.attr = projection.attr.clone();
            updated.dentry.attr_generation = projection.attr.generation;
            updated.body = projection.body.clone();
            mutations.push(put_projection_mutation(
                RecordFamily::Dentry,
                linked.key,
                &updated,
            ));
        }
        if !primary_projection_updated {
            mutations.push(put_projection_mutation(
                RecordFamily::Dentry,
                dentry.clone(),
                projection,
            ));
        }
        if let Some(path_index) = path_index {
            mutations.push(put_projection_mutation(
                RecordFamily::PathIndex,
                path_index,
                projection,
            ));
        }
        if let Some(body) = &projection.body {
            if let Some(old_generation) = old_generation {
                let retained_object_keys = chunk_object_keys(chunks);
                if old_chunks.is_empty() {
                    mutations.extend(self.chunk_manifest_delete_and_gc_mutations(
                        inode,
                        old_generation,
                        version,
                        &retained_object_keys,
                    )?);
                } else {
                    mutations.extend(self.chunk_manifest_delete_and_gc_mutations_from_manifests(
                        inode,
                        old_generation,
                        old_chunks,
                        version,
                        &retained_object_keys,
                    ));
                }
            }
            mutations.push(Mutation {
                family: RecordFamily::ChunkManifest,
                key: chunk_manifest_key(
                    self.mount,
                    inode,
                    body.generation,
                    BODY_SUMMARY_CHUNK_INDEX,
                ),
                op: MutationOp::Put,
                value: Some(Value(encode_body_descriptor(body))),
            });
            for chunk in chunks {
                mutations.push(Mutation {
                    family: RecordFamily::ChunkManifest,
                    key: chunk_manifest_key(self.mount, inode, body.generation, chunk.chunk_index),
                    op: MutationOp::Put,
                    value: Some(Value(encode_chunk_manifest(chunk))),
                });
            }
        }
        self.commit_metadata(MetadataCommand {
            request_id: request_id(kind_name(kind), self.mount, inode, version),
            kind,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry.clone(),
            predicates,
            mutations,
            watch: self
                .watch_projection(
                    projection.dentry.parent,
                    WatchEvent {
                        kind: create_watch_kind(kind),
                        parent: Some(projection.dentry.parent),
                        name: Some(projection.dentry.name.clone()),
                        inode,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        })?;
        Ok(())
    }
}

fn map_remove_empty_dir_commit(
    result: Result<CommitResult, MetadataError>,
) -> Result<(), MetadError> {
    match result {
        Ok(_) => Ok(()),
        Err(MetadataError::PredicateFailed) => Err(MetadError::DirectoryNotEmpty),
        Err(err) => Err(err.into()),
    }
}
