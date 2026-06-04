use super::*;

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
        let attr = InodeAttr {
            inode,
            file_type: FileType::File,
            mode,
            uid,
            gid,
            size: 0,
            generation: version.get(),
            mtime_ms: version.get(),
            ctime_ms: version.get(),
        };
        let projection = projection(parent, name, attr, None);
        self.commit_create_projection(CommandKind::CreateFile, &projection, version)?;
        Ok(projection.into())
    }

    pub fn create_dir_path(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        let (parent, name) = self.resolve_parent_path(path)?;
        self.create_dir(parent, name, mode, uid, gid)
    }

    pub fn create_file_path(
        &self,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, MetadError> {
        let (parent, name) = self.resolve_parent_path(path)?;
        self.create_file(parent, name, mode, uid, gid)
    }

    pub fn create_files_in_dir_path(
        &self,
        parent_path: &str,
        names: Vec<DentryName>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let parent = self.resolve_directory_path(parent_path)?;
        self.create_files_in_dir(parent, names, mode, uid, gid)
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
        ensure_unique_names(&names)?;
        let version = self.next_version()?;
        let inodes = self.next_inodes(names.len())?;
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
                    size: 0,
                    generation: version.get(),
                    mtime_ms: version.get(),
                    ctime_ms: version.get(),
                };
                projection(parent, name, attr, None)
            })
            .collect::<Vec<_>>();
        self.commit_create_projections(CommandKind::CreateFiles, &projections, version)?;
        Ok(projections.into_iter().map(Into::into).collect())
    }

    pub fn remove_file(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        let (entry, dentry_version) = self
            .lookup_plus_versioned(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let version = self.next_version()?;
        let key = dentry_key(self.mount, parent, name);
        let mut mutations = vec![
            delete_mutation(RecordFamily::Dentry, key.clone()),
            delete_mutation(RecordFamily::Inode, inode_key(self.mount, entry.attr.inode)),
        ];
        if let Some(body) = &entry.body {
            mutations.extend(self.chunk_manifest_delete_and_gc_mutations(
                entry.attr.inode,
                body.generation,
                version,
                &HashSet::new(),
            )?);
        }
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"remove-file", self.mount, entry.attr.inode, version),
            kind: CommandKind::RemoveFile,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key,
                    predicate: Predicate::VersionEquals(dentry_version),
                },
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, entry.attr.inode),
                    predicate: Predicate::Exists,
                },
            ],
            mutations,
            watch: vec![self.watch_projection(
                parent,
                WatchEvent {
                    kind: WatchEventKind::Remove,
                    parent: Some(parent),
                    name: Some(name.clone()),
                    inode: entry.attr.inode,
                    version: version.get(),
                },
            )],
        })?;
        Ok(entry)
    }

    pub fn remove_file_path(&self, path: &str) -> Result<DentryWithAttr, MetadError> {
        let (parent, name) = self.resolve_parent_path(path)?;
        self.remove_file(parent, &name)
    }

    pub fn remove_empty_dir(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        let (entry, dentry_version) = self
            .lookup_plus_versioned(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        if entry.attr.inode == InodeId::root() {
            return Err(MetadError::CannotRemoveRoot);
        }
        let version = self.next_version()?;
        let source_key = dentry_key(self.mount, parent, name);
        let child_prefix = dentry_prefix(self.mount, entry.attr.inode);
        match self.commit_metadata(MetadataCommand {
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
            mutations: vec![
                delete_mutation(RecordFamily::Dentry, source_key),
                delete_mutation(RecordFamily::Inode, inode_key(self.mount, entry.attr.inode)),
            ],
            watch: vec![self.watch_projection(
                parent,
                WatchEvent {
                    kind: WatchEventKind::Remove,
                    parent: Some(parent),
                    name: Some(name.clone()),
                    inode: entry.attr.inode,
                    version: version.get(),
                },
            )],
        }) {
            Ok(_) => Ok(entry),
            Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                Err(MetadError::DirectoryNotEmpty)
            }
            Err(err) => Err(err),
        }
    }

    pub fn remove_empty_dir_path(&self, path: &str) -> Result<DentryWithAttr, MetadError> {
        let (parent, name) = self.resolve_parent_path(path)?;
        self.remove_empty_dir(parent, &name)
    }

    pub fn rename(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<DentryWithAttr, MetadError> {
        self.rename_inner(parent, name, new_parent, new_name, false)
            .map(|outcome| outcome.entry)
    }

    pub fn rename_path(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<DentryWithAttr, MetadError> {
        let (parent, name) = self.resolve_parent_path(source)?;
        let (new_parent, new_name) = self.resolve_parent_path(destination)?;
        self.rename(parent, &name, new_parent, new_name)
    }

    pub fn rename_replace(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
    ) -> Result<RenameReplaceResult, MetadError> {
        self.rename_inner(parent, name, new_parent, new_name, true)
    }

    pub fn rename_replace_path(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<RenameReplaceResult, MetadError> {
        let (parent, name) = self.resolve_parent_path(source)?;
        let (new_parent, new_name) = self.resolve_parent_path(destination)?;
        self.rename_replace(parent, &name, new_parent, new_name)
    }

    pub(super) fn rename_inner(
        &self,
        parent: InodeId,
        name: &DentryName,
        new_parent: InodeId,
        new_name: DentryName,
        replace: bool,
    ) -> Result<RenameReplaceResult, MetadError> {
        let (source, source_version) = self
            .lookup_plus_versioned(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if parent == new_parent && *name == new_name {
            return Ok(RenameReplaceResult {
                entry: source,
                replaced: None,
            });
        }
        let destination = self.lookup_plus_versioned(new_parent, &new_name)?;
        if !replace && destination.is_some() {
            return Err(MetadataError::PredicateFailed.into());
        }
        if replace {
            if source.attr.file_type != FileType::File {
                return Err(MetadError::NotFile);
            }
            if let Some((entry, _)) = &destination {
                if entry.attr.file_type != FileType::File {
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
        if let Some(replaced) = &replaced {
            mutations.push(delete_mutation(
                RecordFamily::Inode,
                inode_key(self.mount, replaced.attr.inode),
            ));
            if let Some(body) = &replaced.body {
                mutations.extend(self.chunk_manifest_delete_and_gc_mutations(
                    replaced.attr.inode,
                    body.generation,
                    version,
                    &HashSet::new(),
                )?);
            }
        }
        let mut watch = Vec::new();
        if let Some(replaced) = &replaced {
            watch.push(self.watch_projection(
                new_parent,
                WatchEvent {
                    kind: WatchEventKind::Remove,
                    parent: Some(new_parent),
                    name: Some(projection.dentry.name.clone()),
                    inode: replaced.attr.inode,
                    version: version.get(),
                },
            ));
        }
        watch.push(self.watch_projection(
            parent,
            WatchEvent {
                kind: WatchEventKind::Remove,
                parent: Some(parent),
                name: Some(name.clone()),
                inode: source.attr.inode,
                version: version.get(),
            },
        ));
        watch.push(self.watch_projection(
            new_parent,
            WatchEvent {
                kind: WatchEventKind::Rename,
                parent: Some(new_parent),
                name: Some(projection.dentry.name.clone()),
                inode: source.attr.inode,
                version: version.get(),
            },
        ));

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

    pub(super) fn commit_create_projection(
        &self,
        kind: CommandKind,
        projection: &DentryProjection,
        version: Version,
    ) -> Result<(), MetadError> {
        self.commit_create_projection_with_chunks(kind, projection, &[], version)
    }

    pub(super) fn commit_create_projections(
        &self,
        kind: CommandKind,
        projections: &[DentryProjection],
        version: Version,
    ) -> Result<(), MetadError> {
        let Some(first) = projections.first() else {
            return Ok(());
        };
        let parent = first.dentry.parent;
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::Inode,
            key: inode_key(self.mount, parent),
            predicate: Predicate::Exists,
        }];
        let mut mutations = Vec::with_capacity(projections.len() * 2);
        let mut watch = Vec::with_capacity(projections.len());
        for projection in projections {
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
            mutations.push(Mutation {
                family: RecordFamily::Dentry,
                key: dentry,
                op: MutationOp::Put,
                value: Some(Value(encode_dentry_projection(projection))),
            });
            watch.push(self.watch_projection(
                projection.dentry.parent,
                WatchEvent {
                    kind: create_watch_kind(kind),
                    parent: Some(projection.dentry.parent),
                    name: Some(projection.dentry.name.clone()),
                    inode,
                    version: version.get(),
                },
            ));
        }
        self.commit_metadata(MetadataCommand {
            request_id: request_id(kind_name(kind), self.mount, parent, version),
            kind,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry_prefix(self.mount, parent),
            predicates,
            mutations,
            watch,
        })?;
        Ok(())
    }

    pub(super) fn commit_create_projection_with_chunks(
        &self,
        kind: CommandKind,
        projection: &DentryProjection,
        chunks: &[ChunkManifest],
        version: Version,
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
            Mutation {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                op: MutationOp::Put,
                value: Some(Value(encode_dentry_projection(projection))),
            },
        ];
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
            watch: vec![self.watch_projection(
                projection.dentry.parent,
                WatchEvent {
                    kind: create_watch_kind(kind),
                    parent: Some(projection.dentry.parent),
                    name: Some(projection.dentry.name.clone()),
                    inode,
                    version: version.get(),
                },
            )],
        })?;
        Ok(())
    }

    pub(super) fn commit_replace_projection_with_chunks(
        &self,
        projection: &DentryProjection,
        chunks: &[ChunkManifest],
        dentry_version: Version,
        old_generation: Option<u64>,
        version: Version,
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
            Mutation {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                op: MutationOp::Put,
                value: Some(Value(encode_dentry_projection(projection))),
            },
        ];
        if let Some(body) = &projection.body {
            if let Some(old_generation) = old_generation {
                let retained_object_keys = chunk_object_keys(chunks);
                mutations.extend(self.chunk_manifest_delete_and_gc_mutations(
                    inode,
                    old_generation,
                    version,
                    &retained_object_keys,
                )?);
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
            request_id: request_id(b"replace-artifact", self.mount, inode, version),
            kind: CommandKind::ReplaceArtifact,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::Dentry,
                    key: dentry,
                    predicate: Predicate::VersionEquals(dentry_version),
                },
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, inode),
                    predicate: Predicate::Exists,
                },
            ],
            mutations,
            watch: vec![self.watch_projection(
                projection.dentry.parent,
                WatchEvent {
                    kind: WatchEventKind::PublishArtifact,
                    parent: Some(projection.dentry.parent),
                    name: Some(projection.dentry.name.clone()),
                    inode,
                    version: version.get(),
                },
            )],
        })?;
        Ok(())
    }
}
