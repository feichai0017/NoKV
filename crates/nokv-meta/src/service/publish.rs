use super::*;

struct PreparedArtifactPublish<'a> {
    prepared: PreparedArtifact,
    body: BodyDescriptor,
    chunks: Vec<ChunkManifest>,
    old_chunks: &'a [ChunkManifest],
    mode: u32,
    uid: u32,
    gid: u32,
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn publish_artifact(&self, request: PublishArtifact) -> Result<DentryWithAttr, MetadError> {
        let version = self.next_version()?;
        let inode = self.next_inode()?;
        let StagedArtifactBody {
            body,
            chunks,
            old_chunks: _,
            staged,
        } = self.stage_artifact_body(&request, inode, version)?;
        let now_ms = current_time_ms();
        let attr = InodeAttr {
            inode,
            file_type: FileType::File,
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
            rdev: 0,
            nlink: FileType::File.initial_link_count(),
            size: body.size,
            generation: version.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
        };
        let projection = projection(request.parent, request.name, attr, Some(body));
        if let Err(err) = self.commit_create_projection_with_chunks(
            CommandKind::PublishArtifact,
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

    pub fn replace_artifact(
        &self,
        request: PublishArtifact,
    ) -> Result<RenameReplaceResult, MetadError> {
        let (existing, dentry_version) = self
            .lookup_plus_for_write_plan(request.parent, &request.name)?
            .ok_or(MetadError::NotFound)?;
        if existing.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let version = self.next_version()?;
        let StagedArtifactBody {
            body,
            chunks,
            old_chunks: _,
            staged,
        } = self.stage_artifact_body(&request, existing.attr.inode, version)?;
        let now_ms = current_time_ms();
        let attr = InodeAttr {
            inode: existing.attr.inode,
            file_type: FileType::File,
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
            rdev: 0,
            nlink: existing.attr.nlink,
            size: body.size,
            generation: version.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
        };
        let projection = projection(request.parent, request.name, attr, Some(body));
        let old_generation = existing.body.as_ref().map(|body| body.generation);
        if let Err(err) = self.commit_replace_projection_with_chunks(ReplaceProjectionCommit {
            kind: CommandKind::ReplaceArtifact,
            projection: &projection,
            chunks: &chunks,
            old_chunks: &[],
            dentry_version,
            old_generation,
            version,
            path_index: None,
        }) {
            return Err(MetadError::PublishArtifactFailed {
                source: Box::new(err),
                staged,
            });
        }
        Ok(RenameReplaceResult {
            entry: projection.into(),
            replaced: Some(existing),
        })
    }

    pub fn prepare_artifact_create(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<PreparedArtifact, MetadError> {
        let Some(parent_attr) = self.get_attr_at_version_for_purpose(
            parent,
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Err(MetadError::NotFound);
        };
        if parent_attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        if self.lookup_plus_for_write_plan(parent, &name)?.is_some() {
            return Err(MetadataError::PredicateFailed.into());
        }
        let generation = self.next_version()?;
        let inode = self.next_inode()?;
        let now_ms = current_time_ms();
        Ok(PreparedArtifact {
            parent,
            name,
            path: None,
            inode,
            generation: generation.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
            replace: false,
            dentry_version: None,
            old_generation: None,
        })
    }

    pub fn prepare_artifact_create_path(&self, path: &str) -> Result<PreparedArtifact, MetadError> {
        let components = parse_absolute_path(path)?;
        let (parent, name) = self.resolve_parent_path(path)?;
        let mut prepared = self.prepare_artifact_create(parent, name)?;
        prepared.path = Some(canonical_path(&components)?);
        Ok(prepared)
    }

    pub fn prepare_artifact_replace(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<PreparedArtifact, MetadError> {
        let (existing, dentry_version) = self
            .lookup_plus_for_write_plan(parent, &name)?
            .ok_or(MetadError::NotFound)?;
        if existing.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let generation = self.next_version()?;
        let now_ms = current_time_ms();
        Ok(PreparedArtifact {
            parent,
            name,
            path: None,
            inode: existing.attr.inode,
            generation: generation.get(),
            mtime_ms: now_ms,
            ctime_ms: now_ms,
            replace: true,
            dentry_version: Some(dentry_version.get()),
            old_generation: existing.body.as_ref().map(|body| body.generation),
        })
    }

    pub fn prepare_artifact_replace_path(
        &self,
        path: &str,
    ) -> Result<PreparedArtifact, MetadError> {
        let (parent, name) = self.resolve_parent_path(path)?;
        let components = parse_absolute_path(path)?;
        let mut prepared = self.prepare_artifact_replace(parent, name)?;
        prepared.path = Some(canonical_path(&components)?);
        Ok(prepared)
    }

    pub fn publish_prepared_artifact(
        &self,
        prepared: PreparedArtifact,
        body: BodyDescriptor,
        chunks: Vec<ChunkManifest>,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<RenameReplaceResult, MetadError> {
        self.publish_prepared_artifact_impl(PreparedArtifactPublish {
            prepared,
            body,
            chunks,
            old_chunks: &[],
            mode,
            uid,
            gid,
        })
    }

    fn publish_prepared_artifact_impl(
        &self,
        request: PreparedArtifactPublish<'_>,
    ) -> Result<RenameReplaceResult, MetadError> {
        let PreparedArtifactPublish {
            prepared,
            body,
            chunks,
            old_chunks,
            mode,
            uid,
            gid,
        } = request;
        validate_prepared_artifact(&prepared, &body, &chunks)?;
        let version = Version::new(prepared.generation)?;
        let mut attr = InodeAttr {
            inode: prepared.inode,
            file_type: FileType::File,
            mode,
            uid,
            gid,
            rdev: 0,
            nlink: FileType::File.initial_link_count(),
            size: body.size,
            generation: prepared.generation,
            mtime_ms: prepared.mtime_ms,
            ctime_ms: prepared.ctime_ms,
        };
        if prepared.replace {
            if let Some((existing, _)) =
                self.lookup_plus_for_write_plan(prepared.parent, &prepared.name)?
            {
                attr.nlink = existing.attr.nlink;
            }
        }
        let projection = projection(prepared.parent, prepared.name.clone(), attr, Some(body));
        if prepared.replace {
            let expected_dentry_version =
                Version::new(prepared.dentry_version.ok_or_else(|| {
                    MetadError::InvalidPreparedArtifact(
                        "replace artifact is missing dentry version".to_owned(),
                    )
                })?)?;
            let replaced = self
                .lookup_plus_for_write_plan(prepared.parent, &prepared.name)?
                .and_then(|(existing, current_dentry_version)| {
                    (existing.attr.file_type == FileType::File
                        && existing.attr.inode == prepared.inode
                        && current_dentry_version == expected_dentry_version)
                        .then_some(existing)
                });
            self.commit_replace_projection_with_chunks(ReplaceProjectionCommit {
                kind: CommandKind::ReplaceArtifact,
                projection: &projection,
                chunks: &chunks,
                old_chunks,
                dentry_version: expected_dentry_version,
                old_generation: prepared.old_generation,
                version,
                path_index: prepared
                    .path
                    .as_deref()
                    .map(|path| {
                        parse_absolute_path(path)
                            .map(|components| path_index_key(self.mount, &components))
                    })
                    .transpose()?,
            })?;
            Ok(RenameReplaceResult {
                entry: projection.into(),
                replaced,
            })
        } else {
            if prepared.dentry_version.is_some() || prepared.old_generation.is_some() {
                return Err(MetadError::InvalidPreparedArtifact(
                    "create artifact must not carry replace state".to_owned(),
                ));
            }
            self.commit_create_projection_with_chunks_and_path_index(
                CommandKind::PublishArtifact,
                &projection,
                &chunks,
                version,
                prepared
                    .path
                    .as_deref()
                    .map(|path| {
                        parse_absolute_path(path)
                            .map(|components| path_index_key(self.mount, &components))
                    })
                    .transpose()?,
            )?;
            Ok(RenameReplaceResult {
                entry: projection.into(),
                replaced: None,
            })
        }
    }

    pub fn publish_prepared_artifact_session(
        &self,
        prepared: PreparedArtifact,
        request: PublishArtifactSession,
    ) -> Result<RenameReplaceResult, MetadError> {
        if prepared.parent != request.parent || prepared.name != request.name {
            return Err(MetadError::InvalidPreparedArtifact(
                "prepared artifact target does not match publish session".to_owned(),
            ));
        }
        let version = Version::new(prepared.generation)?;
        let StagedArtifactBody {
            body,
            chunks,
            old_chunks,
            staged,
        } = self.stage_artifact_session(&request, &prepared, version)?;
        self.publish_prepared_artifact_impl(PreparedArtifactPublish {
            prepared,
            body,
            chunks,
            old_chunks: &old_chunks,
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
        })
        .map_err(|err| MetadError::PublishArtifactFailed {
            source: Box::new(err),
            staged,
        })
    }

    pub fn stage_prepared_artifact_ranges(
        &self,
        prepared: &PreparedArtifact,
        manifest_id: &str,
        ranges: &[PublishArtifactRange],
        block_index_base: u64,
    ) -> Result<ChunkedWrite, MetadError> {
        let dirty_ranges = ranges
            .iter()
            .filter(|range| !range.bytes.is_empty())
            .map(|range| ChunkWriteRange {
                logical_offset: range.offset,
                bytes: range.bytes.clone().into(),
            })
            .collect::<Vec<_>>();
        match self.objects.write_ranges_with_block_index_base(
            dirty_ranges,
            ChunkWriteOptions {
                manifest_id: manifest_id.to_owned(),
                mount: self.mount.get(),
                inode: prepared.inode.get(),
                generation: prepared.generation,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
            block_index_base,
        ) {
            Ok(written) => {
                self.object_puts
                    .fetch_add(written.object_puts as u64, Ordering::Relaxed);
                self.object_put_bytes
                    .fetch_add(written.object_put_bytes, Ordering::Relaxed);
                Ok(written)
            }
            Err(err) => {
                if let ObjectError::StagedWriteFailed { staged, .. } = &err {
                    let _ = self.objects.delete_staged(staged);
                }
                Err(err.into())
            }
        }
    }

    pub fn publish_prepared_artifact_staged_session(
        &self,
        prepared: PreparedArtifact,
        request: PublishArtifactStagedSession,
    ) -> Result<RenameReplaceResult, MetadError> {
        if prepared.parent != request.parent || prepared.name != request.name {
            return Err(MetadError::InvalidPreparedArtifact(
                "prepared artifact target does not match staged publish session".to_owned(),
            ));
        }
        let version = Version::new(prepared.generation)?;
        let old_chunks = self.prepared_old_chunks(&prepared)?;
        let chunks = merge_session_chunks(request.size, old_chunks.clone(), request.chunks)?;
        self.manifest_chunks
            .fetch_add(chunks.len() as u64, Ordering::Relaxed);
        self.manifest_blocks
            .fetch_add(manifest_block_count(&chunks), Ordering::Relaxed);
        let body = BodyDescriptor {
            producer: request.producer,
            digest_uri: request.digest_uri,
            size: request.size,
            content_type: request.content_type,
            manifest_id: request.manifest_id,
            generation: version.get(),
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size: DEFAULT_BLOCK_SIZE as u64,
        };
        self.publish_prepared_artifact_impl(PreparedArtifactPublish {
            prepared,
            body,
            chunks,
            old_chunks: &old_chunks,
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
        })
        .map_err(|err| MetadError::PublishArtifactFailed {
            source: Box::new(err),
            staged: request.staged,
        })
    }

    pub(super) fn stage_artifact_body(
        &self,
        request: &PublishArtifact,
        inode: InodeId,
        version: Version,
    ) -> Result<StagedArtifactBody, MetadError> {
        let written = self.objects.write_bytes(
            &request.bytes,
            ChunkWriteOptions {
                manifest_id: request.manifest_id.clone(),
                mount: self.mount.get(),
                inode: inode.get(),
                generation: version.get(),
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
        )?;
        let staged = written.staged_objects()?;
        self.object_puts
            .fetch_add(written.object_puts as u64, Ordering::Relaxed);
        self.object_put_bytes
            .fetch_add(written.object_put_bytes, Ordering::Relaxed);
        self.manifest_chunks
            .fetch_add(written.chunks.len() as u64, Ordering::Relaxed);
        self.manifest_blocks.fetch_add(
            written
                .chunks
                .iter()
                .map(|chunk| chunk.blocks.len() as u64)
                .sum::<u64>(),
            Ordering::Relaxed,
        );
        let chunks = written
            .chunks
            .into_iter()
            .map(|chunk| ChunkManifest {
                chunk_index: chunk.chunk_index,
                logical_offset: chunk.logical_offset,
                len: chunk.len,
                slices: vec![SliceManifest {
                    slice_id: 1,
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
                }],
            })
            .collect();
        Ok(StagedArtifactBody {
            body: BodyDescriptor {
                producer: request.producer.clone(),
                digest_uri: request.digest_uri.clone(),
                size: written.size,
                content_type: request.content_type.clone(),
                manifest_id: written.manifest_id,
                generation: version.get(),
                chunk_size: written.chunk_size,
                block_size: written.block_size,
            },
            chunks,
            old_chunks: Vec::new(),
            staged,
        })
    }

    pub(super) fn stage_artifact_session(
        &self,
        request: &PublishArtifactSession,
        prepared: &PreparedArtifact,
        version: Version,
    ) -> Result<StagedArtifactBody, MetadError> {
        validate_artifact_ranges(request)?;
        let dirty_ranges = request
            .ranges
            .iter()
            .filter(|range| !range.bytes.is_empty())
            .map(|range| ChunkWriteRange {
                logical_offset: range.offset,
                bytes: range.bytes.clone().into(),
            })
            .collect::<Vec<_>>();
        let written = self.objects.write_ranges_with_block_index_base(
            dirty_ranges,
            ChunkWriteOptions {
                manifest_id: request.manifest_id.clone(),
                mount: self.mount.get(),
                inode: prepared.inode.get(),
                generation: version.get(),
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
            0,
        )?;
        let staged = written.staged_objects()?;
        self.object_puts
            .fetch_add(written.object_puts as u64, Ordering::Relaxed);
        self.object_put_bytes
            .fetch_add(written.object_put_bytes, Ordering::Relaxed);

        let old_chunks = self.prepared_old_chunks(prepared)?;
        let dirty_chunks = written.chunk_manifests();
        let chunks = merge_session_chunks(request.size, old_chunks.clone(), dirty_chunks)?;
        self.manifest_chunks
            .fetch_add(chunks.len() as u64, Ordering::Relaxed);
        self.manifest_blocks
            .fetch_add(manifest_block_count(&chunks), Ordering::Relaxed);
        Ok(StagedArtifactBody {
            body: BodyDescriptor {
                producer: request.producer.clone(),
                digest_uri: request.digest_uri.clone(),
                size: request.size,
                content_type: request.content_type.clone(),
                manifest_id: written.manifest_id,
                generation: version.get(),
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE as u64,
            },
            chunks,
            old_chunks,
            staged,
        })
    }

    fn prepared_old_chunks(
        &self,
        prepared: &PreparedArtifact,
    ) -> Result<Vec<ChunkManifest>, MetadError> {
        if !prepared.replace {
            return Ok(Vec::new());
        }
        let Some(generation) = prepared.old_generation else {
            return Ok(Vec::new());
        };
        let version = self.read_version()?;
        let Some(body) = self.body_descriptor_at_version_for_purpose(
            prepared.inode,
            generation,
            version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(Vec::new());
        };
        self.chunk_manifests_for_body_at_version(
            prepared.inode,
            &body,
            version,
            ReadPurpose::WritePlanLocal,
        )
    }
}
