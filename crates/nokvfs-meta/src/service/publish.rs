use super::*;

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
            staged,
        } = self.stage_artifact_body(&request, inode, version)?;
        let attr = InodeAttr {
            inode,
            file_type: FileType::File,
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
            size: body.size,
            generation: version.get(),
            mtime_ms: version.get(),
            ctime_ms: version.get(),
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
            .lookup_plus_versioned(request.parent, &request.name)?
            .ok_or(MetadError::NotFound)?;
        if existing.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let version = self.next_version()?;
        let StagedArtifactBody {
            body,
            chunks,
            staged,
        } = self.stage_artifact_body(&request, existing.attr.inode, version)?;
        let attr = InodeAttr {
            inode: existing.attr.inode,
            file_type: FileType::File,
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
            size: body.size,
            generation: version.get(),
            mtime_ms: version.get(),
            ctime_ms: version.get(),
        };
        let projection = projection(request.parent, request.name, attr, Some(body));
        let old_generation = existing.body.as_ref().map(|body| body.generation);
        if let Err(err) = self.commit_replace_projection_with_chunks(
            CommandKind::ReplaceArtifact,
            &projection,
            &chunks,
            dentry_version,
            old_generation,
            version,
        ) {
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
        let Some(parent_attr) = self.get_attr(parent)? else {
            return Err(MetadError::NotFound);
        };
        if parent_attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        if self.lookup_plus(parent, &name)?.is_some() {
            return Err(MetadataError::PredicateFailed.into());
        }
        let generation = self.next_version()?;
        let inode = self.next_inode()?;
        Ok(PreparedArtifact {
            parent,
            name,
            inode,
            generation: generation.get(),
            replace: false,
            dentry_version: None,
            old_generation: None,
        })
    }

    pub fn prepare_artifact_create_path(&self, path: &str) -> Result<PreparedArtifact, MetadError> {
        let (parent, name) = self.resolve_parent_path(path)?;
        self.prepare_artifact_create(parent, name)
    }

    pub fn prepare_artifact_replace(
        &self,
        parent: InodeId,
        name: DentryName,
    ) -> Result<PreparedArtifact, MetadError> {
        let (existing, dentry_version) = self
            .lookup_plus_versioned(parent, &name)?
            .ok_or(MetadError::NotFound)?;
        if existing.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let generation = self.next_version()?;
        Ok(PreparedArtifact {
            parent,
            name,
            inode: existing.attr.inode,
            generation: generation.get(),
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
        self.prepare_artifact_replace(parent, name)
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
        validate_prepared_artifact(&prepared, &body, &chunks)?;
        let version = Version::new(prepared.generation)?;
        let attr = InodeAttr {
            inode: prepared.inode,
            file_type: FileType::File,
            mode,
            uid,
            gid,
            size: body.size,
            generation: prepared.generation,
            mtime_ms: prepared.generation,
            ctime_ms: prepared.generation,
        };
        let projection = projection(prepared.parent, prepared.name.clone(), attr, Some(body));
        if prepared.replace {
            let expected_dentry_version =
                Version::new(prepared.dentry_version.ok_or_else(|| {
                    MetadError::InvalidPreparedArtifact(
                        "replace artifact is missing dentry version".to_owned(),
                    )
                })?)?;
            let replaced = self
                .lookup_plus_versioned(prepared.parent, &prepared.name)?
                .and_then(|(existing, current_dentry_version)| {
                    (existing.attr.file_type == FileType::File
                        && existing.attr.inode == prepared.inode
                        && current_dentry_version == expected_dentry_version)
                        .then_some(existing)
                });
            self.commit_replace_projection_with_chunks(
                CommandKind::ReplaceArtifact,
                &projection,
                &chunks,
                expected_dentry_version,
                prepared.old_generation,
                version,
            )?;
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
            self.commit_create_projection_with_chunks(
                CommandKind::PublishArtifact,
                &projection,
                &chunks,
                version,
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
            staged,
        } = self.stage_artifact_session(&request, &prepared, version)?;
        self.publish_prepared_artifact(
            prepared,
            body,
            chunks,
            request.mode,
            request.uid,
            request.gid,
        )
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
                bytes: range.bytes.clone(),
            })
            .collect::<Vec<_>>();
        match put_chunked_ranges_with_block_index_base(
            &self.objects,
            &dirty_ranges,
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
                Ok(written)
            }
            Err(err) => {
                if let ObjectError::StagedWriteFailed { staged, .. } = &err {
                    let _ = delete_staged_objects(&self.objects, staged);
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
        let old_chunks = if prepared.replace {
            prepared
                .old_generation
                .map(|generation| {
                    self.chunk_manifests_at_version(
                        prepared.inode,
                        generation,
                        self.read_version()?,
                    )
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let chunks = merge_session_chunks(request.size, old_chunks, request.chunks)?;
        self.manifest_chunks
            .fetch_add(chunks.len() as u64, Ordering::Relaxed);
        self.manifest_blocks.fetch_add(
            chunks
                .iter()
                .map(|chunk| chunk.blocks.len() as u64)
                .sum::<u64>(),
            Ordering::Relaxed,
        );
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
        self.publish_prepared_artifact(
            prepared,
            body,
            chunks,
            request.mode,
            request.uid,
            request.gid,
        )
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
        let written = put_chunked_object(
            &self.objects,
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
                bytes: range.bytes.clone(),
            })
            .collect::<Vec<_>>();
        let written = put_chunked_ranges(
            &self.objects,
            &dirty_ranges,
            ChunkWriteOptions {
                manifest_id: request.manifest_id.clone(),
                mount: self.mount.get(),
                inode: prepared.inode.get(),
                generation: version.get(),
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
            },
        )?;
        let staged = written.staged_objects()?;
        self.object_puts
            .fetch_add(written.object_puts as u64, Ordering::Relaxed);

        let old_chunks = if prepared.replace {
            prepared
                .old_generation
                .map(|generation| {
                    self.chunk_manifests_at_version(
                        prepared.inode,
                        generation,
                        self.read_version()?,
                    )
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let chunks = merge_session_chunks(request.size, old_chunks, written.chunks)?;
        self.manifest_chunks
            .fetch_add(chunks.len() as u64, Ordering::Relaxed);
        self.manifest_blocks.fetch_add(
            chunks
                .iter()
                .map(|chunk| chunk.blocks.len() as u64)
                .sum::<u64>(),
            Ordering::Relaxed,
        );
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
            staged,
        })
    }
}
