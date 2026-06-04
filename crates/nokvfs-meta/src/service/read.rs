use super::*;

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn get_attr(&self, inode: InodeId) -> Result<Option<InodeAttr>, MetadError> {
        let version = self.read_version()?;
        self.get_attr_at_version(inode, version)
    }

    pub(super) fn get_attr_at_version(
        &self,
        inode: InodeId,
        version: Version,
    ) -> Result<Option<InodeAttr>, MetadError> {
        let Some(value) = self.metadata.get(
            RecordFamily::Inode,
            &inode_key(self.mount, inode),
            version,
            ReadPurpose::UserStrong,
        )?
        else {
            return Ok(None);
        };
        decode_inode_attr(&value.0)
            .map(Some)
            .map_err(|err| MetadError::Codec(err.to_string()))
    }

    pub fn lookup_plus(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        self.lookup_plus_versioned(parent, name)
            .map(|entry| entry.map(|(entry, _)| entry))
    }

    pub fn lookup_path(&self, path: &str) -> Result<Option<DentryWithAttr>, MetadError> {
        let mut components = parse_absolute_path(path)?;
        let Some(name) = components.pop() else {
            return Ok(None);
        };
        let parent = self.resolve_components_as_directory(&components)?;
        self.lookup_plus(parent, &name)
    }

    pub(super) fn lookup_plus_versioned(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let version = self.read_version()?;
        self.lookup_plus_at_version(parent, name, version)
    }

    pub(super) fn lookup_plus_at_version(
        &self,
        parent: InodeId,
        name: &DentryName,
        version: Version,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let key = dentry_key(self.mount, parent, name);
        let Some(item) = self.metadata.get_versioned(
            RecordFamily::Dentry,
            &key,
            version,
            ReadPurpose::UserStrong,
        )?
        else {
            return Ok(None);
        };
        Ok(Some((
            crate::layout::decode_dentry_projection(&item.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?
                .into(),
            item.version,
        )))
    }

    pub fn read_dir_plus(&self, parent: InodeId) -> Result<Vec<DentryWithAttr>, MetadError> {
        let version = self.read_version()?;
        self.read_dir_plus_at_version(parent, version)
    }

    pub fn read_dir_plus_path(&self, path: &str) -> Result<Vec<DentryWithAttr>, MetadError> {
        let parent = self.resolve_directory_path(path)?;
        self.read_dir_plus(parent)
    }

    pub(super) fn read_dir_plus_at_version(
        &self,
        parent: InodeId,
        version: Version,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: dentry_prefix(self.mount, parent),
            version,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })?;
        rows.into_iter()
            .map(|item| {
                crate::layout::decode_dentry_projection(&item.value.0)
                    .map(Into::into)
                    .map_err(|err| MetadError::Codec(err.to_string()))
            })
            .collect()
    }

    pub(super) fn resolve_parent_path(
        &self,
        path: &str,
    ) -> Result<(InodeId, DentryName), MetadError> {
        let mut components = parse_absolute_path(path)?;
        let name = components
            .pop()
            .ok_or_else(|| MetadError::InvalidPath("root has no parent".to_owned()))?;
        let parent = self.resolve_components_as_directory(&components)?;
        Ok((parent, name))
    }

    pub(super) fn resolve_directory_path(&self, path: &str) -> Result<InodeId, MetadError> {
        let components = parse_absolute_path(path)?;
        self.resolve_components_as_directory(&components)
    }

    pub(super) fn resolve_components_as_directory(
        &self,
        components: &[DentryName],
    ) -> Result<InodeId, MetadError> {
        self.resolve_components_as_directory_at_version(components, self.read_version()?)
    }

    pub(super) fn resolve_components_as_directory_at_version(
        &self,
        components: &[DentryName],
        version: Version,
    ) -> Result<InodeId, MetadError> {
        let mut current = InodeId::root();
        for name in components {
            let entry = self
                .lookup_plus_at_version(current, name, version)?
                .map(|(entry, _)| entry)
                .ok_or(MetadError::NotFound)?;
            if entry.attr.file_type != FileType::Directory {
                return Err(MetadError::NotDirectory);
            }
            current = entry.attr.inode;
        }
        Ok(current)
    }

    pub fn read_artifact(&self, parent: InodeId, name: &DentryName) -> Result<Vec<u8>, MetadError> {
        let entry = self
            .lookup_plus(parent, name)?
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let body = entry.body.ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file(entry.attr.inode, 0, body.size as usize)
    }

    pub fn body_descriptor(&self, inode: InodeId) -> Result<Option<BodyDescriptor>, MetadError> {
        let Some(attr) = self.get_attr(inode)? else {
            return Ok(None);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        self.body_descriptor_at_version(inode, attr.generation, self.read_version()?)
    }

    pub(super) fn body_descriptor_at_version(
        &self,
        inode: InodeId,
        generation: u64,
        version: Version,
    ) -> Result<Option<BodyDescriptor>, MetadError> {
        let summary_key =
            chunk_manifest_key(self.mount, inode, generation, BODY_SUMMARY_CHUNK_INDEX);
        let Some(value) = self.metadata.get(
            RecordFamily::ChunkManifest,
            &summary_key,
            version,
            ReadPurpose::UserStrong,
        )?
        else {
            return Err(MetadError::MissingBodyDescriptor);
        };
        decode_body_descriptor(&value.0)
            .map(Some)
            .map_err(|err| MetadError::Codec(err.to_string()))
    }

    pub fn read_file(
        &self,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, MetadError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let Some(attr) = self.get_attr(inode)? else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if offset >= attr.size {
            return Ok(Vec::new());
        }
        let body = self.body_descriptor(inode)?.ok_or(MetadError::NotFound)?;
        self.read_file_at_version(inode, &body, offset, len, self.read_version()?)
    }

    pub fn read_symlink(&self, inode: InodeId) -> Result<Vec<u8>, MetadError> {
        let Some(attr) = self.get_attr(inode)? else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::Symlink {
            return Err(MetadError::NotFile);
        }
        let version = self.read_version()?;
        let body = self
            .body_descriptor_at_version(inode, attr.generation, version)?
            .ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version(inode, &body, 0, body.size as usize, version)
    }

    pub fn read_file_plan(
        &self,
        inode: InodeId,
        generation: u64,
        offset: u64,
        len: usize,
    ) -> Result<BodyReadPlan, MetadError> {
        if len == 0 {
            return Ok(BodyReadPlan {
                output_len: 0,
                blocks: Vec::new(),
            });
        }
        let Some(attr) = self.get_attr(inode)? else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if attr.generation != generation {
            return Err(MetadError::StaleBodyGeneration {
                expected: generation,
                current: attr.generation,
            });
        }
        if offset >= attr.size {
            return Ok(BodyReadPlan {
                output_len: 0,
                blocks: Vec::new(),
            });
        }
        let version = self.read_version()?;
        let body = self
            .body_descriptor_at_version(inode, generation, version)?
            .ok_or(MetadError::MissingBodyDescriptor)?;
        if body.size != attr.size {
            return Err(MetadError::BodySizeMismatch {
                descriptor: body.size,
                bytes: attr.size,
            });
        }
        let output_len = len.min((attr.size - offset) as usize);
        Ok(BodyReadPlan {
            output_len,
            blocks: self.read_plan(inode, &body, offset, output_len, version)?,
        })
    }

    pub(super) fn read_file_at_version(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        offset: u64,
        len: usize,
        version: Version,
    ) -> Result<Vec<u8>, MetadError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        if offset >= body.size {
            return Ok(Vec::new());
        }
        let len = len.min((body.size - offset) as usize);
        let plan = self.read_plan(inode, body, offset, len, version)?;
        let cache = if self.block_cache_enabled() {
            Some(&self.block_cache)
        } else {
            None
        };
        let outcome = read_object_blocks(&self.objects, cache, len, &plan)?;
        self.object_gets
            .fetch_add(outcome.object_gets as u64, Ordering::Relaxed);
        self.cache_hits
            .fetch_add(outcome.cache_hits as u64, Ordering::Relaxed);
        Ok(outcome.bytes)
    }

    pub fn read_session_object_blocks(
        &self,
        output_len: usize,
        blocks: &[ObjectReadBlock],
    ) -> Result<Vec<u8>, MetadError> {
        let cache = self.block_cache_enabled().then_some(&self.block_cache);
        let outcome = read_object_blocks(&self.objects, cache, output_len, blocks)?;
        self.object_gets
            .fetch_add(outcome.object_gets as u64, Ordering::Relaxed);
        self.cache_hits
            .fetch_add(outcome.cache_hits as u64, Ordering::Relaxed);
        Ok(outcome.bytes)
    }

    pub(super) fn read_plan(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        offset: u64,
        len: usize,
        version: Version,
    ) -> Result<Vec<ObjectReadBlock>, MetadError> {
        if body.chunk_size == 0 || body.block_size == 0 {
            return Err(ObjectError::InvalidChunkLayout.into());
        }
        let end = offset
            .checked_add(len as u64)
            .ok_or(ObjectError::InvalidRange)?
            .min(body.size);
        if end <= offset {
            return Ok(Vec::new());
        }

        let start_chunk = offset / body.chunk_size;
        let end_chunk = (end - 1) / body.chunk_size;
        let mut plan = Vec::new();
        for chunk_index in start_chunk..=end_chunk {
            let key = chunk_manifest_key(self.mount, inode, body.generation, chunk_index);
            let Some(value) = self.metadata.get(
                RecordFamily::ChunkManifest,
                &key,
                version,
                ReadPurpose::UserStrong,
            )?
            else {
                return Err(MetadError::MissingBodyDescriptor);
            };
            let manifest = decode_chunk_manifest(&value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            for block in manifest.blocks {
                let block_start = block.logical_offset;
                let block_end = block_start
                    .checked_add(block.len)
                    .ok_or(ObjectError::InvalidRange)?;
                let overlap_start = block_start.max(offset);
                let overlap_end = block_end.min(end);
                if overlap_start >= overlap_end {
                    continue;
                }
                let object_offset = block
                    .object_offset
                    .checked_add(overlap_start - block_start)
                    .ok_or(ObjectError::InvalidRange)?;
                plan.push(ObjectReadBlock {
                    object_key: block.object_key,
                    object_offset,
                    len: (overlap_end - overlap_start) as usize,
                    output_offset: (overlap_start - offset) as usize,
                });
            }
        }
        Ok(plan)
    }

    pub(super) fn chunk_manifests_at_version(
        &self,
        inode: InodeId,
        generation: u64,
        version: Version,
    ) -> Result<Vec<ChunkManifest>, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::ChunkManifest,
            prefix: chunk_manifest_prefix(self.mount, inode, generation),
            version,
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        rows.into_iter()
            .filter_map(|row| match chunk_index_from_manifest_key(&row.key) {
                Ok(BODY_SUMMARY_CHUNK_INDEX) => None,
                Ok(_) => Some(Ok(row)),
                Err(err) => Some(Err(err)),
            })
            .map(|row| {
                let row = row?;
                decode_chunk_manifest(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))
            })
            .collect()
    }
}
