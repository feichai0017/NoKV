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
        self.get_attr_at_version_for_purpose(inode, version, ReadPurpose::UserStrong)
    }

    pub(super) fn get_attr_at_version_for_purpose(
        &self,
        inode: InodeId,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<InodeAttr>, MetadError> {
        let Some(value) = self.metadata.get(
            RecordFamily::Inode,
            &inode_key(self.mount, inode),
            version,
            purpose,
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
        self.lookup_path_from_at_version_for_purpose_with_index(
            InodeId::root(),
            path,
            self.read_version()?,
            ReadPurpose::UserStrong,
            false,
        )
        .map(|entry| entry.map(|(entry, _)| entry))
    }

    pub(super) fn lookup_plus_versioned(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let version = self.read_version()?;
        self.lookup_plus_at_version(parent, name, version)
    }

    pub(super) fn lookup_plus_for_write_plan(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let version = self.read_version()?;
        self.lookup_plus_at_version_for_purpose(parent, name, version, ReadPurpose::WritePlanLocal)
    }

    pub(super) fn lookup_plus_at_version(
        &self,
        parent: InodeId,
        name: &DentryName,
        version: Version,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        self.lookup_plus_at_version_for_purpose(parent, name, version, ReadPurpose::UserStrong)
    }

    pub(super) fn lookup_plus_at_version_for_purpose(
        &self,
        parent: InodeId,
        name: &DentryName,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let key = dentry_key(self.mount, parent, name);
        let Some(item) =
            self.metadata
                .get_versioned(RecordFamily::Dentry, &key, version, purpose)?
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

    pub fn read_dir_plus_page(
        &self,
        parent: InodeId,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ReadDirPlusPage, MetadError> {
        let version = self.read_version()?;
        self.read_dir_plus_page_at_version(parent, after, limit, version)
    }

    pub fn read_dir_plus_path_page(
        &self,
        path: &str,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ReadDirPlusPage, MetadError> {
        let parent = self.resolve_directory_path(path)?;
        self.read_dir_plus_page(parent, after, limit)
    }

    pub fn stat_path(&self, path: &str) -> Result<Option<PathMetadata>, MetadError> {
        self.stat_path_from_at_version(InodeId::root(), path, self.read_version()?)
    }

    pub(super) fn read_dir_plus_at_version(
        &self,
        parent: InodeId,
        version: Version,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        self.read_dir_plus_at_version_for_purpose(parent, version, ReadPurpose::UserStrong)
    }

    pub(super) fn read_dir_plus_at_version_for_purpose(
        &self,
        parent: InodeId,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: dentry_prefix(self.mount, parent),
            start_after: None,
            version,
            limit: 0,
            purpose,
        })?;
        self.read_dir_plus_total.fetch_add(1, Ordering::Relaxed);
        self.read_dir_plus_entry_total
            .fetch_add(rows.len() as u64, Ordering::Relaxed);
        let mut entries = Vec::with_capacity(rows.len());
        let mut projection_hits = 0_u64;
        for item in rows {
            let projection = crate::layout::decode_dentry_projection(&item.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            projection_hits += 1;
            entries.push(projection.into());
        }
        self.read_dir_plus_projection_hit_total
            .fetch_add(projection_hits, Ordering::Relaxed);
        Ok(entries)
    }

    pub(super) fn read_dir_plus_page_at_version(
        &self,
        parent: InodeId,
        after: Option<&DentryName>,
        limit: usize,
        version: Version,
    ) -> Result<ReadDirPlusPage, MetadError> {
        let requested = limit.max(1);
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Dentry,
            prefix: dentry_prefix(self.mount, parent),
            start_after: after.map(|name| dentry_key(self.mount, parent, name)),
            version,
            limit: requested.saturating_add(1),
            purpose: ReadPurpose::UserStrong,
        })?;
        self.read_dir_plus_total.fetch_add(1, Ordering::Relaxed);
        let has_more = rows.len() > requested;
        let returned = rows.len().min(requested);
        self.read_dir_plus_entry_total
            .fetch_add(returned as u64, Ordering::Relaxed);
        let mut entries = Vec::<DentryWithAttr>::with_capacity(returned);
        let mut projection_hits = 0_u64;
        for item in rows.into_iter().take(returned) {
            let projection = crate::layout::decode_dentry_projection(&item.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            projection_hits += 1;
            entries.push(projection.into());
        }
        self.read_dir_plus_projection_hit_total
            .fetch_add(projection_hits, Ordering::Relaxed);
        let next_cursor = if has_more {
            entries.last().map(|entry| entry.dentry.name.clone())
        } else {
            None
        };
        Ok(ReadDirPlusPage {
            entries,
            next_cursor,
        })
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
        self.resolve_components_as_directory_from_at_version(InodeId::root(), components, version)
    }

    pub(super) fn resolve_components_as_directory_from_at_version(
        &self,
        root: InodeId,
        components: &[DentryName],
        version: Version,
    ) -> Result<InodeId, MetadError> {
        self.resolve_components_as_directory_from_at_version_for_purpose(
            root,
            components,
            version,
            ReadPurpose::UserStrong,
        )
    }

    pub(super) fn resolve_components_as_directory_from_at_version_for_purpose(
        &self,
        root: InodeId,
        components: &[DentryName],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<InodeId, MetadError> {
        let mut current = root;
        for name in components {
            let entry = self
                .lookup_plus_at_version_for_purpose(current, name, version, purpose)?
                .map(|(entry, _)| entry)
                .ok_or(MetadError::NotFound)?;
            if entry.attr.file_type != FileType::Directory {
                return Err(MetadError::NotDirectory);
            }
            current = entry.attr.inode;
        }
        Ok(current)
    }

    pub(super) fn lookup_path_from_at_version_for_purpose(
        &self,
        root: InodeId,
        path: &str,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        self.lookup_path_from_at_version_for_purpose_with_index(root, path, version, purpose, true)
    }

    fn lookup_path_from_at_version_for_purpose_with_index(
        &self,
        root: InodeId,
        path: &str,
        version: Version,
        purpose: ReadPurpose,
        probe_path_index: bool,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let mut components = parse_absolute_path(path)?;
        if probe_path_index && root == InodeId::root() && !components.is_empty() {
            if let Some(indexed) =
                self.lookup_path_index_components_at_version(&components, version, purpose)?
            {
                return Ok(Some(indexed));
            }
            self.path_index_fallback_total
                .fetch_add(1, Ordering::Relaxed);
        }
        let Some(name) = components.pop() else {
            return Ok(None);
        };
        let parent = self.resolve_components_as_directory_from_at_version_for_purpose(
            root,
            &components,
            version,
            purpose,
        )?;
        self.lookup_plus_at_version_for_purpose(parent, &name, version, purpose)
    }

    fn lookup_path_index_components_at_version(
        &self,
        components: &[DentryName],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let Some((name, parent_components)) = components.split_last() else {
            return Ok(None);
        };
        self.path_index_lookup_total.fetch_add(1, Ordering::Relaxed);
        let key = path_index_key(self.mount, components);
        let Some(item) =
            self.metadata
                .get_versioned(RecordFamily::PathIndex, &key, version, purpose)?
        else {
            self.path_index_miss_total.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        let indexed: DentryWithAttr = crate::layout::decode_dentry_projection(&item.value.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?
            .into();
        let parent = match self.resolve_components_as_directory_from_at_version_for_purpose(
            InodeId::root(),
            parent_components,
            version,
            purpose,
        ) {
            Ok(parent) => parent,
            Err(MetadError::NotFound | MetadError::NotDirectory) => {
                self.path_index_stale_total.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            Err(err) => return Err(err),
        };
        if parent != indexed.dentry.parent || *name != indexed.dentry.name {
            self.path_index_stale_total.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        let Some((canonical, canonical_version)) =
            self.lookup_plus_at_version_for_purpose(parent, name, version, purpose)?
        else {
            self.path_index_stale_total.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        if canonical_version == item.version && canonical == indexed {
            self.path_index_hit_total.fetch_add(1, Ordering::Relaxed);
            return Ok(Some((canonical, canonical_version)));
        }
        self.path_index_stale_total.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    pub(super) fn stat_path_from_at_version(
        &self,
        root: InodeId,
        path: &str,
        version: Version,
    ) -> Result<Option<PathMetadata>, MetadError> {
        self.stat_path_from_at_version_for_purpose(root, path, version, ReadPurpose::UserStrong)
    }

    pub(super) fn stat_path_from_at_version_for_purpose(
        &self,
        root: InodeId,
        path: &str,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<PathMetadata>, MetadError> {
        let components = parse_absolute_path(path)?;
        if components.is_empty() {
            let Some(attr) = self.get_attr_at_version_for_purpose(root, version, purpose)? else {
                return Ok(None);
            };
            if attr.file_type == FileType::File {
                let body = self.body_descriptor_at_version_for_purpose(
                    root,
                    attr.generation,
                    version,
                    purpose,
                )?;
                return Ok(Some(PathMetadata { attr, body }));
            }
            return Ok(Some(PathMetadata { attr, body: None }));
        }
        let Some((entry, _)) =
            self.lookup_path_from_at_version_for_purpose(root, path, version, purpose)?
        else {
            return Ok(None);
        };
        Ok(Some(PathMetadata {
            attr: entry.attr,
            body: entry.body,
        }))
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
        self.body_descriptor_at_version_for_purpose(
            inode,
            generation,
            version,
            ReadPurpose::UserStrong,
        )
    }

    pub(super) fn body_descriptor_at_version_for_purpose(
        &self,
        inode: InodeId,
        generation: u64,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<BodyDescriptor>, MetadError> {
        let summary_key =
            chunk_manifest_key(self.mount, inode, generation, BODY_SUMMARY_CHUNK_INDEX);
        let Some(value) =
            self.metadata
                .get(RecordFamily::ChunkManifest, &summary_key, version, purpose)?
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
        self.read_file_at_version_for_purpose(
            inode,
            body,
            offset,
            len,
            version,
            ReadPurpose::UserStrong,
        )
    }

    pub(super) fn read_file_at_version_for_purpose(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        offset: u64,
        len: usize,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Vec<u8>, MetadError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        if offset >= body.size {
            return Ok(Vec::new());
        }
        let len = len.min((body.size - offset) as usize);
        let plan = self.read_plan_for_purpose(inode, body, offset, len, version, purpose)?;
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
        self.read_plan_for_purpose(inode, body, offset, len, version, ReadPurpose::UserStrong)
    }

    pub(super) fn read_plan_for_purpose(
        &self,
        inode: InodeId,
        body: &BodyDescriptor,
        offset: u64,
        len: usize,
        version: Version,
        purpose: ReadPurpose,
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
            let Some(value) =
                self.metadata
                    .get(RecordFamily::ChunkManifest, &key, version, purpose)?
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
            start_after: None,
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
