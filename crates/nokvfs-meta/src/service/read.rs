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

    pub fn list_indexed_path_page(
        &self,
        path: &str,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ReadDirPlusPage, MetadError> {
        let version = self.read_version()?;
        let components = parse_absolute_path(path)?;
        let parent = self.resolve_components_as_directory_at_version(&components, version)?;
        self.list_indexed_components_page(parent, &components, after, limit, version)
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

    fn list_indexed_components_page(
        &self,
        parent: InodeId,
        components: &[DentryName],
        after: Option<&DentryName>,
        limit: usize,
        version: Version,
    ) -> Result<ReadDirPlusPage, MetadError> {
        let requested = limit.max(1);
        let prefix = path_index_prefix(self.mount, components);
        let mut start_after = after.map(|name| {
            let mut marker = components.to_vec();
            marker.push(name.clone());
            path_index_prefix(self.mount, &marker)
        });
        let scan_limit = requested.saturating_add(1);
        let mut entries = Vec::<DentryWithAttr>::with_capacity(scan_limit);
        let mut stale_rows = 0_u64;
        loop {
            let rows = self.metadata.scan_delimited(DelimitedScanRequest {
                family: RecordFamily::PathIndex,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                delimiter: PATH_INDEX_DELIMITER,
                version,
                limit: scan_limit,
                purpose: ReadPurpose::UserStrong,
            })?;
            if rows.is_empty() {
                break;
            }
            let exhausted = rows.len() < scan_limit;
            let mut last_marker = None;
            for item in rows {
                last_marker = Some(delimited_scan_marker(&item));
                let Some(entry) = self.indexed_path_child(parent, &prefix, item, version)? else {
                    stale_rows += 1;
                    continue;
                };
                entries.push(entry);
                if entries.len() > requested {
                    break;
                }
            }
            if entries.len() > requested || exhausted {
                break;
            }
            let Some(marker) = last_marker else {
                break;
            };
            start_after = Some(marker);
        }
        let next_cursor = if entries.len() > requested {
            entries.truncate(requested);
            entries.last().map(|entry| entry.dentry.name.clone())
        } else {
            None
        };
        self.read_dir_plus_total.fetch_add(1, Ordering::Relaxed);
        self.read_dir_plus_entry_total
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        self.read_dir_plus_projection_hit_total
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        self.path_index_scan_stale_total
            .fetch_add(stale_rows, Ordering::Relaxed);
        Ok(ReadDirPlusPage {
            entries,
            next_cursor,
        })
    }

    fn indexed_path_child(
        &self,
        parent: InodeId,
        prefix: &[u8],
        item: DelimitedScanItem,
        version: Version,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        match item {
            DelimitedScanItem::Key(item) => {
                let name = path_index_child_name(prefix, &item.key, false)?;
                if let Some(cached) =
                    self.cached_validated_path_index(&item.key, item.version, version)?
                {
                    if cached.dentry.parent == parent && cached.dentry.name == name {
                        return Ok(Some(cached));
                    }
                }
                let indexed: DentryWithAttr =
                    crate::layout::decode_dentry_projection(&item.value.0)
                        .map_err(|err| MetadError::Codec(err.to_string()))?
                        .into();
                let Some((canonical, canonical_version)) =
                    self.lookup_plus_at_version(parent, &name, version)?
                else {
                    return Ok(None);
                };
                if canonical_version == item.version && canonical == indexed {
                    self.remember_path_index_lookup(&item.key, version, &canonical, item.version)?;
                    self.remember_validated_path_index(
                        &item.key,
                        item.version,
                        version,
                        &canonical,
                    )?;
                    Ok(Some(canonical))
                } else {
                    Ok(None)
                }
            }
            DelimitedScanItem::CommonPrefix(common) => {
                let name = path_index_child_name(prefix, &common, true)?;
                Ok(self
                    .lookup_plus_at_version(parent, &name, version)?
                    .map(|(entry, _)| entry))
            }
        }
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
        if components.is_empty() {
            return Ok(root);
        }
        if let Some(cached) = self.cached_path_resolution(root, components, version)? {
            return Ok(cached);
        }
        let mut current = root;
        for index in 0..components.len() {
            let prefix = &components[..=index];
            if let Some(cached) = self.cached_path_resolution(root, prefix, version)? {
                current = cached;
                continue;
            }
            let name = &components[index];
            let entry = self
                .lookup_plus_at_version_for_purpose(current, name, version, purpose)?
                .map(|(entry, _)| entry)
                .ok_or(MetadError::NotFound)?;
            if entry.attr.file_type != FileType::Directory {
                return Err(MetadError::NotDirectory);
            }
            current = entry.attr.inode;
            self.remember_path_resolution(root, prefix, version, current)?;
        }
        Ok(current)
    }

    fn cached_path_resolution(
        &self,
        root: InodeId,
        components: &[DentryName],
        version: Version,
    ) -> Result<Option<InodeId>, MetadError> {
        let key = self.path_resolution_cache_key(root, components, version);
        let shard_index = path_cache_shard_index(&key);
        let cache = self.path_resolution_cache[shard_index]
            .lock()
            .map_err(|err| {
                MetadataError::Backend(format!("metadata path resolution cache poisoned: {err}"))
            })?;
        Ok(cache.get(&key).copied())
    }

    fn remember_path_resolution(
        &self,
        root: InodeId,
        components: &[DentryName],
        version: Version,
        inode: InodeId,
    ) -> Result<(), MetadError> {
        let key = self.path_resolution_cache_key(root, components, version);
        let shard_index = path_cache_shard_index(&key);
        let mut cache = self.path_resolution_cache[shard_index]
            .lock()
            .map_err(|err| {
                MetadataError::Backend(format!("metadata path resolution cache poisoned: {err}"))
            })?;
        if cache.len() >= PATH_RESOLUTION_CACHE_MAX_ENTRIES_PER_SHARD {
            cache.clear();
        }
        cache.insert(key, inode);
        Ok(())
    }

    fn path_resolution_cache_key(
        &self,
        root: InodeId,
        components: &[DentryName],
        version: Version,
    ) -> PathResolutionCacheKey {
        PathResolutionCacheKey {
            root: root.get(),
            version: version.get(),
            components_key: path_index_key(self.mount, components),
        }
    }

    fn cached_validated_path_index(
        &self,
        index_key: &[u8],
        index_version: Version,
        read_version: Version,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        let key = self.path_index_validation_cache_key(index_key, index_version, read_version);
        let shard_index = path_cache_shard_index(&key);
        let cache = self.path_index_validation_cache[shard_index]
            .lock()
            .map_err(|err| {
                MetadataError::Backend(format!(
                    "metadata path-index validation cache poisoned: {err}"
                ))
            })?;
        Ok(cache.get(&key).cloned())
    }

    fn cached_path_index_lookup(
        &self,
        index_key: &[u8],
        read_version: Version,
    ) -> Result<Option<(DentryWithAttr, Version)>, MetadError> {
        let key = self.path_index_lookup_cache_key(index_key, read_version);
        let shard_index = path_cache_shard_index(&key);
        let cache = self.path_index_lookup_cache[shard_index]
            .lock()
            .map_err(|err| {
                MetadataError::Backend(format!("metadata path-index lookup cache poisoned: {err}"))
            })?;
        Ok(cache
            .get(&key)
            .map(|value| (value.entry.clone(), value.dentry_version)))
    }

    fn remember_path_index_lookup(
        &self,
        index_key: &[u8],
        read_version: Version,
        entry: &DentryWithAttr,
        dentry_version: Version,
    ) -> Result<(), MetadError> {
        let key = self.path_index_lookup_cache_key(index_key, read_version);
        let shard_index = path_cache_shard_index(&key);
        let mut cache = self.path_index_lookup_cache[shard_index]
            .lock()
            .map_err(|err| {
                MetadataError::Backend(format!("metadata path-index lookup cache poisoned: {err}"))
            })?;
        if cache.len() >= PATH_INDEX_LOOKUP_CACHE_MAX_ENTRIES_PER_SHARD {
            cache.clear();
        }
        cache.insert(
            key,
            PathIndexLookupCacheValue {
                entry: entry.clone(),
                dentry_version,
            },
        );
        Ok(())
    }

    fn remember_validated_path_index(
        &self,
        index_key: &[u8],
        index_version: Version,
        read_version: Version,
        entry: &DentryWithAttr,
    ) -> Result<(), MetadError> {
        let key = self.path_index_validation_cache_key(index_key, index_version, read_version);
        let shard_index = path_cache_shard_index(&key);
        let mut cache = self.path_index_validation_cache[shard_index]
            .lock()
            .map_err(|err| {
                MetadataError::Backend(format!(
                    "metadata path-index validation cache poisoned: {err}"
                ))
            })?;
        if cache.len() >= PATH_INDEX_VALIDATION_CACHE_MAX_ENTRIES_PER_SHARD {
            cache.clear();
        }
        cache.insert(key, entry.clone());
        Ok(())
    }

    fn path_index_lookup_cache_key(
        &self,
        index_key: &[u8],
        read_version: Version,
    ) -> PathIndexLookupCacheKey {
        PathIndexLookupCacheKey {
            read_version: read_version.get(),
            index_key: index_key.to_vec(),
        }
    }

    fn path_index_validation_cache_key(
        &self,
        index_key: &[u8],
        index_version: Version,
        read_version: Version,
    ) -> PathIndexValidationCacheKey {
        PathIndexValidationCacheKey {
            read_version: read_version.get(),
            index_version: index_version.get(),
            index_key: index_key.to_vec(),
        }
    }

    #[cfg(test)]
    pub(super) fn clear_read_path_caches_for_test(&self) {
        for shard in &self.path_resolution_cache {
            shard.lock().unwrap().clear();
        }
        for shard in &self.path_index_lookup_cache {
            shard.lock().unwrap().clear();
        }
        for shard in &self.path_index_validation_cache {
            shard.lock().unwrap().clear();
        }
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
        if probe_path_index && root == InodeId::root() && components.len() > 1 {
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
        if let Some(cached) = self.cached_path_index_lookup(&key, version)? {
            self.path_index_hit_total.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(cached));
        }
        let Some(item) =
            self.metadata
                .get_versioned(RecordFamily::PathIndex, &key, version, purpose)?
        else {
            self.path_index_miss_total.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        if let Some(cached) = self.cached_validated_path_index(&key, item.version, version)? {
            self.path_index_hit_total.fetch_add(1, Ordering::Relaxed);
            return Ok(Some((cached, item.version)));
        }
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
            self.remember_path_index_lookup(&key, version, &canonical, canonical_version)?;
            self.remember_validated_path_index(&key, item.version, version, &canonical)?;
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
        let version = self.read_version()?;
        let entry = self
            .lookup_plus_at_version_for_purpose(parent, name, version, ReadPurpose::UserStrong)?
            .map(|(entry, _)| entry)
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let body = entry.body.ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version(entry.attr.inode, &body, 0, body.size as usize, version)
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
        let version = self.read_version()?;
        let Some(attr) =
            self.get_attr_at_version_for_purpose(inode, version, ReadPurpose::UserStrong)?
        else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if offset >= attr.size {
            return Ok(Vec::new());
        }
        let body = self
            .body_descriptor_at_version_for_purpose(
                inode,
                attr.generation,
                version,
                ReadPurpose::UserStrong,
            )?
            .ok_or(MetadError::NotFound)?;
        self.read_file_at_version(inode, &body, offset, len, version)
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
        let version = self.read_version()?;
        let Some(attr) =
            self.get_attr_at_version_for_purpose(inode, version, ReadPurpose::UserStrong)?
        else {
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
        let body = self
            .body_descriptor_at_version_for_purpose(
                inode,
                generation,
                version,
                ReadPurpose::UserStrong,
            )?
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

    pub fn read_path_plan(
        &self,
        path: &str,
        offset: u64,
        len: usize,
        expected_generation: Option<u64>,
    ) -> Result<PathReadPlan, MetadError> {
        let version = self.read_version()?;
        let entry = self
            .lookup_path_from_at_version_for_purpose(
                InodeId::root(),
                path,
                version,
                ReadPurpose::UserStrong,
            )?
            .map(|(entry, _)| entry)
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if let Some(expected) = expected_generation {
            if entry.attr.generation != expected {
                return Err(MetadError::StaleBodyGeneration {
                    expected,
                    current: entry.attr.generation,
                });
            }
        }
        let body = entry
            .body
            .clone()
            .ok_or(MetadError::MissingBodyDescriptor)?;
        let output_len = if offset >= entry.attr.size {
            0
        } else {
            len.min((entry.attr.size - offset) as usize)
        };
        let blocks = if output_len == 0 {
            Vec::new()
        } else {
            self.read_plan_for_purpose(
                entry.attr.inode,
                &body,
                offset,
                output_len,
                version,
                ReadPurpose::UserStrong,
            )?
        };
        Ok(PathReadPlan {
            metadata: PathMetadata {
                attr: entry.attr,
                body: Some(body),
            },
            plan: BodyReadPlan { output_len, blocks },
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
        let outcome = self.objects.read_blocks(cache, len, &plan)?;
        self.object_gets
            .fetch_add(outcome.object_gets as u64, Ordering::Relaxed);
        self.object_get_bytes
            .fetch_add(outcome.object_get_bytes, Ordering::Relaxed);
        self.cache_hits
            .fetch_add(outcome.cache_hits as u64, Ordering::Relaxed);
        self.cache_hit_bytes
            .fetch_add(outcome.cache_hit_bytes, Ordering::Relaxed);
        Ok(outcome.bytes)
    }

    pub fn read_session_object_blocks(
        &self,
        output_len: usize,
        blocks: &[ObjectReadBlock],
    ) -> Result<Vec<u8>, MetadError> {
        let cache = self.block_cache_enabled().then_some(&self.block_cache);
        let outcome = self.objects.read_blocks(cache, output_len, blocks)?;
        self.object_gets
            .fetch_add(outcome.object_gets as u64, Ordering::Relaxed);
        self.object_get_bytes
            .fetch_add(outcome.object_get_bytes, Ordering::Relaxed);
        self.cache_hits
            .fetch_add(outcome.cache_hits as u64, Ordering::Relaxed);
        self.cache_hit_bytes
            .fetch_add(outcome.cache_hit_bytes, Ordering::Relaxed);
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
            let slices = manifest
                .slices
                .iter()
                .map(|slice| StoredSlice {
                    slice_id: slice.slice_id,
                    logical_offset: slice.logical_offset,
                    len: slice.len,
                    chunks: vec![StoredChunk {
                        chunk_index: manifest.chunk_index,
                        logical_offset: manifest.logical_offset,
                        len: manifest.len,
                        blocks: slice
                            .blocks
                            .iter()
                            .map(|block| StoredBlock {
                                object_key: block.object_key.clone(),
                                logical_offset: block.logical_offset,
                                object_offset: block.object_offset,
                                len: block.len,
                                digest_uri: block.digest_uri.clone(),
                            })
                            .collect(),
                    }],
                })
                .collect::<Vec<_>>();
            let slice_plan = plan_slice_reads(&slices, offset, len)?;
            plan.extend(slice_plan.blocks);
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

fn path_index_child_name(
    prefix: &[u8],
    key: &[u8],
    common_prefix: bool,
) -> Result<DentryName, MetadError> {
    let mut suffix = key.strip_prefix(prefix).ok_or_else(|| {
        MetadError::Codec("path index scan returned a key outside the requested prefix".to_owned())
    })?;
    if common_prefix {
        suffix = suffix
            .strip_suffix(&[PATH_INDEX_DELIMITER])
            .ok_or_else(|| {
                MetadError::Codec("path index common prefix is missing delimiter".to_owned())
            })?;
    }
    if suffix.is_empty() || suffix.contains(&PATH_INDEX_DELIMITER) {
        return Err(MetadError::Codec(
            "path index scan returned a malformed child component".to_owned(),
        ));
    }
    DentryName::new(suffix.to_vec()).map_err(|err| MetadError::Codec(err.to_string()))
}

fn delimited_scan_marker(item: &DelimitedScanItem) -> Vec<u8> {
    match item {
        DelimitedScanItem::Key(item) => item.key.clone(),
        DelimitedScanItem::CommonPrefix(prefix) => prefix.clone(),
    }
}
