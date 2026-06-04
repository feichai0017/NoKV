use super::*;

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn snapshot_subtree(&self, root: InodeId) -> Result<SnapshotPin, MetadError> {
        let Some(attr) = self.get_attr(root)? else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        let created_version = self.next_version()?;
        let read_version = predecessor(created_version)?;
        let pin = SnapshotPin {
            snapshot_id: created_version.get(),
            root,
            read_version: read_version.get(),
            created_version: created_version.get(),
        };
        let key = snapshot_pin_key(self.mount, pin.snapshot_id);
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"snapshot-subtree", self.mount, root, created_version),
            kind: CommandKind::SnapshotSubtree,
            read_version,
            commit_version: created_version,
            primary_family: RecordFamily::Snapshot,
            primary_key: key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Snapshot,
                key: key.clone(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Snapshot,
                key,
                op: MutationOp::Put,
                value: Some(Value(encode_snapshot_pin(&pin))),
            }],
            watch: Vec::new(),
        })?;
        Ok(pin)
    }

    pub fn snapshot_subtree_path(&self, path: &str) -> Result<SnapshotPin, MetadError> {
        let root = self.resolve_directory_path(path)?;
        self.snapshot_subtree(root)
    }

    pub fn retire_snapshot(&self, snapshot_id: u64) -> Result<bool, MetadError> {
        let key = snapshot_pin_key(self.mount, snapshot_id);
        if self.snapshot_pin(snapshot_id)?.is_none() {
            return Ok(false);
        }
        let version = self.next_version()?;
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"retire-snapshot", self.mount, InodeId::root(), version),
            kind: CommandKind::RetireSnapshot,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Snapshot,
            primary_key: key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Snapshot,
                key: key.clone(),
                predicate: Predicate::Exists,
            }],
            mutations: vec![delete_mutation(RecordFamily::Snapshot, key)],
            watch: Vec::new(),
        })?;
        Ok(true)
    }

    pub fn snapshot_pin(&self, snapshot_id: u64) -> Result<Option<SnapshotPin>, MetadError> {
        let value = self.metadata.get(
            RecordFamily::Snapshot,
            &snapshot_pin_key(self.mount, snapshot_id),
            self.read_version()?,
            ReadPurpose::UserStrong,
        )?;
        value
            .map(|value| {
                decode_snapshot_pin(&value.0).map_err(|err| MetadError::Codec(err.to_string()))
            })
            .transpose()
    }

    pub fn get_attr_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> Result<Option<InodeAttr>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        self.get_attr_at_version(inode, version)
    }

    pub fn lookup_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        self.lookup_plus_at_version(parent, name, version)
            .map(|entry| entry.map(|(entry, _)| entry))
    }

    pub fn read_dir_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        self.read_dir_plus_at_version(parent, version)
    }

    pub fn read_file_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, MetadError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let version = self.snapshot_read_version(snapshot_id)?;
        let Some(attr) = self.get_attr_at_version(inode, version)? else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if offset >= attr.size {
            return Ok(Vec::new());
        }
        let body = self
            .body_descriptor_at_version(inode, attr.generation, version)?
            .ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version(inode, &body, offset, len, version)
    }

    pub fn read_artifact_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Vec<u8>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        let entry = self
            .lookup_plus_at_version(parent, name, version)?
            .map(|(entry, _)| entry)
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let body = entry.body.ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version(entry.attr.inode, &body, 0, body.size as usize, version)
    }

    pub fn read_artifact_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Vec<u8>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        let mut components = parse_absolute_path(path)?;
        let name = components
            .pop()
            .ok_or_else(|| MetadError::InvalidPath("root has no file body".to_owned()))?;
        let parent = self.resolve_components_as_directory_at_version(&components, version)?;
        let entry = self
            .lookup_plus_at_version(parent, &name, version)?
            .map(|(entry, _)| entry)
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let body = entry.body.ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version(entry.attr.inode, &body, 0, body.size as usize, version)
    }

    pub(super) fn snapshot_read_version(&self, snapshot_id: u64) -> Result<Version, MetadError> {
        let pin = self
            .snapshot_pin(snapshot_id)?
            .ok_or(MetadError::NotFound)?;
        Version::new(pin.read_version).map_err(Into::into)
    }
}
