use super::*;

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn cleanup_staged_objects(
        &self,
        staged: &StagedObjectSet,
    ) -> Result<ObjectCleanupOutcome, MetadError> {
        self.objects.delete_staged(staged).map_err(Into::into)
    }

    pub fn cleanup_pending_objects(
        &self,
        limit: usize,
    ) -> Result<PendingObjectCleanupOutcome, MetadError> {
        let version = self.read_version()?;
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(self.mount),
            start_after: None,
            version,
            limit,
            purpose: ReadPurpose::UserStrong,
        })?;
        if rows.is_empty() {
            return Ok(PendingObjectCleanupOutcome::default());
        }
        let retention_floor = self.history_retention_floor()?;

        let mut outcome = PendingObjectCleanupOutcome {
            scanned: rows.len(),
            blocked_by_snapshots: 0,
            attempted: 0,
            deleted: 0,
            missing: 0,
            records_removed: 0,
        };
        let mut cleaned_keys = Vec::with_capacity(rows.len());
        for row in rows {
            let record = decode_object_gc_record(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            if retention_floor.is_some_and(|floor| floor.get() < record.enqueue_version) {
                outcome.blocked_by_snapshots += 1;
                continue;
            }
            let key = ObjectKey::new(record.object_key)?;
            outcome.attempted += 1;
            if self.objects.delete(&key)? {
                outcome.deleted += 1;
            } else {
                outcome.missing += 1;
            }
            cleaned_keys.push(row.key);
        }

        if cleaned_keys.is_empty() {
            return Ok(outcome);
        }

        let commit_version = self.next_version()?;
        let records_removed = cleaned_keys.len();
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"cleanup-objects",
                self.mount,
                InodeId::root(),
                commit_version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(commit_version)?,
            commit_version,
            primary_family: RecordFamily::Gc,
            primary_key: gc_queue_prefix(self.mount),
            predicates: Vec::new(),
            mutations: cleaned_keys
                .into_iter()
                .map(|key| delete_mutation(RecordFamily::Gc, key))
                .collect(),
            watch: Vec::new(),
        })?;
        outcome.records_removed = records_removed;
        Ok(outcome)
    }

    pub fn cleanup_history(&self, limit: usize) -> Result<HistoryPruneOutcome, MetadError> {
        let retain_from = self.history_retention_floor()?;
        self.metadata
            .prune_history(HistoryPruneRequest { retain_from, limit })
            .map_err(Into::into)
    }

    pub(super) fn history_retention_floor(&self) -> Result<Option<Version>, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Snapshot,
            prefix: snapshot_pin_prefix(self.mount),
            start_after: None,
            version: self.read_version()?,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })?;
        rows.into_iter()
            .map(|row| {
                let pin = decode_snapshot_pin(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                Version::new(pin.read_version).map_err(MetadError::from)
            })
            .try_fold(None, |floor: Option<Version>, version| {
                let version = version?;
                Ok(Some(floor.map_or(version, |floor| floor.min(version))))
            })
    }

    pub(super) fn chunk_manifest_delete_and_gc_mutations(
        &self,
        inode: InodeId,
        generation: u64,
        enqueue_version: Version,
        retained_object_keys: &HashSet<String>,
    ) -> Result<Vec<Mutation>, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::ChunkManifest,
            prefix: chunk_manifest_prefix(self.mount, inode, generation),
            start_after: None,
            version: self.read_version()?,
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        let mut mutations = Vec::new();
        for row in rows {
            if chunk_index_from_manifest_key(&row.key)? != BODY_SUMMARY_CHUNK_INDEX {
                let manifest = decode_chunk_manifest(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                for (block_index, block) in manifest
                    .slices
                    .iter()
                    .flat_map(|slice| slice.blocks.iter())
                    .enumerate()
                {
                    if retained_object_keys.contains(&block.object_key) {
                        continue;
                    }
                    let record = ObjectGcRecord {
                        inode,
                        generation,
                        object_key: block.object_key.clone(),
                        size: block.len,
                        digest_uri: block.digest_uri.clone(),
                        enqueue_version: enqueue_version.get(),
                    };
                    mutations.push(Mutation {
                        family: RecordFamily::Gc,
                        key: gc_object_key(
                            self.mount,
                            enqueue_version.get(),
                            inode,
                            generation,
                            manifest.chunk_index,
                            block_index as u64,
                        ),
                        op: MutationOp::Put,
                        value: Some(Value(encode_object_gc_record(&record))),
                    });
                }
            }
            mutations.push(delete_mutation(RecordFamily::ChunkManifest, row.key));
        }
        Ok(mutations)
    }
}
