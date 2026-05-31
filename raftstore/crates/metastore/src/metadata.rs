use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::{
    errors, read_committed, scan_key_matches_start, scan_limit, scan_read_version, validation,
    value_is_expired, write_by_start_version, Error, Inner, MemoryMetadataStore,
    MetadataApplyResult, MetadataEngine, Result, ValueKind, VersionedValue,
};

impl MemoryMetadataStore {
    pub fn get_metadata(
        &self,
        req: &metadatapb::MetadataGetRequest,
    ) -> Result<metadatapb::MetadataGetResponse> {
        let inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        Ok(match read_committed(&inner, &req.key, req.version) {
            Some(value) => {
                if value_is_expired(value.expires_at) {
                    return Ok(metadatapb::MetadataGetResponse {
                        not_found: true,
                        ..Default::default()
                    });
                }
                let expires_at = value.expires_at;
                let bytes = value.value;
                let not_found = bytes.is_none();
                metadatapb::MetadataGetResponse {
                    kv: bytes.map(|value| metadatapb::MetadataKv {
                        value,
                        expires_at,
                        ..Default::default()
                    }),
                    not_found,
                    ..Default::default()
                }
            }
            None => metadatapb::MetadataGetResponse {
                not_found: true,
                ..Default::default()
            },
        })
    }

    pub fn batch_get_metadata(
        &self,
        req: &metadatapb::MetadataBatchGetRequest,
    ) -> Result<metadatapb::MetadataBatchGetResponse> {
        if req.requests.is_empty() {
            return Ok(metadatapb::MetadataBatchGetResponse::default());
        }
        let mut responses = Vec::with_capacity(req.requests.len());
        for request in &req.requests {
            responses.push(self.get_metadata(request)?);
        }
        Ok(metadatapb::MetadataBatchGetResponse {
            responses,
            region_error: None,
        })
    }

    pub fn scan_metadata(
        &self,
        req: &metadatapb::MetadataScanRequest,
    ) -> Result<metadatapb::MetadataScanResponse> {
        let inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        let read_version = scan_read_version(req.version);
        let limit = scan_limit(req.limit);
        let mut kvs = Vec::new();
        let mut keys = inner.writes.keys().collect::<Vec<_>>();
        if req.reverse {
            keys.reverse();
        }
        for key in keys {
            if !scan_key_matches_start(key, &req.start_key, req.include_start, req.reverse) {
                continue;
            }
            if let Some(value) = read_committed(&inner, key, read_version) {
                if value_is_expired(value.expires_at) {
                    continue;
                }
                let expires_at = value.expires_at;
                if let Some(bytes) = value.value {
                    kvs.push(metadatapb::MetadataKv {
                        key: key.clone(),
                        value: bytes,
                        version: read_version,
                        expires_at,
                    });
                    if kvs.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(metadatapb::MetadataScanResponse {
            kvs,
            ..Default::default()
        })
    }

    pub fn commit_metadata(
        &self,
        command: &metadatapb::MetadataCommand,
        commit_version: u64,
    ) -> Result<MetadataApplyResult> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        Ok(commit_metadata_inner(&mut inner, command, commit_version))
    }
}

impl MetadataEngine for MemoryMetadataStore {
    fn get_metadata(
        &self,
        req: &metadatapb::MetadataGetRequest,
    ) -> Result<metadatapb::MetadataGetResponse> {
        MemoryMetadataStore::get_metadata(self, req)
    }

    fn batch_get_metadata(
        &self,
        req: &metadatapb::MetadataBatchGetRequest,
    ) -> Result<metadatapb::MetadataBatchGetResponse> {
        MemoryMetadataStore::batch_get_metadata(self, req)
    }

    fn scan_metadata(
        &self,
        req: &metadatapb::MetadataScanRequest,
    ) -> Result<metadatapb::MetadataScanResponse> {
        MemoryMetadataStore::scan_metadata(self, req)
    }

    fn commit_metadata(
        &self,
        command: &metadatapb::MetadataCommand,
        commit_version: u64,
    ) -> Result<MetadataApplyResult> {
        MemoryMetadataStore::commit_metadata(self, command, commit_version)
    }
}

fn commit_metadata_inner(
    inner: &mut Inner,
    command: &metadatapb::MetadataCommand,
    commit_version: u64,
) -> MetadataApplyResult {
    if let Some(error) = validation::metadata_commit_version(command.read_version, commit_version) {
        return metadata_error(command, commit_version, error);
    }
    if metadata_already_applied(inner, command, commit_version) {
        return MetadataApplyResult {
            commit_version,
            applied_mutations: command.mutations.len() as u64,
            error: None,
        };
    }
    for predicate in &command.predicates {
        let read_version = if predicate.read_version == 0 {
            command.read_version
        } else {
            predicate.read_version
        };
        let observed =
            read_committed(inner, &predicate.key, read_version).and_then(|value| value.value);
        if let Some(error) =
            validation::metadata_command_predicate_observation(predicate, observed.as_deref())
        {
            return metadata_error(command, commit_version, error);
        }
    }
    for mutation in &command.mutations {
        if let Some(error) = validation::metadata_command_mutation(mutation) {
            return metadata_error(command, commit_version, error);
        }
        if let Some((commit_ts, _)) = inner
            .writes
            .get(&mutation.key)
            .and_then(|versions| versions.range(command.read_version..).next())
        {
            return metadata_error(
                command,
                commit_version,
                errors::metadata_revision_conflict(&mutation.key, *commit_ts, command.read_version),
            );
        }
        if mutation.assertion_not_exist
            && read_committed(inner, &mutation.key, command.read_version)
                .and_then(|value| value.value)
                .is_some()
        {
            return metadata_error(
                command,
                commit_version,
                errors::metadata_already_exists(&mutation.key),
            );
        }
    }
    for mutation in &command.mutations {
        let value = metadata_mutation_value(mutation, command.read_version);
        inner
            .writes
            .entry(mutation.key.clone())
            .or_default()
            .insert(commit_version, value);
    }
    MetadataApplyResult {
        commit_version,
        applied_mutations: command.mutations.len() as u64,
        error: None,
    }
}

fn metadata_error(
    _command: &metadatapb::MetadataCommand,
    commit_version: u64,
    error: metadatapb::MetadataKeyError,
) -> MetadataApplyResult {
    MetadataApplyResult {
        commit_version,
        applied_mutations: 0,
        error: Some(error),
    }
}

fn metadata_already_applied(
    inner: &Inner,
    command: &metadatapb::MetadataCommand,
    commit_version: u64,
) -> bool {
    let mut any_present = false;
    let mut all_present = true;
    for mutation in &command.mutations {
        let Some((existing_commit, value)) =
            write_by_start_version(inner, &mutation.key, command.read_version)
        else {
            all_present = false;
            continue;
        };
        any_present = true;
        if existing_commit != commit_version || !metadata_mutation_matches_value(mutation, &value) {
            return false;
        }
    }
    any_present && all_present
}

pub fn metadata_mutation_value(
    mutation: &metadatapb::MetadataMutation,
    start_version: u64,
) -> VersionedValue {
    let op = match metadatapb::metadata_mutation::Op::try_from(mutation.op)
        .unwrap_or(metadatapb::metadata_mutation::Op::Put)
    {
        metadatapb::metadata_mutation::Op::Put => ValueKind::Put,
        metadatapb::metadata_mutation::Op::Delete => ValueKind::Delete,
    };
    VersionedValue {
        kind: op,
        start_version,
        value: (op == ValueKind::Put).then(|| mutation.value.clone()),
        expires_at: mutation.expires_at,
    }
}

pub fn metadata_mutation_matches_value(
    mutation: &metadatapb::MetadataMutation,
    value: &VersionedValue,
) -> bool {
    let expected = metadata_mutation_value(mutation, value.start_version);
    value.kind == expected.kind
        && value.value == expected.value
        && value.expires_at == expected.expires_at
}
