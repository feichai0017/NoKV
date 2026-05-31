use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::{
    blocking_lock, errors, read_committed, validation, write_by_start_version, Error, Inner,
    MetadataApplyResult, MetadataEngine, MvccStore, Result, VersionedValue,
};

impl MvccStore {
    pub fn get_metadata(
        &self,
        req: &metadatapb::MetadataGetRequest,
    ) -> Result<metadatapb::MetadataGetResponse> {
        Ok(metadata_get_response_from_kv(self.get(
            &kvpb::GetRequest {
                key: req.key.clone(),
                version: req.version,
            },
        )?))
    }

    pub fn batch_get_metadata(
        &self,
        req: &metadatapb::MetadataBatchGetRequest,
    ) -> Result<metadatapb::MetadataBatchGetResponse> {
        if req.requests.is_empty() {
            return Ok(metadatapb::MetadataBatchGetResponse::default());
        }
        let response = self.batch_get(&kvpb::BatchGetRequest {
            requests: req
                .requests
                .iter()
                .map(|request| kvpb::GetRequest {
                    key: request.key.clone(),
                    version: request.version,
                })
                .collect(),
        })?;
        Ok(metadatapb::MetadataBatchGetResponse {
            responses: response
                .responses
                .into_iter()
                .map(metadata_get_response_from_kv)
                .collect(),
            region_error: None,
        })
    }

    pub fn scan_metadata(
        &self,
        req: &metadatapb::MetadataScanRequest,
    ) -> Result<metadatapb::MetadataScanResponse> {
        if req.reverse {
            return Err(Error::Backend(
                "metadata reverse scans are not supported".to_owned(),
            ));
        }
        Ok(metadata_scan_response_from_kv(self.scan(
            &kvpb::ScanRequest {
                start_key: req.start_key.clone(),
                limit: req.limit,
                version: req.version,
                include_start: req.include_start,
                reverse: false,
            },
        )?))
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

impl MetadataEngine for MvccStore {
    fn get_metadata(
        &self,
        req: &metadatapb::MetadataGetRequest,
    ) -> Result<metadatapb::MetadataGetResponse> {
        MvccStore::get_metadata(self, req)
    }

    fn batch_get_metadata(
        &self,
        req: &metadatapb::MetadataBatchGetRequest,
    ) -> Result<metadatapb::MetadataBatchGetResponse> {
        MvccStore::batch_get_metadata(self, req)
    }

    fn scan_metadata(
        &self,
        req: &metadatapb::MetadataScanRequest,
    ) -> Result<metadatapb::MetadataScanResponse> {
        MvccStore::scan_metadata(self, req)
    }

    fn commit_metadata(
        &self,
        command: &metadatapb::MetadataCommand,
        commit_version: u64,
    ) -> Result<MetadataApplyResult> {
        MvccStore::commit_metadata(self, command, commit_version)
    }
}

pub fn metadata_key_error_from_kv(error: kvpb::KeyError) -> metadatapb::MetadataKeyError {
    metadatapb::MetadataKeyError {
        locked: error.locked.map(|locked| metadatapb::MetadataLocked {
            primary_lock: locked.primary_lock,
            key: locked.key,
            lock_version: locked.lock_version,
            lock_ttl: locked.lock_ttl,
        }),
        write_conflict: error
            .write_conflict
            .map(|conflict| metadatapb::MetadataWriteConflict {
                key: conflict.key,
                primary: conflict.primary,
                conflict_ts: conflict.conflict_ts,
                commit_ts: conflict.commit_ts,
                start_ts: conflict.start_ts,
            }),
        already_exists: error
            .already_exists
            .map(|exists| metadatapb::MetadataKeyAlreadyExists { key: exists.key }),
        retryable: error.retryable,
        abort: error.abort,
    }
}

pub fn metadata_get_response_from_kv(
    response: kvpb::GetResponse,
) -> metadatapb::MetadataGetResponse {
    metadatapb::MetadataGetResponse {
        kv: (!response.not_found && response.error.is_none()).then(|| metadatapb::MetadataKv {
            value: response.value,
            expires_at: response.expires_at,
            ..Default::default()
        }),
        not_found: response.not_found,
        error: response.error.map(metadata_key_error_from_kv),
        region_error: None,
    }
}

pub fn metadata_scan_response_from_kv(
    response: kvpb::ScanResponse,
) -> metadatapb::MetadataScanResponse {
    metadatapb::MetadataScanResponse {
        kvs: response
            .kvs
            .into_iter()
            .map(|kv| metadatapb::MetadataKv {
                key: kv.key,
                value: kv.value,
                version: kv.version,
                expires_at: kv.expires_at,
            })
            .collect(),
        error: response.error.map(metadata_key_error_from_kv),
        region_error: None,
    }
}

fn commit_metadata_inner(
    inner: &mut Inner,
    command: &metadatapb::MetadataCommand,
    commit_version: u64,
) -> MetadataApplyResult {
    if let Some(error) = validation::commit_version(command.read_version, commit_version) {
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
        if let Some(lock) = blocking_lock(inner, &predicate.key, read_version) {
            return metadata_error(
                command,
                commit_version,
                errors::locked(&predicate.key, lock),
            );
        }
        let observed =
            read_committed(inner, &predicate.key, read_version).and_then(|value| value.value);
        if let Some(error) =
            validation::metadata_predicate_observation(predicate, observed.as_deref())
        {
            return metadata_error(command, commit_version, error);
        }
    }
    let primary = command
        .mutations
        .first()
        .map(|mutation| mutation.key.as_slice())
        .unwrap_or_default();
    for mutation in &command.mutations {
        if let Some(error) = validation::metadata_mutation(mutation) {
            return metadata_error(command, commit_version, error);
        }
        if let Some(lock) = inner.locks.get(&mutation.key) {
            return metadata_error(command, commit_version, errors::locked(&mutation.key, lock));
        }
        if let Some((commit_ts, value)) = inner
            .writes
            .get(&mutation.key)
            .and_then(|versions| versions.range(command.read_version..).next())
        {
            return metadata_error(
                command,
                commit_version,
                errors::write_conflict(
                    &mutation.key,
                    primary,
                    *commit_ts,
                    value.start_version,
                    command.read_version,
                ),
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
                errors::already_exists(&mutation.key),
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
        inner.locks.remove(&mutation.key);
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
    error: kvpb::KeyError,
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
        metadatapb::metadata_mutation::Op::Put => kvpb::mutation::Op::Put,
        metadatapb::metadata_mutation::Op::Delete => kvpb::mutation::Op::Delete,
    };
    VersionedValue {
        kind: op,
        start_version,
        value: (op == kvpb::mutation::Op::Put).then(|| mutation.value.clone()),
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
