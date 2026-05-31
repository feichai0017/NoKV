use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::{
    blocking_lock, errors, read_committed, validation, write_by_start_version, Error, Inner,
    MetadataApplyResult, MetadataEngine, MvccStore, Result, VersionedValue,
};

impl MvccStore {
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
    fn commit_metadata(
        &self,
        command: &metadatapb::MetadataCommand,
        commit_version: u64,
    ) -> Result<MetadataApplyResult> {
        MvccStore::commit_metadata(self, command, commit_version)
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
