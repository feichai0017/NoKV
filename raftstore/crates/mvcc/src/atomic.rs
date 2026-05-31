use nokv_proto::nokv::kv::v1 as kvpb;

use crate::{
    apply_mutation, blocking_lock, errors, read_committed, validation, write_by_start_version,
    Error, Inner, MvccStore, Result,
};

impl MvccStore {
    pub fn try_atomic_mutate(
        &self,
        req: &kvpb::TryAtomicMutateRequest,
    ) -> Result<kvpb::TryAtomicMutateResponse> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        if let Some(error) = validation::commit_version(req.start_version, req.commit_version) {
            return Ok(kvpb::TryAtomicMutateResponse {
                error: Some(error),
                ..Default::default()
            });
        }
        if atomic_mutate_already_applied(&inner, req) {
            return Ok(kvpb::TryAtomicMutateResponse {
                applied_keys: req.mutations.len() as u64,
                ..Default::default()
            });
        }
        for predicate in &req.predicates {
            if predicate.key.is_empty() {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(errors::empty_mutation_key()),
                    ..Default::default()
                });
            }
            let read_version = if predicate.read_version == 0 {
                req.start_version
            } else {
                predicate.read_version
            };
            if let Some(lock) = blocking_lock(&inner, &predicate.key, read_version) {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(errors::locked(&predicate.key, lock)),
                    ..Default::default()
                });
            }
            let observed =
                read_committed(&inner, &predicate.key, read_version).and_then(|value| value.value);
            if let Some(error) =
                validation::atomic_predicate_observation(predicate, observed.as_deref())
            {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(error),
                    ..Default::default()
                });
            }
        }
        let primary = req
            .mutations
            .first()
            .map(|mutation| mutation.key.as_slice())
            .unwrap_or_default();
        for mutation in &req.mutations {
            if let Some(error) = validation::atomic_mutation(mutation) {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(error),
                    ..Default::default()
                });
            }
            if let Some(lock) = inner.locks.get(&mutation.key) {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(errors::locked(&mutation.key, lock)),
                    ..Default::default()
                });
            }
            if let Some((commit_ts, value)) = inner
                .writes
                .get(&mutation.key)
                .and_then(|versions| versions.range(req.start_version..).next())
            {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(errors::write_conflict(
                        &mutation.key,
                        primary,
                        *commit_ts,
                        value.start_version,
                        req.start_version,
                    )),
                    ..Default::default()
                });
            }
            if mutation.assertion_not_exist
                && read_committed(&inner, &mutation.key, req.start_version)
                    .and_then(|value| value.value)
                    .is_some()
            {
                return Ok(kvpb::TryAtomicMutateResponse {
                    error: Some(errors::already_exists(&mutation.key)),
                    ..Default::default()
                });
            }
        }
        let applied = req.mutations.len() as u64;
        for mutation in &req.mutations {
            apply_mutation(&mut inner, mutation, req.start_version, req.commit_version);
        }
        Ok(kvpb::TryAtomicMutateResponse {
            applied_keys: applied,
            ..Default::default()
        })
    }
}

fn atomic_mutate_already_applied(inner: &Inner, req: &kvpb::TryAtomicMutateRequest) -> bool {
    let mut any_present = false;
    let mut all_present = true;
    for mutation in &req.mutations {
        let Some((commit_version, value)) =
            write_by_start_version(inner, &mutation.key, req.start_version)
        else {
            all_present = false;
            continue;
        };
        any_present = true;
        if commit_version != req.commit_version
            || !validation::atomic_mutation_matches_value(mutation, &value)
        {
            return false;
        }
    }
    any_present && all_present
}
