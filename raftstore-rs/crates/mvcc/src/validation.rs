//! Validation shared by in-memory and Holt-backed MVCC implementations.

use std::collections::HashSet;

use nokv_proto::nokv::kv::v1 as kvpb;

use crate::{errors, VersionedValue};

pub fn atomic_mutation(mutation: &kvpb::Mutation) -> Option<kvpb::KeyError> {
    if mutation.key.is_empty() {
        return Some(errors::empty_mutation_key());
    }
    match kvpb::mutation::Op::try_from(mutation.op) {
        Ok(kvpb::mutation::Op::Put | kvpb::mutation::Op::Delete) => None,
        _ => Some(errors::unsupported_mutation_op(mutation.op)),
    }
}

pub fn prewrite_mutation(mutation: &kvpb::Mutation) -> Option<kvpb::KeyError> {
    if mutation.key.is_empty() {
        return Some(errors::empty_mutation_key());
    }
    match kvpb::mutation::Op::try_from(mutation.op) {
        Ok(kvpb::mutation::Op::Put | kvpb::mutation::Op::Delete | kvpb::mutation::Op::Lock) => None,
        _ => Some(errors::unsupported_mutation_op(mutation.op)),
    }
}

pub fn commit_version(start_version: u64, commit_version: u64) -> Option<kvpb::KeyError> {
    (commit_version <= start_version).then(|| kvpb::KeyError {
        abort: "commit version must be greater than start version".to_owned(),
        ..Default::default()
    })
}

pub fn atomic_predicate_observation(
    predicate: &kvpb::AtomicPredicate,
    observed: Option<&[u8]>,
) -> Option<kvpb::KeyError> {
    if predicate.key.is_empty() {
        return Some(errors::empty_mutation_key());
    }
    match kvpb::AtomicPredicateKind::try_from(predicate.kind) {
        Ok(kvpb::AtomicPredicateKind::NotExists) => observed
            .is_some()
            .then(|| errors::already_exists(&predicate.key)),
        Ok(kvpb::AtomicPredicateKind::Exists) => {
            observed.is_none().then(errors::invalid_atomic_mutate)
        }
        Ok(kvpb::AtomicPredicateKind::ValueEquals) => (observed
            != Some(predicate.expected_value.as_slice()))
        .then(errors::atomic_predicate_mismatch),
        Err(_) => Some(errors::invalid_atomic_mutate()),
    }
}

pub fn atomic_mutation_matches_value(mutation: &kvpb::Mutation, value: &VersionedValue) -> bool {
    let Ok(op) = kvpb::mutation::Op::try_from(mutation.op) else {
        return false;
    };
    if value.kind != op {
        return false;
    }
    match op {
        kvpb::mutation::Op::Put => {
            value.value.as_deref() == Some(mutation.value.as_slice())
                && value.expires_at == mutation.expires_at
        }
        kvpb::mutation::Op::Delete => value.value.is_none(),
        _ => false,
    }
}

pub fn resolve_lock_keys(req: &kvpb::ResolveLockRequest) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(req.keys.len());
    let mut seen = HashSet::with_capacity(req.keys.len());
    for key in &req.keys {
        if key.is_empty() || !seen.insert(key.clone()) {
            continue;
        }
        keys.push(key.clone());
    }
    keys
}
