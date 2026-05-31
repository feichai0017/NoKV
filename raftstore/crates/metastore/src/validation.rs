//! Validation shared by in-memory and Holt-backed metadata-store implementations.

use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::errors;

pub fn metadata_command_mutation(
    mutation: &metadatapb::MetadataMutation,
) -> Option<metadatapb::MetadataKeyError> {
    if mutation.key.is_empty() {
        return Some(errors::metadata_empty_mutation_key());
    }
    match metadatapb::metadata_mutation::Op::try_from(mutation.op) {
        Ok(metadatapb::metadata_mutation::Op::Put | metadatapb::metadata_mutation::Op::Delete) => {
            None
        }
        _ => Some(errors::metadata_unsupported_mutation_op(mutation.op)),
    }
}

pub fn metadata_commit_version(
    start_version: u64,
    commit_version: u64,
) -> Option<metadatapb::MetadataKeyError> {
    (commit_version <= start_version).then(errors::metadata_commit_version_expired)
}

pub fn metadata_command_predicate_observation(
    predicate: &metadatapb::MetadataPredicate,
    observed: Option<&[u8]>,
) -> Option<metadatapb::MetadataKeyError> {
    if predicate.key.is_empty() {
        return Some(errors::metadata_empty_mutation_key());
    }
    match metadatapb::MetadataPredicateKind::try_from(predicate.kind) {
        Ok(metadatapb::MetadataPredicateKind::NotExists) => observed
            .is_some()
            .then(|| errors::metadata_already_exists(&predicate.key)),
        Ok(metadatapb::MetadataPredicateKind::Exists) => {
            observed.is_none().then(errors::metadata_invalid_mutate)
        }
        Ok(metadatapb::MetadataPredicateKind::ValueEquals) => (observed
            != Some(predicate.expected_value.as_slice()))
        .then(errors::metadata_predicate_mismatch),
        Err(_) => Some(errors::metadata_invalid_mutate()),
    }
}
