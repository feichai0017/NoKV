//! Metadata key-error constructors.

use nokv_proto::nokv::metadata::v1 as metadatapb;

pub fn metadata_empty_mutation_key() -> metadatapb::MetadataKeyError {
    metadata_abort("metadata: empty key in mutation")
}

pub fn metadata_unsupported_mutation_op(op: i32) -> metadatapb::MetadataKeyError {
    metadata_abort(&format!("metadata: unsupported mutation op: {op}"))
}

pub fn metadata_invalid_mutate() -> metadatapb::MetadataKeyError {
    metadata_abort("metadata: invalid mutate")
}

pub fn metadata_predicate_mismatch() -> metadatapb::MetadataKeyError {
    metadata_retryable("metadata: predicate mismatch")
}

pub fn metadata_commit_version_expired() -> metadatapb::MetadataKeyError {
    metadata_abort("commit version must be greater than start version")
}

pub fn metadata_abort(message: &str) -> metadatapb::MetadataKeyError {
    metadatapb::MetadataKeyError {
        abort: message.to_owned(),
        ..Default::default()
    }
}

pub fn metadata_retryable(message: &str) -> metadatapb::MetadataKeyError {
    metadatapb::MetadataKeyError {
        retryable: message.to_owned(),
        ..Default::default()
    }
}

pub fn metadata_revision_conflict(
    key: &[u8],
    conflict_version: u64,
    read_version: u64,
) -> metadatapb::MetadataKeyError {
    metadata_retryable(&format!(
        "metadata: revision conflict key_len={} conflict_version={} read_version={}",
        key.len(),
        conflict_version,
        read_version
    ))
}

pub fn metadata_already_exists(key: &[u8]) -> metadatapb::MetadataKeyError {
    metadatapb::MetadataKeyError {
        already_exists: Some(metadatapb::MetadataKeyAlreadyExists { key: key.to_vec() }),
        ..Default::default()
    }
}
