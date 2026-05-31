//! Metadata and legacy transaction key-error constructors.

use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::LockRecord;

pub fn empty_mutation_key() -> kvpb::KeyError {
    abort("percolator: empty key in mutation")
}

pub fn empty_commit_key() -> kvpb::KeyError {
    abort("percolator: empty key in commit")
}

pub fn empty_rollback_key() -> kvpb::KeyError {
    abort("percolator: empty key in rollback")
}

pub fn unsupported_mutation_op(op: i32) -> kvpb::KeyError {
    abort(&format!(
        "percolator: unsupported mutation op: {}",
        mutation_op_name(op)
    ))
}

pub fn invalid_atomic_mutate() -> kvpb::KeyError {
    abort("percolator: invalid atomic mutate")
}

pub fn atomic_predicate_mismatch() -> kvpb::KeyError {
    retryable("percolator: atomic predicate mismatch")
}

pub fn txn_heartbeat_validation(req: &kvpb::TxnHeartBeatRequest) -> Option<kvpb::KeyError> {
    if req.primary_key.is_empty() {
        return Some(abort("percolator: heartbeat primary key is required"));
    }
    if req.start_version == 0 {
        return Some(abort("percolator: heartbeat start version is required"));
    }
    if req.ttl_extension == 0 {
        return Some(abort("percolator: heartbeat ttl extension is required"));
    }
    if req.current_time == 0 {
        return Some(abort("percolator: heartbeat current time is required"));
    }
    None
}

pub fn txn_heartbeat_primary_mismatch() -> kvpb::KeyError {
    abort("percolator: heartbeat primary key does not match lock primary")
}

pub fn txn_already_rolled_back() -> kvpb::KeyError {
    retryable("percolator: transaction already rolled back")
}

pub fn txn_lock_not_found() -> kvpb::KeyError {
    retryable("percolator: lock not found")
}

pub fn abort(message: &str) -> kvpb::KeyError {
    kvpb::KeyError {
        abort: message.to_owned(),
        ..Default::default()
    }
}

pub fn retryable(message: &str) -> kvpb::KeyError {
    kvpb::KeyError {
        retryable: message.to_owned(),
        ..Default::default()
    }
}

pub fn locked(key: &[u8], lock: &LockRecord) -> kvpb::KeyError {
    kvpb::KeyError {
        locked: Some(kvpb::Locked {
            primary_lock: lock.primary.clone(),
            key: key.to_vec(),
            lock_version: lock.start_version,
            lock_ttl: lock.ttl,
            lock_type: lock.op as i32,
            min_commit_ts: lock.min_commit_ts,
        }),
        ..Default::default()
    }
}

pub fn write_conflict(
    key: &[u8],
    primary: &[u8],
    conflict_ts: u64,
    conflicting_start_ts: u64,
    current_ts: u64,
) -> kvpb::KeyError {
    kvpb::KeyError {
        write_conflict: Some(kvpb::WriteConflict {
            key: key.to_vec(),
            primary: primary.to_vec(),
            conflict_ts,
            commit_ts: current_ts,
            start_ts: conflicting_start_ts,
        }),
        ..Default::default()
    }
}

pub fn already_exists(key: &[u8]) -> kvpb::KeyError {
    kvpb::KeyError {
        already_exists: Some(kvpb::KeyAlreadyExists { key: key.to_vec() }),
        ..Default::default()
    }
}

pub fn commit_ts_expired(key: &[u8], commit_ts: u64, min_commit_ts: u64) -> kvpb::KeyError {
    kvpb::KeyError {
        commit_ts_expired: Some(kvpb::CommitTsExpired {
            key: key.to_vec(),
            commit_ts,
            min_commit_ts,
        }),
        ..Default::default()
    }
}

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

pub fn metadata_locked(key: &[u8], lock: &LockRecord) -> metadatapb::MetadataKeyError {
    metadatapb::MetadataKeyError {
        locked: Some(metadatapb::MetadataLocked {
            primary_lock: lock.primary.clone(),
            key: key.to_vec(),
            lock_version: lock.start_version,
            lock_ttl: lock.ttl,
        }),
        ..Default::default()
    }
}

pub fn metadata_write_conflict(
    key: &[u8],
    primary: &[u8],
    conflict_ts: u64,
    conflicting_start_ts: u64,
    current_ts: u64,
) -> metadatapb::MetadataKeyError {
    metadatapb::MetadataKeyError {
        write_conflict: Some(metadatapb::MetadataWriteConflict {
            key: key.to_vec(),
            primary: primary.to_vec(),
            conflict_ts,
            commit_ts: current_ts,
            start_ts: conflicting_start_ts,
        }),
        ..Default::default()
    }
}

pub fn metadata_already_exists(key: &[u8]) -> metadatapb::MetadataKeyError {
    metadatapb::MetadataKeyError {
        already_exists: Some(metadatapb::MetadataKeyAlreadyExists { key: key.to_vec() }),
        ..Default::default()
    }
}

fn mutation_op_name(op: i32) -> String {
    match kvpb::mutation::Op::try_from(op) {
        Ok(kvpb::mutation::Op::Put) => "Put".to_owned(),
        Ok(kvpb::mutation::Op::Delete) => "Delete".to_owned(),
        Ok(kvpb::mutation::Op::Lock) => "Lock".to_owned(),
        Ok(kvpb::mutation::Op::Rollback) => "Rollback".to_owned(),
        Err(_) => op.to_string(),
    }
}
