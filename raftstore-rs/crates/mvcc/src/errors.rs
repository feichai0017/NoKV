//! Percolator-compatible key-error constructors.

use nokv_proto::nokv::kv::v1 as kvpb;

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

pub fn abort(message: &str) -> kvpb::KeyError {
    kvpb::KeyError {
        abort: message.to_owned(),
        ..Default::default()
    }
}

pub fn predicate(predicate: &kvpb::AtomicPredicate) -> kvpb::KeyError {
    match kvpb::AtomicPredicateKind::try_from(predicate.kind)
        .unwrap_or(kvpb::AtomicPredicateKind::NotExists)
    {
        kvpb::AtomicPredicateKind::NotExists => already_exists(&predicate.key),
        _ => kvpb::KeyError {
            abort: "atomic predicate rejected".to_owned(),
            ..Default::default()
        },
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

pub fn write_conflict(key: &[u8], primary: &[u8], start_ts: u64, commit_ts: u64) -> kvpb::KeyError {
    kvpb::KeyError {
        write_conflict: Some(kvpb::WriteConflict {
            key: key.to_vec(),
            primary: primary.to_vec(),
            conflict_ts: commit_ts,
            commit_ts,
            start_ts,
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
