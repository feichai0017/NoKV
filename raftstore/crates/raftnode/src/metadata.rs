use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use prost::Message;

pub(crate) fn metadata_command_watch_keys(command: &metadatapb::MetadataCommand) -> Vec<Vec<u8>> {
    if !command.watch_keys.is_empty() {
        return command.watch_keys.clone();
    }
    command
        .mutations
        .iter()
        .map(|mutation| mutation.key.clone())
        .collect()
}

pub(crate) fn metadata_key_error_from_kv(error: kvpb::KeyError) -> metadatapb::MetadataKeyError {
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

pub(crate) fn metadata_get_response_from_kv(
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

pub(crate) fn metadata_scan_response_from_kv(
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

pub(crate) fn encode_metadata_response(
    response: &metadatapb::MetadataCommitResponse,
) -> nokv_mvcc::Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(response.encoded_len());
    response
        .encode(&mut payload)
        .map_err(|err| nokv_mvcc::Error::Backend(err.to_string()))?;
    Ok(payload)
}

pub(crate) fn decode_metadata_response(
    payload: &[u8],
) -> nokv_mvcc::Result<metadatapb::MetadataCommitResponse> {
    metadatapb::MetadataCommitResponse::decode(payload)
        .map_err(|err| nokv_mvcc::Error::Backend(err.to_string()))
}
