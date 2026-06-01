use nokv_proto::nokv::metadata::v1 as metadatapb;
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct PersistedMetadataResponseBatch {
    #[prost(bytes = "vec", repeated, tag = "1")]
    responses: Vec<Vec<u8>>,
}

pub(crate) fn metadata_command_watch_keys(command: &metadatapb::MetadataCommand) -> Vec<Vec<u8>> {
    if !command.watch_key_refs.is_empty() {
        return command
            .watch_key_refs
            .iter()
            .map(|key| key.key.clone())
            .collect();
    }
    if !command.watch_keys.is_empty() {
        return command.watch_keys.clone();
    }
    command
        .mutations
        .iter()
        .map(|mutation| mutation.key.clone())
        .collect()
}

pub(crate) fn encode_metadata_response(
    response: &metadatapb::MetadataCommitResponse,
) -> nokv_metadata_state::Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(response.encoded_len());
    response
        .encode(&mut payload)
        .map_err(|err| nokv_metadata_state::Error::Backend(err.to_string()))?;
    Ok(payload)
}

pub(crate) fn decode_metadata_response(
    payload: &[u8],
) -> nokv_metadata_state::Result<metadatapb::MetadataCommitResponse> {
    metadatapb::MetadataCommitResponse::decode(payload)
        .map_err(|err| nokv_metadata_state::Error::Backend(err.to_string()))
}

pub(crate) fn encode_metadata_response_batch(
    responses: &[metadatapb::MetadataCommitResponse],
) -> nokv_metadata_state::Result<Vec<u8>> {
    let mut encoded_responses = Vec::with_capacity(responses.len());
    for response in responses {
        encoded_responses.push(encode_metadata_response(response)?);
    }
    let persisted = PersistedMetadataResponseBatch {
        responses: encoded_responses,
    };
    let mut payload = Vec::with_capacity(persisted.encoded_len());
    persisted
        .encode(&mut payload)
        .map_err(|err| nokv_metadata_state::Error::Backend(err.to_string()))?;
    Ok(payload)
}

pub(crate) fn decode_metadata_response_batch(
    payload: &[u8],
) -> nokv_metadata_state::Result<Vec<metadatapb::MetadataCommitResponse>> {
    let persisted = PersistedMetadataResponseBatch::decode(payload)
        .map_err(|err| nokv_metadata_state::Error::Backend(err.to_string()))?;
    persisted
        .responses
        .iter()
        .map(|payload| decode_metadata_response(payload))
        .collect()
}
