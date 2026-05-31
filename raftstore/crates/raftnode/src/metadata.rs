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

pub(crate) fn encode_metadata_response(
    response: &metadatapb::MetadataCommitResponse,
) -> nokv_metastore::Result<Vec<u8>> {
    let mut payload = Vec::with_capacity(response.encoded_len());
    response
        .encode(&mut payload)
        .map_err(|err| nokv_metastore::Error::Backend(err.to_string()))?;
    Ok(payload)
}

pub(crate) fn decode_metadata_response(
    payload: &[u8],
) -> nokv_metastore::Result<metadatapb::MetadataCommitResponse> {
    metadatapb::MetadataCommitResponse::decode(payload)
        .map_err(|err| nokv_metastore::Error::Backend(err.to_string()))
}
