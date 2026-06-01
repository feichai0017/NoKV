use holt::RangeEntry;
use nokv_metadata_state as metadata_state;
use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::codec::{decode_value, encode_value};
use crate::metrics;
use crate::store::to_backend_error;
use crate::trees::{
    current_tree_for_family, decode_history_key, history_key, history_prefix, HISTORY_TREE,
};
use crate::HoltMetadataStore;

impl HoltMetadataStore {
    pub(crate) fn read_committed(
        &self,
        family: metadatapb::MetadataFamily,
        key: &[u8],
        version: u64,
    ) -> metadata_state::Result<Option<(u64, metadata_state::VersionedValue)>> {
        if let Some(raw) = self
            .store
            .current(family)
            .map_err(to_backend_error)?
            .get(key)
            .map_err(to_backend_error)?
        {
            let (commit_ts, value) = decode_current_value(&raw)?;
            if commit_ts <= version {
                metrics::record_current_hit();
                return Ok(Some((commit_ts, value)));
            }
        }

        metrics::record_history_lookup();
        let prefix = history_prefix(family, key);
        for entry in self
            .store
            .history()
            .map_err(to_backend_error)?
            .range()
            .prefix(&prefix)
        {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((_family, _user_key, commit_ts)) = decode_history_key(&key)? else {
                continue;
            };
            if commit_ts <= version {
                return Ok(Some((commit_ts, decode_value(&value)?)));
            }
        }
        Ok(None)
    }

    pub(crate) fn write_by_start_version(
        &self,
        family: metadatapb::MetadataFamily,
        key: &[u8],
        start_version: u64,
    ) -> metadata_state::Result<Option<(u64, metadata_state::VersionedValue)>> {
        let prefix = history_prefix(family, key);
        for entry in self
            .store
            .history()
            .map_err(to_backend_error)?
            .range()
            .prefix(&prefix)
        {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((_family, _user_key, commit_ts)) = decode_history_key(&key)? else {
                continue;
            };
            let decoded = decode_value(&value)?;
            if decoded.start_version == start_version {
                return Ok(Some((commit_ts, decoded)));
            }
        }
        Ok(None)
    }

    pub(crate) fn first_write_after_or_at(
        &self,
        family: metadatapb::MetadataFamily,
        key: &[u8],
        version: u64,
    ) -> metadata_state::Result<Option<(u64, metadata_state::VersionedValue)>> {
        let prefix = history_prefix(family, key);
        let mut best = None;
        for entry in self
            .store
            .history()
            .map_err(to_backend_error)?
            .range()
            .prefix(&prefix)
        {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((_family, _user_key, commit_ts)) = decode_history_key(&key)? else {
                continue;
            };
            if commit_ts >= version && best.as_ref().is_none_or(|(ts, _)| commit_ts < *ts) {
                best = Some((commit_ts, decode_value(&value)?));
            }
        }
        Ok(best)
    }
}

pub(crate) fn apply_committed(
    batch: &mut holt::DBAtomicBatch,
    family: metadatapb::MetadataFamily,
    key: &[u8],
    commit_ts: u64,
    value: &metadata_state::VersionedValue,
) {
    let encoded = encode_value(value);
    batch.put(HISTORY_TREE, &history_key(family, key, commit_ts), &encoded);
    batch.put(
        current_tree_for_family(family),
        key,
        &encode_current_value(commit_ts, value),
    );
}

pub(crate) fn encode_current_value(
    commit_ts: u64,
    value: &metadata_state::VersionedValue,
) -> Vec<u8> {
    let encoded = encode_value(value);
    let mut out = Vec::with_capacity(8 + encoded.len());
    out.extend_from_slice(&commit_ts.to_be_bytes());
    out.extend_from_slice(&encoded);
    out
}

pub(crate) fn decode_current_value(
    src: &[u8],
) -> metadata_state::Result<(u64, metadata_state::VersionedValue)> {
    if src.len() < 8 {
        return Err(metadata_state::Error::Decode(
            "current metadata value missing commit version".to_owned(),
        ));
    }
    let commit_ts = u64::from_be_bytes(src[..8].try_into().unwrap());
    Ok((commit_ts, decode_value(&src[8..])?))
}
