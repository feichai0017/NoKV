use holt::RangeEntry;
use nokv_metastore as metastore;

use crate::codec::{decode_value, encode_value};
use crate::store::to_backend_error;
use crate::trees::{decode_write_key, write_key, write_prefix, DATA_TREE, WRITE_TREE};
use crate::HoltMetadataStore;

impl HoltMetadataStore {
    pub(crate) fn read_committed(
        &self,
        key: &[u8],
        version: u64,
    ) -> metastore::Result<Option<(u64, metastore::VersionedValue)>> {
        let prefix = write_prefix(key);
        let mut best = None;
        for entry in self
            .store
            .write()
            .map_err(to_backend_error)?
            .range()
            .prefix(&prefix)
        {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((_user_key, commit_ts)) = decode_write_key(&key)? else {
                continue;
            };
            if commit_ts <= version && best.as_ref().is_none_or(|(ts, _)| commit_ts > *ts) {
                best = Some((commit_ts, decode_value(&value)?));
            }
        }
        Ok(best)
    }

    pub(crate) fn write_by_start_version(
        &self,
        key: &[u8],
        start_version: u64,
    ) -> metastore::Result<Option<(u64, metastore::VersionedValue)>> {
        let prefix = write_prefix(key);
        for entry in self
            .store
            .write()
            .map_err(to_backend_error)?
            .range()
            .prefix(&prefix)
        {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((_user_key, commit_ts)) = decode_write_key(&key)? else {
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
        key: &[u8],
        version: u64,
    ) -> metastore::Result<Option<(u64, metastore::VersionedValue)>> {
        let prefix = write_prefix(key);
        let mut best = None;
        for entry in self
            .store
            .write()
            .map_err(to_backend_error)?
            .range()
            .prefix(&prefix)
        {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((_user_key, commit_ts)) = decode_write_key(&key)? else {
                continue;
            };
            if commit_ts >= version && best.as_ref().is_none_or(|(ts, _)| commit_ts < *ts) {
                best = Some((commit_ts, decode_value(&value)?));
            }
        }
        Ok(best)
    }

    pub(crate) fn scan_write_user_keys(&self) -> metastore::Result<Vec<Vec<u8>>> {
        let mut keys = std::collections::BTreeSet::new();
        for entry in self.store.write().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, .. } = entry else {
                continue;
            };
            if let Some((user_key, _commit_ts)) = decode_write_key(&key)? {
                keys.insert(user_key);
            }
        }
        Ok(keys.into_iter().collect())
    }
}

pub(crate) fn apply_committed(
    batch: &mut holt::DBAtomicBatch,
    key: &[u8],
    commit_ts: u64,
    value: &metastore::VersionedValue,
) {
    let encoded = encode_value(value);
    batch.put(WRITE_TREE, &write_key(key, commit_ts), &encoded);
    match value.kind {
        metastore::ValueKind::Put => {
            if let Some(bytes) = &value.value {
                batch.put(DATA_TREE, key, bytes);
            }
        }
        metastore::ValueKind::Delete => {
            batch.delete(DATA_TREE, key);
        }
    }
}
