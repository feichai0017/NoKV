use std::collections::BTreeMap;

use holt::RangeEntry;
use nokv_metastore as metastore;

use crate::store::to_backend_error;
use crate::trees::{decode_write_key, WRITE_TREE};
use crate::HoltMetadataStore;

impl HoltMetadataStore {
    pub fn prune_metadata_versions(
        &self,
        retention_floor: u64,
    ) -> metastore::Result<metastore::MetadataRetentionResult> {
        let _guard = self.lock()?;
        let mut result = metastore::MetadataRetentionResult {
            retention_floor,
            ..Default::default()
        };
        if retention_floor == 0 {
            return Ok(result);
        }

        let mut versions_by_key: BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>)>> = BTreeMap::new();
        for entry in self.store.write().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, .. } = entry else {
                continue;
            };
            let Some((user_key, commit_version)) = decode_write_key(&key)? else {
                continue;
            };
            versions_by_key
                .entry(user_key)
                .or_default()
                .push((commit_version, key));
        }

        let mut prune_keys = Vec::new();
        for versions in versions_by_key.values_mut() {
            versions.sort_by_key(|(commit_version, _)| *commit_version);
            let Some(anchor_version) = versions.iter().rev().find_map(|(commit_version, _)| {
                (*commit_version <= retention_floor).then_some(*commit_version)
            }) else {
                continue;
            };
            result.retained_anchor_versions += 1;
            for (commit_version, key) in versions.iter() {
                if *commit_version < anchor_version {
                    prune_keys.push(key.clone());
                }
            }
        }

        result.pruned_versions = prune_keys.len() as u64;
        if !prune_keys.is_empty() {
            self.atomic(|batch| {
                for key in &prune_keys {
                    batch.delete(WRITE_TREE, key);
                }
            })?;
        }
        Ok(result)
    }
}

impl metastore::MetadataRetentionEngine for HoltMetadataStore {
    fn prune_metadata_versions(
        &self,
        retention_floor: u64,
    ) -> metastore::Result<metastore::MetadataRetentionResult> {
        HoltMetadataStore::prune_metadata_versions(self, retention_floor)
    }
}
