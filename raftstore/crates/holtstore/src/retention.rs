use std::collections::BTreeMap;

use holt::RangeEntry;
use nokv_metadata_state as metadata_state;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use prost::Message;

use crate::store::to_backend_error;
use crate::trees::{
    decode_history_key, watch_apply_retention_key, HISTORY_TREE, REGION_META_TREE, WATCH_APPLY_TREE,
};
use crate::watch_apply::encode_watch_apply_retention_cursor;
use crate::HoltMetadataStore;

impl HoltMetadataStore {
    pub fn prune_metadata_versions(
        &self,
        retention_floor: u64,
    ) -> metadata_state::Result<metadata_state::MetadataRetentionResult> {
        let _guard = self.lock()?;
        let mut result = metadata_state::MetadataRetentionResult {
            retention_floor,
            ..Default::default()
        };
        if retention_floor == 0 {
            return Ok(result);
        }

        let mut versions_by_key: BTreeMap<(i32, Vec<u8>), Vec<(u64, Vec<u8>)>> = BTreeMap::new();
        for entry in self.store.history().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, .. } = entry else {
                continue;
            };
            let Some((family, user_key, commit_version)) = decode_history_key(&key)? else {
                continue;
            };
            versions_by_key
                .entry((family as i32, user_key))
                .or_default()
                .push((commit_version, key));
        }
        // Keep one watch anchor per region so replay can distinguish a real
        // retained frontier from an unpruned history that simply starts later.
        let watch_prune = self
            .watch_apply_prune_keys_locked(retention_floor)
            .map_err(|err| metadata_state::Error::Backend(err.to_string()))?;

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
        result.pruned_watch_events = watch_prune.keys.len() as u64;
        if !prune_keys.is_empty() || !watch_prune.keys.is_empty() {
            self.atomic(|batch| {
                for key in &prune_keys {
                    batch.delete(HISTORY_TREE, key);
                }
                for key in &watch_prune.keys {
                    batch.delete(WATCH_APPLY_TREE, key);
                }
                for frontier in &watch_prune.frontiers {
                    batch.put(
                        REGION_META_TREE,
                        &watch_apply_retention_key(frontier.region_id),
                        &encode_watch_apply_retention_cursor(
                            frontier.term,
                            frontier.index,
                            frontier.commit_version,
                        ),
                    );
                }
            })?;
        }
        Ok(result)
    }

    fn watch_apply_prune_keys_locked(
        &self,
        retention_floor: u64,
    ) -> crate::Result<WatchApplyPrune> {
        let mut events_by_region: BTreeMap<u64, Vec<WatchApplyRetentionCandidate>> =
            BTreeMap::new();
        for entry in self.store.watch_apply()?.range() {
            let entry = entry?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let event = metadatapb::MetadataApplyWatchEvent::decode(value.as_slice())?;
            if event.region_id == 0
                || event.commit_version == 0
                || event.commit_version > retention_floor
            {
                continue;
            }
            events_by_region.entry(event.region_id).or_default().push(
                WatchApplyRetentionCandidate {
                    commit_version: event.commit_version,
                    term: event.term,
                    index: event.index,
                    key,
                },
            );
        }

        let mut prune = WatchApplyPrune::default();
        for (region_id, candidates) in events_by_region.iter_mut() {
            candidates.sort_by_key(|candidate| {
                (
                    candidate.commit_version,
                    candidate.term,
                    candidate.index,
                    candidate.key.clone(),
                )
            });
            let Some(anchor) = candidates.last().cloned() else {
                continue;
            };
            let mut deleted = false;
            for candidate in candidates.iter() {
                if candidate != &anchor {
                    prune.keys.push(candidate.key.clone());
                    deleted = true;
                }
            }
            if deleted {
                prune.frontiers.push(WatchApplyRetentionFrontier {
                    region_id: *region_id,
                    term: anchor.term,
                    index: anchor.index,
                    commit_version: anchor.commit_version,
                });
            }
        }
        Ok(prune)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WatchApplyRetentionCandidate {
    commit_version: u64,
    term: u64,
    index: u64,
    key: Vec<u8>,
}

#[derive(Debug, Default)]
struct WatchApplyPrune {
    keys: Vec<Vec<u8>>,
    frontiers: Vec<WatchApplyRetentionFrontier>,
}

#[derive(Clone, Debug)]
struct WatchApplyRetentionFrontier {
    region_id: u64,
    term: u64,
    index: u64,
    commit_version: u64,
}

impl metadata_state::MetadataRetentionEngine for HoltMetadataStore {
    fn prune_metadata_versions(
        &self,
        retention_floor: u64,
    ) -> metadata_state::Result<metadata_state::MetadataRetentionResult> {
        HoltMetadataStore::prune_metadata_versions(self, retention_floor)
    }
}
