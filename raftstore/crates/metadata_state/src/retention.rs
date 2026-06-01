use crate::{
    Error, Inner, MemoryMetadataStore, MetadataRetentionEngine, MetadataRetentionResult, Result,
};

impl MemoryMetadataStore {
    pub fn prune_metadata_versions(&self, retention_floor: u64) -> Result<MetadataRetentionResult> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        Ok(prune_inner(&mut inner, retention_floor))
    }
}

impl MetadataRetentionEngine for MemoryMetadataStore {
    fn prune_metadata_versions(&self, retention_floor: u64) -> Result<MetadataRetentionResult> {
        MemoryMetadataStore::prune_metadata_versions(self, retention_floor)
    }
}

fn prune_inner(inner: &mut Inner, retention_floor: u64) -> MetadataRetentionResult {
    let mut result = MetadataRetentionResult {
        retention_floor,
        ..Default::default()
    };
    if retention_floor == 0 {
        return result;
    }

    for versions in inner.writes.values_mut() {
        let Some(anchor_version) = versions
            .range(..=retention_floor)
            .next_back()
            .map(|(version, _)| *version)
        else {
            continue;
        };
        result.retained_anchor_versions += 1;
        let pruned = versions
            .range(..anchor_version)
            .map(|(version, _)| *version)
            .collect::<Vec<_>>();
        result.pruned_versions += pruned.len() as u64;
        for version in pruned {
            versions.remove(&version);
        }
    }

    result
}
