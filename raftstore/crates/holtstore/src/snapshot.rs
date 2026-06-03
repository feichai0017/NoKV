use holt::RangeEntry;
use nokv_metadata_state as metadata_state;
use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::codec::decode_value;
use crate::store::to_backend_error;
use crate::trees::{
    current_tree_for_family, decode_history_key, family_from_i32, CURRENT_TREES, HISTORY_TREE,
};
use crate::versions::{apply_committed, decode_current_value};
use crate::HoltMetadataStore;

impl metadata_state::MetadataSnapshotEngine for HoltMetadataStore {
    fn export_metadata_snapshot(&self) -> metadata_state::Result<metadata_state::MetadataSnapshot> {
        let _guard = self.lock()?;
        let mut writes = std::collections::BTreeMap::new();
        for entry in self.store.history().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((family, user_key, commit_version)) = decode_history_key(&key)? else {
                continue;
            };
            writes.insert(
                (family as i32, user_key, commit_version),
                decode_value(&value)?,
            );
        }
        for family in metadata_current_families() {
            for entry in self
                .store
                .tree(current_tree_for_family(family))
                .map_err(to_backend_error)?
                .range()
            {
                let entry = entry.map_err(to_backend_error)?;
                let RangeEntry::Key { key, value, .. } = entry else {
                    continue;
                };
                let (commit_version, current) = decode_current_value(&value)?;
                writes.insert((family as i32, key, commit_version), current);
            }
        }
        let writes = writes
            .into_iter()
            .map(
                |((family, key, commit_version), value)| metadata_state::MetadataSnapshotWrite {
                    family,
                    key,
                    commit_version,
                    value,
                },
            )
            .collect();

        Ok(metadata_state::MetadataSnapshot { writes })
    }

    fn install_metadata_snapshot(
        &self,
        snapshot: metadata_state::MetadataSnapshot,
    ) -> metadata_state::Result<()> {
        let _guard = self.lock()?;
        let mut writes = snapshot.writes;
        writes.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then(left.commit_version.cmp(&right.commit_version))
        });

        let mut current_keys = Vec::new();
        for tree_name in CURRENT_TREES {
            for entry in self
                .store
                .tree(tree_name)
                .map_err(to_backend_error)?
                .range()
            {
                let entry = entry.map_err(to_backend_error)?;
                if let RangeEntry::Key { key, .. } = entry {
                    current_keys.push((tree_name, key));
                }
            }
        }
        let mut history_keys = Vec::new();
        for entry in self.store.history().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            if let RangeEntry::Key { key, .. } = entry {
                history_keys.push(key);
            }
        }
        self.atomic(|batch| {
            for (tree_name, key) in &current_keys {
                batch.delete(tree_name, key);
            }
            for key in &history_keys {
                batch.delete(HISTORY_TREE, key);
            }
            for write in &writes {
                let family = family_from_i32(write.family);
                apply_committed(
                    batch,
                    family,
                    &write.key,
                    write.commit_version,
                    &write.value,
                );
            }
        })?;
        Ok(())
    }
}

fn metadata_current_families() -> [metadatapb::MetadataFamily; 13] {
    [
        metadatapb::MetadataFamily::Unspecified,
        metadatapb::MetadataFamily::Mount,
        metadatapb::MetadataFamily::Inode,
        metadatapb::MetadataFamily::Dentry,
        metadatapb::MetadataFamily::Parent,
        metadatapb::MetadataFamily::Chunk,
        metadatapb::MetadataFamily::Session,
        metadatapb::MetadataFamily::Quota,
        metadatapb::MetadataFamily::Snapshot,
        metadatapb::MetadataFamily::PathIndex,
        metadatapb::MetadataFamily::Watch,
        metadatapb::MetadataFamily::CommandDedupe,
        metadatapb::MetadataFamily::Segment,
    ]
}
