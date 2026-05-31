use holt::RangeEntry;
use nokv_mvcc as mvcc;

use crate::codec::decode_value;
use crate::store::to_backend_error;
use crate::trees::{decode_write_key, DATA_TREE, WRITE_TREE};
use crate::versions::apply_committed;
use crate::HoltMvccStore;

impl mvcc::MvccSnapshotEngine for HoltMvccStore {
    fn export_mvcc_snapshot(&self) -> mvcc::Result<mvcc::MvccSnapshot> {
        let _guard = self.lock()?;
        let mut writes = Vec::new();
        for entry in self.store.write().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            let Some((user_key, commit_version)) = decode_write_key(&key)? else {
                continue;
            };
            writes.push(mvcc::MvccSnapshotWrite {
                key: user_key,
                commit_version,
                value: decode_value(&value)?,
            });
        }
        writes.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then(left.commit_version.cmp(&right.commit_version))
        });

        Ok(mvcc::MvccSnapshot { writes })
    }

    fn install_mvcc_snapshot(&self, snapshot: mvcc::MvccSnapshot) -> mvcc::Result<()> {
        let _guard = self.lock()?;
        let mut writes = snapshot.writes;
        writes.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then(left.commit_version.cmp(&right.commit_version))
        });

        let mut data_keys = Vec::new();
        for entry in self.store.data().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            if let RangeEntry::Key { key, .. } = entry {
                data_keys.push(key);
            }
        }
        let mut write_keys = Vec::new();
        for entry in self.store.write().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            if let RangeEntry::Key { key, .. } = entry {
                write_keys.push(key);
            }
        }
        self.atomic(|batch| {
            for key in &data_keys {
                batch.delete(DATA_TREE, key);
            }
            for key in &write_keys {
                batch.delete(WRITE_TREE, key);
            }
            for write in &writes {
                apply_committed(batch, &write.key, write.commit_version, &write.value);
            }
        })?;
        Ok(())
    }
}
