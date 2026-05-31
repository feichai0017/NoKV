use holt::RangeEntry;
use nokv_mvcc as mvcc;
use nokv_proto::nokv::kv::v1 as kvpb;

use crate::codec::{decode_lock, decode_value, encode_lock};
use crate::mvcc_engine::apply_committed;
use crate::store::to_backend_error;
use crate::trees::{decode_write_key, DATA_TREE, LOCK_TREE, WRITE_TREE};
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

        let mut locks = Vec::new();
        for entry in self.store.lock().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            let RangeEntry::Key { key, value, .. } = entry else {
                continue;
            };
            locks.push(mvcc::MvccSnapshotLock {
                key,
                lock: decode_lock(&value)?,
            });
        }
        locks.sort_by(|left, right| left.key.cmp(&right.key));

        let rollbacks = writes
            .iter()
            .filter(|write| write.value.kind == kvpb::mutation::Op::Rollback)
            .map(|write| mvcc::MvccSnapshotRollback {
                key: write.key.clone(),
                start_version: write.value.start_version,
            })
            .collect();
        Ok(mvcc::MvccSnapshot {
            writes,
            locks,
            rollbacks,
        })
    }

    fn install_mvcc_snapshot(&self, mut snapshot: mvcc::MvccSnapshot) -> mvcc::Result<()> {
        let _guard = self.lock()?;
        let mut rollback_writes = snapshot
            .writes
            .iter()
            .filter(|write| write.value.kind == kvpb::mutation::Op::Rollback)
            .map(|write| (write.key.clone(), write.value.start_version))
            .collect::<std::collections::BTreeSet<_>>();
        for rollback in &snapshot.rollbacks {
            if rollback_writes.insert((rollback.key.clone(), rollback.start_version)) {
                snapshot.writes.push(mvcc::MvccSnapshotWrite {
                    key: rollback.key.clone(),
                    commit_version: rollback.start_version,
                    value: mvcc::VersionedValue {
                        kind: kvpb::mutation::Op::Rollback,
                        start_version: rollback.start_version,
                        value: None,
                        expires_at: 0,
                    },
                });
            }
        }
        snapshot.writes.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then(left.commit_version.cmp(&right.commit_version))
        });
        let encoded_locks = snapshot
            .locks
            .iter()
            .map(|lock| encode_lock(&lock.lock).map(|encoded| (lock.key.clone(), encoded)))
            .collect::<mvcc::Result<Vec<_>>>()?;

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
        let mut lock_keys = Vec::new();
        for entry in self.store.lock().map_err(to_backend_error)?.range() {
            let entry = entry.map_err(to_backend_error)?;
            if let RangeEntry::Key { key, .. } = entry {
                lock_keys.push(key);
            }
        }

        self.atomic(|batch| {
            for key in &data_keys {
                batch.delete(DATA_TREE, key);
            }
            for key in &write_keys {
                batch.delete(WRITE_TREE, key);
            }
            for key in &lock_keys {
                batch.delete(LOCK_TREE, key);
            }
            for write in &snapshot.writes {
                apply_committed(batch, &write.key, write.commit_version, &write.value);
            }
            for (key, encoded) in &encoded_locks {
                batch.put(LOCK_TREE, key, encoded);
            }
        })?;
        Ok(())
    }
}
