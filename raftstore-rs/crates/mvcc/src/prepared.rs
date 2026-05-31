use nokv_proto::nokv::kv::v1 as kvpb;

use crate::{validation, Error, MvccStore, Result, VersionedValue};

impl MvccStore {
    pub fn install_prepared(
        &self,
        req: &kvpb::InstallPreparedMvccEntriesRequest,
    ) -> Result<kvpb::InstallPreparedMvccEntriesResponse> {
        if let Some(error) = validation::install_prepared_request(req) {
            return Ok(kvpb::InstallPreparedMvccEntriesResponse {
                error: Some(error),
                ..Default::default()
            });
        }
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        let mut applied = 0;
        for entry in &req.entries {
            match kvpb::prepared_mvcc_entry::ColumnFamily::try_from(entry.column_family)
                .unwrap_or(kvpb::prepared_mvcc_entry::ColumnFamily::Default)
            {
                kvpb::prepared_mvcc_entry::ColumnFamily::Default
                | kvpb::prepared_mvcc_entry::ColumnFamily::Write => {
                    inner.writes.entry(entry.key.clone()).or_default().insert(
                        entry.version,
                        VersionedValue {
                            kind: if entry.has_value {
                                kvpb::mutation::Op::Put
                            } else {
                                kvpb::mutation::Op::Delete
                            },
                            start_version: entry.version,
                            value: entry.has_value.then(|| entry.value.clone()),
                            expires_at: entry.expires_at,
                        },
                    );
                    applied += 1;
                }
                kvpb::prepared_mvcc_entry::ColumnFamily::Lock => {
                    inner.locks.remove(&entry.key);
                    applied += 1;
                }
            }
        }
        Ok(kvpb::InstallPreparedMvccEntriesResponse {
            applied_entries: applied,
            commit_version: req.commit_version,
            ..Default::default()
        })
    }
}
