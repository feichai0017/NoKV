use nokv_proto::nokv::kv::v1 as kvpb;

use crate::{validation, Error, MvccStore, Result};

impl MvccStore {
    pub fn mvcc_maintenance(
        &self,
        req: &kvpb::MvccMaintenanceRequest,
    ) -> Result<kvpb::MvccMaintenanceResponse> {
        if let Some(error) = validation::mvcc_maintenance_request(req) {
            return Ok(kvpb::MvccMaintenanceResponse {
                error: Some(error),
                ..Default::default()
            });
        }
        let tombstones = req
            .tombstones
            .iter()
            .map(|tombstone| (tombstone.key.clone(), tombstone.version))
            .collect::<Vec<_>>();

        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        for (key, version) in &tombstones {
            if let Some(versions) = inner.writes.get_mut(key) {
                versions.remove(version);
                if versions.is_empty() {
                    inner.writes.remove(key);
                }
            }
        }
        Ok(kvpb::MvccMaintenanceResponse {
            applied_entries: tombstones.len() as u64,
            ..Default::default()
        })
    }
}
