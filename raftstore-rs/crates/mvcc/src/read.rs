use nokv_proto::nokv::kv::v1 as kvpb;

use crate::{
    blocking_lock, errors, read_committed, scan_limit, scan_read_version, value_is_expired, Error,
    MvccStore, Result,
};

impl MvccStore {
    pub fn get(&self, req: &kvpb::GetRequest) -> Result<kvpb::GetResponse> {
        let inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        if let Some(lock) = blocking_lock(&inner, &req.key, req.version) {
            return Ok(kvpb::GetResponse {
                error: Some(errors::locked(&req.key, lock)),
                ..Default::default()
            });
        }
        Ok(match read_committed(&inner, &req.key, req.version) {
            Some(value) => {
                if value_is_expired(value.expires_at) {
                    return Ok(kvpb::GetResponse {
                        not_found: true,
                        ..Default::default()
                    });
                }
                let not_found = value.value.is_none();
                kvpb::GetResponse {
                    value: value.value.unwrap_or_default(),
                    not_found,
                    expires_at: value.expires_at,
                    ..Default::default()
                }
            }
            None => kvpb::GetResponse {
                not_found: true,
                ..Default::default()
            },
        })
    }

    pub fn batch_get(&self, req: &kvpb::BatchGetRequest) -> Result<kvpb::BatchGetResponse> {
        let mut responses = Vec::with_capacity(req.requests.len());
        for get in &req.requests {
            responses.push(self.get(get)?);
        }
        Ok(kvpb::BatchGetResponse { responses })
    }

    pub fn scan(&self, req: &kvpb::ScanRequest) -> Result<kvpb::ScanResponse> {
        let inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        let read_version = scan_read_version(req.version);
        let limit = scan_limit(req.limit);
        let mut kvs = Vec::new();
        let start = req.start_key.as_slice();
        let include_start = req.include_start;
        for key in inner.writes.keys() {
            if key.as_slice() < start || (!include_start && key.as_slice() == start) {
                continue;
            }
            if let Some(lock) = blocking_lock(&inner, key, read_version) {
                return Ok(kvpb::ScanResponse {
                    error: Some(errors::locked(key, lock)),
                    ..Default::default()
                });
            }
            if let Some(value) = read_committed(&inner, key, read_version) {
                if value_is_expired(value.expires_at) {
                    continue;
                }
                if let Some(bytes) = &value.value {
                    kvs.push(kvpb::Kv {
                        key: key.clone(),
                        value: bytes.clone(),
                        version: read_version,
                        expires_at: value.expires_at,
                        ..Default::default()
                    });
                    if kvs.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(kvpb::ScanResponse {
            kvs,
            ..Default::default()
        })
    }
}
