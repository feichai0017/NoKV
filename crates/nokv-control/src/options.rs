use crate::ControlError;

const DEFAULT_ETCD_KEY_PREFIX: &str = "/nokv/control";
const DEFAULT_ETCD_LEASE_TTL_SECONDS: i64 = 10;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EtcdControlStoreOptions {
    endpoints: Vec<String>,
    key_prefix: String,
    lease_ttl_seconds: i64,
}

impl EtcdControlStoreOptions {
    pub fn new(endpoints: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            endpoints: endpoints.into_iter().map(Into::into).collect(),
            key_prefix: DEFAULT_ETCD_KEY_PREFIX.to_owned(),
            lease_ttl_seconds: DEFAULT_ETCD_LEASE_TTL_SECONDS,
        }
    }

    pub fn with_key_prefix(mut self, key_prefix: impl Into<String>) -> Self {
        self.key_prefix = key_prefix.into();
        self
    }

    pub fn with_lease_ttl_seconds(mut self, lease_ttl_seconds: i64) -> Self {
        self.lease_ttl_seconds = lease_ttl_seconds;
        self
    }

    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    pub fn lease_ttl_seconds(&self) -> i64 {
        self.lease_ttl_seconds
    }

    pub fn validate(&self) -> Result<(), ControlError> {
        if self.endpoints.is_empty() {
            return Err(ControlError::InvalidOptions(
                "at least one etcd endpoint is required".to_owned(),
            ));
        }
        if self
            .endpoints
            .iter()
            .any(|endpoint| endpoint.trim().is_empty())
        {
            return Err(ControlError::InvalidOptions(
                "etcd endpoints must not be empty".to_owned(),
            ));
        }
        if self.normalized_key_prefix().is_empty() {
            return Err(ControlError::InvalidOptions(
                "etcd key prefix must not be empty".to_owned(),
            ));
        }
        if self.lease_ttl_seconds <= 0 {
            return Err(ControlError::InvalidOptions(
                "etcd lease TTL must be positive".to_owned(),
            ));
        }
        Ok(())
    }

    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn shard_record_key(&self, shard_id: &crate::ShardId) -> Vec<u8> {
        format!(
            "{}/shards/{}",
            self.normalized_key_prefix(),
            hex_bytes(shard_id.as_str().as_bytes())
        )
        .into_bytes()
    }

    /// Common key prefix for a range scan over every shard record. Only the etcd
    /// backend range-scans; the in-memory store enumerates its map directly.
    #[cfg(feature = "etcd")]
    pub(crate) fn shards_prefix(&self) -> Vec<u8> {
        format!("{}/shards/", self.normalized_key_prefix()).into_bytes()
    }

    /// Stable, identity-only session key for a shard: `{prefix}/sessions/{hex(shard_id)}`.
    ///
    /// The key path carries no epoch or lease id, so it is the *same* key across
    /// owner generations. Its value carries the current lease and it is attached
    /// to the owner's etcd lease, so it auto-deletes on lease expiry. Liveness of
    /// the previous owner is therefore decided by the key's presence inside a
    /// transaction (create_revision/lease compares), not by a separate read — no
    /// epoch/lease_id in the path means there is exactly one session key to guard.
    #[cfg(any(feature = "etcd", test))]
    pub(crate) fn shard_session_key(&self, shard_id: &crate::ShardId) -> Vec<u8> {
        format!(
            "{}/sessions/{}",
            self.normalized_key_prefix(),
            hex_bytes(shard_id.as_str().as_bytes())
        )
        .into_bytes()
    }

    fn normalized_key_prefix(&self) -> String {
        self.key_prefix.trim_end_matches('/').to_owned()
    }
}

#[cfg(any(feature = "etcd", test))]
fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ShardId;

    #[test]
    fn etcd_options_validate_required_fields() {
        assert!(matches!(
            EtcdControlStoreOptions::new(Vec::<String>::new()).validate(),
            Err(ControlError::InvalidOptions(_))
        ));
        assert!(matches!(
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"])
                .with_lease_ttl_seconds(0)
                .validate(),
            Err(ControlError::InvalidOptions(_))
        ));
    }

    #[test]
    fn etcd_keys_are_prefixed_and_shard_ids_are_hex_encoded() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let shard_id = ShardId::new("mount-1:/dataset/train");

        assert_eq!(
            String::from_utf8(options.shard_record_key(&shard_id)).unwrap(),
            "/nokv/test/shards/6d6f756e742d313a2f646174617365742f747261696e"
        );
        // The session key is stable per shard: no epoch/lease_id in the path, so
        // it is the same key across owner generations and is guarded directly
        // inside every txn.
        assert_eq!(
            String::from_utf8(options.shard_session_key(&shard_id)).unwrap(),
            "/nokv/test/sessions/6d6f756e742d313a2f646174617365742f747261696e"
        );
    }
}
