use prost::Message;

use crate::{
    Error, Inner, MemoryMetadataStore, MetadataSnapshot, MetadataSnapshotEngine,
    MetadataSnapshotWrite, Result, ValueKind, VersionedValue,
};

impl MemoryMetadataStore {
    pub fn export_snapshot(&self) -> Result<MetadataSnapshot> {
        let inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        Ok(snapshot_from_inner(&inner))
    }

    pub fn install_snapshot(&self, snapshot: MetadataSnapshot) -> Result<()> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        *inner = inner_from_snapshot(snapshot)?;
        Ok(())
    }
}

impl MetadataSnapshotEngine for MemoryMetadataStore {
    fn export_metadata_snapshot(&self) -> Result<MetadataSnapshot> {
        self.export_snapshot()
    }

    fn install_metadata_snapshot(&self, snapshot: MetadataSnapshot) -> Result<()> {
        self.install_snapshot(snapshot)
    }
}

pub fn encode_metadata_snapshot(snapshot: &MetadataSnapshot) -> Vec<u8> {
    let payload = SnapshotPayload {
        writes: snapshot
            .writes
            .iter()
            .map(|write| SnapshotWrite {
                family: write.family,
                key: write.key.clone(),
                commit_version: write.commit_version,
                kind: write.value.kind as i32,
                start_version: write.value.start_version,
                has_value: write.value.value.is_some(),
                value: write.value.value.clone().unwrap_or_default(),
                expires_at: write.value.expires_at,
                retention_pin_version: write.value.retention_pin_version,
            })
            .collect(),
    };
    payload.encode_to_vec()
}

pub fn decode_metadata_snapshot(bytes: &[u8]) -> Result<MetadataSnapshot> {
    let payload = SnapshotPayload::decode(bytes).map_err(|err| Error::Decode(err.to_string()))?;
    let mut writes = Vec::with_capacity(payload.writes.len());
    for write in payload.writes {
        writes.push(MetadataSnapshotWrite {
            family: write.family,
            key: write.key,
            commit_version: write.commit_version,
            value: VersionedValue {
                kind: ValueKind::from_i32(write.kind),
                start_version: write.start_version,
                value: write.has_value.then_some(write.value),
                expires_at: write.expires_at,
                retention_pin_version: write.retention_pin_version,
            },
        });
    }
    Ok(MetadataSnapshot { writes })
}

fn snapshot_from_inner(inner: &Inner) -> MetadataSnapshot {
    let writes = inner
        .writes
        .iter()
        .flat_map(|(key, versions)| {
            versions
                .iter()
                .map(|(commit_version, value)| MetadataSnapshotWrite {
                    family: 0,
                    key: key.clone(),
                    commit_version: *commit_version,
                    value: value.clone(),
                })
                .collect::<Vec<_>>()
        })
        .collect();
    MetadataSnapshot { writes }
}

fn inner_from_snapshot(snapshot: MetadataSnapshot) -> Result<Inner> {
    let mut inner = Inner::default();
    for write in snapshot.writes {
        inner
            .writes
            .entry(write.key.clone())
            .or_default()
            .insert(write.commit_version, write.value.clone());
    }
    Ok(inner)
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotPayload {
    #[prost(message, repeated, tag = "1")]
    writes: Vec<SnapshotWrite>,
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotWrite {
    #[prost(int32, tag = "8")]
    family: i32,
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
    #[prost(uint64, tag = "2")]
    commit_version: u64,
    #[prost(int32, tag = "3")]
    kind: i32,
    #[prost(uint64, tag = "4")]
    start_version: u64,
    #[prost(bool, tag = "5")]
    has_value: bool,
    #[prost(bytes = "vec", tag = "6")]
    value: Vec<u8>,
    #[prost(uint64, tag = "7")]
    expires_at: u64,
    #[prost(uint64, tag = "9")]
    retention_pin_version: u64,
}
