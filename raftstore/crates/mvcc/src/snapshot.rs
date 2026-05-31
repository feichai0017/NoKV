use prost::Message;

use crate::{
    Error, Inner, MvccSnapshot, MvccSnapshotEngine, MvccSnapshotWrite, MvccStore, Result,
    ValueKind, VersionedValue,
};

impl MvccStore {
    pub fn export_snapshot(&self) -> Result<MvccSnapshot> {
        let inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        Ok(snapshot_from_inner(&inner))
    }

    pub fn install_snapshot(&self, snapshot: MvccSnapshot) -> Result<()> {
        let mut inner = self.inner.lock().map_err(|_| Error::Poisoned)?;
        *inner = inner_from_snapshot(snapshot)?;
        Ok(())
    }
}

impl MvccSnapshotEngine for MvccStore {
    fn export_mvcc_snapshot(&self) -> Result<MvccSnapshot> {
        self.export_snapshot()
    }

    fn install_mvcc_snapshot(&self, snapshot: MvccSnapshot) -> Result<()> {
        self.install_snapshot(snapshot)
    }
}

pub fn encode_mvcc_snapshot(snapshot: &MvccSnapshot) -> Vec<u8> {
    let payload = SnapshotPayload {
        writes: snapshot
            .writes
            .iter()
            .map(|write| SnapshotWrite {
                key: write.key.clone(),
                commit_version: write.commit_version,
                kind: write.value.kind as i32,
                start_version: write.value.start_version,
                has_value: write.value.value.is_some(),
                value: write.value.value.clone().unwrap_or_default(),
                expires_at: write.value.expires_at,
            })
            .collect(),
    };
    payload.encode_to_vec()
}

pub fn decode_mvcc_snapshot(bytes: &[u8]) -> Result<MvccSnapshot> {
    let payload = SnapshotPayload::decode(bytes).map_err(|err| Error::Decode(err.to_string()))?;
    let mut writes = Vec::with_capacity(payload.writes.len());
    for write in payload.writes {
        writes.push(MvccSnapshotWrite {
            key: write.key,
            commit_version: write.commit_version,
            value: VersionedValue {
                kind: ValueKind::from_i32(write.kind),
                start_version: write.start_version,
                value: write.has_value.then_some(write.value),
                expires_at: write.expires_at,
            },
        });
    }
    Ok(MvccSnapshot { writes })
}

fn snapshot_from_inner(inner: &Inner) -> MvccSnapshot {
    let writes = inner
        .writes
        .iter()
        .flat_map(|(key, versions)| {
            versions
                .iter()
                .map(|(commit_version, value)| MvccSnapshotWrite {
                    key: key.clone(),
                    commit_version: *commit_version,
                    value: value.clone(),
                })
                .collect::<Vec<_>>()
        })
        .collect();
    MvccSnapshot { writes }
}

fn inner_from_snapshot(snapshot: MvccSnapshot) -> Result<Inner> {
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
}
