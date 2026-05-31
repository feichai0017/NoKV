use nokv_metastore::{MetadataEngine, MetadataSnapshotEngine};
use nokv_proto::nokv::meta::v1 as metapb;
use prost::Message;

use super::{AppliedMetadataEngine, ApplyStatus, PersistentAppliedMetadataEngine};
use crate::snapshot::{decode_region_snapshot_payload, RegionSnapshotPayload};
use crate::RegionSnapshotEngine;

impl<E> RegionSnapshotEngine for AppliedMetadataEngine<E>
where
    E: MetadataEngine + MetadataSnapshotEngine,
{
    fn region_descriptor(&self) -> nokv_metastore::Result<Option<metapb::RegionDescriptor>> {
        AppliedMetadataEngine::region_descriptor(self)
    }

    fn export_region_snapshot(&self) -> nokv_metastore::Result<Vec<u8>> {
        let status = self.status();
        let metadata_snapshot = {
            let engine = self.inner.engine.lock().map_err(|_| {
                nokv_metastore::Error::Backend("region apply mutex poisoned".to_owned())
            })?;
            engine.export_metadata_snapshot()?
        };
        Ok(RegionSnapshotPayload {
            format_version: 1,
            region_id: status.region_id,
            term: status.term,
            applied_index: status.applied_index,
            metadata_snapshot: nokv_metastore::encode_metadata_snapshot(&metadata_snapshot),
            region_descriptor: self.region_descriptor()?,
        }
        .encode_to_vec())
    }

    fn install_region_snapshot(&self, snapshot: &[u8]) -> nokv_metastore::Result<ApplyStatus> {
        let payload = decode_region_snapshot_payload(snapshot)?;
        if payload.format_version != 1 {
            return Err(nokv_metastore::Error::Decode(format!(
                "unsupported region snapshot format {}",
                payload.format_version
            )));
        }
        if payload.region_id != self.inner.region_id {
            return Err(nokv_metastore::Error::Backend(format!(
                "region snapshot {} cannot install into region {}",
                payload.region_id, self.inner.region_id
            )));
        }
        let current = self.status();
        if payload.applied_index < current.applied_index {
            return Err(nokv_metastore::Error::Backend(format!(
                "stale region snapshot index {} is behind current applied index {}",
                payload.applied_index, current.applied_index
            )));
        }
        if payload.applied_index == current.applied_index
            && current.applied_index != 0
            && payload.term != current.term
        {
            return Err(nokv_metastore::Error::Backend(format!(
                "region snapshot term {} conflicts with current applied term {} at index {}",
                payload.term, current.term, payload.applied_index
            )));
        }
        let metadata_snapshot =
            nokv_metastore::decode_metadata_snapshot(&payload.metadata_snapshot)?;
        if let Some(descriptor) = payload.region_descriptor {
            self.set_region_descriptor(descriptor)?;
        }
        {
            let engine = self.inner.engine.lock().map_err(|_| {
                nokv_metastore::Error::Backend("region apply mutex poisoned".to_owned())
            })?;
            engine.install_metadata_snapshot(metadata_snapshot)?;
        }
        self.record_applied_status(payload.term, payload.applied_index);
        Ok(self.status())
    }
}

impl<E, S> RegionSnapshotEngine for PersistentAppliedMetadataEngine<E, S>
where
    E: MetadataEngine + MetadataSnapshotEngine,
    S: super::RegionMetadataSink,
{
    fn region_descriptor(&self) -> nokv_metastore::Result<Option<metapb::RegionDescriptor>> {
        self.engine.region_descriptor()
    }

    fn export_region_snapshot(&self) -> nokv_metastore::Result<Vec<u8>> {
        self.engine.export_region_snapshot()
    }

    fn install_region_snapshot(&self, snapshot: &[u8]) -> nokv_metastore::Result<ApplyStatus> {
        let status = self.engine.install_region_snapshot(snapshot)?;
        if let Some(descriptor) = self.engine.region_descriptor()? {
            self.sink.save_region_descriptor(&descriptor)?;
        }
        self.sink.save_apply_status(&status)?;
        Ok(status)
    }
}
