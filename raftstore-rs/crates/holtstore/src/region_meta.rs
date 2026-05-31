use holt::RangeEntry;
use nokv_proto::nokv::meta::v1 as metapb;
use prost::Message;

use crate::codec::{decode_apply_state, encode_apply_state};
use crate::trees::{region_apply_state_key, region_descriptor_key, REGION_DESCRIPTOR_PREFIX};
use crate::{Error, HoltMvccStore, HoltStore, RegionApplyState, Result};

impl HoltStore {
    pub fn put_region_descriptor(&self, descriptor: &metapb::RegionDescriptor) -> Result<()> {
        let mut bytes = Vec::with_capacity(descriptor.encoded_len());
        descriptor.encode(&mut bytes)?;
        self.region_meta()?
            .put(&region_descriptor_key(descriptor.region_id), &bytes)?;
        Ok(())
    }

    pub fn get_region_descriptor(
        &self,
        region_id: u64,
    ) -> Result<Option<metapb::RegionDescriptor>> {
        let Some(bytes) = self.region_meta()?.get(&region_descriptor_key(region_id))? else {
            return Ok(None);
        };
        Ok(Some(metapb::RegionDescriptor::decode(bytes.as_slice())?))
    }

    pub fn delete_region_descriptor(&self, region_id: u64) -> Result<()> {
        self.region_meta()?
            .delete(&region_descriptor_key(region_id))?;
        Ok(())
    }

    pub fn region_descriptors(&self) -> Result<Vec<metapb::RegionDescriptor>> {
        let mut out = Vec::new();
        for entry in self.region_meta()?.range().prefix(REGION_DESCRIPTOR_PREFIX) {
            let entry = entry?;
            let RangeEntry::Key { value, .. } = entry else {
                continue;
            };
            out.push(metapb::RegionDescriptor::decode(value.as_slice())?);
        }
        out.sort_by_key(|descriptor| descriptor.region_id);
        Ok(out)
    }

    pub fn load_or_bootstrap_region_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> Result<metapb::RegionDescriptor> {
        if descriptor.region_id == 0 {
            return Err(Error::InvalidMetadata(
                "region descriptor id is required".to_owned(),
            ));
        }
        if let Some(existing) = self.get_region_descriptor(descriptor.region_id)? {
            return Ok(existing);
        }
        self.put_region_descriptor(descriptor)?;
        Ok(descriptor.clone())
    }

    pub fn put_region_apply_state(&self, state: &RegionApplyState) -> Result<()> {
        self.apply_state()?.put(
            &region_apply_state_key(state.region_id),
            &encode_apply_state(state),
        )?;
        Ok(())
    }

    pub fn get_region_apply_state(&self, region_id: u64) -> Result<Option<RegionApplyState>> {
        let Some(bytes) = self
            .apply_state()?
            .get(&region_apply_state_key(region_id))?
        else {
            return Ok(None);
        };
        decode_apply_state(&bytes).map(Some)
    }
}

impl HoltMvccStore {
    pub fn put_region_descriptor(&self, descriptor: &metapb::RegionDescriptor) -> Result<()> {
        self.store.put_region_descriptor(descriptor)
    }

    pub fn get_region_descriptor(
        &self,
        region_id: u64,
    ) -> Result<Option<metapb::RegionDescriptor>> {
        self.store.get_region_descriptor(region_id)
    }

    pub fn delete_region_descriptor(&self, region_id: u64) -> Result<()> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| Error::InvalidMetadata("holt metadata mutex poisoned".to_owned()))?;
        self.store
            .delete_region_descriptor(region_id)
            .and_then(|_| self.store.checkpoint())
    }

    pub fn region_descriptors(&self) -> Result<Vec<metapb::RegionDescriptor>> {
        self.store.region_descriptors()
    }

    pub fn load_or_bootstrap_region_descriptor(
        &self,
        descriptor: &metapb::RegionDescriptor,
    ) -> Result<metapb::RegionDescriptor> {
        self.store.load_or_bootstrap_region_descriptor(descriptor)
    }

    pub fn put_region_apply_state(&self, state: &RegionApplyState) -> Result<()> {
        self.store.put_region_apply_state(state)
    }

    pub fn get_region_apply_state(&self, region_id: u64) -> Result<Option<RegionApplyState>> {
        self.store.get_region_apply_state(region_id)
    }
}
