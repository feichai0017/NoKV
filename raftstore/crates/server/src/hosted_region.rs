use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use nokv_holtstore::HoltMetadataStore;
use nokv_proto::nokv::meta::v1 as metapb;
use nokv_raftnode::{OpenRaftRegion, PersistentAppliedMetadataEngine, RegionDescriptorCatalog};
use nokv_raftstore_server::HoltRegionMetadataSink;

#[derive(Clone)]
pub(crate) struct HostedRegionRegistry<E> {
    regions: Arc<RwLock<BTreeMap<u64, (crate::startup::ServerIdentity, OpenRaftRegion<E>)>>>,
}

impl<E> HostedRegionRegistry<E> {
    pub(crate) fn new(
        regions: impl IntoIterator<Item = (crate::startup::ServerIdentity, OpenRaftRegion<E>)>,
    ) -> Result<Self, String> {
        let registry = Self {
            regions: Arc::new(RwLock::new(BTreeMap::new())),
        };
        for (identity, region) in regions {
            registry.insert(identity, region)?;
        }
        Ok(registry)
    }

    pub(crate) fn insert(
        &self,
        identity: crate::startup::ServerIdentity,
        region: OpenRaftRegion<E>,
    ) -> Result<(), String> {
        if identity.region_id == 0 {
            return Err("hosted region id is required".to_owned());
        }
        let mut regions = self
            .regions
            .write()
            .map_err(|_| "hosted region registry lock poisoned".to_owned())?;
        if regions
            .insert(identity.region_id, (identity, region))
            .is_some()
        {
            return Err(format!("duplicate hosted region {}", identity.region_id));
        }
        Ok(())
    }

    pub(crate) fn get(
        &self,
        region_id: u64,
    ) -> Result<Option<(crate::startup::ServerIdentity, OpenRaftRegion<E>)>, String>
    where
        E: Clone,
    {
        self.regions
            .read()
            .map_err(|_| "hosted region registry lock poisoned".to_owned())
            .map(|regions| regions.get(&region_id).cloned())
    }

    pub(crate) fn remove(
        &self,
        region_id: u64,
    ) -> Result<Option<(crate::startup::ServerIdentity, OpenRaftRegion<E>)>, String>
    where
        E: Clone,
    {
        self.regions
            .write()
            .map_err(|_| "hosted region registry lock poisoned".to_owned())
            .map(|mut regions| regions.remove(&region_id))
    }

    pub(crate) fn snapshot(
        &self,
    ) -> Result<Vec<(crate::startup::ServerIdentity, OpenRaftRegion<E>)>, String>
    where
        E: Clone,
    {
        self.regions
            .read()
            .map_err(|_| "hosted region registry lock poisoned".to_owned())
            .map(|regions| regions.values().cloned().collect())
    }
}

pub(crate) type HoltApplyEngine =
    PersistentAppliedMetadataEngine<HoltMetadataStore, HoltRegionMetadataSink>;
pub(crate) type HoltRegion = OpenRaftRegion<HoltApplyEngine>;

#[derive(Clone)]
pub(crate) struct HoltRegionDescriptorCatalog {
    store: HoltMetadataStore,
}

impl HoltRegionDescriptorCatalog {
    pub(crate) fn new(store: HoltMetadataStore) -> Self {
        Self { store }
    }
}

impl std::fmt::Debug for HoltRegionDescriptorCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HoltRegionDescriptorCatalog")
            .finish_non_exhaustive()
    }
}

impl RegionDescriptorCatalog for HoltRegionDescriptorCatalog {
    fn region_descriptor(
        &self,
        region_id: u64,
    ) -> nokv_metastore::Result<Option<metapb::RegionDescriptor>> {
        self.store
            .get_region_descriptor(region_id)
            .map_err(|err| nokv_metastore::Error::Backend(err.to_string()))
    }
}
