use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use nokv_raftnode::OpenRaftRegion;

use super::startup::ServerIdentity;

#[derive(Clone)]
pub(crate) struct HostedRegionRegistry<E> {
    regions: Arc<RwLock<BTreeMap<u64, (ServerIdentity, OpenRaftRegion<E>)>>>,
}

impl<E> HostedRegionRegistry<E> {
    pub(crate) fn new(
        regions: impl IntoIterator<Item = (ServerIdentity, OpenRaftRegion<E>)>,
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
        identity: ServerIdentity,
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

    pub(crate) fn snapshot(&self) -> Result<Vec<(ServerIdentity, OpenRaftRegion<E>)>, String>
    where
        E: Clone,
    {
        self.regions
            .read()
            .map_err(|_| "hosted region registry lock poisoned".to_owned())
            .map(|regions| regions.values().cloned().collect())
    }
}
