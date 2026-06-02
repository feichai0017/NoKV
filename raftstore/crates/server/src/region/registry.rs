//! Shared region-service registry for process-local multi-region routers.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use nokv_proto::nokv::error::v1 as errorpb;
use nokv_proto::nokv::metadata::v1 as metadatapb;
use tonic::Status;

pub(crate) enum RegionServiceLookup<T> {
    Hosted(T),
    Missing(errorpb::RegionError),
}

#[derive(Clone)]
pub(crate) struct RegionServiceRegistry<T> {
    regions: Arc<RwLock<BTreeMap<u64, T>>>,
}

impl<T> RegionServiceRegistry<T> {
    pub(crate) fn new(regions: impl IntoIterator<Item = (u64, T)>) -> Result<Self, Status> {
        let registry = Self {
            regions: Arc::new(RwLock::new(BTreeMap::new())),
        };
        for (region_id, service) in regions {
            registry.insert_region(region_id, service)?;
        }
        Ok(registry)
    }

    pub(crate) fn insert_region(&self, region_id: u64, service: T) -> Result<(), Status> {
        validate_region_service_id(region_id)?;
        let mut regions = self
            .regions
            .write()
            .map_err(|_| region_service_registry_poisoned())?;
        if regions.insert(region_id, service).is_some() {
            return Err(Status::invalid_argument(format!(
                "duplicate region_id {region_id}"
            )));
        }
        Ok(())
    }

    pub(crate) fn remove_region(&self, region_id: u64) -> Result<Option<T>, Status> {
        validate_region_service_id(region_id)?;
        self.regions
            .write()
            .map_err(|_| region_service_registry_poisoned())
            .map(|mut regions| regions.remove(&region_id))
    }

    pub(crate) fn get_region(&self, region_id: u64) -> Result<Option<T>, Status>
    where
        T: Clone,
    {
        self.regions
            .read()
            .map_err(|_| region_service_registry_poisoned())
            .map(|regions| regions.get(&region_id).cloned())
    }

    pub(crate) fn values(&self) -> Result<Vec<T>, Status>
    where
        T: Clone,
    {
        self.regions
            .read()
            .map_err(|_| region_service_registry_poisoned())
            .map(|regions| regions.values().cloned().collect())
    }

    pub(crate) fn is_empty(&self) -> Result<bool, Status> {
        self.regions
            .read()
            .map_err(|_| region_service_registry_poisoned())
            .map(|regions| regions.is_empty())
    }
}

pub(crate) fn metadata_region_lookup<T>(
    regions: &RegionServiceRegistry<T>,
    context: Option<&metadatapb::MetadataContext>,
) -> Result<RegionServiceLookup<T>, Status>
where
    T: Clone,
{
    let context = context.ok_or_else(|| Status::invalid_argument("context is required"))?;
    if context.region_id == 0 {
        return Err(Status::invalid_argument("region id is required"));
    }
    match regions.get_region(context.region_id)? {
        Some(region) => Ok(RegionServiceLookup::Hosted(region)),
        None => Ok(RegionServiceLookup::Missing(region_not_found_error(
            context.region_id,
        ))),
    }
}

pub(crate) fn admin_region_lookup<T>(
    regions: &RegionServiceRegistry<T>,
    region_id: u64,
) -> Result<T, Status>
where
    T: Clone,
{
    regions.get_region(region_id)?.ok_or_else(|| {
        Status::failed_precondition(format!(
            "region {region_id} is not hosted by this raft admin"
        ))
    })
}

fn region_service_registry_poisoned() -> Status {
    Status::internal("region service registry lock poisoned")
}

fn region_not_found_error(region_id: u64) -> errorpb::RegionError {
    errorpb::RegionError {
        region_not_found: Some(errorpb::RegionNotFound { region_id }),
        ..Default::default()
    }
}

fn validate_region_service_id(region_id: u64) -> Result<(), Status> {
    if region_id == 0 {
        return Err(Status::invalid_argument("region_id is required"));
    }
    Ok(())
}
