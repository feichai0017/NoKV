use std::sync::{Arc, Mutex};

use nokv_proto::nokv::meta::v1 as metapb;
use tonic::Status;

use crate::admission::RegionAdmission;
use crate::RaftRuntimeStatus;

#[derive(Debug, Clone)]
pub(crate) struct RegionAdmissionState {
    inner: Arc<Mutex<RegionAdmission>>,
}

impl Default for RegionAdmissionState {
    fn default() -> Self {
        Self::new(RegionAdmission::default())
    }
}

impl RegionAdmissionState {
    pub(crate) fn new(admission: RegionAdmission) -> Self {
        Self {
            inner: Arc::new(Mutex::new(admission)),
        }
    }

    pub(crate) fn snapshot(&self) -> Result<RegionAdmission, Status> {
        self.inner
            .lock()
            .map_err(|_| admission_state_poisoned())
            .map(|admission| admission.clone())
    }

    pub(crate) fn with_runtime_status(
        &self,
        status: RaftRuntimeStatus,
    ) -> Result<RegionAdmission, Status> {
        Ok(self.snapshot()?.with_runtime_status(status))
    }

    pub(crate) fn with_applied_descriptor_and_runtime_status(
        &self,
        descriptor: Option<metapb::RegionDescriptor>,
        status: RaftRuntimeStatus,
    ) -> Result<RegionAdmission, Status> {
        let Some(descriptor) = descriptor else {
            return self.with_runtime_status(status);
        };
        let admission = RegionAdmission::from_descriptor(&descriptor, status.leader)
            .map_err(|err| {
                Status::failed_precondition(format!("invalid region descriptor: {err}"))
            })?
            .with_runtime_status(status);
        let mut current = self.inner.lock().map_err(|_| admission_state_poisoned())?;
        *current = admission.clone();
        Ok(admission)
    }

    pub(crate) fn validate_region(&self, region_id: u64) -> Result<(), Status> {
        if region_id == 0 {
            return Err(Status::invalid_argument("region_id is required"));
        }
        let admission = self.snapshot()?;
        if region_id != admission.region_id {
            return Err(Status::failed_precondition(format!(
                "region {region_id} is not hosted by this raft admin"
            )));
        }
        Ok(())
    }

    pub(crate) fn descriptor(&self) -> Result<metapb::RegionDescriptor, Status> {
        Ok(self.snapshot()?.descriptor())
    }

    pub(crate) fn add_peer(
        &self,
        peer_id: u64,
        store_id: u64,
    ) -> Result<metapb::RegionDescriptor, Status> {
        let mut admission = self.inner.lock().map_err(|_| admission_state_poisoned())?;
        if admission.peers.insert(peer_id, store_id).is_none() {
            admission.epoch_conf_version += 1;
        }
        Ok(admission.descriptor())
    }

    pub(crate) fn remove_peer(&self, peer_id: u64) -> Result<metapb::RegionDescriptor, Status> {
        let mut admission = self.inner.lock().map_err(|_| admission_state_poisoned())?;
        if admission.peers.remove(&peer_id).is_some() {
            admission.epoch_conf_version += 1;
        }
        Ok(admission.descriptor())
    }
}

fn admission_state_poisoned() -> Status {
    Status::internal("region admission state mutex poisoned")
}
