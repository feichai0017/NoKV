//! Region admission checks for the proto-facing Rust raftstore service.
//!
//! The checks mirror the first Go raftstore gate: reject requests for the wrong
//! store, stale epoch, non-hosted region, non-leader peer, or keys outside the
//! hosted region before the state machine observes the request.

use nokv_proto::nokv::error::v1 as errorpb;
use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::meta::v1 as metapb;
use tonic::Status;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionAdmission {
    pub region_id: u64,
    pub store_id: u64,
    pub peer_id: u64,
    pub epoch_version: u64,
    pub epoch_conf_version: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub leader: bool,
}

impl Default for RegionAdmission {
    fn default() -> Self {
        Self {
            region_id: 1,
            store_id: 1,
            peer_id: 1,
            epoch_version: 1,
            epoch_conf_version: 1,
            start_key: Vec::new(),
            end_key: Vec::new(),
            leader: true,
        }
    }
}

impl RegionAdmission {
    pub(crate) fn admit_optional_keys<'a, I>(
        &self,
        context: Option<&kvpb::Context>,
        keys: I,
    ) -> Result<Option<errorpb::RegionError>, Status>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        self.admit_keys(context, keys, true)
    }

    pub(crate) fn admit_required_keys<'a, I>(
        &self,
        context: Option<&kvpb::Context>,
        keys: I,
    ) -> Result<Option<errorpb::RegionError>, Status>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        self.admit_keys(context, keys, false)
    }

    fn admit_keys<'a, I>(
        &self,
        context: Option<&kvpb::Context>,
        keys: I,
        skip_empty_keys: bool,
    ) -> Result<Option<errorpb::RegionError>, Status>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        self.validate_context(context)?;
        let context = context.expect("context already validated");
        if context.region_id != self.region_id {
            return Ok(Some(self.region_not_found(context.region_id)));
        }
        if let Some(peer) = context.peer.as_ref() {
            if peer.store_id != 0 && peer.store_id != self.store_id {
                return Ok(Some(self.store_not_match(peer.store_id)));
            }
        }
        if !self.epoch_matches(context.region_epoch.as_ref()) {
            return Ok(Some(self.epoch_not_match()));
        }
        if !self.leader {
            return Ok(Some(self.not_leader()));
        }
        for key in keys {
            if skip_empty_keys && key.is_empty() {
                continue;
            }
            if key.is_empty() || !self.key_in_range(key) {
                return Ok(Some(self.key_not_in_region(key)));
            }
        }
        Ok(None)
    }

    fn validate_context(&self, context: Option<&kvpb::Context>) -> Result<(), Status> {
        let context = context.ok_or_else(|| Status::invalid_argument("context is required"))?;
        if context.region_id == 0 {
            return Err(Status::invalid_argument("region id is required"));
        }
        Ok(())
    }

    fn key_in_range(&self, key: &[u8]) -> bool {
        if !self.start_key.is_empty() && key < self.start_key.as_slice() {
            return false;
        }
        if !self.end_key.is_empty() && key >= self.end_key.as_slice() {
            return false;
        }
        true
    }

    fn epoch_matches(&self, epoch: Option<&metapb::RegionEpoch>) -> bool {
        epoch
            .map(|epoch| {
                epoch.version == self.epoch_version && epoch.conf_version == self.epoch_conf_version
            })
            .unwrap_or(false)
    }

    fn epoch(&self) -> metapb::RegionEpoch {
        metapb::RegionEpoch {
            version: self.epoch_version,
            conf_version: self.epoch_conf_version,
        }
    }

    fn peer(&self) -> metapb::RegionPeer {
        metapb::RegionPeer {
            store_id: self.store_id,
            peer_id: self.peer_id,
        }
    }

    fn descriptor(&self) -> metapb::RegionDescriptor {
        metapb::RegionDescriptor {
            region_id: self.region_id,
            start_key: self.start_key.clone(),
            end_key: self.end_key.clone(),
            epoch: Some(self.epoch()),
            peers: vec![self.peer()],
            ..Default::default()
        }
    }

    fn region_not_found(&self, region_id: u64) -> errorpb::RegionError {
        errorpb::RegionError {
            region_not_found: Some(errorpb::RegionNotFound { region_id }),
            ..Default::default()
        }
    }

    fn store_not_match(&self, request_store_id: u64) -> errorpb::RegionError {
        errorpb::RegionError {
            store_not_match: Some(errorpb::StoreNotMatch {
                request_store_id,
                actual_store_id: self.store_id,
            }),
            ..Default::default()
        }
    }

    fn epoch_not_match(&self) -> errorpb::RegionError {
        errorpb::RegionError {
            epoch_not_match: Some(errorpb::EpochNotMatch {
                current_epoch: Some(self.epoch()),
                regions: vec![self.descriptor()],
            }),
            ..Default::default()
        }
    }

    fn not_leader(&self) -> errorpb::RegionError {
        errorpb::RegionError {
            not_leader: Some(errorpb::NotLeader {
                region_id: self.region_id,
                leader: None,
            }),
            ..Default::default()
        }
    }

    fn key_not_in_region(&self, key: &[u8]) -> errorpb::RegionError {
        errorpb::RegionError {
            key_not_in_region: Some(errorpb::KeyNotInRegion {
                key: key.to_vec(),
                region_id: self.region_id,
                start_key: self.start_key.clone(),
                end_key: self.end_key.clone(),
            }),
            ..Default::default()
        }
    }
}
