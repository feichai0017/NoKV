//! Region admission checks for the proto-facing Rust raftstore service.
//!
//! The checks mirror the first Go raftstore gate: reject requests for the wrong
//! store, stale epoch, non-hosted region, non-leader peer, or keys outside the
//! hosted region before the state machine observes the request.

use std::collections::BTreeMap;
use std::fmt;

use nokv_proto::nokv::error::v1 as errorpb;
use nokv_proto::nokv::kv::v1 as kvpb;
use nokv_proto::nokv::meta::v1 as metapb;
use tonic::Status;

use crate::RaftRuntimeStatus;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionAdmission {
    pub region_id: u64,
    pub store_id: u64,
    pub peer_id: u64,
    pub peers: BTreeMap<u64, u64>,
    pub leader_peer_id: u64,
    pub epoch_version: u64,
    pub epoch_conf_version: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub leader: bool,
    pub hosted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionAdmissionConfigError {
    MissingRegionId,
    MissingEpoch,
    MissingPeer,
}

impl fmt::Display for RegionAdmissionConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRegionId => f.write_str("region descriptor id is required"),
            Self::MissingEpoch => f.write_str("region descriptor epoch is required"),
            Self::MissingPeer => f.write_str("region descriptor peer is required"),
        }
    }
}

impl std::error::Error for RegionAdmissionConfigError {}

impl Default for RegionAdmission {
    fn default() -> Self {
        Self {
            region_id: 1,
            store_id: 1,
            peer_id: 1,
            peers: BTreeMap::from([(1, 1)]),
            leader_peer_id: 1,
            epoch_version: 1,
            epoch_conf_version: 1,
            start_key: Vec::new(),
            end_key: Vec::new(),
            leader: true,
            hosted: true,
        }
    }
}

impl RegionAdmission {
    pub fn from_descriptor(
        descriptor: &metapb::RegionDescriptor,
        leader: bool,
    ) -> Result<Self, RegionAdmissionConfigError> {
        if descriptor.region_id == 0 {
            return Err(RegionAdmissionConfigError::MissingRegionId);
        }
        let epoch = descriptor
            .epoch
            .as_ref()
            .ok_or(RegionAdmissionConfigError::MissingEpoch)?;
        let peer = descriptor
            .peers
            .iter()
            .find(|peer| peer.store_id != 0 && peer.peer_id != 0)
            .ok_or(RegionAdmissionConfigError::MissingPeer)?;
        let peers = descriptor
            .peers
            .iter()
            .filter(|peer| peer.store_id != 0 && peer.peer_id != 0)
            .map(|peer| (peer.peer_id, peer.store_id))
            .collect::<BTreeMap<_, _>>();
        Ok(Self {
            region_id: descriptor.region_id,
            store_id: peer.store_id,
            peer_id: peer.peer_id,
            peers,
            leader_peer_id: if leader { peer.peer_id } else { 0 },
            epoch_version: epoch.version,
            epoch_conf_version: epoch.conf_version,
            start_key: descriptor.start_key.clone(),
            end_key: descriptor.end_key.clone(),
            leader,
            hosted: leader,
        })
    }

    pub(crate) fn with_runtime_status(&self, status: RaftRuntimeStatus) -> Self {
        let mut admission = self.clone();
        if status.local_peer_id != 0 {
            admission.peer_id = status.local_peer_id;
            if let Some(store_id) = admission.peers.get(&status.local_peer_id) {
                admission.store_id = *store_id;
            }
        }
        admission.leader = status.leader;
        admission.leader_peer_id = status.leader_peer_id;
        admission.hosted = status.hosted;
        admission
    }

    pub(crate) fn admit_read_optional_keys<'a, I>(
        &self,
        context: Option<&kvpb::Context>,
        keys: I,
    ) -> Result<Option<errorpb::RegionError>, Status>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        self.admit_keys(context, keys, true, AdmissionRole::Read)
    }

    #[cfg(test)]
    pub(crate) fn admit_leader_optional_keys<'a, I>(
        &self,
        context: Option<&kvpb::Context>,
        keys: I,
    ) -> Result<Option<errorpb::RegionError>, Status>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        self.admit_keys(context, keys, true, AdmissionRole::LeaderOnly)
    }

    pub(crate) fn admit_leader_required_keys<'a, I>(
        &self,
        context: Option<&kvpb::Context>,
        keys: I,
    ) -> Result<Option<errorpb::RegionError>, Status>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        self.admit_keys(context, keys, false, AdmissionRole::LeaderOnly)
    }

    fn admit_keys<'a, I>(
        &self,
        context: Option<&kvpb::Context>,
        keys: I,
        skip_empty_keys: bool,
        role: AdmissionRole,
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
        for key in keys {
            if skip_empty_keys && key.is_empty() {
                continue;
            }
            if key.is_empty() || !self.key_in_range(key) {
                return Ok(Some(self.key_not_in_region(key)));
            }
        }
        if !self.hosted {
            return Ok(Some(self.region_not_found(context.region_id)));
        }
        if !self.leader && !self.read_can_run_on_follower(context, role) {
            return Ok(Some(self.non_leader_error(context, role)));
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

    pub(crate) fn key_in_range(&self, key: &[u8]) -> bool {
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

    fn read_can_run_on_follower(&self, context: &kvpb::Context, role: AdmissionRole) -> bool {
        role == AdmissionRole::Read
            && normalize_read_preference(context.read_preference)
                == kvpb::ReadPreference::FollowerPrefer
            && normalize_read_consistency(context.read_consistency)
                == kvpb::ReadConsistency::BoundedStale
    }

    fn epoch(&self) -> metapb::RegionEpoch {
        metapb::RegionEpoch {
            version: self.epoch_version,
            conf_version: self.epoch_conf_version,
        }
    }

    fn non_leader_error(
        &self,
        context: &kvpb::Context,
        role: AdmissionRole,
    ) -> errorpb::RegionError {
        if role == AdmissionRole::Read
            && normalize_read_preference(context.read_preference)
                == kvpb::ReadPreference::FollowerPrefer
        {
            // The Go client falls back to a leader read only when a
            // follower-prefer attempt returns StaleCommand. Returning NotLeader
            // here would stop that fallback path before Rust owns safe follower
            // ReadIndex or bounded-stale serving.
            return self.stale_command();
        }
        self.not_leader()
    }

    fn peer(&self) -> metapb::RegionPeer {
        metapb::RegionPeer {
            store_id: self.store_id,
            peer_id: self.peer_id,
        }
    }

    pub(crate) fn descriptor(&self) -> metapb::RegionDescriptor {
        let peers = if self.peers.is_empty() {
            vec![self.peer()]
        } else {
            self.peers
                .iter()
                .map(|(peer_id, store_id)| metapb::RegionPeer {
                    store_id: *store_id,
                    peer_id: *peer_id,
                })
                .collect()
        };
        metapb::RegionDescriptor {
            region_id: self.region_id,
            start_key: self.start_key.clone(),
            end_key: self.end_key.clone(),
            epoch: Some(self.epoch()),
            peers,
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
        let leader = self
            .peers
            .get(&self.leader_peer_id)
            .map(|store_id| metapb::RegionPeer {
                store_id: *store_id,
                peer_id: self.leader_peer_id,
            });
        errorpb::RegionError {
            not_leader: Some(errorpb::NotLeader {
                region_id: self.region_id,
                leader,
            }),
            ..Default::default()
        }
    }

    fn stale_command(&self) -> errorpb::RegionError {
        errorpb::RegionError {
            stale_command: Some(errorpb::StaleCommand {}),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionRole {
    Read,
    LeaderOnly,
}

fn normalize_read_preference(preference: i32) -> kvpb::ReadPreference {
    kvpb::ReadPreference::try_from(preference).unwrap_or(kvpb::ReadPreference::LeaderOnly)
}

fn normalize_read_consistency(consistency: i32) -> kvpb::ReadConsistency {
    kvpb::ReadConsistency::try_from(consistency).unwrap_or(kvpb::ReadConsistency::Strong)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_can_be_built_from_region_descriptor() {
        let descriptor = metapb::RegionDescriptor {
            region_id: 7,
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            epoch: Some(metapb::RegionEpoch {
                version: 3,
                conf_version: 2,
            }),
            peers: vec![metapb::RegionPeer {
                store_id: 9,
                peer_id: 99,
            }],
            ..Default::default()
        };
        let admission = RegionAdmission::from_descriptor(&descriptor, true).unwrap();
        assert_eq!(admission.region_id, 7);
        assert_eq!(admission.store_id, 9);
        assert_eq!(admission.peer_id, 99);
        assert_eq!(admission.peers, BTreeMap::from([(99, 9)]));
        assert_eq!(admission.leader_peer_id, 99);
        assert_eq!(admission.epoch_version, 3);
        assert_eq!(admission.epoch_conf_version, 2);
        assert_eq!(admission.start_key, b"a".to_vec());
        assert_eq!(admission.end_key, b"z".to_vec());
        assert!(admission.leader);
        assert!(admission.hosted);
    }
}
