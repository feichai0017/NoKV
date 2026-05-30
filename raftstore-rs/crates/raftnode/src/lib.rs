//! OpenRaft boundary for Rust raftstore.
//!
//! The crate intentionally exposes a small NoKV-owned trait instead of leaking
//! OpenRaft types into server, MVCC, or proto-facing code. The concrete
//! OpenRaft-backed implementation will fill this boundary as region replication
//! is brought up.

use std::marker::PhantomData;

pub type NodeId = u64;
pub type RegionId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub region_id: RegionId,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedProposal {
    pub region_id: RegionId,
    pub index: u64,
    pub term: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("raft replication is not wired yet")]
    NotReady,
}

pub trait RegionRaft: Send + Sync {
    fn propose(&self, proposal: Proposal) -> Result<AppliedProposal, Error>;
}

/// Marker for the future OpenRaft-backed implementation. Keeping this type in
/// place makes the dependency explicit while the first slice runs single-node
/// MVCC behind the existing wire contract.
#[derive(Debug, Default)]
pub struct OpenRaftRegion {
    _openraft: PhantomData<openraft::RaftMetrics<u64, ()>>,
}

impl OpenRaftRegion {
    pub fn new() -> Self {
        Self {
            _openraft: PhantomData,
        }
    }
}

impl RegionRaft for OpenRaftRegion {
    fn propose(&self, _proposal: Proposal) -> Result<AppliedProposal, Error> {
        Err(Error::NotReady)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openraft_boundary_does_not_claim_replication_yet() {
        let raft = OpenRaftRegion::new();
        let err = raft
            .propose(Proposal {
                region_id: 1,
                payload: b"cmd".to_vec(),
            })
            .unwrap_err();
        assert!(matches!(err, Error::NotReady));
    }
}
