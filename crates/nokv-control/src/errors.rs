use std::fmt;

use crate::{NodeId, ShardId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlError {
    ShardAlreadyOwned {
        shard_id: ShardId,
        owner: NodeId,
        epoch: u64,
    },
    ShardNotFound(ShardId),
    StaleEpoch {
        shard_id: ShardId,
        expected: u64,
        actual: u64,
    },
    NotOwner {
        shard_id: ShardId,
    },
    StaleLease {
        shard_id: ShardId,
        epoch: u64,
        lease_id: u64,
    },
    InvalidOptions(String),
    Codec(String),
    Backend(String),
}

impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShardAlreadyOwned {
                shard_id,
                owner,
                epoch,
            } => write!(
                f,
                "shard {} is already owned by {} at epoch {}",
                shard_id.as_str(),
                owner.as_str(),
                epoch
            ),
            Self::ShardNotFound(shard_id) => write!(f, "shard {} was not found", shard_id.as_str()),
            Self::StaleEpoch {
                shard_id,
                expected,
                actual,
            } => write!(
                f,
                "shard {} expected epoch {}, actual epoch {}",
                shard_id.as_str(),
                expected,
                actual
            ),
            Self::NotOwner { shard_id } => {
                write!(f, "lease holder does not own shard {}", shard_id.as_str())
            }
            Self::StaleLease {
                shard_id,
                epoch,
                lease_id,
            } => write!(
                f,
                "stale lease for shard {} at epoch {} lease {}",
                shard_id.as_str(),
                epoch,
                lease_id
            ),
            Self::InvalidOptions(err) => write!(f, "invalid control store options: {err}"),
            Self::Codec(err) => write!(f, "control store codec error: {err}"),
            Self::Backend(err) => write!(f, "control store backend error: {err}"),
        }
    }
}

impl std::error::Error for ControlError {}
