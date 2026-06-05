use std::fmt;

use crate::{LogPosition, NodeId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataRaftError {
    ZeroTerm,
    ZeroIndex,
    ZeroNodeId,
    EmptyBatch,
    NoVoters,
    DuplicateNode(NodeId),
    UnknownNode(NodeId),
    ReadNotFresh {
        required: LogPosition,
        applied: Option<LogPosition>,
    },
    Backend(String),
}

impl fmt::Display for MetadataRaftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroTerm => write!(f, "log term must be non-zero"),
            Self::ZeroIndex => write!(f, "log index must be non-zero"),
            Self::ZeroNodeId => write!(f, "cluster node id must be non-zero"),
            Self::EmptyBatch => write!(f, "metadata raft command batch is empty"),
            Self::NoVoters => write!(f, "metadata raft requires at least one voter"),
            Self::DuplicateNode(node) => {
                write!(f, "metadata raft has duplicate node {}", node.get())
            }
            Self::UnknownNode(node) => {
                write!(f, "metadata raft node {} is unknown", node.get())
            }
            Self::ReadNotFresh { required, applied } => {
                let applied = applied
                    .map(|position| format!("{}:{}", position.term.get(), position.index.get()))
                    .unwrap_or_else(|| "none".to_owned());
                write!(
                    f,
                    "metadata read requires applied frontier {}:{}, current applied frontier is {}",
                    required.term.get(),
                    required.index.get(),
                    applied
                )
            }
            Self::Backend(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for MetadataRaftError {}
