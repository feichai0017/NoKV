use std::fmt;

use crate::{LogIndex, LogPosition};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SharedLogError {
    ZeroTerm,
    ZeroIndex,
    EmptyBatch,
    Compacted {
        requested: LogIndex,
        compacted: LogIndex,
    },
    Backend(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayError {
    Log(SharedLogError),
    Apply {
        position: LogPosition,
        batch_position: usize,
        message: String,
    },
    EmptyEntry {
        position: LogPosition,
    },
    NonContiguousLog {
        expected: LogIndex,
        actual: LogIndex,
    },
    IndexOverflow(LogIndex),
}

impl fmt::Display for SharedLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroTerm => write!(f, "log term must be non-zero"),
            Self::ZeroIndex => write!(f, "log index must be non-zero"),
            Self::EmptyBatch => write!(f, "metadata log entry batch is empty"),
            Self::Compacted {
                requested,
                compacted,
            } => write!(
                f,
                "requested log index {} was compacted through {}",
                requested.get(),
                compacted.get()
            ),
            Self::Backend(message) => write!(f, "{message}"),
        }
    }
}

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Log(err) => write!(f, "{err}"),
            Self::Apply {
                position,
                batch_position,
                message,
            } => write!(
                f,
                "failed to apply metadata command at {}:{} batch {}: {}",
                position.term.get(),
                position.index.get(),
                batch_position,
                message
            ),
            Self::EmptyEntry { position } => write!(
                f,
                "metadata log entry at {}:{} contains no commands",
                position.term.get(),
                position.index.get()
            ),
            Self::NonContiguousLog { expected, actual } => write!(
                f,
                "metadata log replay expected index {}, got {}",
                expected.get(),
                actual.get()
            ),
            Self::IndexOverflow(index) => {
                write!(f, "metadata log index overflow after {}", index.get())
            }
        }
    }
}

impl std::error::Error for SharedLogError {}

impl std::error::Error for ReplayError {}

impl From<SharedLogError> for ReplayError {
    fn from(value: SharedLogError) -> Self {
        Self::Log(value)
    }
}
