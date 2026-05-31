//! Segmented append-only Raft log for Rust raftstore.
//!
//! Raft log durability is kept separate from Holt's state-machine WAL because
//! consensus log append/truncate and metadata tree updates have different
//! access patterns and recovery boundaries.

mod codec;
mod errors;
mod store;
mod types;

pub use errors::{Error, Result};
pub use store::SegmentedRaftLog;
pub use types::{LogEntry, LogMarker};

#[cfg(test)]
mod tests;
