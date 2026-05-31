#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt raft log record at offset {offset}")]
    Corrupt { offset: u64 },
    #[error("corrupt raft log marker")]
    CorruptMarker,
    #[error("raft log append has purged entry index {index}, last purged index {purged_index}")]
    AppendPurgedEntry { index: u64, purged_index: u64 },
    #[error("raft log append is not consecutive: expected index {expected}, got {actual}")]
    NonConsecutiveAppend { expected: u64, actual: u64 },
}

pub type Result<T> = std::result::Result<T, Error>;
