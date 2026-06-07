use crate::MetadataRaftError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogTerm(u64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogIndex(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogPosition {
    pub term: LogTerm,
    pub index: LogIndex,
}

impl LogTerm {
    pub fn new(term: u64) -> Result<Self, MetadataRaftError> {
        if term == 0 {
            return Err(MetadataRaftError::ZeroTerm);
        }
        Ok(Self(term))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl LogIndex {
    pub const ZERO: Self = Self(0);

    pub fn new(index: u64) -> Result<Self, MetadataRaftError> {
        if index == 0 {
            return Err(MetadataRaftError::ZeroIndex);
        }
        Ok(Self(index))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl NodeId {
    pub fn new(id: u64) -> Result<Self, MetadataRaftError> {
        if id == 0 {
            return Err(MetadataRaftError::ZeroNodeId);
        }
        Ok(Self(id))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}
