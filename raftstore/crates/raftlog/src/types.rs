#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub region_id: u64,
    pub index: u64,
    pub term: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogMarker {
    pub region_id: u64,
    pub index: u64,
    pub term: u64,
    pub node_id: u64,
}
