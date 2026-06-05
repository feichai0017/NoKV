use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use nokvfs_cluster::{FileMetadataRaftLogSync, NodeId};
use nokvfs_meta::{HistoryGcOptions, ObjectGcOptions};
use nokvfs_object::ObjectStoreConfig;
use nokvfs_types::MountId;

pub const DEFAULT_SERVER_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7777);
pub const DEFAULT_METADATA_CHECKPOINT_ARCHIVE_PREFIX: &str = "metadata/checkpoints";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerOptions {
    pub bind: SocketAddr,
    pub mount: MountId,
    pub meta_path: PathBuf,
    pub metadata_raft_node: NodeId,
    pub metadata_raft_voters: Vec<NodeId>,
    pub metadata_raft_peers: Vec<MetadataRaftPeerOptions>,
    pub metadata_raft_log_sync: FileMetadataRaftLogSync,
    pub metadata_checkpoint_archive_prefix: Option<String>,
    pub object: ObjectStoreConfig,
    pub uid: u32,
    pub gid: u32,
    pub object_gc: ObjectGcOptions,
    pub history_gc: HistoryGcOptions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataRaftPeerOptions {
    pub node: NodeId,
    pub address: SocketAddr,
}
