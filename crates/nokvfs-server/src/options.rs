use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use nokvfs_cluster::{FileSharedLogSync, LogTerm};
use nokvfs_meta::{HistoryGcOptions, ObjectGcOptions};
use nokvfs_object::ObjectStoreConfig;
use nokvfs_types::MountId;

pub const DEFAULT_SERVER_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7777);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerOptions {
    pub bind: SocketAddr,
    pub mount: MountId,
    pub meta_path: PathBuf,
    pub metadata_log_path: Option<PathBuf>,
    pub metadata_log_term: LogTerm,
    pub metadata_log_sync: FileSharedLogSync,
    pub object: ObjectStoreConfig,
    pub uid: u32,
    pub gid: u32,
    pub object_gc: ObjectGcOptions,
    pub history_gc: HistoryGcOptions,
}
