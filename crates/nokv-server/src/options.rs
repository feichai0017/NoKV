use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use nokv_meta::{HistoryGcOptions, ObjectGcOptions};
use nokv_object::ObjectStoreConfig;
use nokv_types::MountId;

pub const DEFAULT_SERVER_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7777);
pub const DEFAULT_METADATA_CHECKPOINT_ARCHIVE_PREFIX: &str = "metadata/checkpoints";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerOptions {
    pub bind: SocketAddr,
    pub mount: MountId,
    pub meta_path: PathBuf,
    pub metadata_mode: MetadataMode,
    pub metadata_checkpoint_archive_prefix: Option<String>,
    pub object: ObjectStoreConfig,
    pub uid: u32,
    pub gid: u32,
    pub object_gc: ObjectGcOptions,
    pub history_gc: HistoryGcOptions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataMode {
    Local,
}

impl MetadataMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
        }
    }
}
