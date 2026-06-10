use std::fmt;

use crate::chunk::ObjectReadBlock;
use crate::store::{ObjectError, ObjectKey, ObjectStore};

/// Data-fabric transport class for one immutable object block.
///
/// This is placement/transport state, not namespace truth. Metadata manifests
/// continue to name the durable object key and digest; the data path records
/// whether each block came from the local hot tier or the object backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DataTransport {
    ObjectTcpGet,
    LocalNvmeRead,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockPlacement {
    pub object_key: ObjectKey,
    pub transport: DataTransport,
}

pub fn resolve_block_placements<Hot>(
    hot: &Hot,
    blocks: &[ObjectReadBlock],
) -> Result<Vec<BlockPlacement>, ObjectError>
where
    Hot: ObjectStore,
{
    blocks
        .iter()
        .map(|block| {
            let key = ObjectKey::new(block.object_key.clone())?;
            let hot_available = matches!(hot.head(&key), Ok(Some(_)));
            Ok(BlockPlacement {
                object_key: key,
                transport: if hot_available {
                    DataTransport::LocalNvmeRead
                } else {
                    DataTransport::ObjectTcpGet
                },
            })
        })
        .collect()
}

impl fmt::Display for DataTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ObjectTcpGet => "object_tcp_get",
            Self::LocalNvmeRead => "local_nvme_read",
        })
    }
}
