use std::fmt;

use fuser::Errno;
use nokv_meta::PublishArtifactRange;
use nokv_object::{ChunkedWrite, PendingChunkedWrite, DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE};
use nokv_types::InodeId;
use sha2::{Digest, Sha256};

use crate::backend::FuseBackend;

use super::errno;

pub(super) const FUSE_WRITEBACK_UPLOAD_THRESHOLD: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub(super) struct WriteHandle<P> {
    pub(super) inode: InodeId,
    pub(super) parent: InodeId,
    pub(super) name: nokv_types::DentryName,
    pub(super) prepared: Option<P>,
    pub(super) mode: u32,
    pub(super) uid: u32,
    pub(super) gid: u32,
    pub(super) base_size: u64,
    pub(super) size: u64,
    pub(super) writer: Option<nokv_object::FileWritePipeline>,
    pub(super) buffered: Vec<BufferedWriteRange>,
    pub(super) pending_uploads: Vec<PendingBufferedUpload>,
    pub(super) sequential_digest: Option<SequentialDigest>,
    pub(super) dirty: bool,
}

#[derive(Clone, Debug)]
pub(super) struct BufferedWriteRange {
    pub(super) offset: u64,
    pub(super) bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(super) struct PendingBufferedUpload {
    pub(super) pending: PendingChunkedWrite,
    pub(super) ranges: Vec<BufferedWriteRange>,
}

#[derive(Clone)]
pub(super) struct SequentialDigest {
    hasher: Sha256,
    pub(super) len: u64,
}

impl SequentialDigest {
    pub(super) fn new() -> Self {
        Self {
            hasher: Sha256::new(),
            len: 0,
        }
    }

    pub(super) fn append(&mut self, offset: u64, data: &[u8]) -> bool {
        if offset != self.len {
            return false;
        }
        self.hasher.update(data);
        self.len = self
            .len
            .saturating_add(u64::try_from(data.len()).unwrap_or(u64::MAX));
        true
    }

    pub(super) fn digest_uri_for_size(&self, size: u64) -> Option<String> {
        if self.len != size {
            return None;
        }
        let digest = self.hasher.clone().finalize();
        Some(format!("sha256:{digest:x}"))
    }
}

impl fmt::Debug for SequentialDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SequentialDigest")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub(super) struct WriteStageReservation<P> {
    pub(super) prepared: P,
    pub(super) manifest_id: String,
    pub(super) block_index_base: u64,
    pub(super) ranges: Vec<BufferedWriteRange>,
}

pub(super) fn fuse_manifest_id(parent: InodeId, inode: InodeId) -> String {
    format!("fuse/{}/{}", parent.get(), inode.get())
}

pub(super) fn staged_range_block_count(offset: u64, len: usize) -> Result<u64, Errno> {
    let mut range_offset = 0_usize;
    let mut count = 0_u64;
    while range_offset < len {
        let logical_offset = offset
            .checked_add(u64::try_from(range_offset).map_err(|_| Errno::EINVAL)?)
            .ok_or(Errno::EINVAL)?;
        let chunk_start = (logical_offset / DEFAULT_CHUNK_SIZE).saturating_mul(DEFAULT_CHUNK_SIZE);
        let next_chunk = chunk_start
            .checked_add(DEFAULT_CHUNK_SIZE)
            .ok_or(Errno::EINVAL)?;
        let remaining_in_chunk =
            usize::try_from(next_chunk - logical_offset).map_err(|_| Errno::EINVAL)?;
        let write_len = DEFAULT_BLOCK_SIZE
            .min(remaining_in_chunk)
            .min(len - range_offset);
        if write_len == 0 {
            return Err(Errno::EINVAL);
        }
        count = count.saturating_add(1);
        range_offset += write_len;
    }
    Ok(count)
}

pub(super) fn push_buffered_write(ranges: &mut Vec<BufferedWriteRange>, offset: u64, data: &[u8]) {
    if let Some(last) = ranges.last_mut() {
        let last_end = last.offset.saturating_add(last.bytes.len() as u64);
        if last_end == offset {
            last.bytes.extend_from_slice(data);
            return;
        }
    }
    ranges.push(BufferedWriteRange {
        offset,
        bytes: data.to_vec(),
    });
}

pub(super) fn take_buffered_upload_ranges(
    ranges: &mut Vec<BufferedWriteRange>,
    force: bool,
) -> Result<Vec<BufferedWriteRange>, Errno> {
    let mut upload = Vec::new();
    let mut retained = Vec::new();
    for mut range in ranges.drain(..) {
        if range.bytes.is_empty() {
            continue;
        }
        let upload_len = if force {
            range.bytes.len()
        } else {
            (range.bytes.len() / FUSE_WRITEBACK_UPLOAD_THRESHOLD) * FUSE_WRITEBACK_UPLOAD_THRESHOLD
        };
        if upload_len == 0 {
            retained.push(range);
            continue;
        }
        if upload_len == range.bytes.len() {
            upload.push(range);
            continue;
        }
        let tail = range.bytes.split_off(upload_len);
        let tail_offset = range
            .offset
            .checked_add(u64::try_from(upload_len).map_err(|_| Errno::EINVAL)?)
            .ok_or(Errno::EINVAL)?;
        upload.push(range);
        retained.push(BufferedWriteRange {
            offset: tail_offset,
            bytes: tail,
        });
    }
    *ranges = retained;
    Ok(upload)
}

pub(super) fn buffered_ranges_block_count(ranges: &[BufferedWriteRange]) -> Result<u64, Errno> {
    ranges.iter().try_fold(0_u64, |count, range| {
        staged_range_block_count(range.offset, range.bytes.len())
            .map(|next| count.saturating_add(next))
    })
}

pub(super) fn buffered_publish_ranges(ranges: &[BufferedWriteRange]) -> Vec<PublishArtifactRange> {
    ranges
        .iter()
        .map(|range| PublishArtifactRange {
            offset: range.offset,
            bytes: range.bytes.clone(),
        })
        .collect()
}

pub(super) fn cleanup_written_objects<B: FuseBackend>(
    backend: &B,
    written: &ChunkedWrite,
) -> Result<(), Errno> {
    let staged = written.staged_objects().map_err(errno)?;
    backend.cleanup_staged_objects(&staged).map_err(errno)
}
