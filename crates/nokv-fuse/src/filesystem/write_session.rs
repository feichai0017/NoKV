use std::collections::HashSet;
use std::sync::Arc;

use fuser::Errno;
use nokv_object::{
    ChunkWriteRange, ChunkedWrite, ObjectBytes, PendingChunkedWrite, DEFAULT_BLOCK_SIZE,
    DEFAULT_CHUNK_SIZE,
};
use nokv_types::InodeId;

use crate::backend::FuseBackend;

use super::errno;

pub(super) const FUSE_WRITEBACK_UPLOAD_THRESHOLD: usize = DEFAULT_BLOCK_SIZE;

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
    /// Logical offsets of the block-aligned regions already staged in the current
    /// write session. FUSE writeback on macFUSE flushes the dirty page set
    /// repeatedly from offset 0, re-dispatching identical bytes for regions that
    /// were already staged. Tracking which block offsets have been staged lets the
    /// writeback skip re-staging them, so a single sequential write produces one
    /// block per logical region instead of dozens of redundant slices (which
    /// otherwise bloats the chunk manifest past its value limit). A re-dispatch
    /// always carries identical bytes, so an offset-level set is sufficient and
    /// avoids hashing the block payload on the (single-threaded on macOS) FUSE write
    /// path. Reset whenever the session republishes under a new generation.
    pub(super) staged_block_offsets: HashSet<u64>,
    pub(super) dirty: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct BufferedWriteRange {
    pub(crate) offset: u64,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(super) struct PendingBufferedUpload {
    pub(super) pending: PendingChunkedWrite,
    pub(super) ranges: Vec<PendingBufferedRange>,
}

#[derive(Clone, Debug)]
pub(crate) struct PendingBufferedRange {
    pub(crate) offset: u64,
    pub(crate) bytes: ObjectBytes,
}

impl PendingBufferedRange {
    pub(crate) fn from_chunk_range(range: ChunkWriteRange) -> Self {
        Self {
            offset: range.logical_offset,
            bytes: range.bytes,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_buffered_owned(range: BufferedWriteRange) -> Self {
        Self {
            offset: range.offset,
            bytes: range.bytes.into(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    pub(crate) fn into_buffered(self) -> BufferedWriteRange {
        BufferedWriteRange {
            offset: self.offset,
            bytes: self.bytes.into_vec(),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct WriteStageReservation<P> {
    pub(super) prepared: P,
    pub(super) manifest_id: String,
    pub(super) block_index_base: u64,
    pub(super) ranges: Vec<ChunkWriteRange>,
}

pub(super) fn fuse_manifest_id(parent: InodeId, inode: InodeId) -> String {
    format!("fuse/{}/{}", parent.get(), inode.get())
}

/// Walk `[offset, offset+len)` as the sequence of block-aligned spans it will be
/// staged as: each span is at most one block, capped at chunk boundaries. This is
/// the single source of truth for how a buffered range maps onto staged blocks;
/// `staged_range_block_count` and `for_each_staged_block` both build on it.
fn for_each_block_span(
    offset: u64,
    len: usize,
    mut visit: impl FnMut(u64, usize, usize) -> Result<(), Errno>,
) -> Result<(), Errno> {
    let mut range_offset = 0_usize;
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
        visit(logical_offset, range_offset, write_len)?;
        range_offset += write_len;
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn staged_range_block_count(offset: u64, len: usize) -> Result<u64, Errno> {
    let mut count = 0_u64;
    for_each_block_span(offset, len, |_logical_offset, _range_offset, _write_len| {
        count = count.saturating_add(1);
        Ok(())
    })?;
    Ok(count)
}

/// Insert a write into the buffered set, keeping it as a sorted, non-overlapping,
/// adjacency-coalesced sparse byte map.
///
/// FUSE writeback does not guarantee that writes arrive append-only or in offset
/// order: the kernel can re-dispatch dirty pages, replaying an earlier offset that
/// is still buffered. A new write therefore overwrites any bytes it covers in
/// place (POSIX overwrite semantics) rather than being appended as a duplicate
/// range. Appending duplicates would later stage overlapping blocks at the same
/// logical offset, which fails prepared-artifact validation (block coverage gap).
///
/// The covered byte set after the call is exactly the old covered set unioned
/// with `[offset, offset+data.len())`; bytes that were never written are never
/// fabricated. Ranges that become contiguous after the insert are coalesced;
/// genuine holes between non-adjacent ranges are preserved as separate ranges.
pub(super) fn push_buffered_write(ranges: &mut Vec<BufferedWriteRange>, offset: u64, data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let write_end = offset.saturating_add(data.len() as u64);

    // Existing ranges that overlap or directly abut the write window: those at
    // [first, last) get merged with the new write into one contiguous range.
    // Ranges merely adjacent (touching end-to-start) are included so the result
    // stays coalesced, but only the overlapping/adjacent ones — never a gapped
    // one — so no unwritten bytes are introduced.
    let first = ranges
        .partition_point(|range| range.offset.saturating_add(range.bytes.len() as u64) < offset);
    let mut last = first;
    while last < ranges.len() && ranges[last].offset <= write_end {
        last += 1;
    }

    if first == last {
        ranges.insert(
            first,
            BufferedWriteRange {
                offset,
                bytes: data.to_vec(),
            },
        );
        return;
    }

    // Fast path: the write merges with exactly one existing range and starts at
    // or after that range's offset (the overwhelmingly common sequential-append
    // and in-place-overwrite cases). Mutate that range's buffer directly instead
    // of rebuilding the whole merged span, keeping the per-write cost O(data.len())
    // — Vec's geometric growth makes repeated appends amortized O(1) — rather than
    // O(accumulated bytes), which would make a streamed write O(n^2) overall.
    if last - first == 1 && offset >= ranges[first].offset {
        let range = &mut ranges[first];
        let start = (offset - range.offset) as usize;
        let overlap = data.len().min(range.bytes.len().saturating_sub(start));
        range.bytes[start..start + overlap].copy_from_slice(&data[..overlap]);
        if overlap < data.len() {
            range.bytes.extend_from_slice(&data[overlap..]);
        }
        return;
    }

    let merged_offset = ranges[first].offset.min(offset);
    let merged_end = ranges[last - 1]
        .offset
        .saturating_add(ranges[last - 1].bytes.len() as u64)
        .max(write_end);
    let mut bytes = vec![0_u8; (merged_end - merged_offset) as usize];
    // Mark which bytes of the merged span are real (covered by an existing range
    // or the new write) so any genuine hole spanned by [first, last) is not
    // emitted as fabricated zeros but split back out as separate ranges.
    let mut covered = vec![false; bytes.len()];
    let mark = |start: usize, src: &[u8], bytes: &mut [u8], covered: &mut [bool]| {
        bytes[start..start + src.len()].copy_from_slice(src);
        for slot in &mut covered[start..start + src.len()] {
            *slot = true;
        }
    };
    for range in &ranges[first..last] {
        let start = (range.offset - merged_offset) as usize;
        mark(start, &range.bytes, &mut bytes, &mut covered);
    }
    let write_start = (offset - merged_offset) as usize;
    mark(write_start, data, &mut bytes, &mut covered);

    // Emit one range per maximal run of covered bytes, preserving holes.
    let mut replacement = Vec::new();
    let mut index = 0_usize;
    while index < covered.len() {
        if !covered[index] {
            index += 1;
            continue;
        }
        let run_start = index;
        while index < covered.len() && covered[index] {
            index += 1;
        }
        replacement.push(BufferedWriteRange {
            offset: merged_offset + run_start as u64,
            bytes: bytes[run_start..index].to_vec(),
        });
    }

    ranges.splice(first..last, replacement);
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

pub(super) fn has_buffered_upload_ready(ranges: &[BufferedWriteRange], force: bool) -> bool {
    if force {
        return ranges.iter().any(|range| !range.bytes.is_empty());
    }
    ranges
        .iter()
        .any(|range| range.bytes.len() >= FUSE_WRITEBACK_UPLOAD_THRESHOLD)
}

/// Drop block-aligned units whose logical offset has already been staged in this
/// session, and return one surviving range per staged block. `staged_offsets`
/// records every block offset that survives so a later re-dispatch of the same
/// region is skipped.
///
/// A FUSE writeback re-dispatch always re-sends byte-identical pages, so matching
/// on the block offset alone is sufficient to suppress the duplicate without
/// hashing the 4 MiB payload — which on macOS (single FUSE worker thread) would
/// otherwise serialize a multi-millisecond SHA-256 pass into the write path for
/// every block. Re-staging an already-covered offset would only append a duplicate
/// chunk that the reader resolves first-writer-wins anyway, so dropping it is both
/// faster and avoids manifest bloat.
pub(super) fn select_unstaged_blocks(
    ranges: Vec<BufferedWriteRange>,
    staged_offsets: &mut HashSet<u64>,
) -> Result<Vec<ChunkWriteRange>, Errno> {
    let mut out = Vec::new();
    for range in ranges {
        if range.bytes.is_empty() {
            continue;
        }
        let bytes = Arc::new(range.bytes);
        let mut spans = Vec::new();
        for_each_block_span(range.offset, bytes.len(), |block_offset, start, len| {
            spans.push((block_offset, start, len));
            Ok(())
        })?;
        if spans.len() == 1 {
            let (block_offset, start, len) = spans[0];
            if !staged_offsets.insert(block_offset) {
                continue;
            }
            push_selected_block(&mut out, block_offset, &bytes, start, len)?;
            continue;
        }
        for (block_offset, start, len) in spans {
            if !staged_offsets.insert(block_offset) {
                continue;
            }
            push_selected_block(&mut out, block_offset, &bytes, start, len)?;
        }
    }
    Ok(out)
}

fn push_selected_block(
    out: &mut Vec<ChunkWriteRange>,
    block_offset: u64,
    bytes: &Arc<Vec<u8>>,
    start: usize,
    len: usize,
) -> Result<(), Errno> {
    let bytes = ObjectBytes::shared_vec_slice(Arc::clone(bytes), start, len).map_err(errno)?;
    out.push(ChunkWriteRange {
        logical_offset: block_offset,
        bytes,
    });
    Ok(())
}

/// Forget the staged-offset entries for every block-aligned unit covered by
/// `ranges`. Used when a staging attempt did not land durably (submission error
/// or async upload failure), so the retry re-stages those blocks rather than
/// treating them as already staged.
pub(super) fn forget_staged_blocks(
    staged_offsets: &mut HashSet<u64>,
    ranges: &[BufferedWriteRange],
) -> Result<(), Errno> {
    for range in ranges {
        for_each_block_span(
            range.offset,
            range.bytes.len(),
            |block_offset, _start, _len| {
                staged_offsets.remove(&block_offset);
                Ok(())
            },
        )?;
    }
    Ok(())
}

pub(super) fn cleanup_written_objects<B: FuseBackend>(
    backend: &B,
    written: &ChunkedWrite,
) -> Result<(), Errno> {
    let staged = written.staged_objects().map_err(errno)?;
    backend.cleanup_staged_objects(&staged).map_err(errno)
}
