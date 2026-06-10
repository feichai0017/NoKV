use crate::store::ObjectError;
use nokv_types::{BlockDescriptor, ChunkManifest, SliceManifest};
use sha2::{Digest, Sha256};

use super::read::ObjectReadBlock;
use super::types::{SliceReadPlan, StoredBlock, StoredChunk, StoredSlice};

pub fn chunk_manifest_from_stored_chunk(chunk: &StoredChunk) -> ChunkManifest {
    ChunkManifest {
        chunk_index: chunk.chunk_index,
        logical_offset: chunk.logical_offset,
        len: chunk.len,
        slices: vec![SliceManifest {
            slice_id: 1,
            logical_offset: chunk.logical_offset,
            len: chunk.len,
            blocks: chunk
                .blocks
                .iter()
                .map(|block| BlockDescriptor {
                    object_key: block.object_key.clone(),
                    logical_offset: block.logical_offset,
                    object_offset: block.object_offset,
                    len: block.len,
                    digest_uri: block.digest_uri.clone(),
                })
                .collect(),
        }],
    }
}

pub fn chunk_manifests_from_stored_chunks(chunks: &[StoredChunk]) -> Vec<ChunkManifest> {
    chunks
        .iter()
        .map(chunk_manifest_from_stored_chunk)
        .collect()
}

pub fn manifest_digest_uri(size: u64, generation: u64, chunks: &[StoredChunk]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(size.to_be_bytes());
    hasher.update(generation.to_be_bytes());
    for chunk in chunks {
        hasher.update(chunk.chunk_index.to_be_bytes());
        hasher.update(chunk.logical_offset.to_be_bytes());
        hasher.update(chunk.len.to_be_bytes());
        for block in &chunk.blocks {
            hasher.update(block.object_key.as_bytes());
            hasher.update([0]);
            hasher.update(block.logical_offset.to_be_bytes());
            hasher.update(block.object_offset.to_be_bytes());
            hasher.update(block.len.to_be_bytes());
            hasher.update(block.digest_uri.as_bytes());
            hasher.update([0]);
        }
    }
    let digest = hasher.finalize();
    format!("manifest-sha256:{digest:x}")
}

pub fn plan_slice_reads(
    slices: &[StoredSlice],
    file_offset: u64,
    len: usize,
) -> Result<SliceReadPlan, ObjectError> {
    if len == 0 {
        return Ok(SliceReadPlan {
            output_len: 0,
            blocks: Vec::new(),
        });
    }
    let request_end = file_offset
        .checked_add(u64::try_from(len).map_err(|_| ObjectError::InvalidRange)?)
        .ok_or(ObjectError::InvalidRange)?;
    let request = ReadInterval {
        start: file_offset,
        end: request_end,
    };
    let mut remaining = vec![request];
    let mut blocks = Vec::new();

    for slice in slices.iter().rev() {
        if remaining.is_empty() || slice.len == 0 {
            break;
        }
        let slice_end = slice
            .logical_offset
            .checked_add(slice.len)
            .ok_or(ObjectError::InvalidRange)?;
        let slice_interval = ReadInterval {
            start: slice.logical_offset,
            end: slice_end,
        };
        if slice_interval.intersect(request).is_none() {
            continue;
        }
        for chunk in &slice.chunks {
            if remaining.is_empty() {
                break;
            }
            let chunk_end = chunk
                .logical_offset
                .checked_add(chunk.len)
                .ok_or(ObjectError::InvalidRange)?;
            let chunk_interval = ReadInterval {
                start: chunk.logical_offset,
                end: chunk_end,
            };
            let Some(chunk_scope) = chunk_interval
                .intersect(slice_interval)
                .and_then(|scope| scope.intersect(request))
            else {
                continue;
            };
            for block in &chunk.blocks {
                if block.len == 0 {
                    return Err(ObjectError::InvalidRange);
                }
                let block_end = block
                    .logical_offset
                    .checked_add(block.len)
                    .ok_or(ObjectError::InvalidRange)?;
                let block_interval = ReadInterval {
                    start: block.logical_offset,
                    end: block_end,
                };
                let Some(block_scope) = block_interval.intersect(chunk_scope) else {
                    continue;
                };
                let mut covered = Vec::new();
                for interval in &remaining {
                    let Some(segment) = interval.intersect(block_scope) else {
                        continue;
                    };
                    let block_skip = segment.start.saturating_sub(block.logical_offset);
                    let object_offset = block
                        .object_offset
                        .checked_add(block_skip)
                        .ok_or(ObjectError::InvalidRange)?;
                    blocks.push(ObjectReadBlock {
                        object_key: block.object_key.clone(),
                        digest_uri: block.digest_uri.clone(),
                        object_offset,
                        object_len: block.len,
                        len: usize::try_from(segment.end - segment.start)
                            .map_err(|_| ObjectError::InvalidRange)?,
                        output_offset: usize::try_from(segment.start - file_offset)
                            .map_err(|_| ObjectError::InvalidRange)?,
                    });
                    covered.push(segment);
                }
                for segment in covered {
                    subtract_interval(&mut remaining, segment);
                }
            }
        }
    }
    blocks.sort_by_key(|block| block.output_offset);
    Ok(SliceReadPlan {
        output_len: len,
        blocks,
    })
}

pub fn plan_chunk_manifest_reads(
    manifests: &[ChunkManifest],
    file_offset: u64,
    len: usize,
) -> Result<SliceReadPlan, ObjectError> {
    let slices = manifests
        .iter()
        .flat_map(|manifest| {
            manifest.slices.iter().map(move |slice| StoredSlice {
                slice_id: slice.slice_id,
                logical_offset: slice.logical_offset,
                len: slice.len,
                chunks: vec![StoredChunk {
                    chunk_index: manifest.chunk_index,
                    logical_offset: manifest.logical_offset,
                    len: manifest.len,
                    blocks: slice
                        .blocks
                        .iter()
                        .map(|block| StoredBlock {
                            object_key: block.object_key.clone(),
                            logical_offset: block.logical_offset,
                            object_offset: block.object_offset,
                            len: block.len,
                            digest_uri: block.digest_uri.clone(),
                        })
                        .collect(),
                }],
            })
        })
        .collect::<Vec<_>>();
    plan_slice_reads(&slices, file_offset, len)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReadInterval {
    start: u64,
    end: u64,
}

impl ReadInterval {
    fn intersect(self, other: ReadInterval) -> Option<ReadInterval> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        (start < end).then_some(ReadInterval { start, end })
    }
}

fn subtract_interval(remaining: &mut Vec<ReadInterval>, covered: ReadInterval) {
    let mut next = Vec::with_capacity(remaining.len().saturating_add(1));
    for interval in remaining.drain(..) {
        let Some(overlap) = interval.intersect(covered) else {
            next.push(interval);
            continue;
        };
        if interval.start < overlap.start {
            next.push(ReadInterval {
                start: interval.start,
                end: overlap.start,
            });
        }
        if overlap.end < interval.end {
            next.push(ReadInterval {
                start: overlap.end,
                end: interval.end,
            });
        }
    }
    *remaining = next;
}
