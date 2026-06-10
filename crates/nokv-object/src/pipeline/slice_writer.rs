use crate::chunk::ChunkWriteRange;
use crate::store::{ObjectBytes, ObjectError};

#[derive(Clone, Debug)]
pub struct ObjectSliceWriter {
    block_size: usize,
    uploaded: u64,
    ranges: Vec<ChunkWriteRange>,
}

impl ObjectSliceWriter {
    pub fn new(block_size: usize) -> Result<Self, ObjectError> {
        if block_size == 0 {
            return Err(ObjectError::InvalidChunkLayout);
        }
        Ok(Self {
            block_size,
            uploaded: 0,
            ranges: Vec::new(),
        })
    }

    pub fn uploaded(&self) -> u64 {
        self.uploaded
    }

    pub fn write_at(
        &mut self,
        logical_offset: u64,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<(), ObjectError> {
        if logical_offset < self.uploaded {
            return Err(ObjectError::Backend(
                "cannot overwrite an uploaded slice block".to_owned(),
            ));
        }
        push_slice_range(&mut self.ranges, logical_offset, bytes.into())
    }

    pub fn flush_to(&mut self, offset: u64) -> Result<Vec<ChunkWriteRange>, ObjectError> {
        if offset < self.uploaded {
            return Err(ObjectError::InvalidRange);
        }
        let flush_to = align_down(offset, self.block_size)?;
        let flushed = take_ranges_before(&mut self.ranges, flush_to)?;
        self.uploaded = flush_to;
        Ok(flushed)
    }

    pub fn finish(&mut self) -> Result<Vec<ChunkWriteRange>, ObjectError> {
        let mut ranges = Vec::new();
        std::mem::swap(&mut ranges, &mut self.ranges);
        if let Some(end) = ranges
            .iter()
            .map(|range| {
                range
                    .logical_offset
                    .saturating_add(range.bytes.len() as u64)
            })
            .max()
        {
            self.uploaded = self.uploaded.max(end);
        }
        Ok(ranges)
    }

    pub fn abort(&mut self) {
        self.ranges.clear();
    }
}

fn align_down(offset: u64, block_size: usize) -> Result<u64, ObjectError> {
    let block_size = u64::try_from(block_size).map_err(|_| ObjectError::InvalidChunkLayout)?;
    Ok(offset / block_size * block_size)
}

fn take_ranges_before(
    ranges: &mut Vec<ChunkWriteRange>,
    offset: u64,
) -> Result<Vec<ChunkWriteRange>, ObjectError> {
    let mut flushed = Vec::new();
    let mut retained = Vec::new();
    for mut range in ranges.drain(..) {
        if range.bytes.is_empty() {
            continue;
        }
        let end = range
            .logical_offset
            .checked_add(range.bytes.len() as u64)
            .ok_or(ObjectError::InvalidRange)?;
        if end <= offset {
            flushed.push(range);
        } else if range.logical_offset >= offset {
            retained.push(range);
        } else {
            let split = usize::try_from(offset - range.logical_offset)
                .map_err(|_| ObjectError::InvalidRange)?;
            let mut bytes = range.bytes.into_vec();
            let tail = bytes.split_off(split);
            range.bytes = bytes.into();
            flushed.push(range);
            retained.push(ChunkWriteRange {
                logical_offset: offset,
                bytes: tail.into(),
            });
        }
    }
    *ranges = retained;
    Ok(flushed)
}

fn push_slice_range(
    ranges: &mut Vec<ChunkWriteRange>,
    logical_offset: u64,
    bytes: ObjectBytes,
) -> Result<(), ObjectError> {
    if bytes.is_empty() {
        return Ok(());
    }
    let write_end = logical_offset
        .checked_add(bytes.len() as u64)
        .ok_or(ObjectError::InvalidRange)?;
    let first = ranges.partition_point(|range| {
        range
            .logical_offset
            .saturating_add(range.bytes.len() as u64)
            < logical_offset
    });
    let mut last = first;
    while last < ranges.len() && ranges[last].logical_offset <= write_end {
        last += 1;
    }
    if first == last {
        ranges.insert(
            first,
            ChunkWriteRange {
                logical_offset,
                bytes,
            },
        );
        return Ok(());
    }

    if last - first == 1 && logical_offset >= ranges[first].logical_offset {
        let range = &mut ranges[first];
        let start = usize::try_from(logical_offset - range.logical_offset)
            .map_err(|_| ObjectError::InvalidRange)?;
        let incoming = bytes.into_vec();
        let mut existing = std::mem::take(&mut range.bytes).into_vec();
        let overlap = incoming.len().min(existing.len().saturating_sub(start));
        existing[start..start + overlap].copy_from_slice(&incoming[..overlap]);
        if overlap < incoming.len() {
            existing.extend_from_slice(&incoming[overlap..]);
        }
        range.bytes = existing.into();
        return Ok(());
    }

    let merged_offset = ranges[first].logical_offset.min(logical_offset);
    let merged_end = ranges[last - 1]
        .logical_offset
        .saturating_add(ranges[last - 1].bytes.len() as u64)
        .max(write_end);
    let merged_len =
        usize::try_from(merged_end - merged_offset).map_err(|_| ObjectError::InvalidRange)?;
    let mut merged = vec![0_u8; merged_len];
    let mut covered = vec![false; merged_len];
    for range in &ranges[first..last] {
        mark_range(
            merged_offset,
            range.logical_offset,
            range.bytes.as_slice(),
            &mut merged,
            &mut covered,
        )?;
    }
    mark_range(
        merged_offset,
        logical_offset,
        bytes.as_slice(),
        &mut merged,
        &mut covered,
    )?;

    let mut replacement = Vec::new();
    let mut index = 0_usize;
    while index < covered.len() {
        if !covered[index] {
            index += 1;
            continue;
        }
        let start = index;
        while index < covered.len() && covered[index] {
            index += 1;
        }
        replacement.push(ChunkWriteRange {
            logical_offset: merged_offset + start as u64,
            bytes: merged[start..index].to_vec().into(),
        });
    }
    ranges.splice(first..last, replacement);
    Ok(())
}

fn mark_range(
    base_offset: u64,
    range_offset: u64,
    bytes: &[u8],
    merged: &mut [u8],
    covered: &mut [bool],
) -> Result<(), ObjectError> {
    let start =
        usize::try_from(range_offset - base_offset).map_err(|_| ObjectError::InvalidRange)?;
    let end = start
        .checked_add(bytes.len())
        .ok_or(ObjectError::InvalidRange)?;
    merged[start..end].copy_from_slice(bytes);
    covered[start..end].fill(true);
    Ok(())
}
