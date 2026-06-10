use crate::cache::{block_range_cache_key, BlockCache};
use crate::store::{ObjectError, ObjectGetRequest, ObjectKey, ObjectRange, ObjectStore};

use super::singleflight::{ObjectReadCoordinator, ObjectReadCoordinatorKey};

#[derive(Clone, Debug)]
pub struct BlockReadOptions {
    pub cache_fill: ReadCacheFillMode,
    pub read_coordinator: Option<ObjectReadCoordinator>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReadCacheFillMode {
    Exact,
    BlockAligned { block_size: usize },
    ForwardAligned { block_size: usize },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectReadBlock {
    pub object_key: String,
    pub digest_uri: String,
    pub object_offset: u64,
    pub object_len: u64,
    pub len: usize,
    pub output_offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockReadOutcome {
    pub bytes: Vec<u8>,
    pub object_gets: usize,
    pub object_get_bytes: u64,
    pub coalesced_gets: usize,
    pub coalesced_get_bytes: u64,
    pub cache_hits: usize,
    pub cache_hit_bytes: u64,
}

#[derive(Clone, Debug)]
struct PendingFetch {
    key: ObjectKey,
    digest_uri: String,
    fetch_offset: u64,
    fetch_len: usize,
    exact_offset: u64,
    exact_len: usize,
    reads_start: usize,
    reads_end: usize,
}

#[derive(Clone, Debug)]
struct FetchedRange {
    object_offset: u64,
    bytes: Vec<u8>,
    object_get: bool,
}

impl Default for BlockReadOptions {
    fn default() -> Self {
        Self {
            cache_fill: ReadCacheFillMode::Exact,
            read_coordinator: None,
        }
    }
}

impl BlockReadOptions {
    pub fn block_aligned_cache_fill(block_size: usize) -> Self {
        Self {
            cache_fill: ReadCacheFillMode::BlockAligned { block_size },
            read_coordinator: None,
        }
    }

    pub fn forward_aligned_cache_fill(block_size: usize) -> Self {
        Self {
            cache_fill: ReadCacheFillMode::ForwardAligned { block_size },
            read_coordinator: None,
        }
    }

    pub fn with_cache_fill(mut self, cache_fill: ReadCacheFillMode) -> Self {
        self.cache_fill = cache_fill;
        self
    }

    pub fn with_read_coordinator(mut self, coordinator: ObjectReadCoordinator) -> Self {
        self.read_coordinator = Some(coordinator);
        self
    }
}

pub fn read_object_blocks_with_cache_options<O, C>(
    store: &O,
    cache: Option<&C>,
    output_len: usize,
    blocks: &[ObjectReadBlock],
    options: BlockReadOptions,
) -> Result<BlockReadOutcome, ObjectError>
where
    O: ObjectStore,
    C: BlockCache + ?Sized,
{
    #[derive(Clone)]
    struct PendingRead {
        key: ObjectKey,
        digest_uri: String,
        object_offset: u64,
        object_len: u64,
        len: usize,
        output_offset: usize,
    }

    let mut out = vec![0_u8; output_len];
    let mut object_gets = 0_usize;
    let mut object_get_bytes = 0_u64;
    let mut coalesced_gets = 0_usize;
    let mut coalesced_get_bytes = 0_u64;
    let mut cache_hits = 0_usize;
    let mut cache_hit_bytes = 0_u64;
    let mut pending = Vec::new();
    for block in blocks {
        let key = ObjectKey::new(block.object_key.clone())?;
        if block.len == 0 {
            return Err(ObjectError::InvalidRange);
        }
        let object_end = checked_read_end(block.object_offset, block.len)?;
        if object_end > block.object_len {
            return Err(ObjectError::InvalidRange);
        }
        let end = block
            .output_offset
            .checked_add(block.len)
            .ok_or(ObjectError::InvalidRange)?;
        if end > out.len() {
            return Err(ObjectError::InvalidRange);
        }
        if let Some(cache) = cache {
            if let Some(cached) =
                cache.get_block_range(key.as_str(), block.object_offset, block.len)?
            {
                if cached.len() != block.len {
                    return Err(ObjectError::InvalidRange);
                }
                cache_hits += 1;
                cache_hit_bytes = cache_hit_bytes.saturating_add(cached.len() as u64);
                out[block.output_offset..end].copy_from_slice(&cached);
                continue;
            }
        }
        pending.push(PendingRead {
            key,
            digest_uri: block.digest_uri.clone(),
            object_offset: block.object_offset,
            object_len: block.object_len,
            len: block.len,
            output_offset: block.output_offset,
        });
    }
    pending.sort_by(|left, right| {
        left.key
            .as_str()
            .cmp(right.key.as_str())
            .then_with(|| left.digest_uri.cmp(&right.digest_uri))
            .then_with(|| left.object_offset.cmp(&right.object_offset))
            .then_with(|| left.output_offset.cmp(&right.output_offset))
    });

    let mut fetches = Vec::new();
    let mut index = 0_usize;
    while index < pending.len() {
        let start = index;
        let mut end = index + 1;
        let first_exact_end = checked_read_end(pending[index].object_offset, pending[index].len)?;
        let (mut fetch_offset, mut fetch_end) = cache_fill_range(
            options.cache_fill,
            pending[index].object_offset,
            pending[index].len,
            pending[index].object_len,
        )?;
        let mut exact_offset = pending[index].object_offset;
        let mut exact_end = first_exact_end;
        while end < pending.len()
            && pending[end].key == pending[start].key
            && pending[end].digest_uri == pending[start].digest_uri
            && pending[end].object_len == pending[start].object_len
        {
            let read_end = checked_read_end(pending[end].object_offset, pending[end].len)?;
            let (next_fetch_offset, next_fetch_end) = cache_fill_range(
                options.cache_fill,
                pending[end].object_offset,
                pending[end].len,
                pending[end].object_len,
            )?;
            if next_fetch_offset > fetch_end {
                break;
            }
            fetch_offset = fetch_offset.min(next_fetch_offset);
            fetch_end = fetch_end.max(next_fetch_end);
            exact_offset = exact_offset.min(pending[end].object_offset);
            exact_end = exact_end.max(read_end);
            end += 1;
        }
        let exact_len =
            usize::try_from(exact_end - exact_offset).map_err(|_| ObjectError::InvalidRange)?;
        let fetch_len =
            usize::try_from(fetch_end - fetch_offset).map_err(|_| ObjectError::InvalidRange)?;
        fetches.push(PendingFetch {
            key: pending[start].key.clone(),
            digest_uri: pending[start].digest_uri.clone(),
            fetch_offset,
            fetch_len,
            exact_offset,
            exact_len,
            reads_start: start,
            reads_end: end,
        });
        index = end;
    }

    let fetched_many = fetch_object_ranges(store, &fetches, &options)?;
    for (fetch, fetched) in fetches.into_iter().zip(fetched_many) {
        if fetched.object_get {
            object_gets += 1;
            object_get_bytes = object_get_bytes.saturating_add(fetched.bytes.len() as u64);
            if fetch.reads_end - fetch.reads_start > 1 {
                coalesced_gets += 1;
                coalesced_get_bytes =
                    coalesced_get_bytes.saturating_add(fetched.bytes.len() as u64);
            }
            if let Some(cache) = cache {
                cache.put_block(
                    block_range_cache_key(
                        fetch.key.as_str(),
                        fetched.object_offset,
                        fetched.bytes.len(),
                    ),
                    fetched.bytes.clone(),
                )?;
            }
        }
        for request in &pending[fetch.reads_start..fetch.reads_end] {
            let relative = request
                .object_offset
                .checked_sub(fetched.object_offset)
                .and_then(|offset| usize::try_from(offset).ok())
                .ok_or(ObjectError::InvalidRange)?;
            let relative_end = relative
                .checked_add(request.len)
                .ok_or(ObjectError::InvalidRange)?;
            if relative_end > fetched.bytes.len() {
                return Err(ObjectError::InvalidRange);
            }
            let bytes = &fetched.bytes[relative..relative_end];
            let output_end = request
                .output_offset
                .checked_add(bytes.len())
                .ok_or(ObjectError::InvalidRange)?;
            out[request.output_offset..output_end].copy_from_slice(bytes);
        }
    }
    Ok(BlockReadOutcome {
        bytes: out,
        object_gets,
        object_get_bytes,
        coalesced_gets,
        coalesced_get_bytes,
        cache_hits,
        cache_hit_bytes,
    })
}

fn checked_read_end(offset: u64, len: usize) -> Result<u64, ObjectError> {
    offset
        .checked_add(u64::try_from(len).map_err(|_| ObjectError::InvalidRange)?)
        .ok_or(ObjectError::InvalidRange)
}

fn cache_fill_range(
    mode: ReadCacheFillMode,
    object_offset: u64,
    len: usize,
    object_len: u64,
) -> Result<(u64, u64), ObjectError> {
    let exact_end = checked_read_end(object_offset, len)?;
    if exact_end > object_len {
        return Err(ObjectError::InvalidRange);
    }
    match mode {
        ReadCacheFillMode::Exact => Ok((object_offset, exact_end)),
        ReadCacheFillMode::BlockAligned { block_size } => {
            if block_size == 0 {
                return Ok((object_offset, exact_end));
            }
            let block_size = u64::try_from(block_size).map_err(|_| ObjectError::InvalidRange)?;
            let fetch_offset = object_offset / block_size * block_size;
            let fetch_end = exact_end
                .checked_add(block_size - 1)
                .ok_or(ObjectError::InvalidRange)?
                / block_size
                * block_size;
            Ok((fetch_offset, fetch_end.min(object_len)))
        }
        ReadCacheFillMode::ForwardAligned { block_size } => {
            if block_size == 0 {
                return Ok((object_offset, exact_end));
            }
            let block_size = u64::try_from(block_size).map_err(|_| ObjectError::InvalidRange)?;
            let block_index = object_offset / block_size;
            let fetch_end = block_index
                .checked_add(1)
                .and_then(|index| index.checked_mul(block_size))
                .ok_or(ObjectError::InvalidRange)?
                .max(exact_end);
            Ok((object_offset, fetch_end.min(object_len)))
        }
    }
}

fn fetch_object_ranges<O>(
    store: &O,
    fetches: &[PendingFetch],
    options: &BlockReadOptions,
) -> Result<Vec<FetchedRange>, ObjectError>
where
    O: ObjectStore,
{
    if matches!(options.cache_fill, ReadCacheFillMode::Exact) {
        let requests = fetches
            .iter()
            .map(|fetch| {
                Ok(ObjectGetRequest::new(
                    fetch.key.clone(),
                    Some(ObjectRange::new(fetch.fetch_offset, fetch.fetch_len)?),
                ))
            })
            .collect::<Result<Vec<_>, ObjectError>>()?;
        let fetched_many = store.get_many(&requests)?;
        if fetched_many.len() != fetches.len() {
            return Err(ObjectError::Backend(
                "object store returned wrong batch length".to_owned(),
            ));
        }
        return Ok(fetches
            .iter()
            .zip(fetched_many)
            .map(|(fetch, bytes)| FetchedRange {
                object_offset: fetch.fetch_offset,
                bytes,
                object_get: true,
            })
            .collect());
    }

    if options.read_coordinator.is_none() {
        return fetch_object_ranges_batched(store, fetches);
    }

    fetches
        .iter()
        .map(|fetch| fetch_object_range(store, fetch, options.read_coordinator.as_ref()))
        .collect()
}

fn fetch_object_ranges_batched<O>(
    store: &O,
    fetches: &[PendingFetch],
) -> Result<Vec<FetchedRange>, ObjectError>
where
    O: ObjectStore,
{
    let requests = fetches
        .iter()
        .map(|fetch| {
            Ok(ObjectGetRequest::new(
                fetch.key.clone(),
                Some(ObjectRange::new(fetch.fetch_offset, fetch.fetch_len)?),
            ))
        })
        .collect::<Result<Vec<_>, ObjectError>>()?;
    let fetched_many = store.get_many(&requests)?;
    if fetched_many.len() != fetches.len() {
        return Err(ObjectError::Backend(
            "object store returned wrong batch length".to_owned(),
        ));
    }
    fetches
        .iter()
        .zip(fetched_many)
        .map(|(fetch, bytes)| {
            if cached_bytes_cover(
                fetch.fetch_offset,
                bytes.len(),
                fetch.exact_offset,
                fetch.exact_len,
            ) {
                return Ok(FetchedRange {
                    object_offset: fetch.fetch_offset,
                    bytes,
                    object_get: true,
                });
            }
            let expanded =
                fetch.fetch_offset != fetch.exact_offset || fetch.fetch_len != fetch.exact_len;
            if expanded {
                let (object_offset, bytes) = fetch_exact_object_range(store, fetch)?;
                return Ok(FetchedRange {
                    object_offset,
                    bytes,
                    object_get: true,
                });
            }
            Err(ObjectError::InvalidRange)
        })
        .collect()
}

fn fetch_object_range<O>(
    store: &O,
    fetch: &PendingFetch,
    coordinator: Option<&ObjectReadCoordinator>,
) -> Result<FetchedRange, ObjectError>
where
    O: ObjectStore,
{
    let fetch_range = || fetch_object_range_uncached(store, fetch);
    if let Some(coordinator) = coordinator {
        let key = ObjectReadCoordinatorKey::new(
            fetch.key.as_str().to_owned(),
            fetch.digest_uri.clone(),
            fetch.fetch_offset,
            fetch.fetch_len,
        );
        let read = coordinator.read_or_wait(key, fetch_range)?;
        return Ok(FetchedRange {
            object_offset: read.object_offset,
            bytes: read.bytes,
            object_get: read.fetched,
        });
    }
    let (object_offset, bytes) = fetch_range()?;
    Ok(FetchedRange {
        object_offset,
        bytes,
        object_get: true,
    })
}

fn fetch_object_range_uncached<O>(
    store: &O,
    fetch: &PendingFetch,
) -> Result<(u64, Vec<u8>), ObjectError>
where
    O: ObjectStore,
{
    let expanded = fetch.fetch_offset != fetch.exact_offset || fetch.fetch_len != fetch.exact_len;
    let range = ObjectRange::new(fetch.fetch_offset, fetch.fetch_len)?;
    match store.get(&fetch.key, Some(range)) {
        Ok(bytes)
            if cached_bytes_cover(
                fetch.fetch_offset,
                bytes.len(),
                fetch.exact_offset,
                fetch.exact_len,
            ) =>
        {
            Ok((fetch.fetch_offset, bytes))
        }
        Ok(_) if expanded => fetch_exact_object_range(store, fetch),
        Ok(_) => Err(ObjectError::InvalidRange),
        Err(_) if expanded => fetch_exact_object_range(store, fetch),
        Err(err) => Err(err),
    }
}

fn fetch_exact_object_range<O>(
    store: &O,
    fetch: &PendingFetch,
) -> Result<(u64, Vec<u8>), ObjectError>
where
    O: ObjectStore,
{
    let range = ObjectRange::new(fetch.exact_offset, fetch.exact_len)?;
    store
        .get(&fetch.key, Some(range))
        .map(|bytes| (fetch.exact_offset, bytes))
}

fn cached_bytes_cover(
    cached_offset: u64,
    cached_len: usize,
    object_offset: u64,
    len: usize,
) -> bool {
    let Some(cached_end) = cached_offset.checked_add(cached_len as u64) else {
        return false;
    };
    let Some(requested_end) = object_offset.checked_add(len as u64) else {
        return false;
    };
    cached_offset <= object_offset && requested_end <= cached_end
}
