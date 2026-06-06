use super::*;
use std::io::{self, Read};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[test]
fn object_key_rejects_unsafe_paths() {
    assert_eq!(ObjectKey::new(""), Err(ObjectError::EmptyKey));
    assert_eq!(ObjectKey::new("/abs"), Err(ObjectError::AbsoluteKey));
    assert_eq!(
        ObjectKey::new("../escape"),
        Err(ObjectError::ParentTraversal)
    );
    assert_eq!(
        ObjectKey::new("./current"),
        Err(ObjectError::CurrentDirectory)
    );
    assert_eq!(ObjectKey::new("bad\0key"), Err(ObjectError::ContainsNul));
}

#[test]
fn memory_object_store_put_head_get_delete() {
    let store = MemoryObjectStore::new();
    let capabilities = store.capabilities();
    assert!(capabilities.range_get);
    assert!(!capabilities.multipart_put);
    let key = ObjectKey::new("runs/1/artifact.bin").unwrap();

    let info = store.put(&key, b"abcdef").unwrap();
    assert_eq!(info.size, 6);
    assert_eq!(store.head(&key).unwrap().unwrap().size, 6);
    assert_eq!(store.get(&key, None).unwrap(), b"abcdef");
    assert_eq!(
        store
            .get(&key, Some(ObjectRange::new(2, 3).unwrap()))
            .unwrap(),
        b"cde"
    );
    assert!(store.delete(&key).unwrap());
    assert!(!store.delete(&key).unwrap());
    assert!(store.head(&key).unwrap().is_none());
}

#[test]
fn chunked_put_and_read_cross_block_range() {
    let store = MemoryObjectStore::new();
    let bytes = b"abcdefghijklmnop".to_vec();
    let written = put_chunked_object(
        &store,
        &bytes,
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 3,
            chunk_size: 8,
            block_size: 4,
        },
    )
    .unwrap();
    assert_eq!(written.size, 16);
    assert_eq!(written.object_puts, 4);
    assert_eq!(written.object_put_bytes, 16);
    assert_eq!(written.chunks.len(), 2);
    assert_eq!(written.chunks[0].blocks.len(), 2);
    assert_eq!(written.chunks[0].blocks[0].object_key, "blocks/1/2/3/0/0");
    assert_eq!(
        written.chunks[0].blocks[0].digest_uri,
        "sha256:88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589"
    );
    let staged = written.staged_objects().unwrap();
    assert_eq!(staged.len(), 4);

    let blocks = vec![
        ObjectReadBlock {
            object_key: "blocks/1/2/3/0/1".to_owned(),
            object_offset: 1,
            len: 3,
            output_offset: 0,
        },
        ObjectReadBlock {
            object_key: "blocks/1/2/3/1/0".to_owned(),
            object_offset: 0,
            len: 2,
            output_offset: 3,
        },
    ];
    let read = read_object_blocks_with_cache(&store, Option::<&MemoryBlockCache>::None, 5, &blocks)
        .unwrap();
    assert_eq!(read.bytes, b"fghij");
    assert_eq!(read.object_gets, 2);
    assert_eq!(read.object_get_bytes, 5);
    assert_eq!(read.cache_hits, 0);
    assert_eq!(read.cache_hit_bytes, 0);

    let cleanup = delete_staged_objects(&store, &staged).unwrap();
    assert_eq!(cleanup.attempted, 4);
    assert_eq!(cleanup.deleted, 4);
    assert_eq!(cleanup.missing, 0);
    let cleanup = delete_staged_objects(&store, &staged).unwrap();
    assert_eq!(cleanup.attempted, 4);
    assert_eq!(cleanup.deleted, 0);
    assert_eq!(cleanup.missing, 4);
}

#[test]
fn stored_chunks_promote_to_canonical_manifest_schema() {
    let store = MemoryObjectStore::new();
    let written = put_chunked_object(
        &store,
        b"abcdefghijklmnop",
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 3,
            chunk_size: 8,
            block_size: 4,
        },
    )
    .unwrap();

    let manifests = written.chunk_manifests();
    assert_eq!(manifests.len(), 2);
    assert_eq!(manifests[0].chunk_index, 0);
    assert_eq!(manifests[0].logical_offset, 0);
    assert_eq!(manifests[0].len, 8);
    assert_eq!(manifests[0].slices.len(), 1);
    assert_eq!(manifests[0].slices[0].slice_id, 1);
    assert_eq!(manifests[0].slices[0].blocks.len(), 2);
    assert_eq!(
        manifests[0].slices[0].blocks[0].object_key,
        "blocks/1/2/3/0/0"
    );

    let digest = manifest_digest_uri(written.size, 3, &written.chunks);
    assert!(digest.starts_with("manifest-sha256:"));
    assert_eq!(
        digest,
        manifest_digest_uri(written.size, 3, &written.chunks)
    );
    assert_ne!(
        digest,
        manifest_digest_uri(written.size, 4, &written.chunks)
    );
}

#[test]
fn chunked_reader_matches_chunked_object_layout() {
    let store = MemoryObjectStore::new();
    let bytes = b"abcdefghijklmnop".to_vec();
    let written = put_chunked_reader(
        &store,
        bytes.as_slice(),
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 3,
            chunk_size: 8,
            block_size: 4,
        },
    )
    .unwrap();
    assert_eq!(written.size, 16);
    assert_eq!(written.object_puts, 4);
    assert_eq!(written.object_put_bytes, 16);
    assert_eq!(written.chunks.len(), 2);
    assert_eq!(written.chunks[0].logical_offset, 0);
    assert_eq!(written.chunks[0].len, 8);
    assert_eq!(written.chunks[1].logical_offset, 8);
    assert_eq!(written.chunks[1].len, 8);
    assert_eq!(written.chunks[0].blocks[0].object_key, "blocks/1/2/3/0/0");
    assert_eq!(written.chunks[0].blocks[1].object_key, "blocks/1/2/3/0/1");
    assert_eq!(written.chunks[1].blocks[0].object_key, "blocks/1/2/3/1/0");
    assert_eq!(
        store
            .get(&ObjectKey::new("blocks/1/2/3/1/1").unwrap(), None)
            .unwrap(),
        b"mnop"
    );
}

struct FailAfterReader {
    bytes: Vec<u8>,
    offset: usize,
    fail_after: usize,
}

impl Read for FailAfterReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.offset >= self.fail_after {
            return Err(io::Error::other("injected reader failure"));
        }
        let end = self
            .bytes
            .len()
            .min(self.fail_after)
            .min(self.offset + buf.len());
        if end == self.offset {
            return Ok(0);
        }
        let len = end - self.offset;
        buf[..len].copy_from_slice(&self.bytes[self.offset..end]);
        self.offset = end;
        Ok(len)
    }
}

#[test]
fn chunked_reader_failure_returns_staged_objects_for_cleanup() {
    let store = MemoryObjectStore::new();
    let err = put_chunked_reader(
        &store,
        FailAfterReader {
            bytes: b"abcdefgh".to_vec(),
            offset: 0,
            fail_after: 4,
        },
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 3,
            chunk_size: 8,
            block_size: 4,
        },
    )
    .unwrap_err();
    let staged = match err {
        ObjectError::StagedWriteFailed { source, staged } => {
            assert!(source.contains("injected reader failure"));
            staged
        }
        err => panic!("unexpected object error: {err:?}"),
    };
    assert_eq!(staged.len(), 1);
    assert!(store.head(&staged.objects()[0].key).unwrap().is_some());

    let cleanup = delete_staged_objects(&store, &staged).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert!(store.head(&staged.objects()[0].key).unwrap().is_none());
}

#[test]
fn chunked_ranges_put_only_dirty_blocks() {
    let store = MemoryObjectStore::new();
    let written = put_chunked_ranges(
        &store,
        &[
            ChunkWriteRange {
                logical_offset: 3,
                bytes: b"XYZ".to_vec(),
            },
            ChunkWriteRange {
                logical_offset: 10,
                bytes: b"mnopq".to_vec(),
            },
        ],
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 4,
            chunk_size: 8,
            block_size: 4,
        },
    )
    .unwrap();
    assert_eq!(written.size, 15);
    assert_eq!(written.object_puts, 3);
    assert_eq!(written.object_put_bytes, 8);
    assert_eq!(written.chunks.len(), 2);
    assert_eq!(written.chunks[0].chunk_index, 0);
    assert_eq!(written.chunks[0].blocks[0].logical_offset, 3);
    assert_eq!(written.chunks[0].blocks[0].object_key, "blocks/1/2/4/0/0");
    assert_eq!(written.chunks[1].chunk_index, 1);
    assert_eq!(written.chunks[1].blocks.len(), 2);
    assert_eq!(
        store
            .get(
                &ObjectKey::new("blocks/1/2/4/1/0").unwrap(),
                Some(ObjectRange::new(0, 4).unwrap())
            )
            .unwrap(),
        b"mnop"
    );
}

#[test]
fn parallel_chunked_ranges_preserve_manifest_order() {
    let store = MemoryObjectStore::new();
    let written = put_chunked_ranges_parallel(
        &store,
        &[
            ChunkWriteRange {
                logical_offset: 0,
                bytes: b"abcdefgh".to_vec(),
            },
            ChunkWriteRange {
                logical_offset: 8,
                bytes: b"ijklmnop".to_vec(),
            },
        ],
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 5,
            chunk_size: 8,
            block_size: 4,
        },
        0,
        4,
    )
    .unwrap();
    assert_eq!(written.object_puts, 4);
    assert_eq!(written.object_put_bytes, 16);
    assert_eq!(written.chunks.len(), 2);
    assert_eq!(written.chunks[0].blocks[0].object_key, "blocks/1/2/5/0/0");
    assert_eq!(written.chunks[0].blocks[1].object_key, "blocks/1/2/5/0/1");
    assert_eq!(written.chunks[1].blocks[0].object_key, "blocks/1/2/5/1/0");
    assert_eq!(
        store
            .get(&ObjectKey::new("blocks/1/2/5/1/1").unwrap(), None)
            .unwrap(),
        b"mnop"
    );
}

#[test]
fn slice_read_plan_overlays_newer_dirty_ranges() {
    let store = MemoryObjectStore::new();
    let old = put_chunked_object(
        &store,
        b"abcdefgh",
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 3,
            chunk_size: 8,
            block_size: 4,
        },
    )
    .unwrap()
    .into_slice(1);
    let updated = put_chunked_ranges(
        &store,
        &[ChunkWriteRange {
            logical_offset: 2,
            bytes: b"XY".to_vec(),
        }],
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 4,
            chunk_size: 8,
            block_size: 4,
        },
    )
    .unwrap()
    .into_slice_at(2, 2, 2);

    let plan = plan_slice_reads(&[old, updated], 0, 8).unwrap();
    let read = read_object_blocks_with_cache(
        &store,
        Option::<&MemoryBlockCache>::None,
        plan.output_len,
        &plan.blocks,
    )
    .unwrap();
    assert_eq!(read.bytes, b"abXYefgh");
}

#[test]
fn slice_read_plan_leaves_sparse_holes_zero_filled() {
    let store = MemoryObjectStore::new();
    let dirty = put_chunked_ranges(
        &store,
        &[ChunkWriteRange {
            logical_offset: 4,
            bytes: b"XY".to_vec(),
        }],
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 3,
            chunk_size: 8,
            block_size: 4,
        },
    )
    .unwrap()
    .into_slice_at(1, 4, 2);

    let plan = plan_slice_reads(&[dirty], 0, 8).unwrap();
    let read = read_object_blocks_with_cache(
        &store,
        Option::<&MemoryBlockCache>::None,
        plan.output_len,
        &plan.blocks,
    )
    .unwrap();
    assert_eq!(read.bytes, b"\0\0\0\0XY\0\0");
}

#[test]
fn block_cache_reuses_object_reads() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcd").unwrap();
    let cache = MemoryBlockCache::default();
    let blocks = vec![ObjectReadBlock {
        object_key: key.as_str().to_owned(),
        object_offset: 0,
        len: 4,
        output_offset: 0,
    }];
    let first = read_object_blocks_with_cache(&store, Some(&cache), 4, &blocks).unwrap();
    let second = read_object_blocks_with_cache(&store, Some(&cache), 4, &blocks).unwrap();
    assert_eq!(first.object_gets, 1);
    assert_eq!(first.object_get_bytes, 4);
    assert_eq!(first.cache_hits, 0);
    assert_eq!(first.cache_hit_bytes, 0);
    assert_eq!(second.object_gets, 0);
    assert_eq!(second.object_get_bytes, 0);
    assert_eq!(second.cache_hits, 1);
    assert_eq!(second.cache_hit_bytes, 4);
}

#[test]
fn object_prefetcher_populates_block_cache() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefgh").unwrap();
    let cache = MemoryBlockCache::default();
    let prefetcher = ObjectPrefetcher::new(
        store.clone(),
        cache.clone(),
        ObjectPrefetchOptions {
            queue_capacity: 4,
            workers: 1,
        },
    );
    let blocks = vec![ObjectReadBlock {
        object_key: key.as_str().to_owned(),
        object_offset: 0,
        len: 8,
        output_offset: 0,
    }];
    assert!(prefetcher
        .submit(ObjectPrefetchRequest::new(8, blocks.clone()))
        .unwrap());

    let mut completed = false;
    for _ in 0..100 {
        if prefetcher.stats().unwrap().completed == 1 {
            completed = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(completed, "prefetch worker did not complete");
    let read = read_object_blocks_with_cache(&store, Some(&cache), 8, &blocks).unwrap();
    assert_eq!(read.bytes, b"abcdefgh");
    assert_eq!(read.object_gets, 0);
    assert_eq!(read.cache_hits, 1);
    assert_eq!(read.cache_hit_bytes, 8);
}

#[test]
fn disk_block_cache_reuses_object_reads() {
    let dir = tempfile::tempdir().unwrap();
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcd").unwrap();
    let cache = DiskBlockCache::new(DiskBlockCacheOptions {
        root: dir.path().join("blocks"),
        max_bytes: 1024,
        max_items: 8,
        ttl: None,
    })
    .unwrap();
    let blocks = vec![ObjectReadBlock {
        object_key: key.as_str().to_owned(),
        object_offset: 0,
        len: 4,
        output_offset: 0,
    }];
    let first = read_object_blocks_with_cache(&store, Some(&cache), 4, &blocks).unwrap();
    let second = read_object_blocks_with_cache(&store, Some(&cache), 4, &blocks).unwrap();
    assert_eq!(first.object_gets, 1);
    assert_eq!(second.object_gets, 0);
    assert_eq!(second.cache_hits, 1);
    assert_eq!(second.cache_hit_bytes, 4);
    let stats = cache.stats().unwrap();
    assert_eq!(stats.items, 1);
    assert_eq!(stats.bytes, 4);
}

#[test]
fn disk_block_cache_enforces_item_and_byte_limits() {
    let dir = tempfile::tempdir().unwrap();
    let cache = DiskBlockCache::new(DiskBlockCacheOptions {
        root: dir.path().join("blocks"),
        max_bytes: 6,
        max_items: 2,
        ttl: None,
    })
    .unwrap();
    cache.put("a".to_owned(), b"aa".to_vec()).unwrap();
    cache.put("b".to_owned(), b"bb".to_vec()).unwrap();
    cache.put("c".to_owned(), b"cccc".to_vec()).unwrap();
    assert!(cache.get("a").unwrap().is_none());
    assert_eq!(cache.get("b").unwrap(), Some(b"bb".to_vec()));
    assert_eq!(cache.get("c").unwrap(), Some(b"cccc".to_vec()));
    let stats = cache.stats().unwrap();
    assert_eq!(stats.items, 2);
    assert_eq!(stats.bytes, 6);
    assert_eq!(stats.evictions, 1);
    assert_eq!(stats.eviction_bytes, 2);
}

#[test]
fn disk_block_cache_expires_entries_by_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let cache = DiskBlockCache::new(DiskBlockCacheOptions {
        root: dir.path().join("blocks"),
        max_bytes: 1024,
        max_items: 8,
        ttl: Some(Duration::from_millis(1)),
    })
    .unwrap();
    cache.put("a".to_owned(), b"aa".to_vec()).unwrap();
    thread::sleep(Duration::from_millis(5));
    assert!(cache.get("a").unwrap().is_none());
    let stats = cache.stats().unwrap();
    assert_eq!(stats.expired, 1);
    assert_eq!(stats.items, 0);
    assert_eq!(stats.bytes, 0);
}

#[test]
fn block_cache_policy_opens_configured_cache() {
    let dir = tempfile::tempdir().unwrap();
    assert!(BlockCachePolicy::Off.open().unwrap().is_none());
    assert!(matches!(
        BlockCachePolicy::Memory(MemoryBlockCacheOptions {
            max_bytes: 16,
            max_items: 2,
            ttl: None,
        })
        .open()
        .unwrap(),
        Some(ObjectBlockCache::Memory(_))
    ));
    assert!(matches!(
        BlockCachePolicy::Disk(DiskBlockCacheOptions {
            root: dir.path().join("blocks"),
            max_bytes: 16,
            max_items: 2,
            ttl: None,
        })
        .open()
        .unwrap(),
        Some(ObjectBlockCache::Disk(_))
    ));
}

#[test]
fn writeback_cache_stages_reads_and_removes_ticket() {
    let dir = tempfile::tempdir().unwrap();
    let cache = WritebackCache::new(WritebackCacheOptions {
        root: dir.path().join("writeback"),
        max_bytes: 1024,
        max_items: 8,
    })
    .unwrap();
    let ticket = cache
        .stage("blocks/1/2/3/0/0".to_owned(), b"checkpoint")
        .unwrap();
    assert_eq!(ticket.len(), 10);
    assert_eq!(cache.read(&ticket).unwrap(), b"checkpoint");
    let stats = cache.stats().unwrap();
    assert_eq!(stats.active_items, 1);
    assert_eq!(stats.active_bytes, 10);
    assert!(cache.remove(&ticket).unwrap());
    assert!(!cache.remove(&ticket).unwrap());
    let stats = cache.stats().unwrap();
    assert_eq!(stats.active_items, 0);
    assert_eq!(stats.active_bytes, 0);
    assert_eq!(stats.removed, 1);
    assert_eq!(stats.removed_bytes, 10);
}

#[test]
fn writeback_cache_rejects_capacity_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let cache = WritebackCache::new(WritebackCacheOptions {
        root: dir.path().join("writeback"),
        max_bytes: 4,
        max_items: 1,
    })
    .unwrap();
    let err = cache
        .stage("blocks/1/2/3/0/0".to_owned(), b"too-large")
        .unwrap_err();
    assert!(matches!(err, ObjectError::Backend(message) if message.contains("capacity")));
}

#[test]
fn object_writeback_uploader_uploads_cached_ranges_and_clears_tickets() {
    let dir = tempfile::tempdir().unwrap();
    let store = MemoryObjectStore::new();
    let cache = WritebackCache::new(WritebackCacheOptions {
        root: dir.path().join("writeback"),
        max_bytes: 1024,
        max_items: 8,
    })
    .unwrap();
    let ticket = cache
        .stage("blocks/1/2/3/0/0".to_owned(), b"checkpoint")
        .unwrap();
    let uploader = ObjectWritebackUploader::new(
        store.clone(),
        cache.clone(),
        ObjectWritebackOptions {
            queue_capacity: 4,
            workers: 1,
            upload_workers_per_request: 1,
        },
    );
    let pending = uploader
        .submit(ObjectWritebackRequest {
            ranges: vec![WritebackUploadRange {
                logical_offset: 0,
                ticket: ticket.clone(),
            }],
            options: ChunkWriteOptions {
                manifest_id: "artifacts/checkpoint".to_owned(),
                mount: 1,
                inode: 2,
                generation: 3,
                chunk_size: 64,
                block_size: 16,
            },
            block_index_base: 0,
        })
        .unwrap();
    let written = pending.wait().unwrap();
    assert_eq!(written.object_puts, 1);
    assert_eq!(written.object_put_bytes, 10);
    assert_eq!(written.chunks[0].blocks[0].object_key, "blocks/1/2/3/0/0");
    let stats = uploader.stats().unwrap();
    assert_eq!(stats.enqueued, 1);
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.uploaded_bytes, 10);
    assert!(cache.read(&ticket).is_err());
    let read = read_object_blocks_with_cache(
        &store,
        None::<&MemoryBlockCache>,
        10,
        &[ObjectReadBlock {
            object_key: "blocks/1/2/3/0/0".to_owned(),
            object_offset: 0,
            len: 10,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(read.bytes, b"checkpoint");
}

#[test]
fn object_writeback_uploader_keeps_cached_ranges_after_upload_failure() {
    let dir = tempfile::tempdir().unwrap();
    let store = FailAfterFirstPut::new();
    let cache = WritebackCache::new(WritebackCacheOptions {
        root: dir.path().join("writeback"),
        max_bytes: 1024,
        max_items: 8,
    })
    .unwrap();
    let ticket = cache
        .stage("blocks/1/2/3/0/0".to_owned(), b"checkpoint-data")
        .unwrap();
    let uploader = ObjectWritebackUploader::new(
        store.clone(),
        cache.clone(),
        ObjectWritebackOptions {
            queue_capacity: 4,
            workers: 1,
            upload_workers_per_request: 1,
        },
    );

    let pending = uploader
        .submit(ObjectWritebackRequest {
            ranges: vec![WritebackUploadRange {
                logical_offset: 0,
                ticket: ticket.clone(),
            }],
            options: ChunkWriteOptions {
                manifest_id: "artifacts/checkpoint".to_owned(),
                mount: 1,
                inode: 2,
                generation: 3,
                chunk_size: 64,
                block_size: 8,
            },
            block_index_base: 0,
        })
        .unwrap();

    let err = pending.wait().unwrap_err();
    assert!(matches!(
        err,
        ObjectError::StagedWriteFailed { ref source, .. }
            if source.contains("injected put failure")
    ));
    assert_eq!(cache.read(&ticket).unwrap(), b"checkpoint-data");
    let cache_stats = cache.stats().unwrap();
    assert_eq!(cache_stats.active_items, 1);
    assert_eq!(cache_stats.active_bytes, 15);
    assert_eq!(cache_stats.removed, 0);
    let upload_stats = uploader.stats().unwrap();
    assert_eq!(upload_stats.enqueued, 1);
    assert_eq!(upload_stats.completed, 0);
    assert_eq!(upload_stats.failed, 1);

    assert_eq!(pending.discard_writeback_cache().unwrap(), 1);
    assert!(cache.read(&ticket).is_err());
    let cache_stats = cache.stats().unwrap();
    assert_eq!(cache_stats.active_items, 0);
    assert_eq!(cache_stats.active_bytes, 0);
    assert_eq!(cache_stats.removed, 1);
    assert_eq!(pending.discard_writeback_cache().unwrap(), 0);
}

#[test]
fn object_prefetcher_accepts_disk_backed_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefgh").unwrap();
    let cache = ObjectBlockCache::from(
        DiskBlockCache::new(DiskBlockCacheOptions {
            root: dir.path().join("blocks"),
            max_bytes: 1024,
            max_items: 8,
            ttl: None,
        })
        .unwrap(),
    );
    let prefetcher = ObjectPrefetcher::new(
        store.clone(),
        cache.clone(),
        ObjectPrefetchOptions {
            queue_capacity: 4,
            workers: 1,
        },
    );
    let blocks = vec![ObjectReadBlock {
        object_key: key.as_str().to_owned(),
        object_offset: 0,
        len: 8,
        output_offset: 0,
    }];
    assert!(prefetcher
        .submit(ObjectPrefetchRequest::new(8, blocks.clone()))
        .unwrap());
    let mut completed = false;
    for _ in 0..100 {
        if prefetcher.stats().unwrap().completed == 1 {
            completed = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(completed, "prefetch worker did not complete");
    let read = read_object_blocks_with_cache(&store, Some(&cache), 8, &blocks).unwrap();
    assert_eq!(read.object_gets, 0);
    assert_eq!(read.cache_hits, 1);
    assert_eq!(read.bytes, b"abcdefgh");
}

#[test]
fn chunk_store_trait_preserves_chunked_write_and_read_path() {
    let store = MemoryObjectStore::new();
    let written = store
        .write_bytes(
            b"abcdefgh",
            ChunkWriteOptions {
                manifest_id: "artifacts/checkpoint".to_owned(),
                mount: 1,
                inode: 2,
                generation: 3,
                chunk_size: 8,
                block_size: 4,
            },
        )
        .unwrap();
    assert_eq!(written.object_puts, 2);

    let read = store
        .read_blocks::<MemoryBlockCache>(
            None,
            4,
            &[ObjectReadBlock {
                object_key: "blocks/1/2/3/0/1".to_owned(),
                object_offset: 0,
                len: 4,
                output_offset: 0,
            }],
        )
        .unwrap();
    assert_eq!(read.bytes, b"efgh");
}

#[test]
fn file_write_pipeline_tracks_staged_objects_and_block_index_frontier() {
    let store = MemoryObjectStore::new();
    let mut writer = FileWritePipeline::new(ChunkWriteOptions {
        manifest_id: "artifacts/checkpoint".to_owned(),
        mount: 1,
        inode: 2,
        generation: 3,
        chunk_size: 8,
        block_size: 4,
    })
    .unwrap();

    let first_base = writer.reserve_blocks(1);
    let first = store
        .write_ranges_with_block_index_base(
            &[ChunkWriteRange {
                logical_offset: 0,
                bytes: b"abcd".to_vec(),
            }],
            writer.options().clone(),
            first_base,
        )
        .unwrap();
    writer.record_write(first).unwrap();

    let second_base = writer.reserve_blocks(1);
    let second = store
        .write_ranges_with_block_index_base(
            &[ChunkWriteRange {
                logical_offset: 4,
                bytes: b"efgh".to_vec(),
            }],
            writer.options().clone(),
            second_base,
        )
        .unwrap();
    writer.record_write(second).unwrap();

    assert!(!writer.is_empty());
    assert_eq!(writer.staged_objects().len(), 2);
    assert_eq!(writer.staged_chunks().len(), 2);
    assert_eq!(writer.dirty_extents().len(), 2);
    assert_eq!(
        writer.staged_chunks()[0].blocks[0].object_key,
        "blocks/1/2/3/0/0"
    );
    assert_eq!(
        writer.staged_chunks()[1].blocks[0].object_key,
        "blocks/1/2/3/0/1"
    );
}

#[test]
fn file_read_pipeline_reports_sequential_readahead_hints() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefghijklmnop").unwrap();
    let cache = MemoryBlockCache::default();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: 4,
    });

    let first = reader
        .read_blocks(
            &store,
            Some(&cache),
            16,
            0,
            4,
            &[ObjectReadBlock {
                object_key: key.as_str().to_owned(),
                object_offset: 0,
                len: 4,
                output_offset: 0,
            }],
        )
        .unwrap();
    assert_eq!(first.blocks.bytes, b"abcd");
    assert_eq!(first.readahead, Some(ReadAheadHint { offset: 4, len: 4 }));

    let second = reader
        .read_blocks(
            &store,
            Some(&cache),
            16,
            4,
            4,
            &[ObjectReadBlock {
                object_key: key.as_str().to_owned(),
                object_offset: 4,
                len: 4,
                output_offset: 0,
            }],
        )
        .unwrap();
    assert_eq!(second.blocks.bytes, b"efgh");
    assert_eq!(second.readahead, Some(ReadAheadHint { offset: 8, len: 4 }));
    let stats = reader.stats();
    assert_eq!(stats.reads, 2);
    assert_eq!(stats.sequential_reads, 2);
    assert_eq!(stats.readahead_hints, 2);
    assert_eq!(stats.readahead_hint_bytes, 8);
}

#[test]
fn file_read_pipeline_does_not_readahead_after_seek() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefghijklmnop").unwrap();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: 4,
    });

    let _ = reader
        .read_blocks::<_, MemoryBlockCache>(
            &store,
            None,
            16,
            0,
            4,
            &[ObjectReadBlock {
                object_key: key.as_str().to_owned(),
                object_offset: 0,
                len: 4,
                output_offset: 0,
            }],
        )
        .unwrap();
    let second = reader
        .read_blocks::<_, MemoryBlockCache>(
            &store,
            None,
            16,
            12,
            4,
            &[ObjectReadBlock {
                object_key: key.as_str().to_owned(),
                object_offset: 12,
                len: 4,
                output_offset: 0,
            }],
        )
        .unwrap();
    assert_eq!(second.blocks.bytes, b"mnop");
    assert_eq!(second.readahead, None);
}

#[test]
fn file_read_pipeline_does_not_readahead_for_initial_random_read() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefghijklmnop").unwrap();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: 4,
    });

    let read = reader
        .read_blocks::<_, MemoryBlockCache>(
            &store,
            None,
            16,
            8,
            4,
            &[ObjectReadBlock {
                object_key: key.as_str().to_owned(),
                object_offset: 8,
                len: 4,
                output_offset: 0,
            }],
        )
        .unwrap();
    assert_eq!(read.blocks.bytes, b"ijkl");
    assert_eq!(read.readahead, None);
    let stats = reader.stats();
    assert_eq!(stats.reads, 1);
    assert_eq!(stats.sequential_reads, 0);
    assert_eq!(stats.readahead_hints, 0);
}

#[test]
fn memory_block_cache_enforces_item_and_byte_limits() {
    let cache = MemoryBlockCache::new(MemoryBlockCacheOptions {
        max_bytes: 4,
        max_items: 2,
        ttl: None,
    });
    cache.put("a".to_owned(), b"aa".to_vec()).unwrap();
    cache.put("b".to_owned(), b"bb".to_vec()).unwrap();
    assert!(cache.get("a").unwrap().is_some());

    cache.put("c".to_owned(), b"cc".to_vec()).unwrap();
    assert!(cache.get("a").unwrap().is_none());
    assert_eq!(cache.get("b").unwrap().as_deref(), Some(&b"bb"[..]));
    assert_eq!(cache.get("c").unwrap().as_deref(), Some(&b"cc"[..]));
    let stats = cache.stats().unwrap();
    assert_eq!(stats.items, 2);
    assert_eq!(stats.bytes, 4);
    assert_eq!(stats.evictions, 1);

    cache.put("large".to_owned(), b"12345".to_vec()).unwrap();
    let stats = cache.stats().unwrap();
    assert_eq!(stats.items, 0);
    assert_eq!(stats.bytes, 0);
    assert!(stats.evictions >= 4);
}

#[test]
fn memory_block_cache_expires_entries_by_ttl() {
    let cache = MemoryBlockCache::new(MemoryBlockCacheOptions {
        max_bytes: 1024,
        max_items: 16,
        ttl: Some(Duration::ZERO),
    });
    cache.put("a".to_owned(), b"aa".to_vec()).unwrap();
    assert!(cache.get("a").unwrap().is_none());
    let stats = cache.stats().unwrap();
    assert_eq!(stats.items, 0);
    assert_eq!(stats.expired, 1);
    assert_eq!(stats.misses, 1);
}

#[test]
fn adjacent_ranges_on_same_object_are_coalesced() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefgh").unwrap();
    let cache = MemoryBlockCache::default();
    let blocks = vec![
        ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            object_offset: 0,
            len: 3,
            output_offset: 0,
        },
        ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            object_offset: 3,
            len: 5,
            output_offset: 3,
        },
    ];

    let first = read_object_blocks_with_cache(&store, Some(&cache), 8, &blocks).unwrap();
    assert_eq!(first.bytes, b"abcdefgh");
    assert_eq!(first.object_gets, 1);
    assert_eq!(first.object_get_bytes, 8);
    assert_eq!(first.coalesced_gets, 1);
    assert_eq!(first.coalesced_get_bytes, 8);
    assert_eq!(first.cache_hits, 0);

    let second = read_object_blocks_with_cache(&store, Some(&cache), 8, &blocks).unwrap();
    assert_eq!(second.bytes, b"abcdefgh");
    assert_eq!(second.object_gets, 0);
    assert_eq!(second.coalesced_gets, 0);
    assert_eq!(second.cache_hits, 2);
    assert_eq!(second.cache_hit_bytes, 8);
}

#[test]
fn unordered_adjacent_ranges_on_same_object_are_coalesced() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefgh").unwrap();
    let blocks = vec![
        ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            object_offset: 4,
            len: 4,
            output_offset: 4,
        },
        ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            object_offset: 0,
            len: 4,
            output_offset: 0,
        },
    ];

    let read = read_object_blocks_with_cache(&store, Option::<&MemoryBlockCache>::None, 8, &blocks)
        .unwrap();
    assert_eq!(read.bytes, b"abcdefgh");
    assert_eq!(read.object_gets, 1);
    assert_eq!(read.coalesced_gets, 1);
    assert_eq!(read.coalesced_get_bytes, 8);
}

#[derive(Clone)]
struct FailAfterFirstPut {
    inner: MemoryObjectStore,
    puts: Arc<Mutex<usize>>,
}

impl FailAfterFirstPut {
    fn new() -> Self {
        Self {
            inner: MemoryObjectStore::new(),
            puts: Arc::new(Mutex::new(0)),
        }
    }
}

impl ObjectStore for FailAfterFirstPut {
    fn put(&self, key: &ObjectKey, bytes: &[u8]) -> Result<ObjectInfo, ObjectError> {
        let mut puts = self.puts.lock().unwrap();
        if *puts >= 1 {
            return Err(ObjectError::Backend("injected put failure".to_owned()));
        }
        *puts += 1;
        self.inner.put(key, bytes)
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.inner.get(key, range)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        self.inner.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        self.inner.delete(key)
    }
}

#[test]
fn chunked_put_failure_returns_staged_objects_for_cleanup() {
    let store = FailAfterFirstPut::new();
    let err = put_chunked_object(
        &store,
        b"abcdefgh",
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 3,
            chunk_size: 8,
            block_size: 4,
        },
    )
    .unwrap_err();
    let staged = match err {
        ObjectError::StagedWriteFailed { source, staged } => {
            assert!(source.contains("injected put failure"));
            staged
        }
        err => panic!("unexpected object error: {err:?}"),
    };
    assert_eq!(staged.len(), 1);
    assert!(store.head(&staged.objects()[0].key).unwrap().is_some());

    let cleanup = delete_staged_objects(&store, &staged).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert!(store.head(&staged.objects()[0].key).unwrap().is_none());
}

#[test]
fn s3_options_validate_required_fields_and_rustfs_defaults() {
    let mut options = S3ObjectStoreOptions::new("");
    assert_eq!(options.validate(), Err(ObjectError::MissingBucket));

    options.bucket = "nokv".to_owned();
    options.region.clear();
    assert_eq!(options.validate(), Err(ObjectError::MissingRegion));

    let options = S3ObjectStoreOptions::rustfs("nokv", "http://127.0.0.1:9000", "access", "secret");
    assert_eq!(options.bucket, "nokv");
    assert_eq!(options.region, "auto");
    assert_eq!(options.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
    assert!(!options.virtual_host_style);
}

#[test]
fn s3_capabilities_enable_range_and_multipart_contract() {
    let store = S3ObjectStore::new(S3ObjectStoreOptions {
        bucket: "nokv".to_owned(),
        root: "/".to_owned(),
        region: "auto".to_owned(),
        endpoint: Some("http://127.0.0.1:9000".to_owned()),
        access_key_id: Some("access".to_owned()),
        secret_access_key: Some("secret".to_owned()),
        session_token: None,
        virtual_host_style: false,
        skip_signature: true,
    })
    .unwrap();
    let capabilities = store.capabilities();
    assert!(capabilities.range_get);
    assert!(capabilities.multipart_put);
    assert_eq!(capabilities.multipart_min_part_bytes, Some(5 * 1024 * 1024));
}

#[test]
fn object_store_config_requires_valid_s3_backend() {
    let config = ObjectStoreConfig::s3(S3ObjectStoreOptions::new(""));
    assert_eq!(config.open().unwrap_err(), ObjectError::MissingBucket);
}

#[test]
fn s3_object_store_contract_from_env() {
    let Ok(bucket) = std::env::var("NOKV_FS_S3_BUCKET") else {
        return;
    };
    let mut options = S3ObjectStoreOptions::new(bucket);
    options.region = std::env::var("NOKV_FS_S3_REGION").unwrap_or_else(|_| "auto".to_owned());
    options.endpoint = std::env::var("NOKV_FS_S3_ENDPOINT").ok();
    options.access_key_id = std::env::var("NOKV_FS_S3_ACCESS_KEY_ID").ok();
    options.secret_access_key = std::env::var("NOKV_FS_S3_SECRET_ACCESS_KEY").ok();
    options.session_token = std::env::var("NOKV_FS_S3_SESSION_TOKEN").ok();
    options.virtual_host_style =
        std::env::var("NOKV_FS_S3_VIRTUAL_HOST_STYLE").as_deref() == Ok("1");
    options.skip_signature = std::env::var("NOKV_FS_S3_SKIP_SIGNATURE").as_deref() == Ok("1");

    let store = S3ObjectStore::new(options).unwrap();
    let key = ObjectKey::new(format!("nokv-test/{}.bin", std::process::id())).unwrap();

    store.put(&key, b"abcdef").unwrap();
    assert_eq!(store.head(&key).unwrap().unwrap().size, 6);
    assert_eq!(
        store
            .get(&key, Some(ObjectRange::new(1, 3).unwrap()))
            .unwrap(),
        b"bcd"
    );
    assert!(store.delete(&key).unwrap());
    assert!(store.head(&key).unwrap().is_none());
}
