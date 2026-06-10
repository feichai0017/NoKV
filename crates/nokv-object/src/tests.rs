use super::*;
use std::io::{self, Read};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

fn read_object_blocks<C>(
    store: &impl ObjectStore,
    cache: Option<&C>,
    output_len: usize,
    blocks: &[ObjectReadBlock],
) -> Result<BlockReadOutcome, ObjectError>
where
    C: BlockCache + ?Sized,
{
    read_object_blocks_with_cache_options(
        store,
        cache,
        output_len,
        blocks,
        BlockReadOptions::default(),
    )
}

fn read_file_blocks<S, C>(
    reader: &mut FileReadPipeline,
    store: &S,
    cache: Option<&C>,
    file_size: u64,
    offset: u64,
    output_len: usize,
    blocks: &[ObjectReadBlock],
) -> Result<FileReadOutcome, ObjectError>
where
    S: ChunkStore,
    C: BlockCache + ?Sized,
{
    reader.read_blocks_with_options(
        store,
        cache,
        FileReadRequest {
            file_size,
            offset,
            output_len,
            blocks,
        },
        BlockReadOptions::default(),
    )
}

#[derive(Clone, Debug, Default)]
struct PointerRecordingObjectStore {
    put_pointers: Arc<Mutex<Vec<usize>>>,
}

impl PointerRecordingObjectStore {
    fn put_pointers(&self) -> Vec<usize> {
        self.put_pointers.lock().unwrap().clone()
    }
}

impl ObjectStore for PointerRecordingObjectStore {
    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        let bytes = bytes.into();
        self.put_pointers
            .lock()
            .unwrap()
            .push(bytes.as_ptr() as usize);
        Ok(ObjectInfo {
            key: key.clone(),
            size: bytes.len() as u64,
        })
    }

    fn get(&self, _key: &ObjectKey, _range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        Err(ObjectError::Backend(
            "test store does not support get".to_owned(),
        ))
    }

    fn head(&self, _key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        Ok(None)
    }

    fn delete(&self, _key: &ObjectKey) -> Result<bool, ObjectError> {
        Ok(false)
    }
}

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

    let info = store.put(&key, b"abcdef".to_vec()).unwrap();
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
fn object_store_put_accepts_shared_payload() {
    let store = PointerRecordingObjectStore::default();
    let key = ObjectKey::new("runs/1/shared.bin").unwrap();
    let bytes: Arc<[u8]> = Arc::from(&b"checkpoint"[..]);
    let pointer = bytes.as_ptr() as usize;

    let info = store.put(&key, ObjectBytes::shared(bytes)).unwrap();

    assert_eq!(info.size, 10);
    assert_eq!(store.put_pointers(), vec![pointer]);
}

#[test]
fn object_bytes_shared_vec_slice_exposes_window_without_copy() {
    let bytes = Arc::new(b"abcdefghijkl".to_vec());
    let pointer = bytes.as_ptr() as usize;

    let slice = ObjectBytes::shared_vec_slice(Arc::clone(&bytes), 4, 4).unwrap();

    assert_eq!(slice.as_slice(), b"efgh");
    assert_eq!(slice.as_ptr() as usize, pointer + 4);
    assert_eq!(slice.clone().into_vec(), b"efgh");
    assert_eq!(
        ObjectBytes::shared_vec_slice(bytes, usize::MAX, 1),
        Err(ObjectError::InvalidRange)
    );
}

#[test]
fn object_store_get_many_preserves_order_and_ranges() {
    let store = MemoryObjectStore::new();
    let a = ObjectKey::new("blocks/a").unwrap();
    let b = ObjectKey::new("blocks/b").unwrap();
    store.put(&a, b"abcdef".to_vec()).unwrap();
    store.put(&b, b"uvwxyz".to_vec()).unwrap();

    let reads = store
        .get_many(&[
            ObjectGetRequest::new(b.clone(), Some(ObjectRange::new(2, 3).unwrap())),
            ObjectGetRequest::new(a.clone(), None),
            ObjectGetRequest::new(a, Some(ObjectRange::new(1, 2).unwrap())),
        ])
        .unwrap();
    assert_eq!(
        reads,
        vec![b"wxy".to_vec(), b"abcdef".to_vec(), b"bc".to_vec()]
    );
}

#[test]
fn local_object_store_put_head_range_and_delete() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(LocalObjectStoreOptions::new(temp.path())).unwrap();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();

    let info = store.put(&key, b"abcdef".to_vec()).unwrap();
    assert_eq!(info.size, 6);
    let stats = store.stats().unwrap();
    assert_eq!(stats.resident_objects, 1);
    assert_eq!(stats.resident_bytes, 6);
    assert_eq!(stats.puts, 1);
    assert_eq!(stats.put_bytes, 6);
    assert!(stats.put_total_ns >= stats.put_write_ns);
    assert!(stats.put_total_ns >= stats.put_rename_ns);
    assert_eq!(store.head(&key).unwrap().unwrap().size, 6);
    assert_eq!(
        store
            .get(&key, Some(ObjectRange::new(2, 3).unwrap()))
            .unwrap(),
        b"cde"
    );
    assert_eq!(
        store
            .get(&key, Some(ObjectRange::new(4, 8).unwrap()))
            .unwrap(),
        b"ef"
    );
    assert_eq!(
        store
            .get(&key, Some(ObjectRange::new(12, 2).unwrap()))
            .unwrap(),
        b""
    );
    assert_eq!(
        store
            .get_if_present(&key, Some(ObjectRange::new(1, 2).unwrap()))
            .unwrap(),
        Some(b"bc".to_vec())
    );
    let missing = ObjectKey::new("blocks/missing").unwrap();
    assert_eq!(store.get_if_present(&missing, None).unwrap(), None);
    let optional_reads = store
        .get_many_if_present(&[
            ObjectGetRequest::new(key.clone(), Some(ObjectRange::new(0, 2).unwrap())),
            ObjectGetRequest::new(missing, None),
        ])
        .unwrap();
    assert_eq!(optional_reads, vec![Some(b"ab".to_vec()), None]);
    let stats = store.stats().unwrap();
    assert_eq!(stats.resident_objects, 1);
    assert_eq!(stats.resident_bytes, 6);
    assert!(store.delete(&key).unwrap());
    assert!(!store.delete(&key).unwrap());
}

#[test]
fn local_object_store_rejects_objects_larger_than_capacity() {
    let temp = tempfile::tempdir().unwrap();
    let store =
        LocalObjectStore::new(LocalObjectStoreOptions::new(temp.path()).with_max_bytes(4)).unwrap();
    let key = ObjectKey::new("blocks/too-large").unwrap();

    let err = store.put(&key, b"abcde".to_vec()).unwrap_err();

    assert!(matches!(err, ObjectError::Backend(message) if message.contains("capacity")));
    assert!(store.head(&key).unwrap().is_none());
    let stats = store.stats().unwrap();
    assert_eq!(stats.admission_rejections, 1);
    assert_eq!(stats.resident_objects, 0);
}

#[test]
fn local_object_store_evicts_lru_entries_to_stay_under_capacity() {
    let temp = tempfile::tempdir().unwrap();
    let store =
        LocalObjectStore::new(LocalObjectStoreOptions::new(temp.path()).with_max_bytes(8)).unwrap();
    let a = ObjectKey::new("blocks/a").unwrap();
    let b = ObjectKey::new("blocks/b").unwrap();
    let c = ObjectKey::new("blocks/c").unwrap();
    store.put(&a, b"aaaa".to_vec()).unwrap();
    store.put(&b, b"bbbb".to_vec()).unwrap();
    assert_eq!(
        store
            .get(&a, Some(ObjectRange::new(0, 2).unwrap()))
            .unwrap(),
        b"aa"
    );

    store.put(&c, b"cccc".to_vec()).unwrap();

    assert!(store.head(&a).unwrap().is_some());
    assert!(store.head(&b).unwrap().is_none());
    assert!(store.head(&c).unwrap().is_some());
    let stats = store.stats().unwrap();
    assert_eq!(stats.resident_objects, 2);
    assert_eq!(stats.resident_bytes, 8);
    assert_eq!(stats.evictions, 1);
    assert_eq!(stats.eviction_bytes, 4);
}

#[test]
fn local_object_store_rebuilds_residency_index_on_open() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(LocalObjectStoreOptions::new(temp.path())).unwrap();
    store
        .put(&ObjectKey::new("blocks/reopen-a").unwrap(), b"aa".to_vec())
        .unwrap();
    store
        .put(
            &ObjectKey::new("blocks/reopen-b").unwrap(),
            b"bbbb".to_vec(),
        )
        .unwrap();

    let reopened = LocalObjectStore::new(LocalObjectStoreOptions::new(temp.path())).unwrap();

    let stats = reopened.stats().unwrap();
    assert_eq!(stats.resident_objects, 2);
    assert_eq!(stats.resident_bytes, 6);
}

#[test]
fn tiered_object_store_reads_hot_before_cold() {
    let hot = MemoryObjectStore::new();
    let cold = MemoryObjectStore::new();
    let store = TieredObjectStore::new(
        hot.clone(),
        cold.clone(),
        TieredObjectStoreOptions {
            put_policy: TieredPutPolicy::ColdOnly,
            populate_hot_on_get: true,
            ..TieredObjectStoreOptions::default()
        },
    );
    let key = ObjectKey::new("blocks/hot").unwrap();
    cold.put(&key, b"cold".to_vec()).unwrap();
    hot.put(&key, b"hot".to_vec()).unwrap();

    assert_eq!(store.get(&key, None).unwrap(), b"hot");
    let stats = store.stats().unwrap();
    assert_eq!(stats.hot_hits, 1);
    assert_eq!(stats.cold_gets, 0);
}

#[test]
fn tiered_object_store_falls_back_to_cold_and_fills_hot_for_full_get() {
    let hot = MemoryObjectStore::new();
    let cold = MemoryObjectStore::new();
    let store = TieredObjectStore::new(
        hot.clone(),
        cold.clone(),
        TieredObjectStoreOptions {
            put_policy: TieredPutPolicy::ColdOnly,
            populate_hot_on_get: true,
            ..TieredObjectStoreOptions::default()
        },
    );
    let key = ObjectKey::new("blocks/cold").unwrap();
    cold.put(&key, b"abcdef".to_vec()).unwrap();

    assert_eq!(store.get(&key, None).unwrap(), b"abcdef");
    assert_eq!(hot.get(&key, None).unwrap(), b"abcdef");
    let stats = store.stats().unwrap();
    assert_eq!(stats.hot_misses, 1);
    assert_eq!(stats.cold_gets, 1);
    assert_eq!(stats.hot_fills, 1);
}

#[test]
fn tiered_object_store_does_not_fill_hot_with_partial_range() {
    let hot = MemoryObjectStore::new();
    let cold = MemoryObjectStore::new();
    let store = TieredObjectStore::new(
        hot.clone(),
        cold.clone(),
        TieredObjectStoreOptions {
            put_policy: TieredPutPolicy::ColdOnly,
            populate_hot_on_get: true,
            ..TieredObjectStoreOptions::default()
        },
    );
    let key = ObjectKey::new("blocks/range").unwrap();
    cold.put(&key, b"abcdef".to_vec()).unwrap();

    assert_eq!(
        store
            .get(&key, Some(ObjectRange::new(2, 2).unwrap()))
            .unwrap(),
        b"cd"
    );
    assert!(hot.head(&key).unwrap().is_none());
    let stats = store.stats().unwrap();
    assert_eq!(stats.cold_gets, 1);
    assert_eq!(stats.hot_fills, 0);
}

#[test]
fn tiered_object_store_fills_hot_with_full_object_range_batch() {
    let hot = MemoryObjectStore::new();
    let cold = MemoryObjectStore::new();
    let store = TieredObjectStore::new(
        hot.clone(),
        cold.clone(),
        TieredObjectStoreOptions {
            put_policy: TieredPutPolicy::ColdOnly,
            populate_hot_on_get: true,
            ..TieredObjectStoreOptions::default()
        },
    );
    let key = ObjectKey::new("blocks/full-range").unwrap();
    cold.put(&key, b"abcdef".to_vec()).unwrap();

    let reads = store
        .get_many(&[ObjectGetRequest::new(
            key.clone(),
            Some(ObjectRange::new(0, 6).unwrap()),
        )])
        .unwrap();

    assert_eq!(reads, vec![b"abcdef".to_vec()]);
    assert_eq!(hot.get(&key, None).unwrap(), b"abcdef");
    let stats = store.stats().unwrap();
    assert_eq!(stats.cold_gets, 1);
    assert_eq!(stats.hot_fills, 1);
}

#[test]
fn tiered_object_store_get_many_reads_hot_and_cold_in_order() {
    let hot = MemoryObjectStore::new();
    let cold = MemoryObjectStore::new();
    let store = TieredObjectStore::new(
        hot.clone(),
        cold.clone(),
        TieredObjectStoreOptions {
            put_policy: TieredPutPolicy::ColdOnly,
            populate_hot_on_get: true,
            ..TieredObjectStoreOptions::default()
        },
    );
    let hot_key = ObjectKey::new("blocks/hot-batch").unwrap();
    let cold_key = ObjectKey::new("blocks/cold-batch").unwrap();
    hot.put(&hot_key, b"hot-data".to_vec()).unwrap();
    cold.put(&cold_key, b"cold-data".to_vec()).unwrap();

    let reads = store
        .get_many(&[
            ObjectGetRequest::new(cold_key.clone(), None),
            ObjectGetRequest::new(hot_key, Some(ObjectRange::new(4, 4).unwrap())),
        ])
        .unwrap();
    assert_eq!(reads, vec![b"cold-data".to_vec(), b"data".to_vec()]);
    assert_eq!(hot.get(&cold_key, None).unwrap(), b"cold-data");
    let stats = store.stats().unwrap();
    assert_eq!(stats.hot_gets, 2);
    assert_eq!(stats.hot_hits, 1);
    assert_eq!(stats.hot_misses, 1);
    assert_eq!(stats.cold_gets, 1);
    assert_eq!(stats.hot_fills, 1);
}

#[test]
fn tiered_object_store_background_fill_coalesces_duplicate_cold_reads() {
    let hot = BlockFirstPutStore::new();
    let cold = MemoryObjectStore::new();
    let store = TieredObjectStore::new(
        hot.clone(),
        cold.clone(),
        TieredObjectStoreOptions {
            put_policy: TieredPutPolicy::ColdOnly,
            populate_hot_on_get: true,
            hot_fill_mode: HotFillMode::Background,
            pending_cold_put_root: None,
        },
    );
    let key = ObjectKey::new("blocks/background-fill").unwrap();
    cold.put(&key, b"abcdef".to_vec()).unwrap();

    let reads = store
        .get_many(&[
            ObjectGetRequest::new(key.clone(), None),
            ObjectGetRequest::new(key.clone(), None),
        ])
        .unwrap();

    assert_eq!(reads, vec![b"abcdef".to_vec(), b"abcdef".to_vec()]);
    hot.wait_for_first_put();
    let stats = store.stats().unwrap();
    assert_eq!(stats.hot_fill_enqueued, 1);
    assert_eq!(stats.hot_fill_coalesced, 1);
    assert_eq!(stats.hot_fills, 0);
    hot.release_first_put();
    wait_until(|| store.stats().unwrap().hot_fills == 1);
    assert_eq!(hot.put_count(), 1);
    assert_eq!(hot.get(&key, None).unwrap(), b"abcdef");
}

#[test]
fn tiered_object_store_hot_publish_returns_before_cold_put_completes() {
    let hot = MemoryObjectStore::new();
    let cold = BlockFirstPutStore::new();
    let store = TieredObjectStore::new(
        hot.clone(),
        cold.clone(),
        TieredObjectStoreOptions {
            put_policy: TieredPutPolicy::HotThenBackgroundCold,
            populate_hot_on_get: true,
            ..TieredObjectStoreOptions::default()
        },
    );
    let key = ObjectKey::new("blocks/hot-publish").unwrap();
    let put_key = key.clone();
    let put_store = store.clone();
    let (done_tx, done_rx) = mpsc::channel();

    thread::spawn(move || {
        done_tx
            .send(put_store.put(&put_key, b"abcdef".to_vec()))
            .unwrap();
    });

    let info = done_rx
        .recv_timeout(Duration::from_millis(100))
        .expect("hot publish should not wait for cold put")
        .unwrap();
    assert_eq!(info.size, 6);
    assert_eq!(hot.get(&key, None).unwrap(), b"abcdef");
    assert_eq!(store.head(&key).unwrap().unwrap().size, 6);
    assert_eq!(store.get(&key, None).unwrap(), b"abcdef");

    cold.wait_for_first_put();
    assert_eq!(store.stats().unwrap().cold_puts, 0);
    cold.release_first_put();
    wait_until(|| store.stats().unwrap().cold_puts == 1);
    assert_eq!(cold.get(&key, None).unwrap(), b"abcdef");
    let stats = store.stats().unwrap();
    assert_eq!(stats.hot_puts, 1);
    assert_eq!(stats.cold_put_errors, 0);
}

#[test]
fn tiered_object_store_hot_publish_reuses_written_bytes_for_cold_put() {
    let hot = BlockFirstGetStore::new();
    let cold = MemoryObjectStore::new();
    let store = TieredObjectStore::new(
        hot.clone(),
        cold.clone(),
        TieredObjectStoreOptions {
            put_policy: TieredPutPolicy::HotThenBackgroundCold,
            populate_hot_on_get: true,
            ..TieredObjectStoreOptions::default()
        },
    );
    let key = ObjectKey::new("blocks/hot-publish-no-reread").unwrap();
    let bytes = Arc::new(b"abcdef".to_vec());

    store
        .put(&key, ObjectBytes::shared_vec(Arc::clone(&bytes)))
        .unwrap();

    wait_until(|| cold.head(&key).unwrap().is_some());
    assert_eq!(cold.get(&key, None).unwrap(), b"abcdef");
    assert_eq!(hot.get_count(), 0);
    assert_eq!(store.stats().unwrap().cold_puts, 1);
}

#[test]
fn tiered_object_store_hot_publish_delete_prevents_cold_resurrection() {
    let hot = MemoryObjectStore::new();
    let cold = BlockFirstPutStore::new();
    let store = TieredObjectStore::new(
        hot.clone(),
        cold.clone(),
        TieredObjectStoreOptions {
            put_policy: TieredPutPolicy::HotThenBackgroundCold,
            populate_hot_on_get: true,
            ..TieredObjectStoreOptions::default()
        },
    );
    let key = ObjectKey::new("blocks/delete-while-cold-pending").unwrap();

    store.put(&key, b"abcdef".to_vec()).unwrap();
    cold.wait_for_first_put();
    assert!(store.delete(&key).unwrap());
    assert!(hot.head(&key).unwrap().is_none());

    cold.release_first_put();
    wait_until(|| store.stats().unwrap().cold_puts == 1);
    assert!(cold.head(&key).unwrap().is_none());
    assert!(store.head(&key).unwrap().is_none());
}

#[test]
fn tiered_object_store_recovers_pending_cold_puts_from_local_hot_root() {
    let hot_dir = tempfile::tempdir().unwrap();
    let pending_root = crate::fabric::default_pending_cold_put_root(hot_dir.path());
    let hot = LocalObjectStore::new(LocalObjectStoreOptions::new(hot_dir.path())).unwrap();
    let failing_cold = AlwaysFailPutStore::new();
    let options = TieredObjectStoreOptions {
        put_policy: TieredPutPolicy::HotThenBackgroundCold,
        populate_hot_on_get: true,
        pending_cold_put_root: Some(pending_root),
        ..TieredObjectStoreOptions::default()
    };
    let store = TieredObjectStore::new(hot, failing_cold, options.clone());
    let key = ObjectKey::new("blocks/recover-pending-cold").unwrap();

    store.put(&key, b"abcdef".to_vec()).unwrap();
    wait_until(|| store.stats().unwrap().cold_put_errors == 1);

    let cold = MemoryObjectStore::new();
    let recovered_hot =
        LocalObjectStore::new(LocalObjectStoreOptions::new(hot_dir.path())).unwrap();
    let recovered = TieredObjectStore::new(recovered_hot, cold.clone(), options.clone());
    assert_eq!(recovered.recover_pending_cold_puts().unwrap(), 1);
    wait_until(|| recovered.stats().unwrap().cold_puts == 1);
    assert_eq!(cold.get(&key, None).unwrap(), b"abcdef");

    let reopened_hot = LocalObjectStore::new(LocalObjectStoreOptions::new(hot_dir.path())).unwrap();
    let reopened = TieredObjectStore::new(reopened_hot, cold, options);
    assert_eq!(reopened.recover_pending_cold_puts().unwrap(), 0);
}

#[test]
fn tiered_object_store_delete_removes_pending_cold_put_before_recovery() {
    let hot_dir = tempfile::tempdir().unwrap();
    let pending_root = crate::fabric::default_pending_cold_put_root(hot_dir.path());
    let hot = LocalObjectStore::new(LocalObjectStoreOptions::new(hot_dir.path())).unwrap();
    let failing_cold = AlwaysFailPutStore::new();
    let options = TieredObjectStoreOptions {
        put_policy: TieredPutPolicy::HotThenBackgroundCold,
        populate_hot_on_get: true,
        pending_cold_put_root: Some(pending_root),
        ..TieredObjectStoreOptions::default()
    };
    let store = TieredObjectStore::new(hot, failing_cold, options.clone());
    let key = ObjectKey::new("blocks/delete-pending-cold").unwrap();

    store.put(&key, b"abcdef".to_vec()).unwrap();
    wait_until(|| store.stats().unwrap().cold_put_errors == 1);
    assert!(store.delete(&key).unwrap());

    let cold = MemoryObjectStore::new();
    let recovered_hot =
        LocalObjectStore::new(LocalObjectStoreOptions::new(hot_dir.path())).unwrap();
    let recovered = TieredObjectStore::new(recovered_hot, cold.clone(), options);
    assert_eq!(recovered.recover_pending_cold_puts().unwrap(), 0);
    assert!(cold.head(&key).unwrap().is_none());
}

#[test]
fn resolve_block_placements_marks_local_hot_blocks_only() {
    let hot = MemoryObjectStore::new();
    let local_key = ObjectKey::new("blocks/local").unwrap();
    hot.put(&local_key, b"cached".to_vec()).unwrap();
    let blocks = vec![
        ObjectReadBlock {
            object_key: "blocks/local".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 6,
            len: 6,
            output_offset: 0,
        },
        ObjectReadBlock {
            object_key: "blocks/cold".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 4,
            len: 4,
            output_offset: 6,
        },
    ];

    let placements = resolve_block_placements(&hot, &blocks).unwrap();

    assert_eq!(placements.len(), 2);
    assert_eq!(placements[0].object_key, local_key);
    assert_eq!(placements[0].transport, DataTransport::LocalNvmeRead);
    assert_eq!(
        placements[1].object_key,
        ObjectKey::new("blocks/cold").unwrap()
    );
    assert_eq!(placements[1].transport, DataTransport::ObjectTcpGet);
}

#[test]
fn layout_read_executor_reports_hot_and_cold_transport_stats() {
    let hot = MemoryObjectStore::new();
    let cold = MemoryObjectStore::new();
    let store = TieredObjectStore::new(
        hot.clone(),
        cold.clone(),
        TieredObjectStoreOptions {
            put_policy: TieredPutPolicy::ColdOnly,
            populate_hot_on_get: false,
            ..TieredObjectStoreOptions::default()
        },
    );
    hot.put(
        &ObjectKey::new("blocks/hot-layout").unwrap(),
        b"hot-data".to_vec(),
    )
    .unwrap();
    cold.put(
        &ObjectKey::new("blocks/cold-layout").unwrap(),
        b"cold-data".to_vec(),
    )
    .unwrap();
    let plan = ObjectReadPlan::new(
        8,
        vec![
            ObjectReadBlock {
                object_key: "blocks/cold-layout".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                object_offset: 0,
                object_len: 9,
                len: 4,
                output_offset: 0,
            },
            ObjectReadBlock {
                object_key: "blocks/hot-layout".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                object_offset: 4,
                object_len: 8,
                len: 4,
                output_offset: 4,
            },
        ],
    );
    let mut pipeline = FileReadPipeline::default();

    let read = LayoutReadExecutor::new(&store)
        .read_plan(
            &mut pipeline,
            Option::<&MemoryBlockCache>::None,
            8,
            0,
            &plan,
        )
        .unwrap();

    assert_eq!(read.bytes, b"colddata");
    assert_eq!(read.stats.planned_blocks, 2);
    assert_eq!(read.stats.local_nvme_hits, 1);
    assert_eq!(read.stats.object_fallbacks, 1);
    assert_eq!(read.stats.object_gets, 2);
    assert_eq!(read.stats.object_get_bytes, 8);
    assert_eq!(
        read.placements
            .iter()
            .map(|placement| placement.transport)
            .collect::<Vec<_>>(),
        vec![DataTransport::ObjectTcpGet, DataTransport::LocalNvmeRead]
    );
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
        "xxh3-64:6497a96f53a89890"
    );
    let staged = written.staged_objects().unwrap();
    assert_eq!(staged.len(), 4);

    let blocks = vec![
        ObjectReadBlock {
            object_key: "blocks/1/2/3/0/1".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 1,
            object_len: 4,
            len: 3,
            output_offset: 0,
        },
        ObjectReadBlock {
            object_key: "blocks/1/2/3/1/0".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 4,
            len: 2,
            output_offset: 3,
        },
    ];
    let read = read_object_blocks(&store, Option::<&MemoryBlockCache>::None, 5, &blocks).unwrap();
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
fn read_object_blocks_fetches_pending_ranges_in_one_batch() {
    let store = BatchCountingStore::new();
    store
        .put(&ObjectKey::new("blocks/a").unwrap(), b"abcdefgh".to_vec())
        .unwrap();
    store
        .put(&ObjectKey::new("blocks/b").unwrap(), b"uvwxyz".to_vec())
        .unwrap();
    let blocks = vec![
        ObjectReadBlock {
            object_key: "blocks/a".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 1,
            object_len: 8,
            len: 3,
            output_offset: 0,
        },
        ObjectReadBlock {
            object_key: "blocks/b".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 2,
            object_len: 6,
            len: 2,
            output_offset: 3,
        },
    ];

    let read = read_object_blocks(&store, Option::<&MemoryBlockCache>::None, 5, &blocks).unwrap();
    assert_eq!(read.bytes, b"bcdwx");
    assert_eq!(read.object_gets, 2);
    assert_eq!(store.batch_sizes(), vec![2]);
}

#[test]
fn read_object_blocks_propagates_batch_get_errors() {
    let store = BatchFailingStore::new();
    store
        .put(&ObjectKey::new("blocks/a").unwrap(), b"abcdefgh".to_vec())
        .unwrap();
    store
        .put(&ObjectKey::new("blocks/b").unwrap(), b"uvwxyz".to_vec())
        .unwrap();
    let blocks = vec![
        ObjectReadBlock {
            object_key: "blocks/a".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 1,
            object_len: 8,
            len: 3,
            output_offset: 0,
        },
        ObjectReadBlock {
            object_key: "blocks/b".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 2,
            object_len: 6,
            len: 2,
            output_offset: 3,
        },
    ];

    let err = read_object_blocks(&store, Option::<&MemoryBlockCache>::None, 5, &blocks)
        .expect_err("batch get errors must not be hidden by per-object reads");
    assert!(matches!(
        err,
        ObjectError::Backend(message) if message == "injected get_many failure"
    ));
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
    let written = put_chunked_ranges_with_block_index_base(
        &store,
        vec![
            ChunkWriteRange {
                logical_offset: 3,
                bytes: b"XYZ".to_vec().into(),
            },
            ChunkWriteRange {
                logical_offset: 10,
                bytes: b"mnopq".to_vec().into(),
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
        0,
        None,
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
fn write_through_populates_block_cache() {
    let store = MemoryObjectStore::new();
    let cache = MemoryBlockCache::new(MemoryBlockCacheOptions {
        max_bytes: 1 << 20,
        max_items: 64,
        ttl: None,
    });
    let written = put_chunked_ranges_parallel(
        &store,
        vec![ChunkWriteRange {
            logical_offset: 0,
            bytes: b"abcdefgh".to_vec().into(),
        }],
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 5,
            chunk_size: 8,
            block_size: 4,
        },
        0,
        2,
        Some(&cache),
    )
    .unwrap();
    // Every uploaded block is written through to the cache (each block is its
    // own object at offset 0), so a read-after-write is served locally instead
    // of re-fetching from the object store.
    for chunk in &written.chunks {
        for block in &chunk.blocks {
            let cached = cache
                .get_block_range(&block.object_key, 0, block.len as usize)
                .unwrap();
            assert_eq!(cached.map(|bytes| bytes.len()), Some(block.len as usize));
        }
    }
    assert_eq!(
        cache
            .get_block_range(&written.chunks[0].blocks[0].object_key, 0, 4)
            .unwrap()
            .as_deref(),
        Some(&b"abcd"[..])
    );
}

#[test]
fn parallel_chunked_ranges_preserve_manifest_order() {
    let store = MemoryObjectStore::new();
    let written = put_chunked_ranges_parallel(
        &store,
        vec![
            ChunkWriteRange {
                logical_offset: 0,
                bytes: b"abcdefgh".to_vec().into(),
            },
            ChunkWriteRange {
                logical_offset: 8,
                bytes: b"ijklmnop".to_vec().into(),
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
        None,
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
fn owned_chunked_range_split_preserves_block_order_and_bytes() {
    let store = MemoryObjectStore::new();
    let written = put_chunked_ranges_parallel(
        &store,
        vec![ChunkWriteRange {
            logical_offset: 0,
            bytes: b"abcdefghijkl".to_vec().into(),
        }],
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 5,
            chunk_size: 8,
            block_size: 4,
        },
        0,
        2,
        None,
    )
    .unwrap();

    assert_eq!(written.object_puts, 3);
    assert_eq!(written.object_put_bytes, 12);
    assert_eq!(written.chunks.len(), 2);
    assert_eq!(written.chunks[0].blocks[0].object_key, "blocks/1/2/5/0/0");
    assert_eq!(written.chunks[0].blocks[1].object_key, "blocks/1/2/5/0/1");
    assert_eq!(written.chunks[1].blocks[0].object_key, "blocks/1/2/5/1/0");
    assert_eq!(
        store
            .get(&ObjectKey::new("blocks/1/2/5/0/0").unwrap(), None)
            .unwrap(),
        b"abcd"
    );
    assert_eq!(
        store
            .get(&ObjectKey::new("blocks/1/2/5/0/1").unwrap(), None)
            .unwrap(),
        b"efgh"
    );
    assert_eq!(
        store
            .get(&ObjectKey::new("blocks/1/2/5/1/0").unwrap(), None)
            .unwrap(),
        b"ijkl"
    );
}

#[test]
fn owned_parallel_chunked_ranges_reuses_aligned_range_allocation() {
    let store = PointerRecordingObjectStore::default();
    let bytes = vec![7_u8; DEFAULT_BLOCK_SIZE];
    let original_ptr = bytes.as_ptr() as usize;
    let written = put_chunked_ranges_parallel(
        &store,
        vec![ChunkWriteRange {
            logical_offset: 0,
            bytes: bytes.into(),
        }],
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 5,
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size: DEFAULT_BLOCK_SIZE,
        },
        0,
        1,
        None,
    )
    .unwrap();

    assert_eq!(store.put_pointers(), vec![original_ptr]);
    assert_eq!(written.object_puts, 1);
    assert_eq!(written.object_put_bytes, DEFAULT_BLOCK_SIZE as u64);
    assert_eq!(written.chunks[0].blocks[0].len, DEFAULT_BLOCK_SIZE as u64);
}

#[test]
fn chunked_range_split_reuses_shared_range_allocation() {
    let store = PointerRecordingObjectStore::default();
    let bytes = b"abcdefghijkl".to_vec();
    let original_ptr = bytes.as_ptr() as usize;
    let written = put_chunked_ranges_parallel(
        &store,
        vec![ChunkWriteRange {
            logical_offset: 0,
            bytes: bytes.into(),
        }],
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 5,
            chunk_size: 8,
            block_size: 4,
        },
        0,
        1,
        None,
    )
    .unwrap();

    assert_eq!(
        store.put_pointers(),
        vec![original_ptr, original_ptr + 4, original_ptr + 8]
    );
    assert_eq!(written.object_puts, 3);
    assert_eq!(written.object_put_bytes, 12);
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
    let updated = put_chunked_ranges_with_block_index_base(
        &store,
        vec![ChunkWriteRange {
            logical_offset: 2,
            bytes: b"XY".to_vec().into(),
        }],
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 4,
            chunk_size: 8,
            block_size: 4,
        },
        0,
        None,
    )
    .unwrap()
    .into_slice_at(2, 2, 2);

    let plan = plan_slice_reads(&[old, updated], 0, 8).unwrap();
    let read = read_object_blocks(
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
    let dirty = put_chunked_ranges_with_block_index_base(
        &store,
        vec![ChunkWriteRange {
            logical_offset: 4,
            bytes: b"XY".to_vec().into(),
        }],
        ChunkWriteOptions {
            manifest_id: "artifacts/checkpoint".to_owned(),
            mount: 1,
            inode: 2,
            generation: 3,
            chunk_size: 8,
            block_size: 4,
        },
        0,
        None,
    )
    .unwrap()
    .into_slice_at(1, 4, 2);

    let plan = plan_slice_reads(&[dirty], 0, 8).unwrap();
    let read = read_object_blocks(
        &store,
        Option::<&MemoryBlockCache>::None,
        plan.output_len,
        &plan.blocks,
    )
    .unwrap();
    assert_eq!(read.bytes, b"\0\0\0\0XY\0\0");
}

#[test]
fn chunk_manifest_read_plan_matches_slice_read_plan() {
    let store = MemoryObjectStore::new();
    let write = put_chunked_object(
        &store,
        b"abcdefghijkl",
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
    let manifests = write.chunk_manifests();

    let plan = plan_chunk_manifest_reads(&manifests, 2, 6).unwrap();
    assert_eq!(
        plan.blocks[0].digest_uri,
        manifests[0].slices[0].blocks[0].digest_uri
    );
    let read = read_object_blocks(
        &store,
        Option::<&MemoryBlockCache>::None,
        plan.output_len,
        &plan.blocks,
    )
    .unwrap();
    assert_eq!(read.bytes, b"cdefgh");
}

#[test]
fn block_cache_reuses_object_reads() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcd".to_vec()).unwrap();
    let cache = CountingBlockCache::default();
    let blocks = vec![ObjectReadBlock {
        object_key: key.as_str().to_owned(),
        digest_uri: "sha256:test".to_owned(),
        object_offset: 0,
        object_len: 4,
        len: 4,
        output_offset: 0,
    }];
    let first = read_object_blocks(&store, Some(&cache), 4, &blocks).unwrap();
    let second = read_object_blocks(&store, Some(&cache), 4, &blocks).unwrap();
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
fn block_cache_reuses_covering_ranges() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefghijklmnop".to_vec()).unwrap();
    let cache = MemoryBlockCache::default();

    let first = read_object_blocks(
        &store,
        Some(&cache),
        12,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 16,
            len: 12,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(first.bytes, b"abcdefghijkl");
    assert_eq!(first.object_gets, 1);
    assert_eq!(first.cache_hits, 0);

    let second = read_object_blocks(
        &store,
        Some(&cache),
        4,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 4,
            object_len: 16,
            len: 4,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(second.bytes, b"efgh");
    assert_eq!(second.object_gets, 0);
    assert_eq!(second.object_get_bytes, 0);
    assert_eq!(second.cache_hits, 1);
    assert_eq!(second.cache_hit_bytes, 4);
}

#[test]
fn object_prefetcher_populates_block_cache() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefgh".to_vec()).unwrap();
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
        digest_uri: "sha256:test".to_owned(),
        object_offset: 0,
        object_len: 8,
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
    let read = read_object_blocks(&store, Some(&cache), 8, &blocks).unwrap();
    assert_eq!(read.bytes, b"abcdefgh");
    assert_eq!(read.object_gets, 0);
    assert_eq!(read.cache_hits, 1);
    assert_eq!(read.cache_hit_bytes, 8);
}

#[test]
fn object_read_coordinator_piggybacks_inflight_block_reads() {
    let store = BlockFirstGetStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefgh".to_vec()).unwrap();
    let blocks = vec![ObjectReadBlock {
        object_key: key.as_str().to_owned(),
        digest_uri: "sha256:test".to_owned(),
        object_offset: 0,
        object_len: 8,
        len: 8,
        output_offset: 0,
    }];
    let coordinator = ObjectReadCoordinator::new();

    let first_store = store.clone();
    let first_blocks = blocks.clone();
    let first_coordinator = coordinator.clone();
    let first = thread::spawn(move || {
        read_object_blocks_with_cache_options::<_, MemoryBlockCache>(
            &first_store,
            None,
            8,
            &first_blocks,
            BlockReadOptions::block_aligned_cache_fill(8).with_read_coordinator(first_coordinator),
        )
        .unwrap()
    });
    store.wait_for_get_count(1);

    let second_store = store.clone();
    let second_blocks = blocks.clone();
    let second_coordinator = coordinator.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let second = thread::spawn(move || {
        started_tx.send(()).unwrap();
        read_object_blocks_with_cache_options::<_, MemoryBlockCache>(
            &second_store,
            None,
            8,
            &second_blocks,
            BlockReadOptions::block_aligned_cache_fill(8).with_read_coordinator(second_coordinator),
        )
        .unwrap()
    });
    started_rx.recv().unwrap();
    thread::sleep(Duration::from_millis(25));
    assert_eq!(store.get_count(), 1);

    store.release_gets();
    let first = first.join().unwrap();
    let second = second.join().unwrap();
    assert_eq!(first.bytes, b"abcdefgh");
    assert_eq!(second.bytes, b"abcdefgh");
    assert_eq!(first.object_gets + second.object_gets, 1);
    assert_eq!(store.get_count(), 1);
}

#[test]
fn object_prefetcher_piggybacks_duplicate_inflight_fetches() {
    let store = BlockFirstGetStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefgh".to_vec()).unwrap();
    let cache = CountingBlockCache::default();
    let prefetcher = ObjectPrefetcher::new(
        store.clone(),
        cache.clone(),
        ObjectPrefetchOptions {
            queue_capacity: 4,
            workers: 2,
        },
    );
    let blocks = vec![ObjectReadBlock {
        object_key: key.as_str().to_owned(),
        digest_uri: "sha256:test".to_owned(),
        object_offset: 0,
        object_len: 8,
        len: 8,
        output_offset: 0,
    }];

    assert!(prefetcher
        .submit(ObjectPrefetchRequest::new(8, blocks.clone()))
        .unwrap());
    store.wait_for_get_count(1);
    assert!(!prefetcher
        .submit(ObjectPrefetchRequest::new(8, blocks.clone()))
        .unwrap());
    thread::sleep(Duration::from_millis(25));
    assert_eq!(store.get_count(), 1);

    store.release_gets();
    wait_until(|| prefetcher.stats().unwrap().completed == 1);
    let stats = prefetcher.stats().unwrap();
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.dropped, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.object_gets, 1);
    assert_eq!(stats.object_get_bytes, 8);
    assert_eq!(store.get_count(), 1);

    let read = read_object_blocks(&store, Some(&cache), 8, &blocks).unwrap();
    assert_eq!(read.bytes, b"abcdefgh");
    assert_eq!(read.object_gets, 0);
    assert_eq!(read.cache_hits, 1);
}

#[test]
fn disk_block_cache_reuses_object_reads() {
    let dir = tempfile::tempdir().unwrap();
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcd".to_vec()).unwrap();
    let cache = DiskBlockCache::new(DiskBlockCacheOptions {
        root: dir.path().join("blocks"),
        max_bytes: 1024,
        max_items: 8,
        ttl: None,
    })
    .unwrap();
    let blocks = vec![ObjectReadBlock {
        object_key: key.as_str().to_owned(),
        digest_uri: "sha256:test".to_owned(),
        object_offset: 0,
        object_len: 4,
        len: 4,
        output_offset: 0,
    }];
    let first = read_object_blocks(&store, Some(&cache), 4, &blocks).unwrap();
    let second = read_object_blocks(&store, Some(&cache), 4, &blocks).unwrap();
    assert_eq!(first.object_gets, 1);
    assert_eq!(second.object_gets, 0);
    assert_eq!(second.cache_hits, 1);
    assert_eq!(second.cache_hit_bytes, 4);
    let stats = cache.stats().unwrap();
    assert_eq!(stats.items, 1);
    assert_eq!(stats.bytes, 4);
}

#[test]
fn disk_block_cache_reuses_covering_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefghijklmnop".to_vec()).unwrap();
    let cache = DiskBlockCache::new(DiskBlockCacheOptions {
        root: dir.path().join("blocks"),
        max_bytes: 1024,
        max_items: 8,
        ttl: None,
    })
    .unwrap();

    let first = read_object_blocks(
        &store,
        Some(&cache),
        12,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 16,
            len: 12,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(first.object_gets, 1);
    assert_eq!(first.cache_hits, 0);

    let second = read_object_blocks(
        &store,
        Some(&cache),
        4,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 4,
            object_len: 16,
            len: 4,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(second.bytes, b"efgh");
    assert_eq!(second.object_gets, 0);
    assert_eq!(second.cache_hits, 1);
    assert_eq!(second.cache_hit_bytes, 4);
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
            ranges: vec![WritebackUploadRange::cache(0, ticket.clone())],
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
    assert_eq!(stats.inline, 0);
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.uploaded_bytes, 10);
    assert!(stats.upload_ns >= stats.upload_max_ns);
    assert!(cache.read(&ticket).is_err());
    let read = read_object_blocks(
        &store,
        None::<&MemoryBlockCache>,
        10,
        &[ObjectReadBlock {
            object_key: "blocks/1/2/3/0/0".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 10,
            len: 10,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(read.bytes, b"checkpoint");
}

#[test]
fn object_writeback_uploader_uploads_inline_ranges_without_cache() {
    let store = MemoryObjectStore::new();
    let uploader = ObjectWritebackUploader::direct(
        store.clone(),
        ObjectWritebackOptions {
            queue_capacity: 4,
            workers: 1,
            upload_workers_per_request: 1,
        },
    );
    let pending = uploader
        .submit(ObjectWritebackRequest {
            ranges: vec![WritebackUploadRange::inline_bytes(
                0,
                b"checkpoint".to_vec(),
            )],
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
    assert_eq!(pending.discard_writeback_cache().unwrap(), 0);
    let stats = uploader.stats().unwrap();
    assert_eq!(stats.enqueued, 1);
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.staged_bytes, 10);
    assert_eq!(stats.uploaded_bytes, 10);
    let read = read_object_blocks(
        &store,
        None::<&MemoryBlockCache>,
        10,
        &[ObjectReadBlock {
            object_key: "blocks/1/2/3/0/0".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 10,
            len: 10,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(read.bytes, b"checkpoint");
}

#[test]
fn object_slice_writer_flushes_complete_blocks_and_finishes_tail() {
    let mut writer = ObjectSliceWriter::new(4).unwrap();
    writer.write_at(0, b"abcdef".to_vec()).unwrap();

    let flushed = writer.flush_to(4).unwrap();
    assert_eq!(writer.uploaded(), 4);
    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].logical_offset, 0);
    assert_eq!(flushed[0].bytes.as_slice(), b"abcd");

    let tail = writer.finish().unwrap();
    assert_eq!(writer.uploaded(), 6);
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].logical_offset, 4);
    assert_eq!(tail[0].bytes.as_slice(), b"ef");
}

#[test]
fn object_slice_writer_rejects_overwrite_before_uploaded_frontier() {
    let mut writer = ObjectSliceWriter::new(4).unwrap();
    writer.write_at(0, b"abcdefgh".to_vec()).unwrap();
    assert_eq!(writer.flush_to(4).unwrap().len(), 1);

    let err = writer.write_at(2, b"XX".to_vec()).unwrap_err();

    assert!(matches!(err, ObjectError::Backend(message) if message.contains("uploaded")));
}

#[test]
fn object_slice_writer_preserves_sparse_holes() {
    let mut writer = ObjectSliceWriter::new(4).unwrap();
    writer.write_at(0, b"ab".to_vec()).unwrap();
    writer.write_at(8, b"ij".to_vec()).unwrap();
    writer.write_at(2, b"cd".to_vec()).unwrap();

    let ranges = writer.finish().unwrap();

    assert_eq!(ranges.len(), 2);
    assert_eq!(ranges[0].logical_offset, 0);
    assert_eq!(ranges[0].bytes.as_slice(), b"abcd");
    assert_eq!(ranges[1].logical_offset, 8);
    assert_eq!(ranges[1].bytes.as_slice(), b"ij");
}

#[test]
fn object_writeback_uploader_moves_inline_range_into_object_put() {
    let store = PointerRecordingObjectStore::default();
    let uploader = ObjectWritebackUploader::direct(
        store.clone(),
        ObjectWritebackOptions {
            queue_capacity: 4,
            workers: 1,
            upload_workers_per_request: 1,
        },
    );
    let bytes = b"checkpoint".to_vec();
    let put_pointer = bytes.as_ptr() as usize;
    let pending = uploader
        .submit(ObjectWritebackRequest {
            ranges: vec![WritebackUploadRange::inline_bytes(0, bytes)],
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
    assert_eq!(store.put_pointers(), vec![put_pointer]);
}

#[test]
fn object_writeback_uploader_coalesces_adjacent_inline_ranges() {
    let store = MemoryObjectStore::new();
    let uploader = ObjectWritebackUploader::direct(
        store.clone(),
        ObjectWritebackOptions {
            queue_capacity: 4,
            workers: 1,
            upload_workers_per_request: 1,
        },
    );
    let pending = uploader
        .submit(ObjectWritebackRequest {
            ranges: vec![
                WritebackUploadRange::inline_bytes(0, b"abcd".to_vec()),
                WritebackUploadRange::inline_bytes(4, b"efgh".to_vec()),
                WritebackUploadRange::inline_bytes(8, b"ij".to_vec()),
            ],
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
    let written = pending.wait().unwrap();
    assert_eq!(written.object_puts, 2);
    assert_eq!(written.object_put_bytes, 10);
    assert_eq!(written.chunks[0].blocks.len(), 2);
    assert_eq!(written.chunks[0].blocks[0].object_key, "blocks/1/2/3/0/0");
    assert_eq!(written.chunks[0].blocks[1].object_key, "blocks/1/2/3/0/1");
    assert_eq!(
        store
            .get(&ObjectKey::new("blocks/1/2/3/0/0").unwrap(), None)
            .unwrap(),
        b"abcdefgh"
    );
    assert_eq!(
        store
            .get(&ObjectKey::new("blocks/1/2/3/0/1").unwrap(), None)
            .unwrap(),
        b"ij"
    );
    let stats = uploader.stats().unwrap();
    assert_eq!(stats.staged_bytes, 10);
    assert_eq!(stats.uploaded_bytes, 10);
}

#[test]
fn object_writeback_uploader_keeps_block_framed_inline_ranges_unmerged() {
    let store = PointerRecordingObjectStore::default();
    let uploader = ObjectWritebackUploader::direct(
        store.clone(),
        ObjectWritebackOptions {
            queue_capacity: 4,
            workers: 1,
            upload_workers_per_request: 1,
        },
    );
    let first = b"abcd".to_vec();
    let second = b"efgh".to_vec();
    let first_ptr = first.as_ptr() as usize;
    let second_ptr = second.as_ptr() as usize;

    let pending = uploader
        .submit(ObjectWritebackRequest {
            ranges: vec![
                WritebackUploadRange::inline_bytes(0, first),
                WritebackUploadRange::inline_bytes(4, second),
            ],
            options: ChunkWriteOptions {
                manifest_id: "artifacts/checkpoint".to_owned(),
                mount: 1,
                inode: 2,
                generation: 3,
                chunk_size: 64,
                block_size: 4,
            },
            block_index_base: 0,
        })
        .unwrap();

    let written = pending.wait().unwrap();
    assert_eq!(written.object_puts, 2);
    assert_eq!(written.object_put_bytes, 8);
    assert_eq!(store.put_pointers(), vec![first_ptr, second_ptr]);
}

#[test]
fn object_writeback_uploader_reports_direct_upload_failure_without_cache_cleanup() {
    let store = FailAfterFirstPut::new();
    let uploader = ObjectWritebackUploader::direct(
        store,
        ObjectWritebackOptions {
            queue_capacity: 4,
            workers: 1,
            upload_workers_per_request: 1,
        },
    );
    let pending = uploader
        .submit(ObjectWritebackRequest {
            ranges: vec![WritebackUploadRange::inline_bytes(
                0,
                b"checkpoint-data".to_vec(),
            )],
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
    assert_eq!(pending.discard_writeback_cache().unwrap(), 0);
    let stats = uploader.stats().unwrap();
    assert_eq!(stats.enqueued, 1);
    assert_eq!(stats.completed, 0);
    assert_eq!(stats.failed, 1);
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
            ranges: vec![WritebackUploadRange::cache(0, ticket.clone())],
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
    assert_eq!(upload_stats.inline, 0);
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
fn object_writeback_uploader_falls_back_inline_when_queue_is_full() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockFirstPutStore::new();
    let cache = WritebackCache::new(WritebackCacheOptions {
        root: dir.path().join("writeback"),
        max_bytes: 4096,
        max_items: 8,
    })
    .unwrap();
    let uploader = ObjectWritebackUploader::new(
        store.clone(),
        cache.clone(),
        ObjectWritebackOptions {
            queue_capacity: 1,
            workers: 1,
            upload_workers_per_request: 1,
        },
    );

    let first = submit_writeback_range(&uploader, &cache, 0, b"first");
    store.wait_for_first_put();
    let second = submit_writeback_range(&uploader, &cache, 16, b"second");
    let third = submit_writeback_range(&uploader, &cache, 32, b"third");

    let third_written = third.wait().unwrap();
    assert_eq!(third_written.chunks[0].blocks[0].logical_offset, 32);
    store.release_first_put();
    first.wait().unwrap();
    second.wait().unwrap();

    let stats = uploader.stats().unwrap();
    assert_eq!(stats.enqueued, 3);
    assert_eq!(stats.inline, 1);
    assert_eq!(stats.completed, 3);
    assert_eq!(stats.failed, 0);
    assert!(stats.queue_max_wait_ns > 0);
    assert!(stats.queue_wait_ns >= stats.queue_max_wait_ns);
    assert!(stats.upload_ns >= stats.upload_max_ns);
}

#[test]
fn object_prefetcher_accepts_disk_backed_block_cache() {
    let dir = tempfile::tempdir().unwrap();
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefgh".to_vec()).unwrap();
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
        digest_uri: "sha256:test".to_owned(),
        object_offset: 0,
        object_len: 8,
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
    let read = read_object_blocks(&store, Some(&cache), 8, &blocks).unwrap();
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
        .read_blocks_with_options::<MemoryBlockCache>(
            None,
            4,
            &[ObjectReadBlock {
                object_key: "blocks/1/2/3/0/1".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                object_offset: 0,
                object_len: 4,
                len: 4,
                output_offset: 0,
            }],
            BlockReadOptions::default(),
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
            vec![ChunkWriteRange {
                logical_offset: 0,
                bytes: b"abcd".to_vec().into(),
            }],
            writer.options().clone(),
            first_base,
        )
        .unwrap();
    writer.record_write(first).unwrap();

    let second_base = writer.reserve_blocks(1);
    let second = store
        .write_ranges_with_block_index_base(
            vec![ChunkWriteRange {
                logical_offset: 4,
                bytes: b"efgh".to_vec().into(),
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
fn file_write_pipeline_flushes_active_slice_uploads() {
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

    writer.write_at(0, b"abcdef".to_vec()).unwrap();
    let first = writer.flush_to(6).unwrap().unwrap();

    assert_eq!(first.block_index_base, 0);
    assert_eq!(first.block_count, 1);
    assert_eq!(first.ranges.len(), 1);
    assert_eq!(first.ranges[0].logical_offset, 0);
    assert_eq!(first.ranges[0].bytes.as_slice(), b"abcd");

    let written = store
        .write_ranges_with_block_index_base(
            first.ranges,
            writer.options().clone(),
            first.block_index_base,
        )
        .unwrap();
    writer.record_write(written).unwrap();

    let tail = writer.finish_upload().unwrap().unwrap();
    assert_eq!(tail.block_index_base, 1);
    assert_eq!(tail.block_count, 1);
    assert_eq!(tail.ranges[0].logical_offset, 4);
    assert_eq!(tail.ranges[0].bytes.as_slice(), b"ef");

    let written = store
        .write_ranges_with_block_index_base(
            tail.ranges,
            writer.options().clone(),
            tail.block_index_base,
        )
        .unwrap();
    writer.record_write(written).unwrap();

    assert_eq!(writer.staged_objects().len(), 2);
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
fn file_write_pipeline_finish_starts_next_slice_for_overwrites() {
    let mut writer = FileWritePipeline::new(ChunkWriteOptions {
        manifest_id: "artifacts/checkpoint".to_owned(),
        mount: 1,
        inode: 2,
        generation: 3,
        chunk_size: 8,
        block_size: 4,
    })
    .unwrap();

    writer.write_at(8, b"ijkl".to_vec()).unwrap();
    let first = writer.finish_upload().unwrap().unwrap();
    assert_eq!(first.block_index_base, 0);
    assert_eq!(first.block_count, 1);
    assert_eq!(first.ranges[0].logical_offset, 8);

    writer.write_at(0, b"abcd".to_vec()).unwrap();
    let second = writer.finish_upload().unwrap().unwrap();
    assert_eq!(second.block_index_base, 1);
    assert_eq!(second.block_count, 1);
    assert_eq!(second.ranges[0].logical_offset, 0);
}

#[test]
fn file_read_pipeline_reports_sequential_readahead_hints() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefghijklmnop".to_vec()).unwrap();
    let cache = MemoryBlockCache::default();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: 4,
    });

    let first = read_file_blocks(
        &mut reader,
        &store,
        Some(&cache),
        16,
        0,
        4,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 16,
            len: 4,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(first.blocks.bytes, b"abcd");
    assert_eq!(first.readahead, Some(ReadAheadHint { offset: 4, len: 4 }));

    let second = read_file_blocks(
        &mut reader,
        &store,
        Some(&cache),
        16,
        4,
        4,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 4,
            object_len: 16,
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
fn file_read_pipeline_uses_configured_readahead_window() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefghijklmnop".to_vec()).unwrap();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: 12,
    });

    let read = read_file_blocks(
        &mut reader,
        &store,
        Option::<&MemoryBlockCache>::None,
        16,
        0,
        4,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 16,
            len: 4,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(read.blocks.bytes, b"abcd");
    assert_eq!(read.readahead, Some(ReadAheadHint { offset: 4, len: 12 }));
    let stats = reader.stats();
    assert_eq!(stats.readahead_hints, 1);
    assert_eq!(stats.readahead_hint_bytes, 12);
}

#[test]
fn file_read_pipeline_throttles_readahead_within_active_window() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefghijklmnop".to_vec()).unwrap();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: 12,
    });

    let first = read_file_blocks(
        &mut reader,
        &store,
        Option::<&MemoryBlockCache>::None,
        16,
        0,
        4,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 16,
            len: 4,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(first.readahead, Some(ReadAheadHint { offset: 4, len: 12 }));

    let second = read_file_blocks(
        &mut reader,
        &store,
        Option::<&MemoryBlockCache>::None,
        16,
        4,
        4,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 4,
            object_len: 16,
            len: 4,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(second.blocks.bytes, b"efgh");
    assert_eq!(second.readahead, None);

    let stats = reader.stats();
    assert_eq!(stats.sequential_reads, 2);
    assert_eq!(stats.readahead_hints, 1);
    assert_eq!(stats.readahead_hint_bytes, 12);
}

#[test]
fn file_read_pipeline_grows_readahead_window_for_long_sequential_stream() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    let bytes = (0..DEFAULT_BLOCK_SIZE * 2)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    store.put(&key, bytes).unwrap();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: DEFAULT_BLOCK_SIZE * 2,
    });

    let first = read_file_blocks::<_, MemoryBlockCache>(
        &mut reader,
        &store,
        None,
        (DEFAULT_BLOCK_SIZE * 4) as u64,
        0,
        DEFAULT_BLOCK_SIZE,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: (DEFAULT_BLOCK_SIZE * 2) as u64,
            len: DEFAULT_BLOCK_SIZE,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(
        first.readahead,
        Some(ReadAheadHint {
            offset: DEFAULT_BLOCK_SIZE as u64,
            len: DEFAULT_BLOCK_SIZE,
        })
    );

    let second = read_file_blocks::<_, MemoryBlockCache>(
        &mut reader,
        &store,
        None,
        (DEFAULT_BLOCK_SIZE * 4) as u64,
        DEFAULT_BLOCK_SIZE as u64,
        DEFAULT_BLOCK_SIZE,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: DEFAULT_BLOCK_SIZE as u64,
            object_len: (DEFAULT_BLOCK_SIZE * 2) as u64,
            len: DEFAULT_BLOCK_SIZE,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(
        second.readahead,
        Some(ReadAheadHint {
            offset: (DEFAULT_BLOCK_SIZE * 2) as u64,
            len: DEFAULT_BLOCK_SIZE * 2,
        })
    );
    let stats = reader.stats();
    assert_eq!(stats.readahead_hints, 2);
    assert_eq!(stats.readahead_hint_bytes, (DEFAULT_BLOCK_SIZE * 3) as u64);
}

#[test]
fn file_read_pipeline_does_not_readahead_after_seek() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdefghijklmnop".to_vec()).unwrap();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: 4,
    });

    let _ = read_file_blocks::<_, MemoryBlockCache>(
        &mut reader,
        &store,
        None,
        16,
        0,
        4,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 16,
            len: 4,
            output_offset: 0,
        }],
    )
    .unwrap();
    let second = read_file_blocks::<_, MemoryBlockCache>(
        &mut reader,
        &store,
        None,
        16,
        12,
        4,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 12,
            object_len: 16,
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
    store.put(&key, b"abcdefghijklmnop".to_vec()).unwrap();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: 4,
    });

    let read = read_file_blocks::<_, MemoryBlockCache>(
        &mut reader,
        &store,
        None,
        16,
        8,
        4,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 8,
            object_len: 16,
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
fn file_read_pipeline_foreground_fills_block_cache_for_large_sequential_read() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    let bytes = (0..DEFAULT_BLOCK_SIZE)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    store.put(&key, bytes.clone()).unwrap();
    let cache = MemoryBlockCache::default();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: DEFAULT_BLOCK_SIZE,
    });

    let first = read_file_blocks(
        &mut reader,
        &store,
        Some(&cache),
        DEFAULT_BLOCK_SIZE as u64,
        0,
        DEFAULT_BLOCK_SIZE / 2,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: DEFAULT_BLOCK_SIZE as u64,
            len: DEFAULT_BLOCK_SIZE / 2,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(first.blocks.bytes, bytes[..DEFAULT_BLOCK_SIZE / 2]);
    assert_eq!(first.blocks.object_gets, 1);
    assert_eq!(
        first.blocks.object_get_bytes,
        (DEFAULT_BLOCK_SIZE / 2) as u64
    );
    assert_eq!(first.blocks.cache_hits, 0);

    let second = read_file_blocks(
        &mut reader,
        &store,
        Some(&cache),
        DEFAULT_BLOCK_SIZE as u64,
        (DEFAULT_BLOCK_SIZE / 2) as u64,
        DEFAULT_BLOCK_SIZE / 2,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: (DEFAULT_BLOCK_SIZE / 2) as u64,
            object_len: DEFAULT_BLOCK_SIZE as u64,
            len: DEFAULT_BLOCK_SIZE / 2,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(
        second.blocks.bytes,
        bytes[DEFAULT_BLOCK_SIZE / 2..DEFAULT_BLOCK_SIZE]
    );
    assert_eq!(second.blocks.object_gets, 1);
    assert_eq!(
        second.blocks.object_get_bytes,
        (DEFAULT_BLOCK_SIZE / 2) as u64
    );
    assert_eq!(second.blocks.cache_hits, 0);

    let third = read_file_blocks(
        &mut reader,
        &store,
        Some(&cache),
        DEFAULT_BLOCK_SIZE as u64,
        (DEFAULT_BLOCK_SIZE - 1024) as u64,
        1024,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: (DEFAULT_BLOCK_SIZE - 1024) as u64,
            object_len: DEFAULT_BLOCK_SIZE as u64,
            len: 1024,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(
        third.blocks.bytes,
        bytes[DEFAULT_BLOCK_SIZE - 1024..DEFAULT_BLOCK_SIZE]
    );
    assert_eq!(third.blocks.object_gets, 0);
    assert_eq!(third.blocks.cache_hits, 1);
    assert_eq!(third.blocks.cache_hit_bytes, 1024);
}

#[test]
fn file_read_pipeline_keeps_small_inner_block_sequential_read_exact() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    let bytes = (0..DEFAULT_BLOCK_SIZE)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    store.put(&key, bytes.clone()).unwrap();
    let cache = MemoryBlockCache::default();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: DEFAULT_BLOCK_SIZE,
    });
    let quarter_block = DEFAULT_BLOCK_SIZE / 4;

    let first = read_file_blocks(
        &mut reader,
        &store,
        Some(&cache),
        DEFAULT_BLOCK_SIZE as u64,
        0,
        quarter_block,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: DEFAULT_BLOCK_SIZE as u64,
            len: quarter_block,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(first.blocks.bytes, bytes[..quarter_block]);
    assert_eq!(first.blocks.object_gets, 1);
    assert_eq!(first.blocks.object_get_bytes, quarter_block as u64);
    assert_eq!(first.blocks.cache_hits, 0);

    let second = read_file_blocks(
        &mut reader,
        &store,
        Some(&cache),
        DEFAULT_BLOCK_SIZE as u64,
        quarter_block as u64,
        quarter_block,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: quarter_block as u64,
            object_len: DEFAULT_BLOCK_SIZE as u64,
            len: quarter_block,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(second.blocks.bytes, bytes[quarter_block..quarter_block * 2]);
    assert_eq!(second.blocks.object_gets, 1);
    assert_eq!(second.blocks.object_get_bytes, quarter_block as u64);
    assert_eq!(second.blocks.cache_hits, 0);

    let cache_stats = cache.stats().unwrap();
    assert_eq!(cache_stats.put_bytes, (quarter_block * 2) as u64);
}

#[test]
fn file_read_pipeline_returns_cache_warmup_for_small_inner_block_read() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    let bytes = (0..DEFAULT_BLOCK_SIZE)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    store.put(&key, bytes.clone()).unwrap();
    let cache = MemoryBlockCache::default();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: DEFAULT_BLOCK_SIZE,
    });
    let quarter_block = DEFAULT_BLOCK_SIZE / 4;

    let read = read_file_blocks(
        &mut reader,
        &store,
        Some(&cache),
        DEFAULT_BLOCK_SIZE as u64,
        quarter_block as u64,
        quarter_block,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: quarter_block as u64,
            object_len: DEFAULT_BLOCK_SIZE as u64,
            len: quarter_block,
            output_offset: 0,
        }],
    )
    .unwrap();

    assert_eq!(read.blocks.bytes, bytes[quarter_block..quarter_block * 2]);
    assert_eq!(read.blocks.object_gets, 1);
    assert_eq!(read.blocks.object_get_bytes, quarter_block as u64);
    let warmup = read.cache_warmup.expect("expected full-block warmup");
    assert_eq!(warmup.output_len, DEFAULT_BLOCK_SIZE);
    assert_eq!(
        warmup.blocks,
        vec![ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: DEFAULT_BLOCK_SIZE as u64,
            len: DEFAULT_BLOCK_SIZE,
            output_offset: 0,
        }]
    );
}

#[test]
fn file_read_pipeline_cache_warmup_populates_block_cache() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    let bytes = (0..DEFAULT_BLOCK_SIZE)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    store.put(&key, bytes.clone()).unwrap();
    let cache = MemoryBlockCache::default();
    let prefetcher = ObjectPrefetcher::new(
        store.clone(),
        cache.clone(),
        ObjectPrefetchOptions {
            queue_capacity: 4,
            workers: 1,
        },
    );
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: DEFAULT_BLOCK_SIZE,
    });
    let quarter_block = DEFAULT_BLOCK_SIZE / 4;

    let first = read_file_blocks(
        &mut reader,
        &store,
        Some(&cache),
        DEFAULT_BLOCK_SIZE as u64,
        quarter_block as u64,
        quarter_block,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: quarter_block as u64,
            object_len: DEFAULT_BLOCK_SIZE as u64,
            len: quarter_block,
            output_offset: 0,
        }],
    )
    .unwrap();
    let warmup = first.cache_warmup.expect("expected full-block warmup");
    assert!(prefetcher.submit(warmup).unwrap());
    wait_until(|| prefetcher.stats().unwrap().completed == 1);

    let second = read_object_blocks(
        &store,
        Some(&cache),
        1024,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: (DEFAULT_BLOCK_SIZE - 1024) as u64,
            object_len: DEFAULT_BLOCK_SIZE as u64,
            len: 1024,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(
        second.bytes,
        bytes[DEFAULT_BLOCK_SIZE - 1024..DEFAULT_BLOCK_SIZE]
    );
    assert_eq!(second.object_gets, 0);
    assert_eq!(second.cache_hits, 1);
    assert_eq!(second.cache_hit_bytes, 1024);
}

#[test]
fn file_read_pipeline_reuses_read_window_without_block_cache() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    let bytes = (0..DEFAULT_BLOCK_SIZE)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    store.put(&key, bytes.clone()).unwrap();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: DEFAULT_BLOCK_SIZE,
    });
    let large_inner_read = DEFAULT_BLOCK_SIZE / 4 + 1;
    let third_offset = DEFAULT_BLOCK_SIZE / 2 + large_inner_read;

    let first = read_file_blocks::<_, MemoryBlockCache>(
        &mut reader,
        &store,
        None,
        DEFAULT_BLOCK_SIZE as u64,
        0,
        DEFAULT_BLOCK_SIZE / 2,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: DEFAULT_BLOCK_SIZE as u64,
            len: DEFAULT_BLOCK_SIZE / 2,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(first.blocks.bytes, bytes[..DEFAULT_BLOCK_SIZE / 2]);
    assert_eq!(first.blocks.object_gets, 1);
    assert_eq!(
        first.blocks.object_get_bytes,
        (DEFAULT_BLOCK_SIZE / 2) as u64
    );
    assert_eq!(first.blocks.cache_hits, 0);

    let second = read_file_blocks::<_, MemoryBlockCache>(
        &mut reader,
        &store,
        None,
        DEFAULT_BLOCK_SIZE as u64,
        (DEFAULT_BLOCK_SIZE / 2) as u64,
        large_inner_read,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: (DEFAULT_BLOCK_SIZE / 2) as u64,
            object_len: DEFAULT_BLOCK_SIZE as u64,
            len: large_inner_read,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(
        second.blocks.bytes,
        bytes[DEFAULT_BLOCK_SIZE / 2..DEFAULT_BLOCK_SIZE / 2 + large_inner_read]
    );
    assert_eq!(second.blocks.object_gets, 1);
    assert_eq!(
        second.blocks.object_get_bytes,
        (DEFAULT_BLOCK_SIZE / 2) as u64
    );
    assert_eq!(second.blocks.cache_hits, 0);

    let third = read_file_blocks::<_, MemoryBlockCache>(
        &mut reader,
        &store,
        None,
        DEFAULT_BLOCK_SIZE as u64,
        third_offset as u64,
        1024,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: third_offset as u64,
            object_len: DEFAULT_BLOCK_SIZE as u64,
            len: 1024,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(third.blocks.bytes, bytes[third_offset..third_offset + 1024]);
    assert_eq!(third.blocks.object_gets, 0);
    assert_eq!(third.blocks.object_get_bytes, 0);
    assert_eq!(third.blocks.cache_hits, 1);
    assert_eq!(third.blocks.cache_hit_bytes, 1024);
}

#[test]
fn file_read_pipeline_keeps_initial_random_read_exact() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    let bytes = (0..DEFAULT_BLOCK_SIZE)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    store.put(&key, bytes.clone()).unwrap();
    let cache = MemoryBlockCache::default();
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: DEFAULT_BLOCK_SIZE,
    });

    let read = read_file_blocks(
        &mut reader,
        &store,
        Some(&cache),
        DEFAULT_BLOCK_SIZE as u64,
        1024,
        1024,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 1024,
            object_len: DEFAULT_BLOCK_SIZE as u64,
            len: 1024,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(read.blocks.bytes, bytes[1024..2048]);
    assert_eq!(read.blocks.object_gets, 1);
    assert_eq!(read.blocks.object_get_bytes, 1024);
    assert_eq!(read.blocks.cache_hits, 0);
    assert_eq!(read.readahead, None);
}

#[test]
fn file_read_pipeline_caches_actual_bytes_for_short_final_block() {
    let store = MemoryObjectStore::new();
    let key = ObjectKey::new("blocks/1/2/3/0/0").unwrap();
    store.put(&key, b"abcdef".to_vec()).unwrap();
    let cache = MemoryBlockCache::default();
    let prefetcher = ObjectPrefetcher::new(
        store.clone(),
        cache.clone(),
        ObjectPrefetchOptions {
            queue_capacity: 4,
            workers: 1,
        },
    );
    let mut reader = FileReadPipeline::new(FileReadPipelineOptions {
        max_readahead_bytes: DEFAULT_BLOCK_SIZE,
    });

    let first = read_file_blocks(
        &mut reader,
        &store,
        Some(&cache),
        6,
        0,
        3,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 6,
            len: 3,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(first.blocks.bytes, b"abc");
    assert_eq!(first.blocks.object_gets, 1);
    assert_eq!(first.blocks.object_get_bytes, 3);
    assert_eq!(first.blocks.cache_hits, 0);

    let second = read_file_blocks(
        &mut reader,
        &store,
        Some(&cache),
        6,
        3,
        3,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 3,
            object_len: 6,
            len: 3,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(second.blocks.bytes, b"def");
    assert_eq!(second.blocks.object_gets, 1);
    assert_eq!(second.blocks.object_get_bytes, 3);
    assert_eq!(second.blocks.cache_hits, 0);
    let warmup = second.cache_warmup.clone().expect("expected short warmup");
    assert_eq!(
        warmup,
        ObjectPrefetchRequest::exact(
            6,
            vec![ObjectReadBlock {
                object_key: key.as_str().to_owned(),
                digest_uri: "sha256:test".to_owned(),
                object_offset: 0,
                object_len: 6,
                len: 6,
                output_offset: 0,
            }]
        )
    );
    assert!(prefetcher.submit(warmup).unwrap());
    wait_until(|| prefetcher.stats().unwrap().completed == 1);

    let middle = read_object_blocks(
        &store,
        Some(&cache),
        4,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 1,
            object_len: 6,
            len: 4,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(middle.bytes, b"bcde");
    assert_eq!(middle.object_gets, 0);
    assert_eq!(middle.cache_hits, 1);
    assert_eq!(middle.cache_hit_bytes, 4);

    let third = read_file_blocks(
        &mut reader,
        &store,
        Some(&cache),
        6,
        3,
        3,
        &[ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 3,
            object_len: 6,
            len: 3,
            output_offset: 0,
        }],
    )
    .unwrap();
    assert_eq!(third.blocks.bytes, b"def");
    assert_eq!(third.blocks.object_gets, 0);
    assert_eq!(third.blocks.cache_hits, 1);
    assert_eq!(third.blocks.cache_hit_bytes, 3);
}

#[test]
fn object_read_plan_cache_evicts_oldest_unused_plan() {
    let mut cache = ObjectReadPlanCache::new(2);
    let a = ObjectReadPlanKey::new(1, 7, 0, 4);
    let b = ObjectReadPlanKey::new(1, 7, 4, 4);
    let c = ObjectReadPlanKey::new(1, 7, 8, 4);
    let plan = ObjectReadPlan::new(
        4,
        vec![ObjectReadBlock {
            object_key: "blocks/demo".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 4,
            len: 4,
            output_offset: 0,
        }],
    );

    cache.insert(a, plan.clone());
    cache.insert(b, plan.clone());
    assert!(cache.get(&a).is_some());
    cache.insert(c, plan);

    assert!(cache.get(&a).is_some());
    assert!(cache.get(&b).is_none());
    assert!(cache.get(&c).is_some());
    assert_eq!(cache.len(), 2);
}

#[test]
fn object_read_plan_cache_reuses_covering_plan() {
    let mut cache = ObjectReadPlanCache::new(2);
    cache.insert(
        ObjectReadPlanKey::new(42, 7, 0, 12),
        ObjectReadPlan::new(
            12,
            vec![ObjectReadBlock {
                object_key: "blocks/demo".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                object_offset: 0,
                object_len: 12,
                len: 12,
                output_offset: 0,
            }],
        ),
    );

    let plan = cache.get(&ObjectReadPlanKey::new(42, 7, 4, 4)).unwrap();
    assert_eq!(plan.output_len, 4);
    assert_eq!(
        plan.blocks,
        vec![ObjectReadBlock {
            object_key: "blocks/demo".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 4,
            object_len: 12,
            len: 4,
            output_offset: 0,
        }]
    );
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
    store.put(&key, b"abcdefgh".to_vec()).unwrap();
    let cache = MemoryBlockCache::default();
    let blocks = vec![
        ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 8,
            len: 3,
            output_offset: 0,
        },
        ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 3,
            object_len: 8,
            len: 5,
            output_offset: 3,
        },
    ];

    let first = read_object_blocks(&store, Some(&cache), 8, &blocks).unwrap();
    assert_eq!(first.bytes, b"abcdefgh");
    assert_eq!(first.object_gets, 1);
    assert_eq!(first.object_get_bytes, 8);
    assert_eq!(first.coalesced_gets, 1);
    assert_eq!(first.coalesced_get_bytes, 8);
    assert_eq!(first.cache_hits, 0);

    let second = read_object_blocks(&store, Some(&cache), 8, &blocks).unwrap();
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
    store.put(&key, b"abcdefgh".to_vec()).unwrap();
    let blocks = vec![
        ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 4,
            object_len: 8,
            len: 4,
            output_offset: 4,
        },
        ObjectReadBlock {
            object_key: key.as_str().to_owned(),
            digest_uri: "sha256:test".to_owned(),
            object_offset: 0,
            object_len: 8,
            len: 4,
            output_offset: 0,
        },
    ];

    let read = read_object_blocks(&store, Option::<&MemoryBlockCache>::None, 8, &blocks).unwrap();
    assert_eq!(read.bytes, b"abcdefgh");
    assert_eq!(read.object_gets, 1);
    assert_eq!(read.coalesced_gets, 1);
    assert_eq!(read.coalesced_get_bytes, 8);
}

#[derive(Clone)]
struct BatchCountingStore {
    inner: MemoryObjectStore,
    batch_sizes: Arc<Mutex<Vec<usize>>>,
}

impl BatchCountingStore {
    fn new() -> Self {
        Self {
            inner: MemoryObjectStore::new(),
            batch_sizes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn batch_sizes(&self) -> Vec<usize> {
        self.batch_sizes.lock().unwrap().clone()
    }
}

impl ObjectStore for BatchCountingStore {
    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        self.inner.put(key, bytes)
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.inner.get(key, range)
    }

    fn get_many(&self, requests: &[ObjectGetRequest]) -> Result<Vec<Vec<u8>>, ObjectError> {
        self.batch_sizes.lock().unwrap().push(requests.len());
        self.inner.get_many(requests)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        self.inner.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        self.inner.delete(key)
    }
}

#[derive(Clone)]
struct BatchFailingStore {
    inner: MemoryObjectStore,
}

impl BatchFailingStore {
    fn new() -> Self {
        Self {
            inner: MemoryObjectStore::new(),
        }
    }
}

impl ObjectStore for BatchFailingStore {
    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        self.inner.put(key, bytes)
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.inner.get(key, range)
    }

    fn get_many(&self, _requests: &[ObjectGetRequest]) -> Result<Vec<Vec<u8>>, ObjectError> {
        Err(ObjectError::Backend("injected get_many failure".to_owned()))
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        self.inner.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        self.inner.delete(key)
    }
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
    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
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

#[derive(Clone)]
struct AlwaysFailPutStore {
    inner: MemoryObjectStore,
}

impl AlwaysFailPutStore {
    fn new() -> Self {
        Self {
            inner: MemoryObjectStore::new(),
        }
    }
}

impl ObjectStore for AlwaysFailPutStore {
    fn put(
        &self,
        _key: &ObjectKey,
        _bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        Err(ObjectError::Backend("injected put failure".to_owned()))
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

#[derive(Clone)]
struct BlockFirstPutStore {
    inner: MemoryObjectStore,
    state: Arc<(Mutex<BlockFirstPutState>, Condvar)>,
}

#[derive(Debug, Default)]
struct BlockFirstPutState {
    started: bool,
    released: bool,
    puts: usize,
}

impl BlockFirstPutStore {
    fn new() -> Self {
        Self {
            inner: MemoryObjectStore::new(),
            state: Arc::new((Mutex::new(BlockFirstPutState::default()), Condvar::new())),
        }
    }

    fn wait_for_first_put(&self) {
        let (lock, ready) = &*self.state;
        let mut state = lock.lock().unwrap();
        while !state.started {
            state = ready.wait(state).unwrap();
        }
    }

    fn release_first_put(&self) {
        let (lock, ready) = &*self.state;
        let mut state = lock.lock().unwrap();
        state.released = true;
        ready.notify_all();
    }

    fn put_count(&self) -> usize {
        let (lock, _) = &*self.state;
        lock.lock().unwrap().puts
    }
}

impl ObjectStore for BlockFirstPutStore {
    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        let (lock, ready) = &*self.state;
        let mut state = lock.lock().unwrap();
        state.puts += 1;
        if state.puts == 1 {
            state.started = true;
            ready.notify_all();
            while !state.released {
                state = ready.wait(state).map_err(ObjectError::from_poisoned_lock)?;
            }
        }
        drop(state);
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

#[derive(Clone)]
struct BlockFirstGetStore {
    inner: MemoryObjectStore,
    state: Arc<(Mutex<BlockFirstGetState>, Condvar)>,
}

#[derive(Debug, Default)]
struct BlockFirstGetState {
    released: bool,
    gets: usize,
}

impl BlockFirstGetStore {
    fn new() -> Self {
        Self {
            inner: MemoryObjectStore::new(),
            state: Arc::new((Mutex::new(BlockFirstGetState::default()), Condvar::new())),
        }
    }

    fn wait_for_get_count(&self, expected: usize) {
        let (lock, ready) = &*self.state;
        let mut state = lock.lock().unwrap();
        while state.gets < expected {
            state = ready.wait(state).unwrap();
        }
    }

    fn release_gets(&self) {
        let (lock, ready) = &*self.state;
        let mut state = lock.lock().unwrap();
        state.released = true;
        ready.notify_all();
    }

    fn get_count(&self) -> usize {
        let (lock, _) = &*self.state;
        lock.lock().unwrap().gets
    }
}

impl ObjectStore for BlockFirstGetStore {
    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        self.inner.put(key, bytes)
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        let (lock, ready) = &*self.state;
        let mut state = lock.lock().map_err(ObjectError::from_poisoned_lock)?;
        state.gets = state.gets.saturating_add(1);
        ready.notify_all();
        while !state.released {
            state = ready.wait(state).map_err(ObjectError::from_poisoned_lock)?;
        }
        drop(state);
        self.inner.get(key, range)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        self.inner.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        self.inner.delete(key)
    }
}

#[derive(Clone, Default)]
struct CountingBlockCache {
    inner: MemoryBlockCache,
    gets: Arc<Mutex<usize>>,
}

impl BlockCache for CountingBlockCache {
    fn get_block(&self, key: &str) -> Result<Option<Vec<u8>>, ObjectError> {
        let mut gets = self.gets.lock().map_err(ObjectError::from_poisoned_lock)?;
        *gets = gets.saturating_add(1);
        drop(gets);
        self.inner.get_block(key)
    }

    fn put_block(&self, key: String, bytes: Vec<u8>) -> Result<(), ObjectError> {
        self.inner.put_block(key, bytes)
    }

    fn stats(&self) -> Result<BlockCacheStats, ObjectError> {
        self.inner.stats()
    }
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..100 {
        if predicate() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("condition was not satisfied before timeout");
}

fn submit_writeback_range<O>(
    uploader: &ObjectWritebackUploader<O>,
    cache: &WritebackCache,
    offset: u64,
    bytes: &[u8],
) -> PendingChunkedWrite
where
    O: ObjectStore + Clone + Send + Sync + 'static,
{
    let ticket = cache
        .stage(format!("blocks/1/2/3/0/{offset}"), bytes)
        .unwrap();
    uploader
        .submit(ObjectWritebackRequest {
            ranges: vec![WritebackUploadRange::cache(offset, ticket)],
            options: ChunkWriteOptions {
                manifest_id: "artifacts/checkpoint".to_owned(),
                mount: 1,
                inode: 2,
                generation: 3,
                chunk_size: 64,
                block_size: 16,
            },
            block_index_base: offset / 16,
        })
        .unwrap()
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
fn configured_s3_store_has_no_tiered_stats() {
    let store = ObjectStoreConfig::s3(S3ObjectStoreOptions {
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
    .open()
    .unwrap();

    assert_eq!(store.tiered_stats().unwrap(), None);
}

#[test]
fn object_store_config_opens_tiered_local_backend() {
    let hot = tempfile::tempdir().unwrap();
    let cold = S3ObjectStoreOptions {
        bucket: "nokv".to_owned(),
        root: "/".to_owned(),
        region: "auto".to_owned(),
        endpoint: Some("http://127.0.0.1:9000".to_owned()),
        access_key_id: Some("access".to_owned()),
        secret_access_key: Some("secret".to_owned()),
        session_token: None,
        virtual_host_style: false,
        skip_signature: true,
    };
    let config = ObjectStoreConfig::tiered_local(hot.path(), cold);
    assert_eq!(config.local_hot_root(), Some(hot.path()));
    assert_eq!(
        config.tiered_options(),
        Some(TieredObjectStoreOptions::default())
    );

    let store = config.open().unwrap();
    assert_eq!(
        store.tiered_stats().unwrap(),
        Some(TieredObjectStoreStats::default())
    );
    let ConfiguredObjectStore::TieredLocal(store) = store else {
        panic!("expected tiered local object store");
    };
    let capabilities = store.capabilities();
    assert!(capabilities.range_get);
    assert!(capabilities.multipart_put);
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

    store.put(&key, b"abcdef".to_vec()).unwrap();
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
