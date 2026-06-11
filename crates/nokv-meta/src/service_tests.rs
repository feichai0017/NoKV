use super::*;
use crate::command::{ReadItem, ScanItem};
use crate::holtstore::HoltMetadataStore;
use nokv_object::{MemoryObjectStore, ObjectBytes};
use nokv_types::{AdvisoryLockKind, AdvisoryLockRequest};
use std::sync::Arc;

#[derive(Clone)]
struct PurposeTrackingStore {
    inner: HoltMetadataStore,
    counts: Arc<PurposeCounts>,
}

#[derive(Default)]
struct PurposeCounts {
    user_strong_gets: AtomicU64,
    write_plan_gets: AtomicU64,
    snapshot_gets: AtomicU64,
    user_strong_scans: AtomicU64,
    write_plan_scans: AtomicU64,
    snapshot_scans: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PurposeCountSnapshot {
    user_strong_gets: u64,
    write_plan_gets: u64,
    snapshot_gets: u64,
    user_strong_scans: u64,
    write_plan_scans: u64,
    snapshot_scans: u64,
}

impl PurposeTrackingStore {
    fn new() -> Self {
        Self {
            inner: HoltMetadataStore::open_memory().unwrap(),
            counts: Arc::new(PurposeCounts::default()),
        }
    }

    fn counts(&self) -> PurposeCountSnapshot {
        PurposeCountSnapshot {
            user_strong_gets: self.counts.user_strong_gets.load(Ordering::Relaxed),
            write_plan_gets: self.counts.write_plan_gets.load(Ordering::Relaxed),
            snapshot_gets: self.counts.snapshot_gets.load(Ordering::Relaxed),
            user_strong_scans: self.counts.user_strong_scans.load(Ordering::Relaxed),
            write_plan_scans: self.counts.write_plan_scans.load(Ordering::Relaxed),
            snapshot_scans: self.counts.snapshot_scans.load(Ordering::Relaxed),
        }
    }

    fn record_get(&self, purpose: ReadPurpose) {
        match purpose {
            ReadPurpose::UserStrong => &self.counts.user_strong_gets,
            ReadPurpose::WritePlanLocal => &self.counts.write_plan_gets,
            ReadPurpose::Snapshot => &self.counts.snapshot_gets,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn record_scan(&self, purpose: ReadPurpose) {
        match purpose {
            ReadPurpose::UserStrong => &self.counts.user_strong_scans,
            ReadPurpose::WritePlanLocal => &self.counts.write_plan_scans,
            ReadPurpose::Snapshot => &self.counts.snapshot_scans,
        }
        .fetch_add(1, Ordering::Relaxed);
    }
}

impl MetadataStore for PurposeTrackingStore {
    fn get_versioned(
        &self,
        family: RecordFamily,
        key: &[u8],
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<ReadItem>, MetadataError> {
        self.record_get(purpose);
        self.inner.get_versioned(family, key, version, purpose)
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<ScanItem>, MetadataError> {
        self.record_scan(request.purpose);
        self.inner.scan(request)
    }

    fn scan_delimited(
        &self,
        request: crate::command::DelimitedScanRequest,
    ) -> Result<Vec<crate::command::DelimitedScanItem>, MetadataError> {
        self.record_scan(request.purpose);
        self.inner.scan_delimited(request)
    }

    fn commit_metadata(&self, command: MetadataCommand) -> Result<CommitResult, MetadataError> {
        self.inner.commit_metadata(command)
    }

    fn commit_independent_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadataError>> {
        self.inner.commit_independent_batch(commands)
    }

    fn committed_request_result(
        &self,
        request_id: &[u8],
    ) -> Result<Option<CommitResult>, MetadataError> {
        self.inner.committed_request_result(request_id)
    }

    fn prune_history(
        &self,
        request: HistoryPruneRequest,
    ) -> Result<HistoryPruneOutcome, MetadataError> {
        self.inner.prune_history(request)
    }
}

impl MetadataStoreStatsProvider for PurposeTrackingStore {
    fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.inner.metadata_store_stats()
    }
}

fn service() -> NoKvFs<HoltMetadataStore, MemoryObjectStore> {
    service_with_objects().0
}

fn service_with_objects() -> (
    NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    MemoryObjectStore,
) {
    let objects = MemoryObjectStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    (service, objects)
}

fn artifact_request(name: DentryName, manifest_id: &str, bytes: &[u8]) -> PublishArtifact {
    PublishArtifact {
        parent: InodeId::root(),
        name,
        producer: "unit-test".to_owned(),
        digest_uri: "sha256:test".to_owned(),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: manifest_id.to_owned(),
        bytes: bytes.to_vec(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    }
}

fn publish_path_artifact<O: ObjectStore>(
    service: &NoKvFs<HoltMetadataStore, O>,
    path: &str,
    manifest_id: &str,
    bytes: &[u8],
) -> DentryWithAttr {
    let prepared = service.prepare_artifact_create_path(path).unwrap();
    service
        .publish_prepared_artifact_session(
            prepared.clone(),
            PublishArtifactSession {
                parent: prepared.parent,
                name: prepared.name,
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: manifest_id.to_owned(),
                size: bytes.len() as u64,
                ranges: vec![PublishArtifactRange {
                    offset: 0,
                    bytes: bytes.to_vec(),
                }],
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap()
        .entry
}

/// Supersede an existing artifact in `parent` (replace -> a fresh generation).
fn republish_path_artifact<O: ObjectStore>(
    service: &NoKvFs<HoltMetadataStore, O>,
    parent: InodeId,
    name: &str,
    manifest_id: &str,
    bytes: &[u8],
) -> DentryWithAttr {
    let prepared = service
        .prepare_artifact_replace(parent, DentryName::new(name.as_bytes().to_vec()).unwrap())
        .unwrap();
    service
        .publish_prepared_artifact_session(
            prepared.clone(),
            PublishArtifactSession {
                parent: prepared.parent,
                name: prepared.name,
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: manifest_id.to_owned(),
                size: bytes.len() as u64,
                ranges: vec![PublishArtifactRange {
                    offset: 0,
                    bytes: bytes.to_vec(),
                }],
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap()
        .entry
}

#[test]
fn publish_multichunk_artifact_succeeds() {
    let service = service();
    // 128 MiB spans two 64 MiB chunks: the multi-chunk publish path the FUSE
    // bigfile workload hits (and currently EIOs on via InvalidPreparedArtifact).
    let size = 128 * 1024 * 1024_usize;
    let bytes = vec![0u8; size];
    let prepared = service.prepare_artifact_create_path("/big.bin").unwrap();
    let result = service.publish_prepared_artifact_session(
        prepared.clone(),
        PublishArtifactSession {
            parent: prepared.parent,
            name: prepared.name,
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "fuse/big".to_owned(),
            size: size as u64,
            ranges: vec![PublishArtifactRange { offset: 0, bytes }],
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    );
    assert!(
        result.is_ok(),
        "multi-chunk publish failed: {:?}",
        result.err()
    );
}

fn block_key(inode: InodeId, generation: u64, chunk: u64, block: u64) -> ObjectKey {
    ObjectKey::new(format!(
        "blocks/1/{}/{}/{}/{}",
        inode.get(),
        generation,
        chunk,
        block
    ))
    .unwrap()
}

fn body_descriptor(generation: u64, size: u64) -> BodyDescriptor {
    BodyDescriptor {
        producer: "unit-test".to_owned(),
        digest_uri: "sha256:test".to_owned(),
        size,
        content_type: "application/octet-stream".to_owned(),
        manifest_id: format!("manifest-{generation}"),
        generation,
        chunk_size: DEFAULT_CHUNK_SIZE,
        block_size: DEFAULT_BLOCK_SIZE as u64,
    }
}

fn one_chunk_manifest(inode: InodeId, generation: u64, len: u64) -> ChunkManifest {
    ChunkManifest {
        chunk_index: 0,
        logical_offset: 0,
        len,
        slices: vec![SliceManifest {
            slice_id: 1,
            logical_offset: 0,
            len,
            blocks: vec![BlockDescriptor {
                object_key: block_key(inode, generation, 0, 0).as_str().to_owned(),
                logical_offset: 0,
                object_offset: 0,
                len,
                digest_uri: "sha256:block".to_owned(),
            }],
        }],
    }
}

#[test]
fn create_dir_then_lookup_and_readdir_use_dentry_projection() {
    let service = service();
    let name = DentryName::new(b"runs".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), name.clone(), 0o755, 1000, 1000)
        .unwrap();

    let lookup = service
        .lookup_plus(InodeId::root(), &name)
        .unwrap()
        .unwrap();
    assert_eq!(lookup, created);

    let entries = service.read_dir_plus(InodeId::root()).unwrap();
    assert_eq!(entries, vec![created]);
    let stats = service.metadata_service_stats();
    assert_eq!(stats.read_dir_plus_total, 1);
    assert_eq!(stats.read_dir_plus_entry_total, 1);
    assert_eq!(stats.read_dir_plus_projection_hit_total, 1);
}

#[test]
fn write_planning_reads_are_marked_local_while_user_reads_stay_strong() {
    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let file_name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    service
        .create_file(InodeId::root(), file_name.clone(), 0o644, 1000, 1000)
        .unwrap();
    let dir_name = DentryName::new(b"runs".to_vec()).unwrap();
    let dir = service
        .create_dir(InodeId::root(), dir_name, 0o755, 1000, 1000)
        .unwrap();

    let before_lookup = metadata.counts();
    assert!(service
        .lookup_plus(InodeId::root(), &file_name)
        .unwrap()
        .is_some());
    let after_lookup = metadata.counts();
    assert!(after_lookup.user_strong_gets > before_lookup.user_strong_gets);
    assert_eq!(after_lookup.write_plan_gets, before_lookup.write_plan_gets);

    service
        .remove_file(InodeId::root(), &file_name)
        .expect("remove file");
    let after_remove = metadata.counts();
    assert_eq!(after_remove.user_strong_gets, after_lookup.user_strong_gets);
    assert!(after_remove.write_plan_gets > after_lookup.write_plan_gets);

    let snapshot = service
        .snapshot_subtree(dir.attr.inode)
        .expect("snapshot subtree");
    let after_snapshot = metadata.counts();
    assert_eq!(
        after_snapshot.user_strong_gets,
        after_remove.user_strong_gets
    );
    assert!(after_snapshot.write_plan_gets > after_remove.write_plan_gets);

    assert!(service
        .get_attr_at_snapshot(snapshot.snapshot_id, dir.attr.inode)
        .unwrap()
        .is_some());
    assert!(service
        .read_dir_plus_at_snapshot(snapshot.snapshot_id, dir.attr.inode)
        .unwrap()
        .is_empty());
    let after_snapshot_reads = metadata.counts();
    assert_eq!(
        after_snapshot_reads.user_strong_gets,
        after_snapshot.user_strong_gets
    );
    assert!(after_snapshot_reads.snapshot_gets > after_snapshot.snapshot_gets);
    assert!(after_snapshot_reads.snapshot_scans > after_snapshot.snapshot_scans);
}

#[test]
fn xattr_round_trips_lists_replaces_and_removes() {
    let service = service();
    let entry = service
        .create_file(
            InodeId::root(),
            DentryName::new(b"note.txt".to_vec()).unwrap(),
            0o644,
            1000,
            1000,
        )
        .unwrap();

    service
        .set_xattr(
            entry.attr.inode,
            b"user.comment",
            b"first".to_vec(),
            XattrSetMode::Create,
        )
        .unwrap();
    assert_eq!(
        service
            .get_xattr(entry.attr.inode, b"user.comment")
            .unwrap(),
        Some(b"first".to_vec())
    );
    assert_eq!(
        service.list_xattr(entry.attr.inode).unwrap(),
        vec![b"user.comment".to_vec()]
    );
    assert!(matches!(
        service.set_xattr(
            entry.attr.inode,
            b"user.comment",
            b"duplicate".to_vec(),
            XattrSetMode::Create,
        ),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));

    service
        .set_xattr(
            entry.attr.inode,
            b"user.comment",
            b"second".to_vec(),
            XattrSetMode::Replace,
        )
        .unwrap();
    assert_eq!(
        service
            .get_xattr(entry.attr.inode, b"user.comment")
            .unwrap(),
        Some(b"second".to_vec())
    );
    assert!(matches!(
        service.set_xattr(
            entry.attr.inode,
            b"user.missing",
            b"value".to_vec(),
            XattrSetMode::Replace,
        ),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));

    service
        .remove_xattr(entry.attr.inode, b"user.comment")
        .unwrap();
    assert_eq!(
        service
            .get_xattr(entry.attr.inode, b"user.comment")
            .unwrap(),
        None
    );
    assert!(service.list_xattr(entry.attr.inode).unwrap().is_empty());
    assert!(matches!(
        service.remove_xattr(entry.attr.inode, b"user.comment"),
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    ));
}

#[test]
fn path_methods_resolve_current_namespace_on_server_side() {
    let service = service();
    let runs = service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let artifact = service
        .create_file_path("/runs/checkpoint.bin", 0o644, 1000, 1000)
        .unwrap();

    assert_eq!(service.lookup_path("/runs").unwrap(), Some(runs.clone()));
    assert_eq!(
        service.lookup_path("/runs/checkpoint.bin").unwrap(),
        Some(artifact.clone())
    );
    assert_eq!(service.read_dir_plus_path("/runs").unwrap(), vec![artifact]);
    assert!(matches!(
        service.create_file_path("relative", 0o644, 1000, 1000),
        Err(MetadError::InvalidPath(_))
    ));
}

#[test]
fn plain_path_create_uses_canonical_namespace_without_path_index() {
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects);
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let artifact = service
        .create_file_path("/runs/checkpoint.bin", 0o644, 1000, 1000)
        .unwrap();
    let components = parse_absolute_path("/runs/checkpoint.bin").unwrap();
    let key = path_index_key(MountId::new(1).unwrap(), &components);
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());

    let before = service.metadata_service_stats();
    assert_eq!(
        service.lookup_path("/runs/checkpoint.bin").unwrap(),
        Some(artifact)
    );
    let after = service.metadata_service_stats();
    assert_eq!(
        after.path_index_lookup_total - before.path_index_lookup_total,
        0
    );
    assert_eq!(
        after.path_index_fallback_total - before.path_index_fallback_total,
        0
    );
}

#[test]
fn prepared_artifact_path_publish_writes_and_uses_validated_path_index() {
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects);
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let prepared = service
        .prepare_artifact_create_path("/runs/checkpoint.bin")
        .unwrap();
    let body = body_descriptor(prepared.generation, 6);
    let artifact = service
        .publish_prepared_artifact(
            prepared.clone(),
            body,
            vec![one_chunk_manifest(prepared.inode, prepared.generation, 6)],
            0o644,
            1000,
            1000,
        )
        .unwrap()
        .entry;
    let components = parse_absolute_path("/runs/checkpoint.bin").unwrap();
    let key = path_index_key(MountId::new(1).unwrap(), &components);
    let indexed = metadata
        .get(
            RecordFamily::PathIndex,
            &key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .expect("artifact path index entry");
    let projection = decode_dentry_projection(&indexed.0).unwrap();
    assert_eq!(DentryWithAttr::from(projection), artifact);

    let before = service.metadata_service_stats();
    let metadata = service
        .stat_path("/runs/checkpoint.bin")
        .unwrap()
        .expect("artifact stat");
    assert_eq!(metadata.attr, artifact.attr);
    assert_eq!(metadata.body, artifact.body);
    let after = service.metadata_service_stats();
    assert_eq!(
        after.path_index_lookup_total - before.path_index_lookup_total,
        1
    );
    assert_eq!(after.path_index_hit_total - before.path_index_hit_total, 1);
    assert_eq!(
        after.path_index_fallback_total - before.path_index_fallback_total,
        0
    );

    let before = service.metadata_service_stats();
    assert_eq!(service.stat_path("/runs/missing.bin").unwrap(), None);
    let after = service.metadata_service_stats();
    assert_eq!(
        after.path_index_lookup_total - before.path_index_lookup_total,
        1
    );
    assert_eq!(
        after.path_index_miss_total - before.path_index_miss_total,
        1
    );
    assert_eq!(
        after.path_index_fallback_total - before.path_index_fallback_total,
        1
    );
}

#[test]
fn artifact_path_rename_moves_live_path_index() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let archive = service
        .create_dir_path("/archive", 0o755, 1000, 1000)
        .unwrap();
    let artifact = publish_path_artifact(&service, "/runs/a.bin", "runs/a.bin", b"a");
    let old_components = parse_absolute_path("/runs/a.bin").unwrap();
    let new_components = parse_absolute_path("/archive/a.bin").unwrap();
    let old_key = path_index_key(MountId::new(1).unwrap(), &old_components);
    let new_key = path_index_key(MountId::new(1).unwrap(), &new_components);
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &old_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_some());

    let renamed = service
        .rename_path("/runs/a.bin", "/archive/a.bin")
        .unwrap();
    let old_index = metadata
        .get(
            RecordFamily::PathIndex,
            &old_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap();
    let new_index = metadata
        .get(
            RecordFamily::PathIndex,
            &new_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .expect("renamed artifact path index");

    assert!(old_index.is_none());
    assert_eq!(renamed.attr.inode, artifact.attr.inode);
    let indexed = decode_dentry_projection(&new_index.0).unwrap();
    assert_eq!(indexed.dentry.parent, archive.attr.inode);
    assert_eq!(indexed.dentry.name.as_bytes(), b"a.bin");
    assert_eq!(indexed.attr.inode, artifact.attr.inode);
}

#[test]
fn plain_directory_path_rename_does_not_create_path_index() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let runs = service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let source_components = parse_absolute_path("/runs").unwrap();
    let destination_components = parse_absolute_path("/archive").unwrap();
    let source_key = path_index_key(MountId::new(1).unwrap(), &source_components);
    let destination_key = path_index_key(MountId::new(1).unwrap(), &destination_components);
    let before = metadata.metadata_store_stats();

    let renamed = service.rename_path("/runs", "/archive").unwrap();
    let after = metadata.metadata_store_stats();

    assert_eq!(renamed.attr.inode, runs.attr.inode);
    assert_eq!(after.current_put_total - before.current_put_total, 1);
    assert_eq!(after.current_delete_total - before.current_delete_total, 1);
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &source_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &destination_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
}

#[test]
fn artifact_path_remove_deletes_live_path_index() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.bin", "runs/a.bin", b"a");
    let components = parse_absolute_path("/runs/a.bin").unwrap();
    let key = path_index_key(MountId::new(1).unwrap(), &components);
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_some());

    service.remove_file_path("/runs/a.bin").unwrap();

    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
}

#[test]
fn path_resolution_cache_reuses_parent_directory_for_indexed_stats() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.bin", "runs/a.bin", b"a");
    publish_path_artifact(&service, "/runs/b.bin", "runs/b.bin", b"b");
    service.clear_read_path_caches_for_test();

    let before_store = metadata.metadata_store_stats();
    let before_service = service.metadata_service_stats();
    assert!(service.stat_path("/runs/a.bin").unwrap().is_some());
    let after_first_store = metadata.metadata_store_stats();
    assert!(service.stat_path("/runs/b.bin").unwrap().is_some());
    let after_second_store = metadata.metadata_store_stats();
    let after_service = service.metadata_service_stats();

    let first_gets = after_first_store.get_total - before_store.get_total;
    let second_gets = after_second_store.get_total - after_first_store.get_total;
    assert_eq!(first_gets, 3);
    assert_eq!(second_gets, 2);
    assert_eq!(
        after_service.path_index_hit_total - before_service.path_index_hit_total,
        2
    );
    assert_eq!(
        after_service.path_index_fallback_total - before_service.path_index_fallback_total,
        0
    );
}

#[test]
fn validated_path_index_cache_reuses_stat_validation_for_indexed_list() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let first = publish_path_artifact(&service, "/runs/a.bin", "runs/a.bin", b"a");
    let second = publish_path_artifact(&service, "/runs/b.bin", "runs/b.bin", b"b");
    service.clear_read_path_caches_for_test();

    assert!(service.stat_path("/runs/a.bin").unwrap().is_some());
    assert!(service.stat_path("/runs/b.bin").unwrap().is_some());

    let before_store = metadata.metadata_store_stats();
    let page = service.list_indexed_path_page("/runs", None, 10).unwrap();
    let after_store = metadata.metadata_store_stats();

    assert_eq!(page.entries, vec![first, second]);
    assert_eq!(page.next_cursor, None);
    assert_eq!(after_store.get_total - before_store.get_total, 0);
}

#[test]
fn validated_path_index_lookup_cache_reuses_repeated_stat_result() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let artifact = publish_path_artifact(&service, "/runs/a.bin", "runs/a.bin", b"a");
    service.clear_read_path_caches_for_test();

    let first = service
        .stat_path("/runs/a.bin")
        .unwrap()
        .expect("first stat");
    assert_eq!(first.attr, artifact.attr);

    let before_store = metadata.metadata_store_stats();
    let second = service
        .stat_path("/runs/a.bin")
        .unwrap()
        .expect("second stat");
    let after_store = metadata.metadata_store_stats();

    assert_eq!(second.attr, artifact.attr);
    assert_eq!(after_store.get_total - before_store.get_total, 0);
}

#[test]
fn namespace_find_body_facets_do_not_require_body_projection() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.json", "runs/a.json", br#"{"loss":0.4}"#);
    publish_path_artifact(&service, "/runs/b.log", "runs/b.log", b"loss=0.3\n");

    let result = service
        .find_paths(NamespaceFindRequest {
            path: "/runs".to_owned(),
            predicates: Vec::new(),
            sort: Vec::new(),
            include: Vec::new(),
            facets: vec![NamespaceFindField::body_content_type()],
            cursor: None,
            limit: 10,
        })
        .unwrap();

    assert_eq!(result.match_count, 2);
    assert!(result.matches.iter().all(|card| card.body.is_none()));
    assert_eq!(result.facets.len(), 1);
    assert_eq!(
        result.facets[0].field,
        NamespaceFindField::body_content_type()
    );
    assert_eq!(result.facets[0].values[0].count, 2);
}

#[test]
fn namespace_find_tolerates_exists_predicate_payloads() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.json", "runs/a.json", br#"{"loss":0.4}"#);

    let result = service
        .find_paths(NamespaceFindRequest {
            path: "/runs".to_owned(),
            predicates: vec![NamespacePredicate {
                field: NamespaceFindField::body_content_type(),
                op: NamespacePredicateOp::Exists,
                value: Some(NamespacePredicateValue::String("ignored".to_owned())),
            }],
            sort: Vec::new(),
            include: Vec::new(),
            facets: Vec::new(),
            cursor: None,
            limit: 10,
        })
        .unwrap();

    assert_eq!(result.match_count, 1);
    assert_eq!(result.matches[0].path, "/runs/a.json");
}

#[test]
fn namespace_grep_cursor_resumes_at_next_unemitted_match() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(
        &service,
        "/runs/train.log",
        "runs/train.log",
        b"loss=1\nloss=2\n",
    );

    let first = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs/train.log".to_owned(),
            pattern: "loss".to_owned(),
            recursive: false,
            cursor: None,
            limit: 1,
            max_files: None,
            max_bytes: None,
        })
        .unwrap();
    assert_eq!(first.matches.len(), 1);
    assert_eq!(first.matches[0].line_number, 1);

    let second = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs/train.log".to_owned(),
            pattern: "loss".to_owned(),
            recursive: false,
            cursor: first.next_cursor,
            limit: 1,
            max_files: None,
            max_bytes: None,
        })
        .unwrap();
    assert_eq!(second.matches.len(), 1);
    assert_eq!(second.matches[0].line_number, 2);
}

#[test]
fn namespace_read_bytes_honors_returned_cursor() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.bin", "runs/a.bin", b"abcdef");

    let first = service
        .read_page(
            "/runs/a.bin",
            NamespaceReadOptions {
                format: NamespaceReadFormat::Bytes,
                cursor: None,
                offset: 0,
                limit: 2,
                expected_generation: None,
            },
        )
        .unwrap();
    assert_eq!(first.bytes.as_deref(), Some(b"ab".as_slice()));

    let second = service
        .read_page(
            "/runs/a.bin",
            NamespaceReadOptions {
                format: NamespaceReadFormat::Bytes,
                cursor: first.next_cursor,
                offset: 0,
                limit: 2,
                expected_generation: None,
            },
        )
        .unwrap();
    assert_eq!(second.bytes.as_deref(), Some(b"cd".as_slice()));
}

#[test]
fn register_namespace_index_rejects_rows_outside_registered_path() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();

    let err = service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/runs".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("run.status"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: false,
                facetable: true,
            }],
            rows: vec![NamespaceIndexRow {
                path: "/archive/a.json".to_owned(),
                values: Vec::new(),
            }],
        })
        .unwrap_err();

    assert!(
        matches!(err, MetadError::InvalidQuery(message) if message.contains("outside registered namespace"))
    );
}

#[test]
fn register_namespace_index_uses_metadata_predicate_fence() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();

    let before = metadata.metadata_store_stats();
    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/runs".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("run.status"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: false,
                facetable: true,
            }],
            rows: vec![NamespaceIndexRow {
                path: "/runs/a.json".to_owned(),
                values: vec![NamespaceIndexValue {
                    field: NamespaceFindField::new("run.status"),
                    value: NamespacePredicateValue::String("completed".to_owned()),
                }],
            }],
        })
        .unwrap();
    let after = metadata.metadata_store_stats();

    assert_eq!(after.predicate_total - before.predicate_total, 1);
}

#[test]
fn stale_path_index_falls_back_to_canonical_namespace() {
    let service = service();
    let runs = service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let archive = service
        .create_dir_path("/archive", 0o755, 1000, 1000)
        .unwrap();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create_path("/runs/checkpoint.bin")
        .unwrap();
    let artifact = service
        .publish_prepared_artifact(
            prepared.clone(),
            body_descriptor(prepared.generation, 6),
            vec![one_chunk_manifest(prepared.inode, prepared.generation, 6)],
            0o644,
            1000,
            1000,
        )
        .unwrap()
        .entry;

    assert!(service.stat_path("/runs/checkpoint.bin").unwrap().is_some());

    service
        .rename(runs.attr.inode, &name, archive.attr.inode, name.clone())
        .unwrap();

    let before = service.metadata_service_stats();
    assert_eq!(service.stat_path("/runs/checkpoint.bin").unwrap(), None);
    let after = service.metadata_service_stats();
    assert_eq!(
        after.path_index_lookup_total - before.path_index_lookup_total,
        1
    );
    assert_eq!(
        after.path_index_stale_total - before.path_index_stale_total,
        1
    );
    assert_eq!(
        after.path_index_fallback_total - before.path_index_fallback_total,
        1
    );

    let mut moved_artifact = artifact;
    moved_artifact.dentry.parent = archive.attr.inode;

    let before = service.metadata_service_stats();
    let metadata = service
        .stat_path("/archive/checkpoint.bin")
        .unwrap()
        .expect("moved artifact stat");
    assert_eq!(metadata.attr, moved_artifact.attr);
    assert_eq!(metadata.body, moved_artifact.body);
    let after = service.metadata_service_stats();
    assert_eq!(
        after.path_index_miss_total - before.path_index_miss_total,
        1
    );
    assert_eq!(
        after.path_index_fallback_total - before.path_index_fallback_total,
        1
    );
}

#[test]
fn path_index_page_lists_immediate_indexed_children_with_holt_delimiter() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let epoch = service
        .create_dir_path("/runs/epoch", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_file_path("/runs/plain.txt", 0o644, 1000, 1000)
        .unwrap();
    publish_path_artifact(&service, "/runs/top.bin", "runs/top.bin", b"top");
    publish_path_artifact(
        &service,
        "/runs/epoch/ckpt.bin",
        "runs/epoch/ckpt.bin",
        b"ckpt",
    );

    let before = metadata.metadata_store_stats();
    let first = service.list_indexed_path_page("/runs", None, 1).unwrap();
    let after_first = metadata.metadata_store_stats();
    assert_eq!(first.entries, vec![epoch]);
    assert_eq!(
        first.next_cursor.as_ref().map(DentryName::as_bytes),
        Some(b"epoch".as_slice())
    );
    assert_eq!(
        after_first.scan_key_returned_total - before.scan_key_returned_total,
        2
    );

    let second = service
        .list_indexed_path_page("/runs", first.next_cursor.as_ref(), 10)
        .unwrap();
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].dentry.name.as_bytes(), b"top.bin");
    assert_eq!(second.entries[0].attr.file_type, FileType::File);
    assert_eq!(second.next_cursor, None);
}

#[test]
fn path_index_page_skips_stale_rows_without_truncating_visible_children() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/archive", 0o755, 1000, 1000)
        .unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/runs/aaa", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(
        &service,
        "/runs/aaa/stale.bin",
        "runs/aaa/stale.bin",
        b"stale",
    );
    service.rename_path("/runs/aaa", "/archive/aaa").unwrap();
    let first_valid = publish_path_artifact(&service, "/runs/bbb.bin", "runs/bbb.bin", b"bbb");
    let second_valid = publish_path_artifact(&service, "/runs/ccc.bin", "runs/ccc.bin", b"ccc");

    let before_store = metadata.metadata_store_stats();
    let before_service = service.metadata_service_stats();
    let first = service.list_indexed_path_page("/runs", None, 1).unwrap();
    let after_first_store = metadata.metadata_store_stats();
    let after_first_service = service.metadata_service_stats();
    assert_eq!(first.entries, vec![first_valid]);
    assert_eq!(
        first.next_cursor.as_ref().map(DentryName::as_bytes),
        Some(b"bbb.bin".as_slice())
    );
    assert!(
        after_first_store.scan_key_returned_total - before_store.scan_key_returned_total > 2,
        "stale index row should force an extra delimiter scan page"
    );
    assert_eq!(
        after_first_service.read_dir_plus_entry_total - before_service.read_dir_plus_entry_total,
        1
    );
    assert_eq!(
        after_first_service.read_dir_plus_projection_hit_total
            - before_service.read_dir_plus_projection_hit_total,
        1
    );
    assert!(
        after_first_service.path_index_scan_stale_total
            - before_service.path_index_scan_stale_total
            >= 1,
        "stale derived path-index rows should be reported separately from live entries"
    );

    let second = service
        .list_indexed_path_page("/runs", first.next_cursor.as_ref(), 1)
        .unwrap();
    assert_eq!(second.entries, vec![second_valid]);
    assert_eq!(second.next_cursor, None);
}

#[test]
fn directory_rename_leaves_descendant_path_index_as_derived_stale_cache() {
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects);
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let prepared = service
        .prepare_artifact_create_path("/runs/checkpoint.bin")
        .unwrap();
    let artifact = service
        .publish_prepared_artifact(
            prepared.clone(),
            body_descriptor(prepared.generation, 6),
            vec![one_chunk_manifest(prepared.inode, prepared.generation, 6)],
            0o644,
            1000,
            1000,
        )
        .unwrap()
        .entry;
    let old_components = parse_absolute_path("/runs/checkpoint.bin").unwrap();
    let old_key = path_index_key(MountId::new(1).unwrap(), &old_components);
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &old_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_some());

    service.rename_path("/runs", "/archive").unwrap();

    let renamed_dir_key = path_index_key(
        MountId::new(1).unwrap(),
        &parse_absolute_path("/runs").unwrap(),
    );
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &renamed_dir_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_none());
    assert!(metadata
        .get(
            RecordFamily::PathIndex,
            &old_key,
            Version::new(u64::MAX).unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap()
        .is_some());
    assert!(matches!(
        service.lookup_path("/runs/checkpoint.bin"),
        Err(MetadError::NotFound)
    ));
    assert_eq!(
        service.lookup_path("/archive/checkpoint.bin").unwrap(),
        Some(artifact)
    );
}

#[test]
fn create_file_publishes_metadata_without_body_descriptor() {
    let service = service();
    let name = DentryName::new(b"empty.txt".to_vec()).unwrap();
    let created = service
        .create_file(InodeId::root(), name.clone(), 0o644, 1000, 1000)
        .unwrap();
    assert_eq!(created.attr.file_type, FileType::File);
    assert_eq!(created.attr.size, 0);
    assert!(created.body.is_none());
    assert_eq!(
        service.lookup_plus(InodeId::root(), &name).unwrap(),
        Some(created)
    );
}

#[test]
fn new_file_attrs_use_wall_clock_timestamps() {
    let service = service();
    let before = current_time_ms().saturating_sub(1_000);

    let created = service
        .create_file(
            InodeId::root(),
            DentryName::new(b"empty.txt".to_vec()).unwrap(),
            0o644,
            1000,
            1000,
        )
        .unwrap();
    assert!(created.attr.mtime_ms >= before);
    assert!(created.attr.ctime_ms >= before);
    assert!(created.attr.mtime_ms > created.attr.generation);

    let published = service
        .publish_artifact(artifact_request(
            DentryName::new(b"artifact.bin".to_vec()).unwrap(),
            "artifact",
            b"body",
        ))
        .unwrap();
    assert!(published.attr.mtime_ms >= before);
    assert!(published.attr.ctime_ms >= before);
    assert!(published.attr.mtime_ms > published.attr.generation);
}

#[test]
fn create_symlink_round_trips_target_and_unlinks_like_file() {
    let service = service();
    let name = DentryName::new(b"latest".to_vec()).unwrap();
    let created = service
        .create_symlink(
            InodeId::root(),
            name.clone(),
            b"runs/42/checkpoint.bin".to_vec(),
            0o777,
            1000,
            1000,
        )
        .unwrap();

    assert_eq!(created.attr.file_type, FileType::Symlink);
    assert_eq!(created.attr.size, 22);
    assert_eq!(
        service.read_symlink(created.attr.inode).unwrap(),
        b"runs/42/checkpoint.bin"
    );
    assert_eq!(
        created.body.as_ref().unwrap().digest_uri,
        "sha256:15a533489b90109ab69bd64dabcc260602c854b6b4a472b20aefa0eabcee3a24"
    );
    assert_eq!(
        service.lookup_plus(InodeId::root(), &name).unwrap(),
        Some(created.clone())
    );

    let removed = service.remove_file(InodeId::root(), &name).unwrap();
    assert_eq!(removed.attr.file_type, FileType::Symlink);
    assert_eq!(service.lookup_plus(InodeId::root(), &name).unwrap(), None);
}

#[test]
fn create_special_node_persists_type_and_rdev_without_body() {
    let service = service();
    let fifo_name = DentryName::new(b"events.fifo".to_vec()).unwrap();
    let fifo = service
        .create_special_node(
            InodeId::root(),
            fifo_name.clone(),
            SpecialNodeSpec {
                file_type: FileType::NamedPipe,
                mode: 0o644,
                rdev: 0,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();
    assert_eq!(fifo.attr.file_type, FileType::NamedPipe);
    assert_eq!(fifo.attr.rdev, 0);
    assert_eq!(fifo.attr.size, 0);
    assert!(fifo.body.is_none());
    assert_eq!(
        service.lookup_plus(InodeId::root(), &fifo_name).unwrap(),
        Some(fifo.clone())
    );

    let char_name = DentryName::new(b"accelerator0".to_vec()).unwrap();
    let char_device = service
        .create_special_node(
            InodeId::root(),
            char_name.clone(),
            SpecialNodeSpec {
                file_type: FileType::CharDevice,
                mode: 0o660,
                rdev: 0x1234,
                uid: 0,
                gid: 44,
            },
        )
        .unwrap();
    assert_eq!(char_device.attr.file_type, FileType::CharDevice);
    assert_eq!(char_device.attr.rdev, 0x1234);
    assert!(char_device.body.is_none());
    assert!(service
        .read_dir_plus(InodeId::root())
        .unwrap()
        .iter()
        .any(|entry| entry.attr == char_device.attr));

    let removed = service.remove_file(InodeId::root(), &char_name).unwrap();
    assert_eq!(removed.attr.file_type, FileType::CharDevice);
    assert_eq!(
        service.lookup_plus(InodeId::root(), &char_name).unwrap(),
        None
    );
}

#[test]
fn advisory_locks_detect_conflicts_and_support_partial_unlock() {
    let service = service();
    let name = DentryName::new(b"locked.bin".to_vec()).unwrap();
    let file = service
        .create_file(InodeId::root(), name, 0o644, 1000, 1000)
        .unwrap();
    let inode = file.attr.inode;
    let read_owner = 11;
    let write_owner = 22;

    service
        .set_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: read_owner,
            start: 0,
            end: 99,
            kind: AdvisoryLockKind::Read,
            pid: 1100,
            wait: false,
        })
        .unwrap();
    service
        .set_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: 33,
            start: 20,
            end: 30,
            kind: AdvisoryLockKind::Read,
            pid: 3300,
            wait: false,
        })
        .unwrap();

    let conflict = service
        .get_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: write_owner,
            start: 50,
            end: 60,
            kind: AdvisoryLockKind::Write,
            pid: 2200,
            wait: false,
        })
        .unwrap()
        .unwrap();
    assert_eq!(conflict.owner, read_owner);
    assert_eq!(conflict.kind, AdvisoryLockKind::Read);
    assert!(matches!(
        service.set_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: write_owner,
            start: 50,
            end: 60,
            kind: AdvisoryLockKind::Write,
            pid: 2200,
            wait: false,
        }),
        Err(MetadError::LockConflict(_))
    ));

    service
        .set_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: read_owner,
            start: 40,
            end: 70,
            kind: AdvisoryLockKind::Unlock,
            pid: 1100,
            wait: false,
        })
        .unwrap();
    assert!(service
        .get_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: write_owner,
            start: 50,
            end: 60,
            kind: AdvisoryLockKind::Write,
            pid: 2200,
            wait: false,
        })
        .unwrap()
        .is_none());
    assert!(service
        .get_advisory_lock(AdvisoryLockRequest {
            inode,
            owner: write_owner,
            start: 10,
            end: 20,
            kind: AdvisoryLockKind::Write,
            pid: 2200,
            wait: false,
        })
        .unwrap()
        .is_some());
}

#[test]
fn snapshot_preserves_symlink_target() {
    let service = service();
    let name = DentryName::new(b"latest".to_vec()).unwrap();
    let created = service
        .create_symlink(
            InodeId::root(),
            name.clone(),
            b"runs/old".to_vec(),
            0o777,
            1000,
            1000,
        )
        .unwrap();
    let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();
    service.remove_file(InodeId::root(), &name).unwrap();
    service
        .create_symlink(
            InodeId::root(),
            name,
            b"runs/new".to_vec(),
            0o777,
            1000,
            1000,
        )
        .unwrap();

    assert_eq!(
        service
            .read_symlink_at_snapshot(snapshot.snapshot_id, created.attr.inode)
            .unwrap(),
        b"runs/old"
    );
}

#[test]
fn update_attrs_truncates_and_extends_sparse_file() {
    let service = service();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint-v1", b"abcdef"))
        .unwrap();

    let shrunk = service
        .update_attrs(
            InodeId::root(),
            &name,
            UpdateAttr {
                size: Some(3),
                ..UpdateAttr::default()
            },
        )
        .unwrap();
    assert_eq!(shrunk.attr.inode, published.attr.inode);
    assert_eq!(shrunk.attr.size, 3);
    assert_eq!(service.read_file(shrunk.attr.inode, 0, 8).unwrap(), b"abc");
    assert_eq!(
        shrunk.body.as_ref().unwrap().digest_uri,
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );

    let grown = service
        .update_attrs(
            InodeId::root(),
            &name,
            UpdateAttr {
                size: Some(6),
                ..UpdateAttr::default()
            },
        )
        .unwrap();
    assert_eq!(grown.attr.size, 6);
    assert_eq!(
        service.read_file(grown.attr.inode, 0, 8).unwrap(),
        b"abc\0\0\0"
    );
    assert_eq!(
        grown.body.as_ref().unwrap().digest_uri,
        "sha256:dd0b251b2bf91037a1e4fc8416a24ae00bcb9a8c252dc7e2361f2fc015f51c16"
    );
}

#[test]
fn attr_only_update_preserves_body_generation_and_readability() {
    // `cp` preserves metadata, so it chmods a file it just wrote. An attribute-
    // only `update_attrs` (no size change) must not advance `attr.generation`:
    // the body summary / chunk manifests are keyed by generation and reads
    // resolve the body via `attr.generation`, so bumping it would point the
    // dentry at a generation that has no body, surfacing as MissingBodyDescriptor
    // on the next read (the cp corruption this regression guards).
    let service = service();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint-v1", b"abcdef"))
        .unwrap();
    let body_generation = published.body.as_ref().unwrap().generation;
    assert_eq!(published.attr.generation, body_generation);

    let chmodded = service
        .update_attrs(
            InodeId::root(),
            &name,
            UpdateAttr {
                mode: Some(0o600),
                ..UpdateAttr::default()
            },
        )
        .unwrap();

    assert_eq!(chmodded.attr.mode, 0o600);
    assert_eq!(chmodded.attr.size, published.attr.size);
    // Generation is the content version; an attribute-only change keeps it.
    assert_eq!(chmodded.attr.generation, body_generation);
    assert_eq!(chmodded.body.as_ref().unwrap().generation, body_generation);
    // The body is still resolvable and intact after the metadata-only update.
    assert_eq!(
        service.read_file(chmodded.attr.inode, 0, 6).unwrap(),
        b"abcdef"
    );

    // A size change still advances the generation (new body content).
    let resized = service
        .update_attrs(
            InodeId::root(),
            &name,
            UpdateAttr {
                size: Some(3),
                ..UpdateAttr::default()
            },
        )
        .unwrap();
    assert!(resized.attr.generation > body_generation);
    assert_eq!(
        resized.attr.generation,
        resized.body.as_ref().unwrap().generation
    );
    assert_eq!(service.read_file(resized.attr.inode, 0, 8).unwrap(), b"abc");
}

#[test]
fn replace_publish_refreshes_stale_dentry_version_after_attr_update() {
    // Reproduces the cp setattr-mid-write -> release publish CAS: a write handle
    // prepares an artifact-replace (pinning the dentry version), then a `setattr`
    // (here a chmod via update_attrs) advances the dentry version out-of-band.
    // Publishing with the stale pinned version must fail the CAS (PredicateFailed
    // -> EIO), and re-reading the live version via `current_dentry_version` (what
    // publish_handle now does) before publishing must make it succeed without
    // losing the body.
    let service = service();
    let name = DentryName::new(b"y.bin".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(name.clone(), "y-v1", b"abcdef"))
        .unwrap();

    // The write handle's prepared-replace, capturing the current dentry version.
    let mut prepared = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();
    let pinned_version = prepared.dentry_version.unwrap();

    // An intervening chmod advances the dentry version, stranding `prepared`.
    service
        .update_attrs(
            InodeId::root(),
            &name,
            UpdateAttr {
                mode: Some(0o600),
                ..UpdateAttr::default()
            },
        )
        .unwrap();
    let current_version = service
        .current_dentry_version(InodeId::root(), &name)
        .unwrap()
        .unwrap();
    assert_ne!(
        current_version, pinned_version,
        "chmod must advance the dentry version"
    );

    let new_body = body_descriptor(prepared.generation, 3);
    let new_chunks = vec![one_chunk_manifest(prepared.inode, prepared.generation, 3)];

    // Publishing with the stale pinned version fails the CAS, exactly the cp EIO.
    let stale = service.publish_prepared_artifact(
        prepared.clone(),
        new_body.clone(),
        new_chunks.clone(),
        0o600,
        1000,
        1000,
    );
    assert!(
        matches!(
            stale,
            Err(MetadError::Metadata(MetadataError::PredicateFailed))
        ),
        "stale dentry version must fail the replace CAS, got {stale:?}"
    );

    // Rebinding the guard to the live version (the publish_handle refresh) lets the
    // replace CAS pass and commit the new body.
    prepared.dentry_version = Some(current_version);
    let published = service
        .publish_prepared_artifact(prepared, new_body, new_chunks, 0o600, 1000, 1000)
        .unwrap()
        .entry;
    assert_eq!(published.attr.size, 3);
    assert_eq!(published.attr.mode, 0o600);
    let committed = service
        .stat_path("/y.bin")
        .unwrap()
        .expect("artifact still resolvable after refreshed publish");
    assert_eq!(committed.attr.inode, published.attr.inode);
    assert_eq!(committed.attr.size, 3);
    assert_eq!(committed.body.as_ref().unwrap().size, 3);
}

#[test]
fn update_root_attrs_changes_root_inode_without_dentry_projection() {
    let service = service();
    let updated = service
        .update_root_attrs(UpdateAttr {
            mode: Some(0o700),
            uid: Some(42),
            gid: Some(43),
            ..UpdateAttr::default()
        })
        .unwrap();

    assert_eq!(updated.mode, 0o700);
    assert_eq!(updated.uid, 42);
    assert_eq!(updated.gid, 43);
    assert_eq!(service.get_attr(InodeId::root()).unwrap().unwrap(), updated);
}

#[test]
fn history_writes_are_snapshot_retention_driven() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();

    let before_hot = metadata.metadata_store_stats();
    service
        .update_root_attrs(UpdateAttr {
            mode: Some(0o700),
            ..UpdateAttr::default()
        })
        .unwrap();
    let after_hot = metadata.metadata_store_stats();
    assert_eq!(
        after_hot.history_write_total - before_hot.history_write_total,
        0
    );

    let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();
    assert_eq!(metadata.metadata_store_stats().active_snapshot_pin_total, 1);
    let snapshot_attr = service
        .get_attr_at_snapshot(snapshot.snapshot_id, InodeId::root())
        .unwrap()
        .unwrap();
    let before_retained = metadata.metadata_store_stats();
    service
        .update_root_attrs(UpdateAttr {
            mode: Some(0o750),
            ..UpdateAttr::default()
        })
        .unwrap();
    let after_retained = metadata.metadata_store_stats();

    assert_eq!(
        after_retained.history_write_total - before_retained.history_write_total,
        1
    );
    assert_eq!(
        service
            .get_attr_at_snapshot(snapshot.snapshot_id, InodeId::root())
            .unwrap()
            .unwrap(),
        snapshot_attr
    );
    assert_eq!(
        service.get_attr(InodeId::root()).unwrap().unwrap().mode,
        0o750
    );

    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    assert_eq!(metadata.metadata_store_stats().active_snapshot_pin_total, 0);
    let before_retired_hot = metadata.metadata_store_stats();
    service
        .update_root_attrs(UpdateAttr {
            mode: Some(0o755),
            ..UpdateAttr::default()
        })
        .unwrap();
    let after_retired_hot = metadata.metadata_store_stats();
    assert_eq!(
        after_retired_hot.history_write_total - before_retired_hot.history_write_total,
        0
    );
}

#[test]
fn create_file_hot_path_write_attribution_is_bounded() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let before = metadata.metadata_store_stats();

    service
        .create_file(
            InodeId::root(),
            DentryName::new(b"empty.txt".to_vec()).unwrap(),
            0o644,
            1000,
            1000,
        )
        .unwrap();

    let after = metadata.metadata_store_stats();
    assert_eq!(after.commit_total - before.commit_total, 1);
    assert_eq!(after.current_put_total - before.current_put_total, 2);
    assert_eq!(after.current_delete_total - before.current_delete_total, 0);
    assert_eq!(after.history_write_total - before.history_write_total, 0);
    assert_eq!(after.watch_write_total - before.watch_write_total, 0);
    assert_eq!(after.dedupe_write_total - before.dedupe_write_total, 1);
}

#[test]
fn create_files_in_dir_coalesces_into_one_metadata_command() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let before = metadata.metadata_store_stats();
    let before_service = service.metadata_service_stats();

    let entries = service
        .create_files_in_dir_path(
            "/runs",
            vec![
                DentryName::new(b"a.bin".to_vec()).unwrap(),
                DentryName::new(b"b.bin".to_vec()).unwrap(),
            ],
            0o644,
            1000,
            1000,
        )
        .unwrap();

    let after = metadata.metadata_store_stats();
    let after_service = service.metadata_service_stats();
    assert_eq!(entries.len(), 2);
    assert_eq!(after.commit_total - before.commit_total, 1);
    assert_eq!(after.current_put_total - before.current_put_total, 4);
    assert_eq!(after.current_delete_total - before.current_delete_total, 0);
    assert_eq!(after.history_write_total - before.history_write_total, 0);
    assert_eq!(after.watch_write_total - before.watch_write_total, 0);
    assert_eq!(after.dedupe_write_total - before.dedupe_write_total, 1);
    assert_eq!(
        after_service.create_files_batch_total - before_service.create_files_batch_total,
        1
    );
    assert_eq!(
        after_service.create_files_entry_total - before_service.create_files_entry_total,
        2
    );
    let listed = service.read_dir_plus_path("/runs").unwrap();
    assert_eq!(listed.len(), 2);
}

#[test]
fn create_dirs_in_dir_coalesces_into_one_metadata_command() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let before = metadata.metadata_store_stats();
    let before_service = service.metadata_service_stats();

    let entries = service
        .create_dirs_in_dir_path(
            "/runs",
            vec![
                DentryName::new(b"a".to_vec()).unwrap(),
                DentryName::new(b"b".to_vec()).unwrap(),
            ],
            0o755,
            1000,
            1000,
        )
        .unwrap();

    let after = metadata.metadata_store_stats();
    let after_service = service.metadata_service_stats();
    assert_eq!(entries.len(), 2);
    assert!(entries
        .iter()
        .all(|entry| entry.attr.file_type == FileType::Directory));
    assert_eq!(after.commit_total - before.commit_total, 1);
    assert_eq!(after.current_put_total - before.current_put_total, 4);
    assert_eq!(after.current_delete_total - before.current_delete_total, 0);
    assert_eq!(after.history_write_total - before.history_write_total, 0);
    assert_eq!(after.watch_write_total - before.watch_write_total, 0);
    assert_eq!(after.dedupe_write_total - before.dedupe_write_total, 1);
    assert_eq!(
        after_service.create_dirs_batch_total - before_service.create_dirs_batch_total,
        1
    );
    assert_eq!(
        after_service.create_dirs_entry_total - before_service.create_dirs_entry_total,
        2
    );
    let listed = service.read_dir_plus_path("/runs").unwrap();
    assert_eq!(listed.len(), 2);
}

#[test]
fn remove_files_in_dir_coalesces_into_one_holt_apply() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service
        .create_files_in_dir_path(
            "/runs",
            vec![
                DentryName::new(b"a.bin".to_vec()).unwrap(),
                DentryName::new(b"b.bin".to_vec()).unwrap(),
                DentryName::new(b"keep.bin".to_vec()).unwrap(),
            ],
            0o644,
            1000,
            1000,
        )
        .unwrap();
    let before = metadata.metadata_store_stats();

    let removed = service
        .remove_files_in_dir_path(
            "/runs",
            vec![
                DentryName::new(b"a.bin".to_vec()).unwrap(),
                DentryName::new(b"b.bin".to_vec()).unwrap(),
            ],
        )
        .unwrap();

    let after = metadata.metadata_store_stats();
    assert_eq!(removed.len(), 2);
    assert!(removed.iter().all(Result::is_ok));
    assert_eq!(after.commit_total - before.commit_total, 2);
    assert_eq!(after.current_delete_total - before.current_delete_total, 4);
    assert_eq!(after.history_write_total - before.history_write_total, 0);
    assert_eq!(after.watch_write_total - before.watch_write_total, 0);
    assert_eq!(after.dedupe_write_total - before.dedupe_write_total, 2);
    assert_eq!(after.atomic_apply_total - before.atomic_apply_total, 1);
    assert_eq!(
        after.atomic_apply_command_total - before.atomic_apply_command_total,
        2
    );
    let listed = service.read_dir_plus_path("/runs").unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].dentry.name.as_bytes(), b"keep.bin");
}

#[test]
fn remove_empty_dirs_in_dir_coalesces_into_one_holt_apply() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_batches_in_dir_path(vec![CreateInDirPathBatch {
            parent_path: "/runs".to_owned(),
            names: vec![
                DentryName::new(b"a".to_vec()).unwrap(),
                DentryName::new(b"b".to_vec()).unwrap(),
                DentryName::new(b"keep".to_vec()).unwrap(),
            ],
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        }])
        .remove(0)
        .unwrap();
    let before = metadata.metadata_store_stats();

    let removed = service
        .remove_empty_dirs_in_dir_path(
            "/runs",
            vec![
                DentryName::new(b"a".to_vec()).unwrap(),
                DentryName::new(b"b".to_vec()).unwrap(),
            ],
        )
        .unwrap();

    let after = metadata.metadata_store_stats();
    assert_eq!(removed.len(), 2);
    assert!(removed[0].is_ok());
    assert!(removed[1].is_ok());
    assert_eq!(after.commit_total - before.commit_total, 2);
    assert_eq!(after.current_delete_total - before.current_delete_total, 4);
    assert_eq!(after.history_write_total - before.history_write_total, 0);
    assert_eq!(after.watch_write_total - before.watch_write_total, 0);
    assert_eq!(after.dedupe_write_total - before.dedupe_write_total, 2);
    assert_eq!(after.atomic_apply_total - before.atomic_apply_total, 1);
    assert_eq!(
        after.atomic_apply_command_total - before.atomic_apply_command_total,
        2
    );
    let listed = service.read_dir_plus_path("/runs").unwrap();
    let names = listed
        .iter()
        .map(|entry| entry.dentry.name.as_bytes())
        .collect::<Vec<_>>();
    assert_eq!(names, vec![b"keep".as_slice()]);
}

#[test]
fn read_dir_plus_page_returns_cursor_without_materializing_full_directory() {
    let metadata = HoltMetadataStore::open_memory().unwrap();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service
        .create_files_in_dir_path(
            "/runs",
            vec![
                DentryName::new(b"a.bin".to_vec()).unwrap(),
                DentryName::new(b"b.bin".to_vec()).unwrap(),
                DentryName::new(b"c.bin".to_vec()).unwrap(),
            ],
            0o644,
            1000,
            1000,
        )
        .unwrap();
    let runs = service.lookup_path("/runs").unwrap().unwrap();

    let before_store = metadata.metadata_store_stats();
    let first = service
        .read_dir_plus_page(runs.attr.inode, None, 2)
        .unwrap();
    let after_first_store = metadata.metadata_store_stats();
    assert_eq!(
        first
            .entries
            .iter()
            .map(|entry| entry.dentry.name.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"a.bin".as_slice(), b"b.bin".as_slice()]
    );
    assert_eq!(
        first.next_cursor.as_ref().map(DentryName::as_bytes),
        Some(b"b.bin".as_slice())
    );
    assert_eq!(
        after_first_store.scan_key_returned_total - before_store.scan_key_returned_total,
        3
    );

    let before_service = service.metadata_service_stats();
    let second = service
        .read_dir_plus_page(runs.attr.inode, first.next_cursor.as_ref(), 2)
        .unwrap();
    let after_service = service.metadata_service_stats();
    assert_eq!(
        second
            .entries
            .iter()
            .map(|entry| entry.dentry.name.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"c.bin".as_slice()]
    );
    assert_eq!(second.next_cursor, None);
    assert_eq!(
        after_service.read_dir_plus_entry_total - before_service.read_dir_plus_entry_total,
        1
    );
    assert_eq!(
        after_service.read_dir_plus_projection_hit_total
            - before_service.read_dir_plus_projection_hit_total,
        1
    );
}

#[test]
fn publish_artifact_stores_body_then_publishes_metadata() {
    let service = service();
    let name = DentryName::new(b"checkpoint.json".to_vec()).unwrap();
    let before_publish = service.object_stats();
    let published = service
        .publish_artifact(PublishArtifact {
            content_type: "application/json".to_owned(),
            ..artifact_request(name.clone(), "runs/1/checkpoint.json", b"{\"x\":1}")
        })
        .unwrap();
    assert_eq!(
        service.object_stats().object_put_bytes,
        before_publish.object_put_bytes + 7
    );

    let lookup = service
        .lookup_plus(InodeId::root(), &name)
        .unwrap()
        .unwrap();
    assert_eq!(lookup, published);
    assert_eq!(lookup.attr.size, 7);
    assert_eq!(
        lookup.body.as_ref().unwrap().manifest_id,
        "runs/1/checkpoint.json"
    );

    let bytes = service
        .read_artifact(InodeId::root(), &name)
        .expect("read artifact body");
    assert_eq!(bytes, b"{\"x\":1}");

    let body = service
        .body_descriptor(published.attr.inode)
        .expect("read body descriptor")
        .expect("body descriptor exists");
    assert_eq!(body.manifest_id, "runs/1/checkpoint.json");
    assert_eq!(body.generation, published.attr.generation);
    let range = service
        .read_file(published.attr.inode, 2, 3)
        .expect("read file range");
    assert_eq!(range, b"x\":");
    let before_cache = service.object_stats();
    let cached = service
        .read_file(published.attr.inode, 2, 3)
        .expect("read cached file range");
    assert_eq!(cached, b"x\":");
    let after_cache = service.object_stats();
    assert!(after_cache.cache_hits > before_cache.cache_hits);
    assert_eq!(
        after_cache.cache_hit_bytes,
        before_cache.cache_hit_bytes + 3
    );
}

#[test]
fn read_file_uses_one_attr_read_for_body_and_manifest_plan() {
    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            DentryName::new(b"checkpoint.bin".to_vec()).unwrap(),
            "checkpoint/body",
            b"abcdef",
        ))
        .unwrap();

    let before = metadata.counts();
    assert_eq!(
        service.read_file(published.attr.inode, 1, 3).unwrap(),
        b"bcd"
    );
    let after = metadata.counts();
    assert_eq!(
        after.user_strong_gets - before.user_strong_gets,
        3,
        "read_file should read inode, body summary, and one chunk manifest"
    );
    assert_eq!(after.write_plan_gets, before.write_plan_gets);
}

#[test]
fn read_artifact_uses_dentry_projection_body_descriptor() {
    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/body", b"abcdef"))
        .unwrap();

    let before = metadata.counts();
    assert_eq!(
        service.read_artifact(InodeId::root(), &name).unwrap(),
        b"abcdef"
    );
    let after = metadata.counts();
    assert_eq!(
        after.user_strong_gets - before.user_strong_gets,
        2,
        "read_artifact should read dentry projection and one chunk manifest"
    );
    assert_eq!(after.write_plan_gets, before.write_plan_gets);
}

#[test]
fn open_path_read_plan_uses_dentry_projection_body_descriptor() {
    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            DentryName::new(b"checkpoint.bin".to_vec()).unwrap(),
            "checkpoint/body",
            b"abcdef",
        ))
        .unwrap();

    let before = metadata.counts();
    let open = service
        .open_path_read_plan("/checkpoint.bin", 1, 3, Some(published.attr.generation))
        .unwrap();
    let after = metadata.counts();
    assert_eq!(open.metadata.attr.inode, published.attr.inode);
    assert_eq!(open.plan.output_len, 3);
    assert_eq!(open.plan.blocks.len(), 1);
    assert_eq!(
        after.user_strong_gets - before.user_strong_gets,
        2,
        "open_path_read_plan should read dentry projection and one chunk manifest"
    );
    assert_eq!(after.write_plan_gets, before.write_plan_gets);
}

#[test]
fn open_path_read_plan_returns_zero_write_lease_and_projected_plan() {
    let metadata = PurposeTrackingStore::new();
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        metadata.clone(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            DentryName::new(b"checkpoint.bin".to_vec()).unwrap(),
            "checkpoint/body",
            b"abcdef",
        ))
        .unwrap();

    let before_counts = metadata.counts();
    let before_commits = service.metadata_store_stats().commit_total;
    let open = service
        .open_path_read_plan("/checkpoint.bin", 1, 3, Some(published.attr.generation))
        .unwrap();
    let after_counts = metadata.counts();

    assert_eq!(open.metadata.attr.inode, published.attr.inode);
    assert_eq!(open.lease.inode, published.attr.inode);
    assert_eq!(open.lease.generation, published.attr.generation);
    assert!(open.lease.read_version >= published.attr.generation);
    assert_eq!(open.plan.output_len, 3);
    assert_eq!(open.plan.blocks.len(), 1);
    assert_eq!(
        service.metadata_store_stats().commit_total,
        before_commits,
        "layout-open must not persist read state"
    );
    assert_eq!(
        after_counts.user_strong_gets - before_counts.user_strong_gets,
        2,
        "layout-open should read dentry projection and one chunk manifest"
    );
    assert_eq!(after_counts.write_plan_gets, before_counts.write_plan_gets);
}

#[test]
fn read_file_plan_returns_ranges_without_fetching_objects() {
    let service = service();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name, "checkpoint/body", b"hello metadata"))
        .unwrap();
    let before = service.object_stats();
    let plan = service
        .read_file_plan(published.attr.inode, published.attr.generation, 6, 6)
        .unwrap();
    assert_eq!(plan.output_len, 6);
    assert_eq!(plan.blocks.len(), 1);
    assert_eq!(plan.blocks[0].object_offset, 6);
    assert_eq!(plan.blocks[0].len, 6);
    assert_eq!(plan.blocks[0].output_offset, 0);
    assert!(plan.blocks[0].digest_uri.starts_with("xxh3-64:"));
    assert_eq!(service.object_stats().object_gets, before.object_gets);

    let stale = service
        .read_file_plan(published.attr.inode, published.attr.generation - 1, 0, 1)
        .unwrap_err();
    assert!(
        matches!(stale, MetadError::StaleBodyGeneration { .. }),
        "unexpected error: {stale:?}"
    );
}

#[test]
fn prepared_artifact_publish_commits_manifest_without_object_fetch() {
    let service = service();
    let name = DentryName::new(b"metadata.bin".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create(InodeId::root(), name.clone())
        .unwrap();
    let body = body_descriptor(prepared.generation, 6);
    let result = service
        .publish_prepared_artifact(
            prepared.clone(),
            body,
            vec![one_chunk_manifest(prepared.inode, prepared.generation, 6)],
            0o644,
            1000,
            1000,
        )
        .unwrap();
    assert_eq!(result.replaced, None);
    assert_eq!(result.entry.attr.inode, prepared.inode);
    let lookup = service
        .lookup_plus(InodeId::root(), &name)
        .unwrap()
        .unwrap();
    assert_eq!(lookup, result.entry);
    let plan = service
        .read_file_plan(prepared.inode, prepared.generation, 1, 3)
        .unwrap();
    assert_eq!(plan.output_len, 3);
    assert_eq!(plan.blocks[0].object_offset, 1);
}

#[test]
fn prepared_artifact_publish_rejects_duplicate_chunk_range() {
    let service = service();
    let name = DentryName::new(b"duplicate-chunk.bin".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create(InodeId::root(), name)
        .unwrap();
    let body = body_descriptor(prepared.generation, 12);
    let chunks = vec![
        one_chunk_manifest(prepared.inode, prepared.generation, 6),
        one_chunk_manifest(prepared.inode, prepared.generation, 6),
    ];

    let err = service
        .publish_prepared_artifact(prepared, body, chunks, 0o644, 1000, 1000)
        .unwrap_err();
    assert!(
        matches!(err, MetadError::InvalidPreparedArtifact(_)),
        "unexpected error: {err:?}"
    );
}

#[test]
fn prepared_artifact_publish_rejects_slice_block_gap() {
    let service = service();
    let name = DentryName::new(b"slice-gap.bin".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create(InodeId::root(), name)
        .unwrap();
    let body = body_descriptor(prepared.generation, 6);
    let mut chunk = one_chunk_manifest(prepared.inode, prepared.generation, 6);
    chunk.slices[0].blocks[0].len = 3;

    let err = service
        .publish_prepared_artifact(prepared, body, vec![chunk], 0o644, 1000, 1000)
        .unwrap_err();
    assert!(
        matches!(err, MetadError::InvalidPreparedArtifact(_)),
        "unexpected error: {err:?}"
    );
}

#[test]
fn prepared_artifact_publish_rejects_block_larger_than_manifest_block_size() {
    let service = service();
    let name = DentryName::new(b"oversized-block.bin".to_vec()).unwrap();
    let prepared = service
        .prepare_artifact_create(InodeId::root(), name)
        .unwrap();
    let mut body = body_descriptor(prepared.generation, 6);
    body.block_size = 3;
    let chunk = one_chunk_manifest(prepared.inode, prepared.generation, 6);

    let err = service
        .publish_prepared_artifact(prepared, body, vec![chunk], 0o644, 1000, 1000)
        .unwrap_err();
    assert!(
        matches!(err, MetadError::InvalidPreparedArtifact(_)),
        "unexpected error: {err:?}"
    );
}

#[test]
fn prepared_artifact_replace_rejects_stale_dentry_version() {
    let service = service();
    let name = DentryName::new(b"replace-metadata.bin".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(name.clone(), "old", b"old"))
        .unwrap();
    let prepared = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();
    service
        .replace_artifact(artifact_request(name, "newer", b"newer"))
        .unwrap();
    let err = service
        .publish_prepared_artifact(
            prepared.clone(),
            body_descriptor(prepared.generation, 6),
            vec![one_chunk_manifest(prepared.inode, prepared.generation, 6)],
            0o644,
            1000,
            1000,
        )
        .unwrap_err();
    assert!(
        matches!(err, MetadError::Metadata(MetadataError::PredicateFailed)),
        "unexpected error: {err:?}"
    );
}

#[test]
fn prepared_artifact_replace_retry_is_idempotent() {
    let service = service();
    let name = DentryName::new(b"retry-metadata.bin".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(name.clone(), "old", b"old"))
        .unwrap();
    let prepared = service
        .prepare_artifact_replace(InodeId::root(), name)
        .unwrap();
    let body = body_descriptor(prepared.generation, 6);
    let chunks = vec![one_chunk_manifest(prepared.inode, prepared.generation, 6)];
    let first = service
        .publish_prepared_artifact(
            prepared.clone(),
            body.clone(),
            chunks.clone(),
            0o644,
            1000,
            1000,
        )
        .unwrap();
    assert!(first.replaced.is_some());
    let second = service
        .publish_prepared_artifact(prepared, body, chunks, 0o644, 1000, 1000)
        .unwrap();
    assert_eq!(second.entry, first.entry);
    assert_eq!(second.replaced, None);
}

#[test]
fn prepared_artifact_session_uploads_only_dirty_ranges_and_reuses_old_blocks() {
    let service = service();
    let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            name.clone(),
            "artifact.bin",
            b"abcdefghij",
        ))
        .unwrap();
    let before = service.object_stats();
    let prepared = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();
    let before_scan = service.metadata_store_stats();
    let replaced = service
        .publish_prepared_artifact_session(
            prepared,
            PublishArtifactSession {
                parent: InodeId::root(),
                name,
                producer: "unit-test".to_owned(),
                digest_uri: "unknown".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "artifact.bin".to_owned(),
                size: 10,
                ranges: vec![PublishArtifactRange {
                    offset: 3,
                    bytes: b"XYZ".to_vec(),
                }],
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();
    let after = service.object_stats();
    let after_scan = service.metadata_store_stats();
    assert_eq!(after.object_puts, before.object_puts + 1);
    assert_eq!(after.object_put_bytes, before.object_put_bytes + 3);
    assert_eq!(
        after_scan.scan_key_visited_total,
        before_scan.scan_key_visited_total
    );
    assert_eq!(
        after_scan.scan_key_returned_total,
        before_scan.scan_key_returned_total
    );
    assert_eq!(replaced.entry.attr.inode, published.attr.inode);
    assert_eq!(
        service.read_file(published.attr.inode, 0, 10).unwrap(),
        b"abcXYZghij"
    );
    let gc = service.cleanup_pending_objects(10).unwrap();
    assert_eq!(gc.attempted, 0);
}

#[test]
fn prepared_artifact_session_splits_noncontiguous_dirty_blocks() {
    let service = service();
    let name = DentryName::new(b"sparse-dirty.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            name.clone(),
            "sparse-dirty-v1",
            b"abcdefghijklmnop",
        ))
        .unwrap();
    let prepared = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();

    service
        .publish_prepared_artifact_session(
            prepared,
            PublishArtifactSession {
                parent: InodeId::root(),
                name,
                producer: "unit-test".to_owned(),
                digest_uri: "unknown".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "sparse-dirty-v2".to_owned(),
                size: 16,
                ranges: vec![
                    PublishArtifactRange {
                        offset: 2,
                        bytes: b"XY".to_vec(),
                    },
                    PublishArtifactRange {
                        offset: 10,
                        bytes: b"UV".to_vec(),
                    },
                ],
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();

    assert_eq!(
        service.read_file(published.attr.inode, 0, 16).unwrap(),
        b"abXYefghijUVmnop"
    );
}

#[test]
fn prepared_artifact_staged_session_preserves_dirty_slice_overlay() {
    let service = service();
    let name = DentryName::new(b"staged-dirty.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(
            name.clone(),
            "staged-dirty-v1",
            b"abcdefghijklmnop",
        ))
        .unwrap();
    let prepared = service
        .prepare_artifact_replace(InodeId::root(), name.clone())
        .unwrap();
    let written = service
        .stage_prepared_artifact_ranges(
            &prepared,
            "staged-dirty-v2",
            &[
                PublishArtifactRange {
                    offset: 2,
                    bytes: b"XY".to_vec(),
                },
                PublishArtifactRange {
                    offset: 10,
                    bytes: b"UV".to_vec(),
                },
            ],
            0,
        )
        .unwrap();
    let staged = written.staged_objects().unwrap();
    let chunks = written.chunk_manifests();

    service
        .publish_prepared_artifact_staged_session(
            prepared,
            PublishArtifactStagedSession {
                parent: InodeId::root(),
                name: name.clone(),
                producer: "unit-test".to_owned(),
                digest_uri: "unknown".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "staged-dirty-v2".to_owned(),
                size: 16,
                chunks,
                staged,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        )
        .unwrap();

    assert_eq!(
        service.read_file(published.attr.inode, 0, 16).unwrap(),
        b"abXYefghijUVmnop"
    );
    let metadata = service.lookup_path("/staged-dirty.bin").unwrap().unwrap();
    let body = metadata.body.as_ref().unwrap();
    let manifests = service
        .chunk_manifests_for_body_at_version(
            published.attr.inode,
            body,
            service.read_version().unwrap(),
            ReadPurpose::UserStrong,
        )
        .unwrap();
    assert_eq!(manifests[0].slices.len(), 3);
    assert_eq!(manifests[0].slices[1].logical_offset, 2);
    assert_eq!(manifests[0].slices[2].logical_offset, 10);
}

#[test]
fn replace_artifact_preserves_inode_and_returns_old_body() {
    let service = service();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let first = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
        .unwrap();
    let replaced = service
        .replace_artifact(artifact_request(
            name.clone(),
            "checkpoint/new",
            b"new-body",
        ))
        .unwrap();

    assert_eq!(replaced.entry.attr.inode, first.attr.inode);
    assert!(replaced.entry.attr.generation > first.attr.generation);
    assert_eq!(replaced.replaced, Some(first.clone()));
    assert_eq!(
        service.lookup_plus(InodeId::root(), &name).unwrap(),
        Some(replaced.entry.clone())
    );
    assert_eq!(
        service.read_artifact(InodeId::root(), &name).unwrap(),
        b"new-body"
    );
    assert_eq!(
        replaced.replaced.unwrap().body.unwrap().manifest_id,
        "checkpoint/old"
    );
}

#[test]
fn get_attr_reads_root_inode() {
    let service = service();
    let root = service.get_attr(InodeId::root()).unwrap().unwrap();
    assert_eq!(root.inode, InodeId::root());
    assert_eq!(root.file_type, FileType::Directory);
}

#[test]
fn remove_file_deletes_namespace_and_returns_old_body() {
    let service = service();
    let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "artifact.bin", b"old"))
        .unwrap();

    let removed = service.remove_file(InodeId::root(), &name).unwrap();
    assert_eq!(removed, published);
    assert_eq!(removed.body.as_ref().unwrap().manifest_id, "artifact.bin");
    assert!(service
        .lookup_plus(InodeId::root(), &name)
        .unwrap()
        .is_none());
    assert!(service.get_attr(removed.attr.inode).unwrap().is_none());
}

#[test]
fn hardlink_updates_link_count_and_defers_body_gc_until_last_unlink() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
    let link_name = DentryName::new(b"artifact.link".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "artifact.bin", b"old"))
        .unwrap();
    let body = published.body.clone().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);

    let linked = service
        .link(published.attr.inode, InodeId::root(), link_name.clone())
        .unwrap();
    assert_eq!(linked.attr.inode, published.attr.inode);
    assert_eq!(linked.attr.nlink, 2);
    assert_eq!(
        service
            .lookup_plus(InodeId::root(), &name)
            .unwrap()
            .unwrap()
            .attr
            .nlink,
        2
    );
    assert_eq!(
        service
            .lookup_plus(InodeId::root(), &link_name)
            .unwrap()
            .unwrap()
            .attr
            .nlink,
        2
    );

    let removed = service.remove_file(InodeId::root(), &name).unwrap();
    assert_eq!(removed.attr.inode, published.attr.inode);
    assert!(service
        .lookup_plus(InodeId::root(), &name)
        .unwrap()
        .is_none());
    let remaining = service
        .lookup_plus(InodeId::root(), &link_name)
        .unwrap()
        .unwrap();
    assert_eq!(remaining.attr.nlink, 1);
    assert_eq!(
        service
            .get_attr(published.attr.inode)
            .unwrap()
            .unwrap()
            .nlink,
        1
    );
    assert_eq!(
        service.read_artifact(InodeId::root(), &link_name).unwrap(),
        b"old"
    );
    assert!(objects.head(&object).unwrap().is_some());
    assert_eq!(
        service.cleanup_pending_objects(100).unwrap(),
        PendingObjectCleanupOutcome::default()
    );

    let removed_last = service.remove_file(InodeId::root(), &link_name).unwrap();
    assert_eq!(removed_last.attr.inode, published.attr.inode);
    assert!(service.get_attr(published.attr.inode).unwrap().is_none());
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert!(objects.head(&object).unwrap().is_none());
}

#[test]
fn hardlink_rejects_directories() {
    let service = service();
    let dir = service
        .create_dir(
            InodeId::root(),
            DentryName::new(b"dir".to_vec()).unwrap(),
            0o755,
            1000,
            1000,
        )
        .unwrap();
    let err = service
        .link(
            dir.attr.inode,
            InodeId::root(),
            DentryName::new(b"dir-link".to_vec()).unwrap(),
        )
        .unwrap_err();
    assert!(matches!(err, MetadError::NotFile));
}

#[test]
fn remove_file_queues_old_body_for_object_cleanup() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "artifact.bin", b"old"))
        .unwrap();
    let body = published.body.clone().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    assert!(objects.head(&object).unwrap().is_some());

    let removed = service.remove_file(InodeId::root(), &name).unwrap();
    assert_eq!(removed, published);
    assert!(objects.head(&object).unwrap().is_some());

    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.scanned, 1);
    assert_eq!(cleanup.attempted, 1);
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.missing, 0);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&object).unwrap().is_none());
    assert_eq!(
        service.cleanup_pending_objects(100).unwrap(),
        PendingObjectCleanupOutcome::default()
    );
}

#[test]
fn read_lease_grace_blocks_recent_object_gc_records() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let first = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
        .unwrap();
    let old_body = first.body.clone().unwrap();
    let old_object = block_key(first.attr.inode, old_body.generation, 0, 0);
    let replaced = service
        .replace_artifact(artifact_request(
            name.clone(),
            "checkpoint/new",
            b"new-body",
        ))
        .unwrap();
    let new_body = replaced.entry.body.clone().unwrap();
    let new_object = block_key(replaced.entry.attr.inode, new_body.generation, 0, 0);

    let blocked = service
        .cleanup_pending_objects_with_grace(100, std::time::Duration::from_secs(3_600))
        .unwrap();
    assert_eq!(blocked.scanned, 1);
    assert_eq!(blocked.blocked_by_snapshots, 0);
    assert_eq!(blocked.blocked_by_read_leases, 1);
    assert_eq!(blocked.attempted, 0);
    assert_eq!(blocked.records_removed, 0);
    assert!(objects.head(&old_object).unwrap().is_some());
    assert!(objects.head(&new_object).unwrap().is_some());

    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&old_object).unwrap().is_none());
    assert!(objects.head(&new_object).unwrap().is_some());
}

#[test]
fn replace_artifact_cleanup_deletes_only_old_generation() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let first = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
        .unwrap();
    let old_body = first.body.clone().unwrap();
    let old_object = block_key(first.attr.inode, old_body.generation, 0, 0);
    let replaced = service
        .replace_artifact(artifact_request(
            name.clone(),
            "checkpoint/new",
            b"new-body",
        ))
        .unwrap();
    let new_body = replaced.entry.body.clone().unwrap();
    let new_object = block_key(replaced.entry.attr.inode, new_body.generation, 0, 0);
    assert!(objects.head(&old_object).unwrap().is_some());
    assert!(objects.head(&new_object).unwrap().is_some());

    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&old_object).unwrap().is_none());
    assert!(objects.head(&new_object).unwrap().is_some());
    assert_eq!(
        service.read_artifact(InodeId::root(), &name).unwrap(),
        b"new-body"
    );
}

#[test]
fn snapshot_after_replace_does_not_block_old_object_cleanup() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let first = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
        .unwrap();
    let old_body = first.body.clone().unwrap();
    let old_object = block_key(first.attr.inode, old_body.generation, 0, 0);
    let replaced = service
        .replace_artifact(artifact_request(
            name.clone(),
            "checkpoint/new",
            b"new-body",
        ))
        .unwrap();
    let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();

    assert_eq!(
        service
            .read_artifact_at_snapshot(snapshot.snapshot_id, InodeId::root(), &name)
            .unwrap(),
        b"new-body"
    );
    assert!(objects.head(&old_object).unwrap().is_some());

    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.scanned, 1);
    assert_eq!(cleanup.blocked_by_snapshots, 0);
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&old_object).unwrap().is_none());
    assert_eq!(
        service.read_artifact(InodeId::root(), &name).unwrap(),
        b"new-body"
    );
    assert_eq!(
        replaced.entry.body.unwrap().generation,
        snapshot.read_version
    );
}

#[test]
fn snapshot_preserves_old_artifact_and_blocks_object_gc_until_retired() {
    let (service, objects) = service_with_objects();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let first = service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
        .unwrap();
    let old_body = first.body.clone().unwrap();
    let old_object = block_key(first.attr.inode, old_body.generation, 0, 0);
    let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();

    let replaced = service
        .replace_artifact(artifact_request(
            name.clone(),
            "checkpoint/new",
            b"new-body",
        ))
        .unwrap();
    let new_body = replaced.entry.body.clone().unwrap();
    let new_object = block_key(replaced.entry.attr.inode, new_body.generation, 0, 0);

    assert_eq!(
        service
            .read_artifact_at_snapshot(snapshot.snapshot_id, InodeId::root(), &name)
            .unwrap(),
        b"old"
    );
    assert_eq!(
        service
            .get_attr_at_snapshot(snapshot.snapshot_id, first.attr.inode)
            .unwrap(),
        Some(first.attr.clone())
    );
    assert_eq!(
        service
            .read_file_at_snapshot(snapshot.snapshot_id, first.attr.inode, 0, 3)
            .unwrap(),
        b"old"
    );
    assert_eq!(
        service
            .read_dir_plus_at_snapshot(snapshot.snapshot_id, InodeId::root())
            .unwrap(),
        vec![first.clone()]
    );
    assert_eq!(
        service.read_artifact(InodeId::root(), &name).unwrap(),
        b"new-body"
    );
    let blocked = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(blocked.scanned, 1);
    assert_eq!(blocked.blocked_by_snapshots, 1);
    assert_eq!(blocked.attempted, 0);
    assert!(objects.head(&old_object).unwrap().is_some());
    assert!(objects.head(&new_object).unwrap().is_some());

    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    assert!(!service.retire_snapshot(snapshot.snapshot_id).unwrap());
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&old_object).unwrap().is_none());
    assert!(objects.head(&new_object).unwrap().is_some());
}

#[test]
fn snapshot_path_reads_are_rooted_at_snapshot_subtree_and_support_ranges() {
    let service = service();
    let scope = service
        .create_dir_path("/scope", 0o755, 1000, 1000)
        .unwrap();
    let nested = service
        .create_dir_path("/scope/nested", 0o755, 1000, 1000)
        .unwrap();
    let outside = service
        .create_dir_path("/outside", 0o755, 1000, 1000)
        .unwrap();
    let name = DentryName::new(b"model.bin".to_vec()).unwrap();
    let inside_old = service
        .publish_artifact(PublishArtifact {
            parent: nested.attr.inode,
            name: name.clone(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:inside-old".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "inside-old".to_owned(),
            bytes: b"inside-old".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    service
        .publish_artifact(PublishArtifact {
            parent: outside.attr.inode,
            name: name.clone(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:outside".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "outside".to_owned(),
            bytes: b"outside".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    let snapshot = service.snapshot_subtree_path("/scope").unwrap();
    service
        .replace_artifact(PublishArtifact {
            parent: nested.attr.inode,
            name: name.clone(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:inside-new".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "inside-new".to_owned(),
            bytes: b"inside-new".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();

    let root = service
        .stat_path_at_snapshot(snapshot.snapshot_id, "/")
        .unwrap()
        .unwrap();
    assert_eq!(root.attr.inode, scope.attr.inode);
    assert_eq!(
        service
            .read_dir_plus_path_at_snapshot(snapshot.snapshot_id, "/")
            .unwrap(),
        vec![nested.clone()]
    );
    let file = service
        .stat_path_at_snapshot(snapshot.snapshot_id, "/nested/model.bin")
        .unwrap()
        .unwrap();
    assert_eq!(file.attr.generation, inside_old.attr.generation);
    assert_eq!(file.body, inside_old.body);
    assert_eq!(
        service
            .read_file_path_at_snapshot(snapshot.snapshot_id, "/nested/model.bin", 7, 3)
            .unwrap(),
        b"old"
    );
    assert!(matches!(
        service.read_file_path_at_snapshot(snapshot.snapshot_id, "/outside/model.bin", 0, 7),
        Err(MetadError::NotFound)
    ));
}

#[test]
fn history_cleanup_keeps_snapshot_reads_until_snapshot_retired() {
    let service = service();
    let name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(name.clone(), "checkpoint/old", b"old"))
        .unwrap();
    let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();
    service
        .replace_artifact(artifact_request(
            name.clone(),
            "checkpoint/new",
            b"new-body",
        ))
        .unwrap();

    let retained = service.cleanup_history(100).unwrap();
    assert!(retained.retained_by_snapshots > 0);
    assert_eq!(
        service
            .read_artifact_at_snapshot(snapshot.snapshot_id, InodeId::root(), &name)
            .unwrap(),
        b"old"
    );

    assert!(service.retire_snapshot(snapshot.snapshot_id).unwrap());
    let pruned = service.cleanup_history(100).unwrap();
    assert!(pruned.removed > 0);
    assert_eq!(
        service
            .metadata
            .get(
                RecordFamily::Dentry,
                &dentry_key(service.mount, InodeId::root(), &name),
                Version::new(snapshot.read_version).unwrap(),
                ReadPurpose::Snapshot,
            )
            .unwrap(),
        None
    );
}

#[test]
fn remove_empty_dir_rejects_non_empty_directory() {
    let service = service();
    let dir = DentryName::new(b"runs".to_vec()).unwrap();
    let child = DentryName::new(b"1".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), dir.clone(), 0o755, 1000, 1000)
        .unwrap();
    service
        .create_dir(created.attr.inode, child, 0o755, 1000, 1000)
        .unwrap();

    let err = service.remove_empty_dir(InodeId::root(), &dir).unwrap_err();
    assert!(matches!(err, MetadError::DirectoryNotEmpty));
    assert!(service
        .lookup_plus(InodeId::root(), &dir)
        .unwrap()
        .is_some());
}

#[test]
fn remove_empty_dir_deletes_empty_directory() {
    let service = service();
    let dir = DentryName::new(b"runs".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), dir.clone(), 0o755, 1000, 1000)
        .unwrap();

    let removed = service.remove_empty_dir(InodeId::root(), &dir).unwrap();
    assert_eq!(removed, created);
    assert!(service
        .lookup_plus(InodeId::root(), &dir)
        .unwrap()
        .is_none());
    assert!(service.get_attr(created.attr.inode).unwrap().is_none());
}

#[test]
fn remove_empty_dir_allows_directory_after_last_child_removed() {
    let service = service();
    let dir = DentryName::new(b"runs".to_vec()).unwrap();
    let child = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), dir.clone(), 0o755, 1000, 1000)
        .unwrap();
    service
        .publish_artifact(PublishArtifact {
            parent: created.attr.inode,
            name: child.clone(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "runs/checkpoint.bin".to_owned(),
            bytes: b"payload".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();

    service.remove_file(created.attr.inode, &child).unwrap();
    let removed = service.remove_empty_dir(InodeId::root(), &dir).unwrap();

    assert_eq!(removed, created);
    assert!(service
        .lookup_plus(InodeId::root(), &dir)
        .unwrap()
        .is_none());
}

#[test]
fn rename_moves_dentry_without_changing_inode() {
    let service = service();
    let old_name = DentryName::new(b"old".to_vec()).unwrap();
    let new_name = DentryName::new(b"new".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), old_name.clone(), 0o755, 1000, 1000)
        .unwrap();

    let renamed = service
        .rename(
            InodeId::root(),
            &old_name,
            InodeId::root(),
            new_name.clone(),
        )
        .unwrap();
    assert_eq!(renamed.attr.inode, created.attr.inode);
    assert!(service
        .lookup_plus(InodeId::root(), &old_name)
        .unwrap()
        .is_none());
    assert_eq!(
        service.lookup_plus(InodeId::root(), &new_name).unwrap(),
        Some(renamed)
    );
}

#[test]
fn rename_replace_returns_replaced_file_body() {
    let service = service();
    let source_name = DentryName::new(b"stage".to_vec()).unwrap();
    let final_name = DentryName::new(b"final".to_vec()).unwrap();
    let source = service
        .publish_artifact(artifact_request(source_name.clone(), "stage", b"new"))
        .unwrap();
    let old = service
        .publish_artifact(artifact_request(final_name.clone(), "final-old", b"old"))
        .unwrap();

    let result = service
        .rename_replace(
            InodeId::root(),
            &source_name,
            InodeId::root(),
            final_name.clone(),
        )
        .unwrap();
    assert_eq!(result.entry.attr.inode, source.attr.inode);
    assert_eq!(result.replaced, Some(old.clone()));
    assert!(service
        .lookup_plus(InodeId::root(), &source_name)
        .unwrap()
        .is_none());
    assert_eq!(
        service.lookup_plus(InodeId::root(), &final_name).unwrap(),
        Some(result.entry)
    );
    assert!(service.get_attr(old.attr.inode).unwrap().is_none());
}

#[test]
fn watch_replay_returns_typed_events_after_cursor() {
    let service = service();
    let cursor = service.watch_subtree(InodeId::root()).unwrap();
    let name = DentryName::new(b"runs".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), name.clone(), 0o755, 1000, 1000)
        .unwrap();

    let events = service.replay_watch(InodeId::root(), cursor, 100).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.kind, WatchEventKind::Create);
    assert_eq!(events[0].event.parent, Some(InodeId::root()));
    assert_eq!(events[0].event.name, Some(name.clone()));
    assert_eq!(events[0].event.inode, created.attr.inode);

    let next_name = DentryName::new(b"checkpoint.bin".to_vec()).unwrap();
    service
        .publish_artifact(artifact_request(
            next_name.clone(),
            "checkpoint.bin",
            b"body",
        ))
        .unwrap();
    let resumed = service
        .replay_watch(InodeId::root(), events[0].cursor, 100)
        .unwrap();
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].event.kind, WatchEventKind::PublishArtifact);
    assert_eq!(resumed[0].event.name, Some(next_name));
}

#[test]
fn watch_replay_resumes_from_cursor_without_scanning_old_events() {
    let service = service();
    let cursor = service.watch_subtree(InodeId::root()).unwrap();
    for name in ["a", "b", "c"] {
        service
            .create_dir(
                InodeId::root(),
                DentryName::new(name.as_bytes().to_vec()).unwrap(),
                0o755,
                1000,
                1000,
            )
            .unwrap();
    }

    let before_first = service.metadata_store_stats();
    let first = service.replay_watch(InodeId::root(), cursor, 1).unwrap();
    let after_first = service.metadata_store_stats();
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0].event.name.as_ref().map(DentryName::as_bytes),
        Some(b"a".as_slice())
    );
    assert_eq!(
        after_first.scan_key_visited_total - before_first.scan_key_visited_total,
        1
    );
    assert_eq!(
        after_first.scan_key_returned_total - before_first.scan_key_returned_total,
        1
    );

    let before_second = service.metadata_store_stats();
    let second = service
        .replay_watch(InodeId::root(), first[0].cursor, 1)
        .unwrap();
    let after_second = service.metadata_store_stats();
    assert_eq!(second.len(), 1);
    assert_eq!(
        second[0].event.name.as_ref().map(DentryName::as_bytes),
        Some(b"b".as_slice())
    );
    assert_eq!(
        after_second.scan_key_visited_total - before_second.scan_key_visited_total,
        1
    );
    assert_eq!(
        after_second.scan_key_returned_total - before_second.scan_key_returned_total,
        1
    );
}

#[test]
fn rename_replay_notifies_old_and_new_parent_scopes() {
    let service = service();
    let old_parent_name = DentryName::new(b"old-parent".to_vec()).unwrap();
    let new_parent_name = DentryName::new(b"new-parent".to_vec()).unwrap();
    let old_parent = service
        .create_dir(InodeId::root(), old_parent_name, 0o755, 1000, 1000)
        .unwrap();
    let new_parent = service
        .create_dir(InodeId::root(), new_parent_name, 0o755, 1000, 1000)
        .unwrap();
    let file_name = DentryName::new(b"artifact".to_vec()).unwrap();
    let source = service
        .create_file(old_parent.attr.inode, file_name.clone(), 0o644, 1000, 1000)
        .unwrap();
    let old_cursor = service.watch_subtree(old_parent.attr.inode).unwrap();
    let new_cursor = service.watch_subtree(new_parent.attr.inode).unwrap();

    service
        .rename(
            old_parent.attr.inode,
            &file_name,
            new_parent.attr.inode,
            file_name.clone(),
        )
        .unwrap();

    let old_events = service
        .replay_watch(old_parent.attr.inode, old_cursor, 100)
        .unwrap();
    assert_eq!(old_events.len(), 1);
    assert_eq!(old_events[0].event.kind, WatchEventKind::Remove);
    assert_eq!(old_events[0].event.inode, source.attr.inode);

    let new_events = service
        .replay_watch(new_parent.attr.inode, new_cursor, 100)
        .unwrap();
    assert_eq!(new_events.len(), 1);
    assert_eq!(new_events[0].event.kind, WatchEventKind::Rename);
    assert_eq!(new_events[0].event.name, Some(file_name));
    assert_eq!(new_events[0].event.inode, source.attr.inode);
}

#[test]
fn watch_replay_survives_service_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let cursor = service.watch_subtree(InodeId::root()).unwrap();
    let name = DentryName::new(b"runs".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), name.clone(), 0o755, 1000, 1000)
        .unwrap();
    drop(service);

    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
    let events = reopened.replay_watch(InodeId::root(), cursor, 100).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.kind, WatchEventKind::Create);
    assert_eq!(events[0].event.name, Some(name));
    assert_eq!(events[0].event.inode, created.attr.inode);
}

#[test]
fn open_existing_recovers_inode_and_version_allocators() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let first = service
        .create_dir(
            InodeId::root(),
            DentryName::new(b"first".to_vec()).unwrap(),
            0o755,
            1000,
            1000,
        )
        .unwrap();
    drop(service);

    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
    let second = reopened
        .create_dir(
            InodeId::root(),
            DentryName::new(b"second".to_vec()).unwrap(),
            0o755,
            1000,
            1000,
        )
        .unwrap();
    assert!(second.attr.inode > first.attr.inode);
    assert!(second.attr.generation > first.attr.generation);
}

#[test]
fn refresh_allocator_state_advances_live_read_version_after_external_commit() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let original = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    original.bootstrap_root(0o755, 1000, 1000).unwrap();

    let external = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
    let external_file = external
        .create_file_path("/external.bin", 0o644, 1000, 1000)
        .unwrap();
    assert!(original.stat_path("/external.bin").unwrap().is_none());

    original.refresh_allocator_state().unwrap();
    let visible = original
        .stat_path("/external.bin")
        .unwrap()
        .expect("external commit should be visible after refresh");
    assert_eq!(visible.attr, external_file.attr);
    let local_file = original
        .create_file_path("/after-refresh.bin", 0o644, 1000, 1000)
        .unwrap();
    assert!(local_file.attr.inode > external_file.attr.inode);
    assert!(local_file.attr.generation > external_file.attr.generation);
}

#[test]
fn open_existing_recovers_after_dentry_only_rename() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let old_name = DentryName::new(b"old".to_vec()).unwrap();
    let new_name = DentryName::new(b"new".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), old_name.clone(), 0o755, 1000, 1000)
        .unwrap();
    let renamed = service
        .rename(
            InodeId::root(),
            &old_name,
            InodeId::root(),
            new_name.clone(),
        )
        .unwrap();
    assert_eq!(renamed.attr.inode, created.attr.inode);
    drop(service);

    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
    assert!(reopened
        .lookup_plus(InodeId::root(), &old_name)
        .unwrap()
        .is_none());
    assert_eq!(
        reopened.lookup_plus(InodeId::root(), &new_name).unwrap(),
        Some(renamed)
    );
    assert_eq!(reopened.read_dir_plus(InodeId::root()).unwrap().len(), 1);
}

#[test]
fn open_existing_does_not_reuse_removed_inode() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let first_name = DentryName::new(b"first".to_vec()).unwrap();
    let second_name = DentryName::new(b"second".to_vec()).unwrap();
    let first = service
        .create_file(InodeId::root(), first_name.clone(), 0o644, 1000, 1000)
        .unwrap();
    service.remove_file(InodeId::root(), &first_name).unwrap();
    drop(service);

    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
    let second = reopened
        .create_file(InodeId::root(), second_name.clone(), 0o644, 1000, 1000)
        .unwrap();
    assert!(second.attr.inode > first.attr.inode);
    assert!(second.attr.generation > first.attr.generation);
    assert!(reopened
        .lookup_plus(InodeId::root(), &first_name)
        .unwrap()
        .is_none());
    assert_eq!(
        reopened.lookup_plus(InodeId::root(), &second_name).unwrap(),
        Some(second)
    );
}

#[test]
fn pending_object_gc_survives_service_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
    let published = service
        .publish_artifact(artifact_request(name.clone(), "artifact.bin", b"old"))
        .unwrap();
    let body = published.body.clone().unwrap();
    let object = block_key(published.attr.inode, body.generation, 0, 0);
    service.remove_file(InodeId::root(), &name).unwrap();
    drop(service);

    let reopened =
        NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects.clone()).unwrap();
    let cleanup = reopened.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(cleanup.records_removed, 1);
    assert!(objects.head(&object).unwrap().is_none());
}

#[test]
fn snapshot_pin_survives_service_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let snapshot = service.snapshot_subtree(InodeId::root()).unwrap();
    drop(service);

    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
    assert_eq!(
        reopened.snapshot_pin(snapshot.snapshot_id).unwrap(),
        Some(snapshot)
    );
    assert_eq!(reopened.metadata_store_stats().active_snapshot_pin_total, 1);
}

#[test]
fn failed_publish_returns_staged_objects_for_cleanup_and_does_not_reuse_identity() {
    let dir = tempfile::tempdir().unwrap();
    let objects = MemoryObjectStore::new();
    let metadata = HoltMetadataStore::open_file(dir.path().join("meta")).unwrap();
    let service = NoKvFs::new(MountId::new(1).unwrap(), metadata.clone(), objects.clone());
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    let name = DentryName::new(b"artifact.bin".to_vec()).unwrap();
    let first = service
        .publish_artifact(artifact_request(name.clone(), "first", b"first"))
        .unwrap();
    let err = service
        .publish_artifact(artifact_request(name.clone(), "duplicate", b"duplicate"))
        .unwrap_err();
    let staged = match err {
        MetadError::PublishArtifactFailed { source, staged } => {
            assert!(matches!(
                *source,
                MetadError::Metadata(MetadataError::PredicateFailed)
            ));
            staged
        }
        err => panic!("unexpected publish error: {err:?}"),
    };
    assert_eq!(staged.len(), 1);
    for object in staged.objects() {
        assert!(objects.head(&object.key).unwrap().is_some());
    }
    assert_eq!(
        service.lookup_plus(InodeId::root(), &name).unwrap(),
        Some(first.clone())
    );

    let cleanup = service.cleanup_staged_objects(&staged).unwrap();
    assert_eq!(cleanup.attempted, staged.len());
    assert_eq!(cleanup.deleted, staged.len());
    assert_eq!(cleanup.missing, 0);
    for object in staged.objects() {
        assert!(objects.head(&object.key).unwrap().is_none());
    }
    drop(service);

    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
    let next_name = DentryName::new(b"next.bin".to_vec()).unwrap();
    let next = reopened
        .publish_artifact(artifact_request(next_name, "next", b"next"))
        .unwrap();

    assert!(next.attr.inode.get() > first.attr.inode.get() + 1);
    assert!(next.attr.generation > first.attr.generation + 1);
}

fn dname(raw: &[u8]) -> DentryName {
    DentryName::new(raw.to_vec()).unwrap()
}

fn block_count_for(objects: &MemoryObjectStore, inode: InodeId, generation: u64) -> usize {
    // Count the published blocks the base file owns under its (inode, generation).
    let mut count = 0;
    let mut block = 0;
    while objects
        .head(&block_key(inode, generation, 0, block))
        .unwrap()
        .is_some()
    {
        count += 1;
        block += 1;
    }
    count
}

#[test]
fn clone_subtree_shares_base_blocks_diverges_on_write_and_keeps_gc_safe() {
    let (service, objects) = service_with_objects();
    // 1. Base namespace: /base with files a ("AAA..") and b ("BBB..").
    let base = service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    let a = publish_path_artifact(&service, "/base/a", "base/a", &vec![b'A'; 4096]);
    let b = publish_path_artifact(&service, "/base/b", "base/b", &vec![b'B'; 4096]);
    let a_gen = a.body.as_ref().unwrap().generation;
    let b_gen = b.body.as_ref().unwrap().generation;
    let a_block = block_key(a.attr.inode, a_gen, 0, 0);
    let b_block = block_key(b.attr.inode, b_gen, 0, 0);
    assert!(objects.head(&a_block).unwrap().is_some());
    assert!(objects.head(&b_block).unwrap().is_some());
    let objects_after_base = objects.object_count();

    // 2. Writable O(1)-ish fork of /base.
    let fork = service.clone_subtree_path("/base").unwrap();
    assert_ne!(fork.root, base.attr.inode);

    // 3. Sharing: the fork sees the base content, with NO duplicate blocks.
    let fork_a = service
        .lookup_plus(fork.root, &dname(b"a"))
        .unwrap()
        .unwrap();
    let fork_b = service
        .lookup_plus(fork.root, &dname(b"b"))
        .unwrap()
        .unwrap();
    assert_ne!(
        fork_a.attr.inode, a.attr.inode,
        "fork must use a fresh inode"
    );
    // Shared files keep the source's content generation (the CoW sharing signal).
    assert_eq!(fork_a.attr.generation, a_gen);
    assert_eq!(fork_b.attr.generation, b_gen);
    assert_eq!(fork_b.body.as_ref().unwrap().generation, b_gen);
    assert_eq!(
        service.read_artifact(fork.root, &dname(b"a")).unwrap(),
        vec![b'A'; 4096]
    );
    assert_eq!(
        service.read_artifact(fork.root, &dname(b"b")).unwrap(),
        vec![b'B'; 4096]
    );
    // Zero-copy: clone added metadata only, not object blocks.
    assert_eq!(
        objects.object_count(),
        objects_after_base,
        "clone must share base blocks, not copy them"
    );
    // The fork's a/b manifests reference the SAME object keys as the base.
    assert_eq!(
        service
            .read_file_plan(fork_a.attr.inode, fork_a.attr.generation, 0, 4096)
            .unwrap()
            .blocks[0]
            .object_key,
        a_block.as_str()
    );

    // 4. Divergence: rewrite a in the fork and add a new file c.
    service
        .replace_artifact(PublishArtifact {
            parent: fork.root,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:zzz".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "fork/a".to_owned(),
            bytes: vec![b'Z'; 4096],
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    service
        .publish_artifact(PublishArtifact {
            parent: fork.root,
            name: dname(b"c"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:ccc".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "fork/c".to_owned(),
            bytes: vec![b'C'; 4096],
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();

    // 4a. Fork now sees a="ZZZ..", b="BBB..", c present.
    assert_eq!(
        service.read_artifact(fork.root, &dname(b"a")).unwrap(),
        vec![b'Z'; 4096]
    );
    assert_eq!(
        service.read_artifact(fork.root, &dname(b"b")).unwrap(),
        vec![b'B'; 4096]
    );
    assert_eq!(
        service.read_artifact(fork.root, &dname(b"c")).unwrap(),
        vec![b'C'; 4096]
    );
    // 4b. Base is unchanged: a="AAA..", no c.
    assert_eq!(
        service
            .read_artifact(base.attr.inode, &dname(b"a"))
            .unwrap(),
        vec![b'A'; 4096]
    );
    assert!(service
        .lookup_plus(base.attr.inode, &dname(b"c"))
        .unwrap()
        .is_none());

    // 6. Diff reports exactly { modified: a, added: c }; b (shared) is not reported.
    let mut diff = service.diff_subtrees(base.attr.inode, fork.root).unwrap();
    diff.sort_by(|left, right| left.path.cmp(&right.path));
    let summary: Vec<(&str, SubtreeDeltaKind)> =
        diff.iter().map(|d| (d.path.as_str(), d.kind)).collect();
    assert_eq!(
        summary,
        vec![
            ("/a", SubtreeDeltaKind::Modified),
            ("/c", SubtreeDeltaKind::Added),
        ]
    );
    // The enriched diff carries the changed file's content digest.
    assert!(diff
        .iter()
        .find(|d| d.path == "/a")
        .unwrap()
        .digest
        .is_some());

    // 5. GC safety: reclaim must NOT touch base blocks the fork's divergent write
    // abandoned but the base still references; they are owned by the base inode and
    // protected by the fork's retained snapshot pin.
    let reclaim = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(reclaim.deleted, 0, "no base block may be reclaimed yet");
    assert!(objects.head(&a_block).unwrap().is_some());
    assert!(objects.head(&b_block).unwrap().is_some());
    assert_eq!(
        service
            .read_artifact(base.attr.inode, &dname(b"a"))
            .unwrap(),
        vec![b'A'; 4096]
    );

    // Drop the fork: remove its files and retire its snapshot pin. The fork-only
    // blocks (the divergent a' and the new c) then become reclaimable, while the
    // base's blocks remain because the base still references them.
    let fork_a_diverged = service
        .lookup_plus(fork.root, &dname(b"a"))
        .unwrap()
        .unwrap();
    let fork_c = service
        .lookup_plus(fork.root, &dname(b"c"))
        .unwrap()
        .unwrap();
    let fork_a_block = block_key(
        fork_a_diverged.attr.inode,
        fork_a_diverged.body.as_ref().unwrap().generation,
        0,
        0,
    );
    let fork_c_block = block_key(
        fork_c.attr.inode,
        fork_c.body.as_ref().unwrap().generation,
        0,
        0,
    );
    service.remove_file(fork.root, &dname(b"a")).unwrap();
    service.remove_file(fork.root, &dname(b"b")).unwrap();
    service.remove_file(fork.root, &dname(b"c")).unwrap();
    assert!(service.retire_snapshot(fork.snapshot_id).unwrap());
    let reclaim = service.cleanup_pending_objects(100).unwrap();
    assert!(reclaim.deleted >= 2, "fork-only blocks must be reclaimable");
    assert!(objects.head(&fork_a_block).unwrap().is_none());
    assert!(objects.head(&fork_c_block).unwrap().is_none());
    // Base remains fully intact and readable.
    assert!(objects.head(&a_block).unwrap().is_some());
    assert!(objects.head(&b_block).unwrap().is_some());
    assert_eq!(
        service
            .read_artifact(base.attr.inode, &dname(b"a"))
            .unwrap(),
        vec![b'A'; 4096]
    );
    assert_eq!(
        service
            .read_artifact(base.attr.inode, &dname(b"b"))
            .unwrap(),
        vec![b'B'; 4096]
    );
    assert_eq!(block_count_for(&objects, a.attr.inode, a_gen), 1);
}

#[test]
fn clone_subtree_copies_nested_dirs_and_diff_reports_removed() {
    let service = service();
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/base/sub", 0o755, 1000, 1000)
        .unwrap();
    publish_path_artifact(&service, "/base/sub/deep", "base/deep", b"deep-bytes");
    publish_path_artifact(&service, "/base/top", "base/top", b"top-bytes");

    let fork = service.clone_subtree_path("/base").unwrap();
    // Nested structure is reproduced under fresh inodes.
    let sub = service
        .lookup_plus(fork.root, &dname(b"sub"))
        .unwrap()
        .unwrap();
    assert_eq!(sub.attr.file_type, FileType::Directory);
    assert_eq!(
        service
            .read_artifact(sub.attr.inode, &dname(b"deep"))
            .unwrap(),
        b"deep-bytes"
    );

    // Identical subtree => no deltas.
    let base = service.resolve_directory_path("/base").unwrap();
    assert!(service.diff_subtrees(base, fork.root).unwrap().is_empty());

    // Remove a nested file in the fork => Removed delta at the nested path,
    // direction base -> fork.
    service
        .remove_file(sub.attr.inode, &dname(b"deep"))
        .unwrap();
    let removed = service.diff_subtrees(base, fork.root).unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].path, "/sub/deep");
    assert_eq!(removed[0].kind, SubtreeDeltaKind::Removed);
    assert_eq!(removed[0].size_delta, -(b"deep-bytes".len() as i64));
    // Reversed direction reports it as Added, with the net size flipped.
    let added = service.diff_subtrees(fork.root, base).unwrap();
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].path, "/sub/deep");
    assert_eq!(added[0].kind, SubtreeDeltaKind::Added);
    assert_eq!(added[0].size_delta, b"deep-bytes".len() as i64);
}

#[test]
fn clone_subtree_path_rejects_non_directory() {
    let service = service();
    publish_path_artifact(&service, "/file.bin", "file", b"bytes");
    assert!(matches!(
        service.clone_subtree_path("/file.bin"),
        Err(MetadError::NotDirectory)
    ));
}

fn read_artifact_at_path(
    service: &NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    path: &str,
) -> Vec<u8> {
    let (parent, name) = service.resolve_parent_path(path).unwrap();
    service.read_artifact(parent, &name).unwrap()
}

#[test]
fn rollback_subtree_restores_snapshot_shares_blocks_and_reclaims_delta() {
    let (service, objects) = service_with_objects();
    // 1. Build /ws with files a="A1", b="B1", sub/c="C1" (real object data).
    let ws = service.create_dir_path("/ws", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/ws/sub", 0o755, 1000, 1000)
        .unwrap();
    let a = publish_path_artifact(&service, "/ws/a", "ws/a", &vec![b'1'; 4096]);
    let b = publish_path_artifact(&service, "/ws/b", "ws/b", &vec![b'2'; 4096]);
    let c = publish_path_artifact(&service, "/ws/sub/c", "ws/sub/c", &vec![b'3'; 4096]);
    let a_gen = a.body.as_ref().unwrap().generation;
    let b_gen = b.body.as_ref().unwrap().generation;
    let c_gen = c.body.as_ref().unwrap().generation;
    let a1_block = block_key(a.attr.inode, a_gen, 0, 0);
    let b1_block = block_key(b.attr.inode, b_gen, 0, 0);
    let c1_block = block_key(c.attr.inode, c_gen, 0, 0);
    assert!(objects.head(&a1_block).unwrap().is_some());
    assert!(objects.head(&b1_block).unwrap().is_some());
    assert!(objects.head(&c1_block).unwrap().is_some());

    // 2. Snapshot /ws.
    let snap = service.snapshot_subtree_path("/ws").unwrap();

    // 3. Diverge /ws: rewrite a->"A2", add d="D1", delete b.
    service
        .replace_artifact(PublishArtifact {
            parent: ws.attr.inode,
            name: dname(b"a"),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:a2".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "ws/a2".to_owned(),
            bytes: vec![b'4'; 4096],
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    let d = publish_path_artifact(&service, "/ws/d", "ws/d", &vec![b'5'; 4096]);
    service.remove_file(ws.attr.inode, &dname(b"b")).unwrap();
    // Capture the delta's private blocks so we can assert their fate.
    let a_diverged = service
        .lookup_plus(ws.attr.inode, &dname(b"a"))
        .unwrap()
        .unwrap();
    let a2_block = block_key(
        a_diverged.attr.inode,
        a_diverged.body.as_ref().unwrap().generation,
        0,
        0,
    );
    let d1_block = block_key(d.attr.inode, d.body.as_ref().unwrap().generation, 0, 0);
    assert!(objects.head(&a2_block).unwrap().is_some());
    assert!(objects.head(&d1_block).unwrap().is_some());
    // Pre-rollback /ws is the diverged state.
    assert_eq!(read_artifact_at_path(&service, "/ws/a"), vec![b'4'; 4096]);
    assert!(service
        .lookup_plus(ws.attr.inode, &dname(b"b"))
        .unwrap()
        .is_none());

    // 4. Roll /ws back to the snapshot.
    service
        .rollback_subtree_path("/ws", snap.snapshot_id)
        .unwrap();

    // 5. /ws now exactly matches the snapshot: a="A1", b="B1" (restored), sub/c="C1",
    //    and d is gone. The target keeps its inode identity.
    assert_eq!(
        service.resolve_directory_path("/ws").unwrap(),
        ws.attr.inode,
        "rollback keeps the target root's identity"
    );
    assert_eq!(read_artifact_at_path(&service, "/ws/a"), vec![b'1'; 4096]);
    assert_eq!(read_artifact_at_path(&service, "/ws/b"), vec![b'2'; 4096]);
    assert_eq!(
        read_artifact_at_path(&service, "/ws/sub/c"),
        vec![b'3'; 4096]
    );
    assert!(
        service
            .lookup_plus(ws.attr.inode, &dname(b"d"))
            .unwrap()
            .is_none(),
        "the delta-only file d must be gone after rollback"
    );

    // 6. The rolled-back /ws is identical to a fresh clone of the snapshot: an empty
    //    diff in both directions.
    let reference = service
        .clone_subtree_path_into("/ws", "/reference")
        .unwrap();
    assert!(service
        .diff_subtrees(ws.attr.inode, reference.root)
        .unwrap()
        .is_empty());
    assert!(service
        .diff_subtrees(reference.root, ws.attr.inode)
        .unwrap()
        .is_empty());

    // 7. GC: the discarded delta's private blocks (A2, D1) are reclaimable, while the
    //    restored shared blocks (A1, B1, C1) survive and stay readable. The reference
    //    clone shares the snapshot's blocks, so retire its pin too before reclaiming.
    assert!(service.retire_snapshot(snap.snapshot_id).unwrap());
    assert!(service.retire_snapshot(reference.snapshot_id).unwrap());
    let reclaim = service.cleanup_pending_objects(100).unwrap();
    assert!(
        reclaim.deleted >= 2,
        "delta-only blocks must be reclaimable"
    );
    assert!(
        objects.head(&a2_block).unwrap().is_none(),
        "A2 must be reclaimed"
    );
    assert!(
        objects.head(&d1_block).unwrap().is_none(),
        "D1 must be reclaimed"
    );
    assert!(
        objects.head(&a1_block).unwrap().is_some(),
        "A1 must survive"
    );
    assert!(
        objects.head(&b1_block).unwrap().is_some(),
        "B1 must survive"
    );
    assert!(
        objects.head(&c1_block).unwrap().is_some(),
        "C1 must survive"
    );
    // Restored content is still readable from the shared blocks after reclaim.
    assert_eq!(read_artifact_at_path(&service, "/ws/a"), vec![b'1'; 4096]);
    assert_eq!(read_artifact_at_path(&service, "/ws/b"), vec![b'2'; 4096]);
    assert_eq!(
        read_artifact_at_path(&service, "/ws/sub/c"),
        vec![b'3'; 4096]
    );
}

#[test]
fn rollback_subtree_rejects_foreign_or_missing_snapshot() {
    let service = service();
    service.create_dir_path("/ws", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/other", 0o755, 1000, 1000)
        .unwrap();
    let other_root = service.resolve_directory_path("/other").unwrap();
    let snap = service.snapshot_subtree_path("/other").unwrap();

    // A snapshot of /other cannot roll back /ws.
    assert!(matches!(
        service.rollback_subtree_path("/ws", snap.snapshot_id),
        Err(MetadError::InvalidPath(_))
    ));
    // An unknown snapshot id is not found.
    assert!(matches!(
        service.rollback_subtree_path("/ws", snap.snapshot_id + 9_999),
        Err(MetadError::NotFound)
    ));
    // The rejected target is untouched and the legitimate one still works.
    assert!(service
        .rollback_subtree(other_root, snap.snapshot_id)
        .is_ok());
}

#[test]
fn metadata_backup_then_restore_into_fresh_store_recovers_namespace() {
    let (service, objects) = service_with_objects();
    // Build a small namespace; file bodies land in the shared object store.
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service.create_dir_path("/data", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.bin", "m-a", b"alpha-body");
    publish_path_artifact(&service, "/data/b.bin", "m-b", b"bravo-body-2");
    let want_runs = service.lookup_path("/runs/a.bin").unwrap();
    let want_data = service.lookup_path("/data/b.bin").unwrap();
    assert!(want_runs.is_some());

    let config = MetadataArchiveConfig::new("meta/checkpoints", 3);
    let backup = service.backup_metadata(&config).unwrap();
    assert!(backup.image_bytes > 0);
    assert!(backup.checkpoint_key.starts_with("meta/checkpoints/ckpt/"));

    // Simulate total loss of the metadata node: a brand-new empty Holt store,
    // pointed at the SAME object store (the clone shares the backing map).
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    // The fresh node has no namespace at all (not even a root) until restore.
    let outcome = recovered.restore_metadata(&config).unwrap();
    assert_eq!(
        outcome.as_ref().map(|o| o.checkpoint_key.as_str()),
        Some(backup.checkpoint_key.as_str())
    );

    // Namespace entries (dentry + attr + body descriptor) are identical after
    // restore, and a subsequent create allocates a fresh, non-colliding inode.
    assert_eq!(recovered.lookup_path("/runs/a.bin").unwrap(), want_runs);
    assert_eq!(recovered.lookup_path("/data/b.bin").unwrap(), want_data);
    let fresh = publish_path_artifact(&recovered, "/runs/c.bin", "m-c", b"charlie");
    assert_ne!(fresh.attr.inode, want_runs.unwrap().attr.inode);
}

#[test]
fn restore_metadata_without_archive_returns_none() {
    let (service, _objects) = service_with_objects();
    let config = MetadataArchiveConfig::new("meta/empty", 3);
    assert!(service.restore_metadata(&config).unwrap().is_none());
}

#[test]
fn metadata_backup_retains_only_keep_last_checkpoints() {
    let (service, objects) = service_with_objects();
    let config = MetadataArchiveConfig::new("meta/ck", 2);
    let b1 = service.backup_metadata(&config).unwrap();
    let _b2 = service.backup_metadata(&config).unwrap();
    let b3 = service.backup_metadata(&config).unwrap();
    // keep_last=2: the third backup prunes exactly the first checkpoint object.
    assert_eq!(b3.pruned, 1);
    let pruned = ObjectKey::new(b1.checkpoint_key.clone()).unwrap();
    assert!(objects.head(&pruned).unwrap().is_none());
    let live = ObjectKey::new(b3.checkpoint_key.clone()).unwrap();
    assert!(objects.head(&live).unwrap().is_some());
    // Restore (into a fresh store) always selects the latest checkpoint.
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    let restored = recovered.restore_metadata(&config).unwrap().unwrap();
    assert_eq!(restored.checkpoint_key, b3.checkpoint_key);
}

use nokv_object::{ObjectInfo, ObjectRange};
use std::sync::atomic::AtomicUsize;

/// An [`ObjectStore`] wrapper that injects PUT failures to simulate a crash at a
/// chosen point (e.g. after the checkpoint object is written but before the
/// `CURRENT` pointer is swapped). Reads and deletes always pass through.
#[derive(Clone)]
struct FaultObjectStore {
    inner: MemoryObjectStore,
    fail_put_substring: Arc<Mutex<Option<String>>>,
    injected_put_failures: Arc<AtomicUsize>,
}

impl FaultObjectStore {
    fn new(inner: MemoryObjectStore) -> Self {
        Self {
            inner,
            fail_put_substring: Arc::new(Mutex::new(None)),
            injected_put_failures: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn fail_puts_containing(&self, substring: &str) {
        *self.fail_put_substring.lock().unwrap() = Some(substring.to_owned());
    }

    fn clear_faults(&self) {
        *self.fail_put_substring.lock().unwrap() = None;
    }

    fn injected_put_failures(&self) -> usize {
        self.injected_put_failures.load(Ordering::Relaxed)
    }
}

impl ObjectStore for FaultObjectStore {
    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        if let Some(substring) = self.fail_put_substring.lock().unwrap().clone() {
            if key.as_str().contains(&substring) {
                self.injected_put_failures.fetch_add(1, Ordering::Relaxed);
                return Err(ObjectError::Backend("injected put fault".to_owned()));
            }
        }
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
fn backup_archive_crash_between_checkpoint_and_pointer_is_consistent() {
    let backing = MemoryObjectStore::new();
    let objects = FaultObjectStore::new(backing.clone());
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        objects.clone(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    publish_path_artifact(&service, "/runs/a.bin", "m-a", b"alpha");

    let config = MetadataArchiveConfig::new("meta/ck", 4);
    // First backup completes: CURRENT -> checkpoint #1 (captures only /runs/a.bin).
    let good = service.backup_metadata(&config).unwrap();

    // Add /runs/b.bin, then crash the second backup at the pointer swap: the
    // checkpoint object is written, but the CURRENT manifest PUT fails.
    publish_path_artifact(&service, "/runs/b.bin", "m-b", b"bravo");
    objects.fail_puts_containing("/CURRENT");
    let err = service.backup_metadata(&config).unwrap_err();
    assert!(matches!(err, MetadError::Object(_)));
    assert_eq!(objects.injected_put_failures(), 1);
    objects.clear_faults();

    // CURRENT still names the first, complete checkpoint — never the orphaned
    // second one. Restore into a fresh node recovers the pre-crash state.
    let recovered = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        backing.clone(),
    );
    let restored = recovered.restore_metadata(&config).unwrap().unwrap();
    assert_eq!(restored.checkpoint_key, good.checkpoint_key);
    assert!(recovered.lookup_path("/runs/a.bin").unwrap().is_some());
    assert!(
        recovered.lookup_path("/runs/b.bin").unwrap().is_none(),
        "restore must not expose the torn (uncommitted) checkpoint"
    );

    // With the fault cleared, the archive recovers forward cleanly.
    publish_path_artifact(&service, "/runs/c.bin", "m-c", b"charlie");
    let next = service.backup_metadata(&config).unwrap();
    assert_ne!(next.checkpoint_key, good.checkpoint_key);
}

#[test]
fn object_gc_converges_under_create_delete_churn() {
    let (service, objects) = service_with_objects();
    // Churn: create many small files; delete the even rounds (their blocks must
    // be reclaimed) and keep the odd rounds (their blocks must never be deleted).
    let mut live_keys = Vec::new();
    for round in 0..20u32 {
        let name = DentryName::new(format!("churn-{round}.bin").into_bytes()).unwrap();
        let published = service
            .publish_artifact(artifact_request(
                name.clone(),
                &format!("m{round}"),
                b"payload",
            ))
            .unwrap();
        let body = published.body.clone().unwrap();
        let key = block_key(published.attr.inode, body.generation, 0, 0);
        if round % 2 == 0 {
            service.remove_file(InodeId::root(), &name).unwrap();
        } else {
            live_keys.push(key);
        }
    }

    // Drive GC to convergence with a small per-iteration limit so the queue is
    // drained across several batches rather than one sweep.
    let mut total_deleted = 0;
    let mut guard = 0;
    loop {
        let outcome = service.cleanup_pending_objects(4).unwrap();
        total_deleted += outcome.deleted;
        if outcome.scanned == 0 {
            break;
        }
        guard += 1;
        assert!(guard < 1000, "object GC did not converge");
    }

    // Exactly the 10 deleted files were reclaimed, and the queue is now empty.
    assert_eq!(total_deleted, 10);
    assert_eq!(
        service.cleanup_pending_objects(100).unwrap(),
        PendingObjectCleanupOutcome::default()
    );
    // Every kept file's block survived: owns_block_object_key never over-deleted.
    for key in &live_keys {
        assert!(
            objects.head(key).unwrap().is_some(),
            "live block was wrongly GC'd: {}",
            key.as_str()
        );
    }
}

#[test]
fn fsck_detects_dangling_block_after_out_of_band_object_loss() {
    let (service, objects) = service_with_objects();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let a = publish_path_artifact(&service, "/runs/a.bin", "m-a", b"alpha-body");
    publish_path_artifact(&service, "/runs/b.bin", "m-b", b"bravo-body");

    // A healthy namespace has no dangling references.
    let clean = service.fsck_dangling_blocks(0).unwrap();
    assert!(
        clean.is_consistent(),
        "unexpected dangling: {:?}",
        clean.dangling
    );
    assert_eq!(clean.files_scanned, 2);
    assert!(clean.blocks_checked >= 2);

    // Delete one file's backing object out-of-band: drift that object-first
    // ordering cannot prevent once the metadata is already committed.
    let body = a.body.clone().unwrap();
    let lost = block_key(a.attr.inode, body.generation, 0, 0);
    assert!(objects.delete(&lost).unwrap());

    // fsck flags exactly that reference, and nothing else.
    let report = service.fsck_dangling_blocks(0).unwrap();
    assert!(!report.is_consistent());
    assert_eq!(report.dangling.len(), 1);
    assert_eq!(report.dangling[0].inode, a.attr.inode.get());
    assert_eq!(report.dangling[0].object_key, lost.as_str());
}

/// Set up `/runs/a.bin`, snapshot `/runs` with `lease_ms`, then free the block so
/// it is GC-enqueued *after* the snapshot's read version (i.e. protected while
/// the pin is live). Returns the freed block's object key.
fn snapshot_then_free_block(
    service: &NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    lease_ms: u64,
) -> (SnapshotPin, ObjectKey) {
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let published = publish_path_artifact(service, "/runs/a.bin", "m-a", b"payload");
    let body = published.body.clone().unwrap();
    let block = block_key(published.attr.inode, body.generation, 0, 0);
    let runs = service.resolve_directory_path("/runs").unwrap();
    let pin = service.snapshot_subtree_with_lease(runs, lease_ms).unwrap();
    service.remove_file_path("/runs/a.bin").unwrap();
    (pin, block)
}

#[test]
fn expired_snapshot_pin_does_not_block_object_gc() {
    let (service, objects) = service_with_objects();
    // Lease of 0 ms: the pin is expired the moment GC inspects it.
    let (_pin, block) = snapshot_then_free_block(&service, 0);
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.blocked_by_snapshots, 0);
    assert_eq!(cleanup.deleted, 1);
    assert!(objects.head(&block).unwrap().is_none());
}

#[test]
fn live_snapshot_pin_blocks_object_gc_until_retired() {
    let (service, objects) = service_with_objects();
    let (pin, block) = snapshot_then_free_block(&service, 3_600_000);

    // A live pin protects the freed block.
    let blocked = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(blocked.blocked_by_snapshots, 1);
    assert_eq!(blocked.deleted, 0);
    assert!(objects.head(&block).unwrap().is_some());

    // Retiring it releases the protection.
    assert!(service.retire_snapshot(pin.snapshot_id).unwrap());
    let cleanup = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert!(objects.head(&block).unwrap().is_none());
}

#[test]
fn renew_snapshot_restores_protection_for_an_expired_lease() {
    let (service, objects) = service_with_objects();
    // Pin starts expired, but is renewed before any GC pass reaps it.
    let (pin, block) = snapshot_then_free_block(&service, 0);
    assert!(service.renew_snapshot(pin.snapshot_id, 3_600_000).unwrap());

    let blocked = service.cleanup_pending_objects(100).unwrap();
    assert_eq!(blocked.blocked_by_snapshots, 1);
    assert!(objects.head(&block).unwrap().is_some());

    // Renewing a pin that no longer exists is a no-op.
    assert!(!service
        .renew_snapshot(pin.snapshot_id + 9_999, 1_000)
        .unwrap());
}

#[test]
fn gc_reaps_expired_snapshot_pins_but_keeps_live_ones() {
    let (service, _objects) = service_with_objects();
    service.create_dir_path("/a", 0o755, 1000, 1000).unwrap();
    service.create_dir_path("/b", 0o755, 1000, 1000).unwrap();
    let a = service.resolve_directory_path("/a").unwrap();
    let b = service.resolve_directory_path("/b").unwrap();
    let expired = service.snapshot_subtree_with_lease(a, 0).unwrap();
    let live = service.snapshot_subtree_with_lease(b, 3_600_000).unwrap();

    // An object-GC pass reaps expired pins as housekeeping, keeping live ones.
    service.cleanup_pending_objects(100).unwrap();
    assert!(service.snapshot_pin(expired.snapshot_id).unwrap().is_none());
    assert!(service.snapshot_pin(live.snapshot_id).unwrap().is_some());
}

#[test]
fn clone_is_batched_per_dir_and_diff_is_o_tree() {
    // Pins the measured complexity: clone is batched per source directory (one
    // commit per directory, NOT one per entry — well below the JuiceFS-class
    // per-entry cost), while diff still walks the whole tree (O(tree)) — a one-file
    // change costs the same full-tree walk, so diff is not yet O(changes) (tracked
    // future work).
    let (service, _objects) = service_with_objects();
    let dirs = 6usize;
    let files = 6usize;
    service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
    for d in 0..dirs {
        service
            .create_dir_path(&format!("/base/d{d}"), 0o755, 1000, 1000)
            .unwrap();
        for f in 0..files {
            publish_path_artifact(
                &service,
                &format!("/base/d{d}/f{f}.bin"),
                &format!("m{d}-{f}"),
                b"x",
            );
        }
    }
    let entries = dirs * (1 + files); // each d{d} directory + its files

    // CLONE: batched per source directory — one commit per directory, NOT one per
    // entry — so commit count stays far below the entry count.
    let before = service.metadata_store_stats().commit_total;
    service.clone_subtree_path_into("/base", "/fork").unwrap();
    let clone_commits = service.metadata_store_stats().commit_total - before;
    assert!(
        clone_commits < entries as u64,
        "clone batches per directory, not per entry: entries={entries} commits={clone_commits}"
    );
    assert!(
        clone_commits >= dirs as u64,
        "clone still commits at least once per directory: dirs={dirs} commits={clone_commits}"
    );

    // DIFF (clean): scans scale with the directory count → O(tree).
    let before = service.metadata_store_stats().scan_total;
    let clean = service.diff_subtrees_path("/base", "/fork").unwrap();
    let scans_clean = service.metadata_store_stats().scan_total - before;
    assert!(clean.is_empty(), "a fresh clone diffs clean: {clean:?}");
    assert!(
        scans_clean >= dirs as u64,
        "diff walks every directory: dirs={dirs} scans={scans_clean}"
    );

    // DIFF after ONE change: still the full-tree walk → NOT O(changes).
    publish_path_artifact(&service, "/fork/d0/added.bin", "m-added", b"yy");
    let before = service.metadata_store_stats().scan_total;
    let dirty = service.diff_subtrees_path("/base", "/fork").unwrap();
    let scans_dirty = service.metadata_store_stats().scan_total - before;
    assert_eq!(dirty.len(), 1);
    assert_eq!(dirty[0].kind, SubtreeDeltaKind::Added);
    assert!(
        scans_dirty >= scans_clean,
        "diff cost does not shrink with change count (O(tree), not O(changes)): \
         clean={scans_clean} dirty={scans_dirty}"
    );
}

#[test]
#[ignore = "scale bench; run: cargo test -p nokv-meta --release -- --ignored bench_clone_and_diff_scale --nocapture"]
fn bench_clone_and_diff_scale() {
    use std::time::Instant;
    // The constant behind the O(entries) clone / O(tree) diff, in release. Tells us
    // whether the best-of-N demo (clone N forks of a node_modules-scale tree, diff
    // each) is viable as-is or needs the clone-commit batching first.
    eprintln!("\nentries     clone_ms   us/entry   diff_clean_ms   diff_1change_ms");
    for &(dirs, files) in &[
        (10usize, 10usize),
        (50, 20),
        (100, 50),
        (200, 80),
        (300, 100),
    ] {
        let entries = dirs * (1 + files);
        let (service, _objects) = service_with_objects();
        service.create_dir_path("/base", 0o755, 1000, 1000).unwrap();
        for d in 0..dirs {
            service
                .create_dir_path(&format!("/base/d{d}"), 0o755, 1000, 1000)
                .unwrap();
            for f in 0..files {
                publish_path_artifact(
                    &service,
                    &format!("/base/d{d}/f{f}.bin"),
                    &format!("m{d}-{f}"),
                    b"x",
                );
            }
        }

        let t = Instant::now();
        service.clone_subtree_path_into("/base", "/fork").unwrap();
        let clone_ms = t.elapsed().as_secs_f64() * 1000.0;

        let t = Instant::now();
        let clean = service.diff_subtrees_path("/base", "/fork").unwrap();
        let diff_clean_ms = t.elapsed().as_secs_f64() * 1000.0;
        assert!(clean.is_empty());

        publish_path_artifact(&service, "/fork/d0/added.bin", "m-added", b"yy");
        let t = Instant::now();
        let dirty = service.diff_subtrees_path("/base", "/fork").unwrap();
        let diff_1change_ms = t.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(dirty.len(), 1);

        eprintln!(
            "{entries:7}   {clone_ms:8.2}   {:8.2}   {diff_clean_ms:13.2}   {diff_1change_ms:15.2}",
            clone_ms * 1000.0 / entries as f64
        );
    }
}

#[test]
fn publish_checkpoint_is_atomic_multi_shard_and_range_readable() {
    let (service, _objects) = service_with_objects();
    let ckpt = service.create_dir_path("/ckpt", 0o755, 1000, 1000).unwrap();
    let shards: Vec<CheckpointShard> = (0..5u8)
        .map(|i| CheckpointShard {
            name: DentryName::new(format!("shard{i}").into_bytes()).unwrap(),
            bytes: vec![b'A' + i; 100 + 50 * i as usize],
        })
        .collect();

    // ATOMIC: all 5 shards land together — far fewer commits than 5 separate
    // publishes (one batched commit, not one-per-shard).
    let before = service.metadata_store_stats().commit_total;
    let handle = service
        .publish_checkpoint(ckpt.attr.inode, shards, 1000, 1000)
        .unwrap();
    let commits = service.metadata_store_stats().commit_total - before;
    assert_eq!(handle.shards.len(), 5);
    assert!(
        commits <= 2,
        "checkpoint shards must commit atomically in one batched command, not per shard: commits={commits}"
    );

    // All shards visible after the single publish.
    for i in 0..5u8 {
        assert!(service
            .lookup_path(&format!("/ckpt/shard{i}"))
            .unwrap()
            .is_some());
    }

    // RESHARD-ON-READ: an arbitrary byte range of a shard returns the right bytes
    // (what a differently-parallelized restore reads — a plain range read).
    let s1 = service.lookup_path("/ckpt/shard1").unwrap().unwrap();
    assert_eq!(s1.attr.size, 150);
    assert_eq!(
        service.read_file(s1.attr.inode, 40, 60).unwrap(),
        vec![b'B'; 60]
    );

    // CoW version pin: snapshot the checkpoint dir = a parallelism-agnostic version.
    let pin = service.snapshot_subtree(ckpt.attr.inode).unwrap();
    assert!(service.snapshot_pin(pin.snapshot_id).unwrap().is_some());
}

#[test]
fn open_read_is_zero_write_and_generation_cas_catches_supersede() {
    let (service, _objects) = service_with_objects();
    let data = service.create_dir_path("/data", 0o755, 1000, 1000).unwrap();
    let v1 = publish_path_artifact(&service, "/data/ckpt.bin", "ckpt", b"AAAA");

    // open_read writes ZERO metadata and captures the current (generation, version).
    let before = service.metadata_store_stats().commit_total;
    let lease = service.open_read(v1.attr.inode).unwrap();
    assert_eq!(
        service.metadata_store_stats().commit_total,
        before,
        "read-mode open must create zero metadata state"
    );
    assert_eq!(lease.inode, v1.attr.inode);
    assert_eq!(lease.generation, v1.attr.generation);

    // The leased generation is the reshard-on-read substrate: an arbitrary byte
    // range read against it succeeds (a differently-parallelized consumer's read).
    let plan = service
        .read_file_plan(lease.inode, lease.generation, 1, 2)
        .unwrap();
    assert_eq!(plan.output_len, 2);

    // Supersede the artifact (immutable CoW rewrite -> a new generation).
    let v2 = republish_path_artifact(&service, data.attr.inode, "ckpt.bin", "ckpt", b"BBBBBB");
    assert_ne!(v2.attr.generation, v1.attr.generation);

    // The stale lease's generation no longer matches the live attr: the CAS in
    // read_file_plan fails fast instead of returning stale/reclaimed bytes.
    assert!(matches!(
        service.read_file_plan(lease.inode, lease.generation, 0, 4),
        Err(MetadError::StaleBodyGeneration { .. })
    ));
    // open_read_expecting(old gen) rejects too; a fresh open observes the new gen.
    assert!(matches!(
        service.open_read_expecting(v1.attr.inode, Some(v1.attr.generation)),
        Err(MetadError::StaleBodyGeneration { .. })
    ));
    let lease2 = service.open_read(v1.attr.inode).unwrap();
    assert_eq!(lease2.generation, v2.attr.generation);
    assert!(lease2.read_version >= lease.read_version);
}

/// Externally persist a durable allocator record (simulating a control-plane
/// epoch bump or another incarnation writing the System record).
fn commit_allocator_record(
    service: &NoKvFs<HoltMetadataStore, MemoryObjectStore>,
    version: u64,
    next_inode: u64,
    epoch: u64,
) {
    let commit_version = Version::new(version).unwrap();
    let key = allocator_key(MountId::new(1).unwrap());
    service
        .commit_metadata(MetadataCommand {
            request_id: request_id(
                b"test-alloc-epoch",
                MountId::new(1).unwrap(),
                InodeId::root(),
                commit_version,
            ),
            kind: CommandKind::ReserveAllocator,
            read_version: predecessor(commit_version).unwrap(),
            commit_version,
            primary_family: RecordFamily::System,
            primary_key: key.clone(),
            predicates: Vec::new(),
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key,
                op: MutationOp::Put,
                value: Some(Value(encode_allocator_state(version, next_inode, epoch))),
            }],
            watch: Vec::new(),
        })
        .unwrap();
}

#[test]
fn allocator_epoch_recovers_monotonically_via_fetch_max() {
    let service = service();
    assert_eq!(
        service.allocator_epoch(),
        1,
        "a single owner starts at epoch 1"
    );

    // A control plane bumps the durable epoch (ownership transfer / new incarnation).
    commit_allocator_record(&service, 100, 500, 5);
    service.refresh_allocator_state().unwrap();
    assert_eq!(
        service.allocator_epoch(),
        5,
        "refresh folds in the higher durable epoch"
    );

    // A record carrying a LOWER epoch (a stale incarnation) must never lower it:
    // recovery is fetch_max, so the allocation-authority epoch never regresses —
    // a stale owner can't re-persist itself as current.
    commit_allocator_record(&service, 200, 600, 2);
    service.refresh_allocator_state().unwrap();
    assert_eq!(
        service.allocator_epoch(),
        5,
        "epoch must be monotonic across refresh (fetch_max, not store)"
    );
}
