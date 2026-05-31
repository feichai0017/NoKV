use nokv_proto::nokv::metadata::v1 as metadatapb;

use crate::{decode_metadata_snapshot, encode_metadata_snapshot, MemoryMetadataStore};

fn put_command(
    key: impl Into<Vec<u8>>,
    value: impl Into<Vec<u8>>,
    read_version: u64,
) -> metadatapb::MetadataCommand {
    let key = key.into();
    metadatapb::MetadataCommand {
        request_id: read_version.to_be_bytes().to_vec(),
        read_version,
        predicates: vec![metadatapb::MetadataPredicate {
            key: key.clone(),
            kind: metadatapb::MetadataPredicateKind::NotExists as i32,
            read_version,
            ..Default::default()
        }],
        mutations: vec![metadatapb::MetadataMutation {
            op: metadatapb::metadata_mutation::Op::Put as i32,
            key,
            value: value.into(),
            assertion_not_exist: true,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn overwrite_command(
    key: impl Into<Vec<u8>>,
    value: impl Into<Vec<u8>>,
    read_version: u64,
) -> metadatapb::MetadataCommand {
    let key = key.into();
    metadatapb::MetadataCommand {
        request_id: read_version.to_be_bytes().to_vec(),
        read_version,
        mutations: vec![metadatapb::MetadataMutation {
            op: metadatapb::metadata_mutation::Op::Put as i32,
            key,
            value: value.into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn metadata_command_applies_put_and_reads_by_version() {
    let store = MemoryMetadataStore::new();
    let result = store
        .commit_metadata(&put_command(b"k", b"v1", 10), 11)
        .unwrap();

    assert!(result.error.is_none());
    assert_eq!(result.applied_mutations, 1);
    assert!(
        store
            .get_metadata(&metadatapb::MetadataGetRequest {
                key: b"k".to_vec(),
                version: 10,
                ..Default::default()
            })
            .unwrap()
            .not_found
    );
    let latest = store
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"k".to_vec(),
            version: 11,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(latest.kv.unwrap().value, b"v1");
}

#[test]
fn metadata_predicate_failure_does_not_partially_apply() {
    let store = MemoryMetadataStore::new();
    store
        .commit_metadata(&put_command(b"k", b"v1", 10), 11)
        .unwrap();

    let failed = store
        .commit_metadata(&put_command(b"k", b"v2", 11), 12)
        .unwrap();

    assert!(failed.error.unwrap().already_exists.is_some());
    let latest = store
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"k".to_vec(),
            version: 12,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(latest.kv.unwrap().value, b"v1");
}

#[test]
fn metadata_snapshot_round_trips_committed_versions() {
    let store = MemoryMetadataStore::new();
    store
        .commit_metadata(&put_command(b"a", b"v1", 10), 11)
        .unwrap();
    store
        .commit_metadata(&put_command(b"b", b"v2", 20), 21)
        .unwrap();

    let snapshot = store.export_snapshot().unwrap();
    let restored = MemoryMetadataStore::new();
    restored
        .install_snapshot(decode_metadata_snapshot(&encode_metadata_snapshot(&snapshot)).unwrap())
        .unwrap();

    let scan = restored
        .scan_metadata(&metadatapb::MetadataScanRequest {
            start_key: b"a".to_vec(),
            include_start: true,
            limit: 10,
            version: 21,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        scan.kvs
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect::<Vec<_>>(),
        vec![
            (b"a".to_vec(), b"v1".to_vec()),
            (b"b".to_vec(), b"v2".to_vec())
        ]
    );
}

#[test]
fn metadata_retention_prunes_only_versions_hidden_by_floor_anchor() {
    let store = MemoryMetadataStore::new();
    store
        .commit_metadata(&overwrite_command(b"k", b"v1", 10), 11)
        .unwrap();
    store
        .commit_metadata(&overwrite_command(b"k", b"v2", 20), 21)
        .unwrap();
    store
        .commit_metadata(&overwrite_command(b"k", b"v3", 30), 31)
        .unwrap();

    let pruned = store.prune_metadata_versions(25).unwrap();
    assert_eq!(pruned.retention_floor, 25);
    assert_eq!(pruned.retained_anchor_versions, 1);
    assert_eq!(pruned.pruned_versions, 1);

    let at_floor = store
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"k".to_vec(),
            version: 25,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(at_floor.kv.unwrap().value, b"v2");
    let latest = store
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"k".to_vec(),
            version: 31,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(latest.kv.unwrap().value, b"v3");

    let snapshot = store.export_snapshot().unwrap();
    assert_eq!(
        snapshot
            .writes
            .into_iter()
            .map(|write| write.commit_version)
            .collect::<Vec<_>>(),
        vec![21, 31]
    );
}

#[test]
fn metadata_retention_keeps_only_version_when_it_is_the_floor_anchor() {
    let store = MemoryMetadataStore::new();
    store
        .commit_metadata(&overwrite_command(b"k", b"v1", 10), 11)
        .unwrap();

    let pruned = store.prune_metadata_versions(25).unwrap();
    assert_eq!(pruned.retained_anchor_versions, 1);
    assert_eq!(pruned.pruned_versions, 0);

    let at_floor = store
        .get_metadata(&metadatapb::MetadataGetRequest {
            key: b"k".to_vec(),
            version: 25,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(at_floor.kv.unwrap().value, b"v1");
}
