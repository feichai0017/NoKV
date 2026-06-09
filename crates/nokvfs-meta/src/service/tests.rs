use super::*;
use crate::holtstore::HoltMetadataStore;
use nokvfs_object::MemoryObjectStore;

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
        blocks: vec![BlockDescriptor {
            object_key: block_key(inode, generation, 0, 0).as_str().to_owned(),
            logical_offset: 0,
            object_offset: 0,
            len,
            digest_uri: "sha256:block".to_owned(),
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
fn namespace_cards_report_directory_and_file_metadata() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service
        .publish_artifact(PublishArtifact {
            parent: service.resolve_directory_path("/runs").unwrap(),
            name: DentryName::new(b"metrics.json".to_vec()).unwrap(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "application/json".to_owned(),
            manifest_id: "runs/metrics.json".to_owned(),
            bytes: br#"["loss","accuracy"]"#.to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();

    let runs = service.stat_card("/runs").unwrap().unwrap();
    assert_eq!(runs.path, "/runs");
    assert_eq!(runs.kind, NamespaceCardKind::Directory);
    assert_eq!(runs.entry_count, Some(1));
    assert!(runs.evidence.contains("nokv-native:///runs@generation:"));
    assert!(runs.snapshot_id.is_some());
    assert!(runs.catalog.filterable.iter().any(|field| {
        field.field == NamespaceFindField::name()
            && field.operators.contains(&NamespacePredicateOp::Suffix)
    }));
    assert!(runs
        .catalog
        .sortable
        .contains(&NamespaceSortField::size_bytes()));
    assert!(runs.catalog.facetable.contains(&NamespaceFindField::kind()));
    assert!(runs.catalog.projections.contains(&NamespaceInclude::Body));

    let file = service.stat_card("/runs/metrics.json").unwrap().unwrap();
    assert_eq!(file.kind, NamespaceCardKind::File);
    assert_eq!(file.size_bytes, Some(19));
    assert_eq!(file.body.as_ref().unwrap().content_type, "application/json");
    assert_eq!(
        file.schema.as_ref().unwrap().record_type,
        NamespaceRecordType::JsonArray
    );
    assert_eq!(file.record_count.as_ref().unwrap().count, 2);
    assert_eq!(
        file.record_count.as_ref().unwrap().provenance,
        RecordCountProvenance::StructuredBody
    );
    assert_eq!(
        file.sample,
        vec!["\"loss\"".to_owned(), "\"accuracy\"".to_owned()]
    );
}

#[test]
fn namespace_list_pages_are_cursor_bounded() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service
        .create_files_in_dir_path(
            "/runs",
            vec![
                DentryName::new(b"a.txt".to_vec()).unwrap(),
                DentryName::new(b"b.txt".to_vec()).unwrap(),
            ],
            0o644,
            1000,
            1000,
        )
        .unwrap();

    let page = service
        .list_page(
            "/runs",
            NamespaceListOptions {
                limit: 1,
                ..NamespaceListOptions::default()
            },
        )
        .unwrap();
    assert_eq!(page.path, "/runs");
    assert_eq!(page.entry_count, 2);
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, "a.txt");
    assert_eq!(page.next_cursor, Some("1".to_owned()));
    assert!(page.truncated);

    let next = service
        .list_page(
            "/runs",
            NamespaceListOptions {
                limit: 1,
                cursor: Some(page.next_cursor.unwrap()),
            },
        )
        .unwrap();
    assert_eq!(next.entries[0].name, "b.txt");
    assert_eq!(next.next_cursor, None);
    assert!(!next.truncated);
}

#[test]
fn namespace_list_page_entries_do_not_parse_child_bodies() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let parent = service.resolve_directory_path("/runs").unwrap();
    service
        .publish_artifact(PublishArtifact {
            parent,
            name: DentryName::new(b"bad.json".to_vec()).unwrap(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "application/json".to_owned(),
            manifest_id: "runs/bad.json".to_owned(),
            bytes: b"{not-json".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();

    let before = service.object_stats();
    let page = service
        .list_page(
            "/runs",
            NamespaceListOptions {
                limit: 10,
                ..NamespaceListOptions::default()
            },
        )
        .unwrap();

    assert_eq!(service.object_stats().object_gets, before.object_gets);
    assert_eq!(page.entries.len(), 1);
    let entry = &page.entries[0];
    assert_eq!(entry.name, "bad.json");
    assert_eq!(entry.record_count, None);
    assert_eq!(entry.schema, None);
    assert!(entry.sample.is_empty());
    assert_eq!(entry.body, None);
    assert!(entry.catalog.filterable.is_empty());
    assert!(entry.catalog.sortable.is_empty());
    assert!(entry.catalog.facetable.is_empty());
    assert!(entry.catalog.projections.is_empty());
    assert!(entry.indexed_values.is_empty());
}

#[test]
fn namespace_list_page_entries_do_not_load_registered_index_values() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/runs/run-a", 0o755, 1000, 1000)
        .unwrap();
    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/runs".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("run.status"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: true,
                facetable: true,
            }],
            rows: vec![NamespaceIndexRow {
                path: "/runs/run-a".to_owned(),
                values: vec![NamespaceIndexValue {
                    field: NamespaceFindField::new("run.status"),
                    value: NamespacePredicateValue::String("completed".to_owned()),
                }],
            }],
        })
        .unwrap();

    let page = service
        .list_page(
            "/runs",
            NamespaceListOptions {
                limit: 10,
                ..NamespaceListOptions::default()
            },
        )
        .unwrap();

    assert_eq!(page.entries.len(), 1);
    let entry = &page.entries[0];
    assert_eq!(entry.name, "run-a");
    assert!(entry.catalog.filterable.is_empty());
    assert!(entry.catalog.sortable.is_empty());
    assert!(entry.catalog.facetable.is_empty());
    assert!(entry.catalog.projections.is_empty());
    assert!(entry.indexed_values.is_empty());
}

#[test]
fn namespace_find_uses_declared_predicates_and_sort_fields() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let parent = service.resolve_directory_path("/runs").unwrap();
    for (name, bytes) in [
        ("small.stderr.txt", b"err".as_slice()),
        ("large.stderr.txt", b"larger error".as_slice()),
        ("stdout.txt", b"out".as_slice()),
    ] {
        service
            .publish_artifact(PublishArtifact {
                parent,
                name: DentryName::new(name.as_bytes().to_vec()).unwrap(),
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                content_type: "text/plain".to_owned(),
                manifest_id: format!("runs/{name}"),
                bytes: bytes.to_vec(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();
    }

    let result = service
        .find_paths(NamespaceFindRequest {
            path: "/runs".to_owned(),
            predicates: vec![NamespacePredicate {
                field: NamespaceFindField::name(),
                op: NamespacePredicateOp::Suffix,
                value: NamespacePredicateValue::String("stderr.txt".to_owned()),
            }],
            sort: vec![NamespaceSort {
                field: NamespaceSortField::size_bytes(),
                direction: NamespaceSortDirection::Desc,
            }],
            include: Vec::new(),
            facets: Vec::new(),
            limit: 1,
            cursor: None,
        })
        .unwrap();
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].path, "/runs/large.stderr.txt");
    assert_eq!(result.matches[0].size_bytes, Some(12));
    assert_eq!(result.next_cursor, Some("1".to_owned()));
    assert!(result.truncated);
    assert!(result.scanned_entries >= 3);
    assert!(result.matches[0].body.is_none());
    assert!(result.matches[0].schema.is_none());
    assert!(result.matches[0].sample.is_empty());

    let unsupported = service.find_paths(NamespaceFindRequest {
        path: "/runs".to_owned(),
        predicates: vec![NamespacePredicate {
            field: NamespaceFindField::body_content_type(),
            op: NamespacePredicateOp::GreaterThan,
            value: NamespacePredicateValue::String("text/plain".to_owned()),
        }],
        sort: Vec::new(),
        include: Vec::new(),
        facets: Vec::new(),
        limit: 10,
        cursor: None,
    });
    assert!(matches!(unsupported, Err(MetadError::InvalidQuery(_))));
}

#[test]
fn namespace_grep_scans_file_content_with_generation_evidence_and_cursor() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/runs/run-a", 0o755, 1000, 1000)
        .unwrap();
    let runs = service.resolve_directory_path("/runs").unwrap();
    let run_a = service.resolve_directory_path("/runs/run-a").unwrap();
    let nested = service
        .publish_artifact(PublishArtifact {
            parent: run_a,
            name: DentryName::new(b"stdout.txt".to_vec()).unwrap(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "text/plain".to_owned(),
            manifest_id: "runs/run-a/stdout.txt".to_owned(),
            bytes: b"alpha\nneedle one\nneedle two\n".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    service
        .publish_artifact(PublishArtifact {
            parent: runs,
            name: DentryName::new(b"top.txt".to_vec()).unwrap(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "text/plain".to_owned(),
            manifest_id: "runs/top.txt".to_owned(),
            bytes: b"needle top\n".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    service
        .publish_artifact(PublishArtifact {
            parent: run_a,
            name: DentryName::new(b"binary.bin".to_vec()).unwrap(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "runs/run-a/binary.bin".to_owned(),
            bytes: b"\0needle hidden\n".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();

    let non_recursive = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs".to_owned(),
            pattern: "needle".to_owned(),
            recursive: false,
            limit: 10,
            cursor: None,
            max_files: None,
            max_bytes: None,
        })
        .unwrap();
    assert_eq!(
        non_recursive
            .matches
            .iter()
            .map(|m| m.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/runs/top.txt"]
    );
    assert!(!non_recursive.truncated);

    let first = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs".to_owned(),
            pattern: "needle".to_owned(),
            recursive: true,
            limit: 1,
            cursor: None,
            max_files: None,
            max_bytes: None,
        })
        .unwrap();
    assert_eq!(first.path, "/runs");
    assert_eq!(first.pattern, "needle");
    assert_eq!(first.matches.len(), 1);
    assert_eq!(first.matches[0].path, "/runs/run-a/stdout.txt");
    assert_eq!(first.matches[0].line_number, 2);
    assert_eq!(first.matches[0].snippet, "needle one");
    assert_eq!(first.matches[0].generation, nested.attr.generation);
    assert_eq!(
        first.matches[0].evidence,
        format!(
            "nokv-native:///runs/run-a/stdout.txt@generation:{}#L2",
            nested.attr.generation
        )
    );
    assert!(first.snapshot_id.is_some());
    assert!(first.bytes_read > 0);
    assert!(first.truncated);

    let second = service
        .grep_paths(NamespaceGrepRequest {
            path: "/runs".to_owned(),
            pattern: "needle".to_owned(),
            recursive: true,
            limit: 10,
            cursor: first.next_cursor,
            max_files: None,
            max_bytes: None,
        })
        .unwrap();
    assert_eq!(
        second
            .matches
            .iter()
            .map(|m| (m.path.as_str(), m.line_number))
            .collect::<Vec<_>>(),
        vec![("/runs/run-a/stdout.txt", 3), ("/runs/top.txt", 1)]
    );
    assert!(second
        .matches
        .iter()
        .all(|m| !m.evidence.contains("binary.bin")));
    assert!(!second.truncated);
}

#[test]
fn namespace_find_uses_catalog_field_ids_and_registered_index_values() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    let run_a = service
        .create_dir_path("/runs/run-a", 0o755, 1000, 1000)
        .unwrap();
    let run_b = service
        .create_dir_path("/runs/run-b", 0o755, 1000, 1000)
        .unwrap();

    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/runs".to_owned(),
            fields: vec![
                NamespaceIndexField {
                    field: NamespaceFindField::new("run.status"),
                    operators: vec![NamespacePredicateOp::Eq],
                    sortable: false,
                    facetable: true,
                },
                NamespaceIndexField {
                    field: NamespaceFindField::new("run.script"),
                    operators: vec![NamespacePredicateOp::Eq],
                    sortable: true,
                    facetable: true,
                },
            ],
            rows: vec![
                NamespaceIndexRow {
                    path: "/runs/run-a".to_owned(),
                    values: vec![
                        NamespaceIndexValue {
                            field: NamespaceFindField::new("run.status"),
                            value: NamespacePredicateValue::String("completed".to_owned()),
                        },
                        NamespaceIndexValue {
                            field: NamespaceFindField::new("run.script"),
                            value: NamespacePredicateValue::String("train.py".to_owned()),
                        },
                    ],
                },
                NamespaceIndexRow {
                    path: "/runs/run-b".to_owned(),
                    values: vec![
                        NamespaceIndexValue {
                            field: NamespaceFindField::new("run.status"),
                            value: NamespacePredicateValue::String("cancelled".to_owned()),
                        },
                        NamespaceIndexValue {
                            field: NamespaceFindField::new("run.script"),
                            value: NamespacePredicateValue::String("eval.py".to_owned()),
                        },
                    ],
                },
            ],
        })
        .unwrap();

    let card = service.stat_card("/runs").unwrap().unwrap();
    assert_eq!(
        card.record_count,
        Some(NamespaceRecordCount {
            count: 2,
            provenance: RecordCountProvenance::MaterializedIndex,
        })
    );
    assert!(card.catalog.filterable.iter().any(|capability| {
        capability.field == NamespaceFindField::new("run.status")
            && capability.operators == vec![NamespacePredicateOp::Eq]
    }));
    assert!(card
        .catalog
        .facetable
        .contains(&NamespaceFindField::new("run.script")));
    let status_facet = card
        .catalog
        .facets
        .iter()
        .find(|facet| facet.field == NamespaceFindField::new("run.status"))
        .expect("run.status facet summary must be available");
    assert_eq!(status_facet.distinct_count, 2);
    assert!(!status_facet.truncated);
    assert_eq!(
        status_facet.values,
        vec![
            NamespaceFacetValue {
                value: NamespacePredicateValue::String("cancelled".to_owned()),
                count: 1,
            },
            NamespaceFacetValue {
                value: NamespacePredicateValue::String("completed".to_owned()),
                count: 1,
            },
        ]
    );

    let result = service
        .find_paths(NamespaceFindRequest {
            path: "/runs".to_owned(),
            predicates: vec![NamespacePredicate {
                field: NamespaceFindField::new("run.status"),
                op: NamespacePredicateOp::Eq,
                value: NamespacePredicateValue::String("completed".to_owned()),
            }],
            sort: vec![NamespaceSort {
                field: NamespaceSortField::new("run.script"),
                direction: NamespaceSortDirection::Asc,
            }],
            include: Vec::new(),
            facets: Vec::new(),
            limit: 10,
            cursor: None,
        })
        .unwrap();

    assert_eq!(result.match_count, 1);
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].path, "/runs/run-a");
    assert_eq!(result.matches[0].inode, run_a.attr.inode);
    assert_eq!(
        result.matches[0].indexed_values,
        vec![
            NamespaceIndexValue {
                field: NamespaceFindField::new("run.status"),
                value: NamespacePredicateValue::String("completed".to_owned()),
            },
            NamespaceIndexValue {
                field: NamespaceFindField::new("run.script"),
                value: NamespacePredicateValue::String("train.py".to_owned()),
            },
        ]
    );
    assert_eq!(run_b.attr.file_type, FileType::Directory);
}

#[test]
fn namespace_find_returns_requested_facets_after_predicates_before_pagination() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    for run_id in ["run-a", "run-b", "run-c"] {
        service
            .create_dir_path(&format!("/runs/{run_id}"), 0o755, 1000, 1000)
            .unwrap();
    }

    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/runs".to_owned(),
            fields: vec![
                NamespaceIndexField {
                    field: NamespaceFindField::new("run.status"),
                    operators: vec![NamespacePredicateOp::Eq],
                    sortable: true,
                    facetable: true,
                },
                NamespaceIndexField {
                    field: NamespaceFindField::new("run.script"),
                    operators: vec![NamespacePredicateOp::Eq],
                    sortable: true,
                    facetable: true,
                },
            ],
            rows: vec![
                NamespaceIndexRow {
                    path: "/runs/run-a".to_owned(),
                    values: vec![
                        NamespaceIndexValue {
                            field: NamespaceFindField::new("run.status"),
                            value: NamespacePredicateValue::String("completed".to_owned()),
                        },
                        NamespaceIndexValue {
                            field: NamespaceFindField::new("run.script"),
                            value: NamespacePredicateValue::String("train.py".to_owned()),
                        },
                    ],
                },
                NamespaceIndexRow {
                    path: "/runs/run-b".to_owned(),
                    values: vec![
                        NamespaceIndexValue {
                            field: NamespaceFindField::new("run.status"),
                            value: NamespacePredicateValue::String("completed".to_owned()),
                        },
                        NamespaceIndexValue {
                            field: NamespaceFindField::new("run.script"),
                            value: NamespacePredicateValue::String("eval.py".to_owned()),
                        },
                    ],
                },
                NamespaceIndexRow {
                    path: "/runs/run-c".to_owned(),
                    values: vec![
                        NamespaceIndexValue {
                            field: NamespaceFindField::new("run.status"),
                            value: NamespacePredicateValue::String("cancelled".to_owned()),
                        },
                        NamespaceIndexValue {
                            field: NamespaceFindField::new("run.script"),
                            value: NamespacePredicateValue::String("train.py".to_owned()),
                        },
                    ],
                },
            ],
        })
        .unwrap();

    let result = service
        .find_paths(NamespaceFindRequest {
            path: "/runs".to_owned(),
            predicates: vec![NamespacePredicate {
                field: NamespaceFindField::new("run.status"),
                op: NamespacePredicateOp::Eq,
                value: NamespacePredicateValue::String("completed".to_owned()),
            }],
            sort: vec![],
            include: vec![],
            facets: vec![NamespaceFindField::new("run.script")],
            cursor: None,
            limit: 1,
        })
        .unwrap();

    assert_eq!(result.match_count, 2);
    assert_eq!(result.matches.len(), 1);
    assert_eq!(
        result.facets,
        vec![NamespaceFacetSummary {
            field: NamespaceFindField::new("run.script"),
            values: vec![
                NamespaceFacetValue {
                    value: NamespacePredicateValue::String("eval.py".to_owned()),
                    count: 1,
                },
                NamespaceFacetValue {
                    value: NamespacePredicateValue::String("train.py".to_owned()),
                    count: 1,
                },
            ],
            distinct_count: 2,
            truncated: false,
        }]
    );
}

#[test]
fn namespace_stat_caps_high_cardinality_facet_summaries() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    for index in 0..12 {
        service
            .create_dir_path(&format!("/runs/run-{index:02}"), 0o755, 1000, 1000)
            .unwrap();
    }

    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/runs".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("run.id"),
                operators: vec![NamespacePredicateOp::Eq],
                sortable: true,
                facetable: true,
            }],
            rows: (0..12)
                .map(|index| NamespaceIndexRow {
                    path: format!("/runs/run-{index:02}"),
                    values: vec![NamespaceIndexValue {
                        field: NamespaceFindField::new("run.id"),
                        value: NamespacePredicateValue::String(format!("run-{index:02}")),
                    }],
                })
                .collect(),
        })
        .unwrap();

    let card = service.stat_card("/runs").unwrap().unwrap();
    let run_id_facet = card
        .catalog
        .facets
        .iter()
        .find(|facet| facet.field == NamespaceFindField::new("run.id"))
        .expect("run.id facet summary must be available");

    assert_eq!(run_id_facet.distinct_count, 12);
    assert_eq!(run_id_facet.values.len(), 10);
    assert!(run_id_facet.truncated);
}

#[test]
fn namespace_find_sorts_missing_index_values_last() {
    let service = service();
    service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
    service
        .create_dir_path("/runs/with-metric", 0o755, 1000, 1000)
        .unwrap();
    service
        .create_dir_path("/runs/missing-metric", 0o755, 1000, 1000)
        .unwrap();

    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: "/runs".to_owned(),
            fields: vec![NamespaceIndexField {
                field: NamespaceFindField::new("metric.val_loss.min_scaled"),
                operators: vec![NamespacePredicateOp::GreaterThanOrEqual],
                sortable: true,
                facetable: false,
            }],
            rows: vec![
                NamespaceIndexRow {
                    path: "/runs/with-metric".to_owned(),
                    values: vec![NamespaceIndexValue {
                        field: NamespaceFindField::new("metric.val_loss.min_scaled"),
                        value: NamespacePredicateValue::U64(3),
                    }],
                },
                NamespaceIndexRow {
                    path: "/runs/missing-metric".to_owned(),
                    values: Vec::new(),
                },
            ],
        })
        .unwrap();

    let ascending = service
        .find_paths(NamespaceFindRequest {
            path: "/runs".to_owned(),
            predicates: Vec::new(),
            sort: vec![NamespaceSort {
                field: NamespaceSortField::new("metric.val_loss.min_scaled"),
                direction: NamespaceSortDirection::Asc,
            }],
            include: Vec::new(),
            facets: Vec::new(),
            limit: 1,
            cursor: None,
        })
        .unwrap();
    assert_eq!(ascending.matches[0].path, "/runs/with-metric");

    let descending = service
        .find_paths(NamespaceFindRequest {
            path: "/runs".to_owned(),
            predicates: Vec::new(),
            sort: vec![NamespaceSort {
                field: NamespaceSortField::new("metric.val_loss.min_scaled"),
                direction: NamespaceSortDirection::Desc,
            }],
            include: Vec::new(),
            facets: Vec::new(),
            limit: 2,
            cursor: None,
        })
        .unwrap();
    assert_eq!(descending.matches[0].path, "/runs/with-metric");
    assert_eq!(descending.matches[1].path, "/runs/missing-metric");
}

#[test]
fn namespace_read_defaults_to_structured_json_pages_and_requires_bytes_format_for_raw() {
    let service = service();
    let published = service
        .publish_artifact(PublishArtifact {
            parent: InodeId::root(),
            name: DentryName::new(b"index.json".to_vec()).unwrap(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "application/json".to_owned(),
            manifest_id: "index.json".to_owned(),
            bytes: br#"["a","b","c"]"#.to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();

    let structured = service
        .read_page(
            "/index.json",
            NamespaceReadOptions {
                limit: 2,
                ..NamespaceReadOptions::default()
            },
        )
        .unwrap();
    assert_eq!(structured.format, NamespaceReadFormat::Structured);
    assert_eq!(structured.record_count, Some(3));
    assert_eq!(
        structured.items,
        vec![
            NamespaceReadItem {
                index: 0,
                value_json: "\"a\"".to_owned(),
                evidence: format!(
                    "nokv-native:///index.json@generation:{}#item:0",
                    published.attr.generation
                ),
            },
            NamespaceReadItem {
                index: 1,
                value_json: "\"b\"".to_owned(),
                evidence: format!(
                    "nokv-native:///index.json@generation:{}#item:1",
                    published.attr.generation
                ),
            },
        ]
    );
    assert_eq!(structured.next_cursor, Some("2".to_owned()));
    assert!(structured.truncated);
    assert!(structured.bytes.is_none());

    let bytes = service
        .read_page(
            "/index.json",
            NamespaceReadOptions {
                format: NamespaceReadFormat::Bytes,
                offset: 0,
                limit: 4,
                expected_generation: Some(published.attr.generation),
                ..NamespaceReadOptions::default()
            },
        )
        .unwrap();
    assert_eq!(bytes.format, NamespaceReadFormat::Bytes);
    assert_eq!(bytes.bytes.as_deref(), Some(br#"["a""#.as_slice()));
    assert!(bytes.items.is_empty());
}

#[test]
fn namespace_read_structures_json_object_yaml_mapping_and_text_lines() {
    let service = service();
    for (name, content_type, bytes, expected_type, expected_items) in [
        (
            "object.json",
            "application/json",
            br#"{"status":"completed","script":"train.py"}"#.as_slice(),
            NamespaceRecordType::JsonObject,
            vec![
                r#"{"key":"script","value":"train.py"}"#,
                r#"{"key":"status","value":"completed"}"#,
            ],
        ),
        (
            "params.yaml",
            "application/x-yaml",
            b"alpha: 1\nbeta: true\n".as_slice(),
            NamespaceRecordType::YamlMapping,
            vec![
                r#"{"key":"alpha","value":1}"#,
                r#"{"key":"beta","value":true}"#,
            ],
        ),
        (
            "log.txt",
            "text/plain",
            b"first\nsecond\n".as_slice(),
            NamespaceRecordType::TextLines,
            vec![
                r#"{"line":1,"text":"first"}"#,
                r#"{"line":2,"text":"second"}"#,
            ],
        ),
    ] {
        service
            .publish_artifact(PublishArtifact {
                parent: InodeId::root(),
                name: DentryName::new(name.as_bytes().to_vec()).unwrap(),
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                content_type: content_type.to_owned(),
                manifest_id: name.to_owned(),
                bytes: bytes.to_vec(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();
        let page = service
            .read_page(
                &format!("/{name}"),
                NamespaceReadOptions {
                    limit: 10,
                    ..NamespaceReadOptions::default()
                },
            )
            .unwrap();

        assert_eq!(page.record_type, Some(expected_type));
        assert_eq!(
            page.items
                .iter()
                .map(|item| item.value_json.as_str())
                .collect::<Vec<_>>(),
            expected_items
        );
        assert!(page.bytes.is_none());
    }

    service
        .publish_artifact(PublishArtifact {
            parent: InodeId::root(),
            name: DentryName::new(b"binary.bin".to_vec()).unwrap(),
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:test".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "binary.bin".to_owned(),
            bytes: vec![0, 1, 2],
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap();
    assert!(matches!(
        service.read_page("/binary.bin", NamespaceReadOptions::default()),
        Err(MetadError::InvalidQuery(_))
    ));
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
    assert_eq!(after.watch_write_total - before.watch_write_total, 1);
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
    assert_eq!(entries.len(), 2);
    assert_eq!(after.commit_total - before.commit_total, 1);
    assert_eq!(after.current_put_total - before.current_put_total, 4);
    assert_eq!(after.current_delete_total - before.current_delete_total, 0);
    assert_eq!(after.history_write_total - before.history_write_total, 0);
    assert_eq!(after.watch_write_total - before.watch_write_total, 2);
    assert_eq!(after.dedupe_write_total - before.dedupe_write_total, 1);
    let listed = service.read_dir_plus_path("/runs").unwrap();
    assert_eq!(listed.len(), 2);
}

#[test]
fn publish_artifact_stores_body_then_publishes_metadata() {
    let service = service();
    let name = DentryName::new(b"checkpoint.json".to_vec()).unwrap();
    let published = service
        .publish_artifact(PublishArtifact {
            content_type: "application/json".to_owned(),
            ..artifact_request(name.clone(), "runs/1/checkpoint.json", b"{\"x\":1}")
        })
        .unwrap();

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
    assert!(service.object_stats().cache_hits > before_cache.cache_hits);
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
    assert_eq!(after.object_puts, before.object_puts + 1);
    assert_eq!(replaced.entry.attr.inode, published.attr.inode);
    assert_eq!(
        service.read_file(published.attr.inode, 0, 10).unwrap(),
        b"abcXYZghij"
    );
    let gc = service.cleanup_pending_objects(10).unwrap();
    assert_eq!(gc.attempted, 0);
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
    let name = DentryName::new(b"runs".to_vec()).unwrap();
    let created = service
        .create_dir(InodeId::root(), name.clone(), 0o755, 1000, 1000)
        .unwrap();
    drop(service);

    let reopened = NoKvFs::open_existing(MountId::new(1).unwrap(), metadata, objects).unwrap();
    let events = reopened
        .replay_watch(InodeId::root(), WatchCursor::default(), 100)
        .unwrap();
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
