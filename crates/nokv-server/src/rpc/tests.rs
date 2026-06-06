use super::*;
use crate::server::tests::test_server;
use nokv_cluster::MetadataRaftRpcClient;
use nokv_protocol::{
    decode_envelope, encode_request, WireBlockDescriptor, WireBodyDescriptor, WireChunkManifest,
    WireDentryWithAttr, WireMetadataError, WireMetadataRaftLeaderId, WireMetadataRaftVote,
    WireMetadataRaftVoteRequest, WireSliceManifest, WireStagedObjectSet, WireUpdateAttr,
    WireXattrSetMode,
};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;

fn request_envelope(server: &Server, request: MetadataRpcRequest) -> MetadataRpcEnvelope {
    let body = encode_request(&request).unwrap();
    let response = handle_binary_rpc(server, &body).unwrap();
    decode_envelope(&response).unwrap()
}

fn expect_dentry(envelope: MetadataRpcEnvelope) -> WireDentryWithAttr {
    assert!(envelope.ok, "unexpected error envelope: {envelope:?}");
    match envelope.result.unwrap() {
        MetadataRpcResult::Dentry { entry: Some(entry) } => *entry,
        other => panic!("unexpected dentry result: {other:?}"),
    }
}

fn expect_attr(envelope: MetadataRpcEnvelope) -> nokv_protocol::WireInodeAttr {
    assert!(envelope.ok, "unexpected error envelope: {envelope:?}");
    match envelope.result.unwrap() {
        MetadataRpcResult::InodeAttr { attr: Some(attr) } => attr,
        other => panic!("unexpected attr result: {other:?}"),
    }
}

fn metadata_raft_vote_request(term: u64, voted_for: u64) -> WireMetadataRaftVoteRequest {
    WireMetadataRaftVoteRequest {
        vote: WireMetadataRaftVote {
            leader_id: WireMetadataRaftLeaderId {
                term,
                voted_for: Some(voted_for),
            },
            committed: false,
        },
        last_log_id: None,
    }
}

#[test]
fn rpc_creates_and_lists_directory() {
    let server = test_server();
    let created = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDir {
            parent: 1,
            name: "runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    assert_eq!(created.dentry.name_hex, "72756e73");

    let envelope = request_envelope(&server, MetadataRpcRequest::ReadDirPlus { parent: 1 });
    let entries = match envelope.result.unwrap() {
        MetadataRpcResult::Dentries { entries } => entries,
        other => panic!("unexpected readdir result: {other:?}"),
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name_hex, "72756e73");
}

#[test]
fn rpc_supports_remote_fuse_inode_operations() {
    let server = test_server();
    let file = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateFile {
            parent: 1,
            name: "checkpoint.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ));
    let updated = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::UpdateAttrs {
            parent: 1,
            name: "checkpoint.bin".to_owned(),
            changes: WireUpdateAttr {
                size: Some(128),
                ..WireUpdateAttr::default()
            },
        },
    ));
    assert_eq!(updated.attr.inode, file.attr.inode);
    assert_eq!(updated.attr.size, 128);

    let root = expect_attr(request_envelope(
        &server,
        MetadataRpcRequest::UpdateRootAttrs {
            changes: WireUpdateAttr {
                mode: Some(0o700),
                ..WireUpdateAttr::default()
            },
        },
    ));
    assert_eq!(root.mode, 0o700);

    let set_xattr = request_envelope(
        &server,
        MetadataRpcRequest::SetXattr {
            inode: file.attr.inode,
            name_hex: "757365722e636f6d6d656e74".to_owned(),
            value: b"training checkpoint".to_vec(),
            mode: WireXattrSetMode::Create,
        },
    );
    assert!(set_xattr.ok, "unexpected setxattr error: {set_xattr:?}");
    assert!(matches!(set_xattr.result, Some(MetadataRpcResult::Unit)));

    let get_xattr = request_envelope(
        &server,
        MetadataRpcRequest::GetXattr {
            inode: file.attr.inode,
            name_hex: "757365722e636f6d6d656e74".to_owned(),
        },
    );
    match get_xattr.result.unwrap() {
        MetadataRpcResult::XattrValue { value } => {
            assert_eq!(value, Some(b"training checkpoint".to_vec()));
        }
        other => panic!("unexpected getxattr result: {other:?}"),
    }

    let list_xattr = request_envelope(
        &server,
        MetadataRpcRequest::ListXattr {
            inode: file.attr.inode,
        },
    );
    match list_xattr.result.unwrap() {
        MetadataRpcResult::XattrNames { names_hex } => {
            assert_eq!(names_hex, vec!["757365722e636f6d6d656e74"]);
        }
        other => panic!("unexpected listxattr result: {other:?}"),
    }

    let remove_xattr = request_envelope(
        &server,
        MetadataRpcRequest::RemoveXattr {
            inode: file.attr.inode,
            name_hex: "757365722e636f6d6d656e74".to_owned(),
        },
    );
    assert!(
        matches!(remove_xattr.result, Some(MetadataRpcResult::Unit)),
        "unexpected removexattr result: {remove_xattr:?}"
    );

    let special = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateSpecialNode {
            parent: 1,
            name: "accelerator0".to_owned(),
            file_type: "char_device".to_owned(),
            mode: 0o660,
            rdev: 0x1234,
            uid: 0,
            gid: 44,
        },
    ));
    assert_eq!(special.dentry.child_type, "char_device");
    assert_eq!(special.attr.file_type, "char_device");
    assert_eq!(special.attr.rdev, 0x1234);

    let snapshot = request_envelope(&server, MetadataRpcRequest::SnapshotSubtree { root: 1 });
    let snapshot_id = match snapshot.result.unwrap() {
        MetadataRpcResult::Snapshot { snapshot } => snapshot.snapshot_id,
        other => panic!("unexpected snapshot result: {other:?}"),
    };
    let pin = request_envelope(&server, MetadataRpcRequest::SnapshotPin { snapshot_id });
    match pin.result.unwrap() {
        MetadataRpcResult::SnapshotPin {
            snapshot: Some(snapshot),
        } => assert_eq!(snapshot.snapshot_id, snapshot_id),
        other => panic!("unexpected snapshot pin result: {other:?}"),
    }
}

#[test]
fn rpc_create_file_prepared_returns_entry_and_write_token() {
    let server = test_server();
    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::CreateFilePrepared {
            parent: 1,
            name: "checkpoint.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    );
    let (entry, prepared) = match envelope.result.unwrap() {
        MetadataRpcResult::CreatedPreparedArtifact { entry, prepared } => (*entry, prepared),
        other => panic!("unexpected create prepared result: {other:?}"),
    };
    assert_eq!(entry.dentry.name_hex, "636865636b706f696e742e62696e");
    assert_eq!(entry.attr.file_type, "file");
    assert_eq!(prepared.parent, 1);
    assert_eq!(prepared.name, "checkpoint.bin");
    assert_eq!(prepared.inode, entry.attr.inode);
    assert!(prepared.replace);
    assert_eq!(prepared.old_generation, None);
    assert!(prepared.generation > entry.attr.generation);
}

#[test]
fn rpc_supports_remote_fuse_advisory_locks() {
    let server = test_server();
    let file = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateFile {
            parent: 1,
            name: "locked.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ));
    let set = request_envelope(
        &server,
        MetadataRpcRequest::SetAdvisoryLock {
            inode: file.attr.inode,
            owner: 7,
            start: 0,
            end: 99,
            kind: "write".to_owned(),
            pid: 700,
            wait: false,
        },
    );
    assert!(set.ok, "unexpected set lock error: {set:?}");

    let get = request_envelope(
        &server,
        MetadataRpcRequest::GetAdvisoryLock {
            inode: file.attr.inode,
            owner: 8,
            start: 10,
            end: 20,
            kind: "read".to_owned(),
            pid: 800,
        },
    );
    match get.result.unwrap() {
        MetadataRpcResult::AdvisoryLock { lock: Some(lock) } => {
            assert_eq!(lock.owner, 7);
            assert_eq!(lock.kind, "write");
        }
        other => panic!("unexpected lock result: {other:?}"),
    }

    let conflict = request_envelope(
        &server,
        MetadataRpcRequest::SetAdvisoryLock {
            inode: file.attr.inode,
            owner: 8,
            start: 10,
            end: 20,
            kind: "read".to_owned(),
            pid: 800,
            wait: false,
        },
    );
    assert!(!conflict.ok);
    assert!(matches!(
        conflict.error_kind,
        Some(WireMetadataError::LockConflict { .. })
    ));
}

#[test]
fn rpc_accepts_metadata_raft_vote_on_store() {
    let server = test_server();
    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::MetadataRaftVote {
            request: metadata_raft_vote_request(2, 2),
        },
    );

    assert!(envelope.ok, "unexpected metadata raft error: {envelope:?}");
    let response = match envelope.result.unwrap() {
        MetadataRpcResult::MetadataRaftVote { response } => response,
        other => panic!("unexpected metadata raft vote result: {other:?}"),
    };
    assert!(response.vote.leader_id.term >= 1);
}

#[test]
fn rpc_read_path_plan_returns_metadata_and_object_plan() {
    let server = test_server();
    let prepared = server
        .service()
        .prepare_artifact_create_path("/artifact.bin")
        .unwrap();
    let published = server
        .service()
        .publish_prepared_artifact(
            prepared.clone(),
            nokv_types::BodyDescriptor {
                producer: "rpc-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                size: 12,
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "artifact.bin".to_owned(),
                generation: prepared.generation,
                chunk_size: nokv_object::DEFAULT_CHUNK_SIZE,
                block_size: nokv_object::DEFAULT_BLOCK_SIZE as u64,
            },
            vec![nokv_types::ChunkManifest {
                chunk_index: 0,
                logical_offset: 0,
                len: 12,
                slices: vec![nokv_types::SliceManifest {
                    slice_id: 1,
                    logical_offset: 0,
                    len: 12,
                    blocks: vec![nokv_types::BlockDescriptor {
                        object_key: "blocks/demo".to_owned(),
                        logical_offset: 0,
                        object_offset: 0,
                        len: 12,
                        digest_uri: "sha256:test".to_owned(),
                    }],
                }],
            }],
            0o644,
            1000,
            1000,
        )
        .unwrap()
        .entry;

    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::ReadPathPlan {
            path: "/artifact.bin".to_owned(),
            offset: 6,
            len: 6,
            expected_generation: Some(published.attr.generation),
        },
    );
    assert!(envelope.ok, "unexpected read path plan error: {envelope:?}");
    let MetadataRpcResult::PathReadPlan { metadata, plan } = envelope.result.unwrap() else {
        panic!("unexpected read path plan result")
    };
    assert_eq!(metadata.attr.inode, published.attr.inode.get());
    assert_eq!(metadata.body.unwrap().digest_uri, "sha256:test");
    assert_eq!(plan.output_len, 6);
    assert_eq!(plan.blocks.len(), 1);
    assert_eq!(plan.blocks[0].object_offset, 6);
    assert_eq!(plan.blocks[0].len, 6);

    let stale = request_envelope(
        &server,
        MetadataRpcRequest::ReadPathPlan {
            path: "/artifact.bin".to_owned(),
            offset: 0,
            len: 1,
            expected_generation: Some(published.attr.generation - 1),
        },
    );
    assert!(!stale.ok);
    assert!(matches!(
        stale.error_kind,
        Some(WireMetadataError::StaleBodyGeneration { .. })
    ));
}

#[test]
fn rpc_path_ops_resolve_on_server_side() {
    let server = test_server();
    let dir = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    assert_eq!(dir.dentry.name_hex, "72756e73");
    let file = expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateFilePath {
            path: "/runs/checkpoint.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ));
    assert_eq!(file.dentry.name_hex, "636865636b706f696e742e62696e");

    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::ReadDirPlusPath {
            path: "/runs".to_owned(),
        },
    );
    let entries = match envelope.result.unwrap() {
        MetadataRpcResult::Dentries { entries } => entries,
        other => panic!("unexpected readdir result: {other:?}"),
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name_hex, "636865636b706f696e742e62696e");
}

#[test]
fn rpc_lists_directory_pages_with_name_cursor() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    for name in ["a.bin", "b.bin", "c.bin"] {
        expect_dentry(request_envelope(
            &server,
            MetadataRpcRequest::CreateFilePath {
                path: format!("/runs/{name}"),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        ));
    }

    let first = request_envelope(
        &server,
        MetadataRpcRequest::ReadDirPlusPathPage {
            path: "/runs".to_owned(),
            after_name_hex: None,
            limit: 2,
        },
    );
    let (entries, cursor) = match first.result.unwrap() {
        MetadataRpcResult::DentriesPage {
            entries,
            next_name_hex,
        } => (entries, next_name_hex),
        other => panic!("unexpected page result: {other:?}"),
    };
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].dentry.name_hex, "612e62696e");
    assert_eq!(entries[1].dentry.name_hex, "622e62696e");
    assert_eq!(cursor.as_deref(), Some("622e62696e"));

    let second = request_envelope(
        &server,
        MetadataRpcRequest::ReadDirPlusPathPage {
            path: "/runs".to_owned(),
            after_name_hex: cursor,
            limit: 2,
        },
    );
    let (entries, cursor) = match second.result.unwrap() {
        MetadataRpcResult::DentriesPage {
            entries,
            next_name_hex,
        } => (entries, next_name_hex),
        other => panic!("unexpected page result: {other:?}"),
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name_hex, "632e62696e");
    assert_eq!(cursor, None);
}

#[test]
fn rpc_lists_indexed_path_pages_without_plain_namespace_entries() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateFilePath {
            path: "/runs/plain.txt".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ));
    let prepared = match request_envelope(
        &server,
        MetadataRpcRequest::PrepareArtifactPath {
            path: "/runs/metrics.json".to_owned(),
            replace: false,
        },
    )
    .result
    .unwrap()
    {
        MetadataRpcResult::PreparedArtifact { prepared } => prepared,
        other => panic!("unexpected prepare result: {other:?}"),
    };
    let published = request_envelope(
        &server,
        MetadataRpcRequest::PublishPreparedArtifact {
            body: Box::new(WireBodyDescriptor {
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:metrics".to_owned(),
                size: 2,
                content_type: "application/json".to_owned(),
                manifest_id: "metrics.json".to_owned(),
                generation: prepared.generation,
                chunk_size: 64 * 1024 * 1024,
                block_size: 4 * 1024 * 1024,
            }),
            chunks: vec![WireChunkManifest {
                chunk_index: 0,
                logical_offset: 0,
                len: 2,
                slices: vec![WireSliceManifest {
                    slice_id: 1,
                    logical_offset: 0,
                    len: 2,
                    blocks: vec![WireBlockDescriptor {
                        object_key: format!("blocks/1/{}/{}", prepared.inode, prepared.generation),
                        logical_offset: 0,
                        object_offset: 0,
                        len: 2,
                        digest_uri: "sha256:block".to_owned(),
                    }],
                }],
            }],
            prepared,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    );
    assert!(published.ok, "unexpected publish error: {published:?}");

    let page = request_envelope(
        &server,
        MetadataRpcRequest::ReadIndexedPathPage {
            path: "/runs".to_owned(),
            after_name_hex: None,
            limit: 100,
        },
    );
    let entries = match page.result.unwrap() {
        MetadataRpcResult::DentriesPage {
            entries,
            next_name_hex,
        } => {
            assert_eq!(next_name_hex, None);
            entries
        }
        other => panic!("unexpected indexed page result: {other:?}"),
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name_hex, "6d6574726963732e6a736f6e");
}

#[test]
fn rpc_batch_preserves_ordered_per_request_results() {
    let server = test_server();
    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::CreateDirPath {
                    path: "/runs".to_owned(),
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/a.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/a.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
            ],
        },
    );
    let results = match envelope.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected batch result: {other:?}"),
    };
    assert_eq!(results.len(), 3);
    assert!(results[0].ok);
    assert!(results[1].ok);
    assert!(!results[2].ok);
    assert!(results[2].error.is_some());
    assert_eq!(
        results[2].error_kind,
        Some(WireMetadataError::PredicateFailed)
    );
}

#[test]
fn rpc_batch_coalesces_same_parent_create_dir_paths() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    let before = server.service().metadata_store_stats();

    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::CreateDirPath {
                    path: "/runs/a".to_owned(),
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateDirPath {
                    path: "/runs/b".to_owned(),
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                },
            ],
        },
    );

    let results = match envelope.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected batch result: {other:?}"),
    };
    let after = server.service().metadata_store_stats();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| result.ok));
    assert_eq!(after.commit_total - before.commit_total, 1);
    let listed = request_envelope(
        &server,
        MetadataRpcRequest::ReadDirPlusPath {
            path: "/runs".to_owned(),
        },
    );
    let entries = match listed.result.unwrap() {
        MetadataRpcResult::Dentries { entries } => entries,
        other => panic!("unexpected readdir result: {other:?}"),
    };
    assert_eq!(entries.len(), 2);
}

#[test]
fn rpc_batch_coalesces_same_parent_remove_file_paths() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    let created = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/a.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/b.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/keep.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
            ],
        },
    );
    let created = match created.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected create batch result: {other:?}"),
    };
    assert!(created.iter().all(|result| result.ok));
    let before = server.service().metadata_store_stats();

    let removed = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::RemoveFilePath {
                    path: "/runs/a.bin".to_owned(),
                },
                MetadataRpcRequest::RemoveFilePath {
                    path: "/runs/b.bin".to_owned(),
                },
            ],
        },
    );

    let results = match removed.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected remove batch result: {other:?}"),
    };
    let after = server.service().metadata_store_stats();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| result.ok));
    assert_eq!(after.commit_total - before.commit_total, 2);
    assert_eq!(after.atomic_apply_total - before.atomic_apply_total, 1);
    assert_eq!(
        after.atomic_apply_command_total - before.atomic_apply_command_total,
        2
    );
    let listed = request_envelope(
        &server,
        MetadataRpcRequest::ReadDirPlusPath {
            path: "/runs".to_owned(),
        },
    );
    let entries = match listed.result.unwrap() {
        MetadataRpcResult::Dentries { entries } => entries,
        other => panic!("unexpected readdir result: {other:?}"),
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name_hex, "6b6565702e62696e");
}

#[test]
fn rpc_batch_keeps_duplicate_remove_file_paths_independent() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateFilePath {
            path: "/runs/a.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ));

    let removed = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::RemoveFilePath {
                    path: "/runs/a.bin".to_owned(),
                },
                MetadataRpcRequest::RemoveFilePath {
                    path: "/runs/a.bin".to_owned(),
                },
            ],
        },
    );

    let results = match removed.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected remove batch result: {other:?}"),
    };
    assert_eq!(results.len(), 2);
    assert!(results[0].ok);
    assert!(!results[1].ok);
    assert_eq!(results[1].error_kind, Some(WireMetadataError::NotFound));
}

#[test]
fn rpc_batch_coalesces_same_parent_remove_empty_dir_paths() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    let created = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::CreateDirPath {
                    path: "/runs/a".to_owned(),
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateDirPath {
                    path: "/runs/b".to_owned(),
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateDirPath {
                    path: "/runs/keep".to_owned(),
                    mode: 0o755,
                    uid: 1000,
                    gid: 1000,
                },
            ],
        },
    );
    let created = match created.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected create batch result: {other:?}"),
    };
    assert!(created.iter().all(|result| result.ok));
    let before = server.service().metadata_store_stats();

    let removed = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::RemoveEmptyDirPath {
                    path: "/runs/a".to_owned(),
                },
                MetadataRpcRequest::RemoveEmptyDirPath {
                    path: "/runs/b".to_owned(),
                },
            ],
        },
    );

    let results = match removed.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected rmdir batch result: {other:?}"),
    };
    let after = server.service().metadata_store_stats();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| result.ok));
    assert_eq!(after.commit_total - before.commit_total, 2);
    assert_eq!(after.atomic_apply_total - before.atomic_apply_total, 1);
    assert_eq!(
        after.atomic_apply_command_total - before.atomic_apply_command_total,
        2
    );
    let listed = request_envelope(
        &server,
        MetadataRpcRequest::ReadDirPlusPath {
            path: "/runs".to_owned(),
        },
    );
    let entries = match listed.result.unwrap() {
        MetadataRpcResult::Dentries { entries } => entries,
        other => panic!("unexpected readdir result: {other:?}"),
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name_hex, "6b656570");
}

#[test]
fn rpc_batch_keeps_duplicate_remove_empty_dir_paths_independent() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs/a".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));

    let removed = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::RemoveEmptyDirPath {
                    path: "/runs/a".to_owned(),
                },
                MetadataRpcRequest::RemoveEmptyDirPath {
                    path: "/runs/a".to_owned(),
                },
            ],
        },
    );

    let results = match removed.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected rmdir batch result: {other:?}"),
    };
    assert_eq!(results.len(), 2);
    assert!(results[0].ok);
    assert!(!results[1].ok);
    assert_eq!(results[1].error_kind, Some(WireMetadataError::NotFound));
}

#[test]
fn rpc_batch_coalesces_multi_parent_create_files_into_metadata_batch() {
    let server = test_server();
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs/a".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));
    expect_dentry(request_envelope(
        &server,
        MetadataRpcRequest::CreateDirPath {
            path: "/runs/b".to_owned(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ));

    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::Batch {
            requests: vec![
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/a/one.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
                MetadataRpcRequest::CreateFilePath {
                    path: "/runs/b/two.bin".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                },
            ],
        },
    );

    let results = match envelope.result.unwrap() {
        MetadataRpcResult::Batch { results } => results,
        other => panic!("unexpected batch result: {other:?}"),
    };
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| result.ok));
    assert!(server.stats_json().contains("\"atomic_apply_max_batch\":2"));
}

#[test]
fn framed_rpc_coalesces_pipelined_create_frames_into_metadata_batch() {
    let server = Arc::new(test_server());
    server
        .service()
        .create_dir_path("/runs", 0o755, 1000, 1000)
        .unwrap();
    server
        .service()
        .create_dir_path("/runs/a", 0o755, 1000, 1000)
        .unwrap();
    server
        .service()
        .create_dir_path("/runs/b", 0o755, 1000, 1000)
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(FRAMED_RPC_MAGIC).unwrap();
    write_frame(
        &mut client,
        1,
        0,
        &encode_request(&MetadataRpcRequest::CreateFilePath {
            path: "/runs/a/one.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(
        &mut client,
        2,
        0,
        &encode_request(&MetadataRpcRequest::CreateFilePath {
            path: "/runs/b/two.bin".to_owned(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        })
        .unwrap(),
    )
    .unwrap();
    client.shutdown(std::net::Shutdown::Write).unwrap();

    let server_thread = {
        let server = Arc::clone(&server);
        thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            crate::http::handle_stream(server, stream).unwrap();
        })
    };

    let first = read_frame(&mut client).unwrap().unwrap();
    let second = read_frame(&mut client).unwrap().unwrap();
    assert_eq!(first.request_id, 1);
    assert_eq!(second.request_id, 2);
    assert!(decode_envelope(&first.payload).unwrap().ok);
    assert!(decode_envelope(&second.payload).unwrap().ok);
    server_thread.join().unwrap();

    assert!(server.stats_json().contains("\"atomic_apply_max_batch\":2"));
}

#[test]
fn metadata_raft_framed_rpc_client_sends_vote() {
    let server = Arc::new(test_server());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_thread = {
        let server = Arc::clone(&server);
        thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            crate::http::handle_stream(server, stream).unwrap();
        })
    };

    let client = MetadataRaftFramedRpcClient::default();
    let response = client
        .vote_metadata_raft(1, &addr.to_string(), metadata_raft_vote_request(2, 2))
        .unwrap();

    assert!(response.vote.leader_id.term >= 1);
    drop(client);
    server_thread.join().unwrap();
}

#[test]
fn rpc_reports_predicate_errors_without_panicking() {
    let server = test_server();
    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::RemoveEmptyDir {
            parent: 1,
            name: "missing".to_owned(),
        },
    );
    assert!(!envelope.ok);
    assert!(envelope.error.is_some());
}

#[test]
fn rpc_rejects_malformed_binary_request() {
    let server = test_server();
    let response = handle_binary_rpc(&server, b"not-msgpack").unwrap();
    let envelope = decode_envelope(&response).unwrap();
    assert!(!envelope.ok);
    assert!(matches!(
        envelope.error_kind,
        Some(WireMetadataError::Protocol { .. })
    ));
}

#[test]
fn framed_rpc_worker_count_is_bounded() {
    let workers = default_framed_rpc_worker_count();
    assert!(workers >= MIN_FRAMED_RPC_WORKERS);
    assert!(workers <= MAX_FRAMED_RPC_WORKERS);
}

#[test]
fn framed_rpc_client_reuses_peer_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let mut request_ids = Vec::new();
        for _ in 0..2 {
            let frame = read_frame(&mut stream).unwrap().unwrap();
            request_ids.push(frame.request_id);
            let response = encode_envelope(&MetadataRpcEnvelope {
                ok: true,
                result: Some(MetadataRpcResult::RetiredSnapshot { retired: false }),
                error: None,
                error_kind: None,
                metadata_position: None,
            })
            .unwrap();
            write_frame(&mut stream, frame.request_id, frame.flags, &response).unwrap();
        }
        request_ids
    });

    let client = FramedRpcClient::new(addr);
    let first = client
        .call(&MetadataRpcRequest::RetireSnapshot { snapshot_id: 1 })
        .unwrap();
    let second = client
        .call(&MetadataRpcRequest::RetireSnapshot { snapshot_id: 2 })
        .unwrap();
    assert!(first.ok);
    assert!(second.ok);
    assert_eq!(server_thread.join().unwrap(), vec![1, 2]);
}

#[test]
fn rpc_prepares_and_publishes_artifact_manifest() {
    let server = test_server();
    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::PrepareArtifact {
            parent: 1,
            name: "artifact.bin".to_owned(),
            replace: false,
        },
    );
    let prepared = match envelope.result.unwrap() {
        MetadataRpcResult::PreparedArtifact { prepared } => prepared,
        other => panic!("unexpected prepare result: {other:?}"),
    };
    let request = MetadataRpcRequest::PublishPreparedArtifact {
        body: Box::new(WireBodyDescriptor {
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:body".to_owned(),
            size: 4,
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "artifact.bin".to_owned(),
            generation: prepared.generation,
            chunk_size: 64 * 1024 * 1024,
            block_size: 4 * 1024 * 1024,
        }),
        chunks: vec![WireChunkManifest {
            chunk_index: 0,
            logical_offset: 0,
            len: 4,
            slices: vec![WireSliceManifest {
                slice_id: 1,
                logical_offset: 0,
                len: 4,
                blocks: vec![WireBlockDescriptor {
                    object_key: format!("blocks/1/{}/{}", prepared.inode, prepared.generation),
                    logical_offset: 0,
                    object_offset: 0,
                    len: 4,
                    digest_uri: "sha256:block".to_owned(),
                }],
            }],
        }],
        prepared,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    };
    let envelope = request_envelope(&server, request);
    let result = match envelope.result.unwrap() {
        MetadataRpcResult::RenameReplace { entry, replaced } => (entry, replaced),
        other => panic!("unexpected publish result: {other:?}"),
    };
    assert_eq!(result.0.dentry.name_hex, "61727469666163742e62696e");
    assert!(result.1.is_none());
}

#[test]
fn rpc_staged_session_publish_preserves_old_prefix_on_shrink() {
    let server = test_server();
    let prepared = match request_envelope(
        &server,
        MetadataRpcRequest::PrepareArtifact {
            parent: 1,
            name: "checkpoint.bin".to_owned(),
            replace: false,
        },
    )
    .result
    .unwrap()
    {
        MetadataRpcResult::PreparedArtifact { prepared } => prepared,
        other => panic!("unexpected prepare result: {other:?}"),
    };
    let old_object_key = format!("blocks/1/{}/{}", prepared.inode, prepared.generation);
    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::PublishPreparedArtifact {
            body: Box::new(WireBodyDescriptor {
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:old".to_owned(),
                size: 14,
                content_type: "application/octet-stream".to_owned(),
                manifest_id: "checkpoint.bin".to_owned(),
                generation: prepared.generation,
                chunk_size: 64 * 1024 * 1024,
                block_size: 4 * 1024 * 1024,
            }),
            chunks: vec![WireChunkManifest {
                chunk_index: 0,
                logical_offset: 0,
                len: 14,
                slices: vec![WireSliceManifest {
                    slice_id: 1,
                    logical_offset: 0,
                    len: 14,
                    blocks: vec![WireBlockDescriptor {
                        object_key: old_object_key.clone(),
                        logical_offset: 0,
                        object_offset: 0,
                        len: 14,
                        digest_uri: "sha256:block-old".to_owned(),
                    }],
                }],
            }],
            prepared,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    );
    assert!(envelope.ok, "initial publish failed: {envelope:?}");
    let inode = match envelope.result.unwrap() {
        MetadataRpcResult::RenameReplace { entry, .. } => entry.attr.inode,
        other => panic!("unexpected publish result: {other:?}"),
    };

    let prepared = match request_envelope(
        &server,
        MetadataRpcRequest::PrepareArtifact {
            parent: 1,
            name: "checkpoint.bin".to_owned(),
            replace: true,
        },
    )
    .result
    .unwrap()
    {
        MetadataRpcResult::PreparedArtifact { prepared } => prepared,
        other => panic!("unexpected prepare result: {other:?}"),
    };
    let new_generation = prepared.generation;
    let envelope = request_envelope(
        &server,
        MetadataRpcRequest::PublishPreparedArtifactStagedSession {
            prepared,
            producer: "unit-test".to_owned(),
            digest_uri: "sha256:shrunk".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            manifest_id: "checkpoint.bin".to_owned(),
            size: 5,
            chunks: Vec::new(),
            staged: WireStagedObjectSet::default(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    );
    assert!(envelope.ok, "staged-session publish failed: {envelope:?}");

    let plan = request_envelope(
        &server,
        MetadataRpcRequest::ReadBodyPlan {
            inode,
            generation: new_generation,
            offset: 0,
            len: 10,
        },
    );
    let plan = match plan.result.unwrap() {
        MetadataRpcResult::BodyReadPlan { plan } => plan,
        other => panic!("unexpected body plan result: {other:?}"),
    };
    assert_eq!(plan.output_len, 5);
    assert_eq!(plan.blocks.len(), 1);
    assert_eq!(plan.blocks[0].object_key, old_object_key);
    assert_eq!(plan.blocks[0].object_offset, 0);
    assert_eq!(plan.blocks[0].len, 5);
    assert_eq!(plan.blocks[0].output_offset, 0);
}
