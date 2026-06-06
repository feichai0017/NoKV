use super::*;
use nokv_object::{MemoryObjectStore, ObjectKey};
use nokv_protocol::{decode_request, encode_envelope, WireDentryRecord, WireInodeAttr};
use nokv_types::AdvisoryLockKind;
use std::net::TcpListener;
use std::thread;

fn serve_one(body: &'static str) -> SocketAddr {
    serve_many(vec![response_body(body)])
}

fn serve_many(bodies: Vec<Vec<u8>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        for body in bodies {
            let (request_id, flags, request) = read_frame(&mut stream).unwrap();
            decode_request(&request).expect("framed request is binary metadata rpc");
            write_frame(&mut stream, request_id, flags, &body).unwrap();
        }
    });
    addr
}

fn serve_one_request<F>(handler: F) -> SocketAddr
where
    F: FnOnce(MetadataRpcRequest) -> Vec<u8> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        let response = handler(request);
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    addr
}

fn response_body(json: &str) -> Vec<u8> {
    let envelope: MetadataRpcEnvelope = serde_json::from_str(json).unwrap();
    encode_envelope(&envelope).unwrap()
}

fn read_not_fresh_response(
    required: WireMetadataPosition,
    applied: Option<WireMetadataPosition>,
) -> Vec<u8> {
    encode_envelope(&MetadataRpcEnvelope {
        ok: false,
        result: None,
        error: Some("metadata read is not fresh".to_owned()),
        error_kind: Some(WireMetadataError::ReadNotFresh { required, applied }),
        metadata_position: None,
    })
    .unwrap()
}

fn dentry_response(parent: u64, name: &str, inode: u64, generation: u64) -> Vec<u8> {
    dentry_response_with_position(parent, name, inode, generation, None)
}

fn dentry_batch_response(names: &[String], first_inode: u64) -> Vec<u8> {
    let results = names
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            let inode = first_inode + u64::try_from(idx).expect("test index fits u64");
            MetadataRpcEnvelope {
                ok: true,
                result: Some(MetadataRpcResult::Dentry {
                    entry: Some(Box::new(WireDentryWithAttr {
                        dentry: WireDentryRecord {
                            parent: 2,
                            name_hex: name
                                .as_bytes()
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>(),
                            child: inode,
                            child_type: "file".to_owned(),
                            attr_generation: inode,
                        },
                        attr: WireInodeAttr {
                            inode,
                            file_type: "file".to_owned(),
                            mode: 0o644,
                            uid: 1000,
                            gid: 1000,
                            rdev: 0,
                            size: 0,
                            generation: inode,
                            mtime_ms: inode,
                            ctime_ms: inode,
                        },
                        body: None,
                    })),
                }),
                error: None,
                error_kind: None,
                metadata_position: None,
            }
        })
        .collect();
    encode_envelope(&MetadataRpcEnvelope {
        ok: true,
        result: Some(MetadataRpcResult::Batch { results }),
        error: None,
        error_kind: None,
        metadata_position: None,
    })
    .unwrap()
}

fn dentry_response_with_position(
    parent: u64,
    name: &str,
    inode: u64,
    generation: u64,
    metadata_position: Option<WireMetadataPosition>,
) -> Vec<u8> {
    let envelope = MetadataRpcEnvelope {
        ok: true,
        result: Some(MetadataRpcResult::Dentry {
            entry: Some(Box::new(WireDentryWithAttr {
                dentry: WireDentryRecord {
                    parent,
                    name_hex: name
                        .as_bytes()
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>(),
                    child: inode,
                    child_type: "file".to_owned(),
                    attr_generation: generation,
                },
                attr: WireInodeAttr {
                    inode,
                    file_type: "file".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    rdev: 0,
                    size: 0,
                    generation,
                    mtime_ms: generation,
                    ctime_ms: generation,
                },
                body: None,
            })),
        }),
        error: None,
        error_kind: None,
        metadata_position,
    };
    encode_envelope(&envelope).unwrap()
}

fn dentry_response_for_request(request: &MetadataRpcRequest) -> Vec<u8> {
    let MetadataRpcRequest::CreateFilePath { path, .. } = request else {
        panic!("unexpected pipelined request: {request:?}");
    };
    let name = path.rsplit('/').next().expect("path has a final component");
    let inode = match name {
        "a.bin" => 40,
        "b.bin" => 41,
        other => panic!("unexpected file name: {other}"),
    };
    dentry_response(2, name, inode, inode)
}

fn artifact_metadata(manifest_id: &str) -> ArtifactMetadata {
    ArtifactMetadata {
        producer: "unit-test".to_owned(),
        digest_uri: "sha256:demo".to_owned(),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: manifest_id.to_owned(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    }
}

#[test]
fn service_mkdir_sends_metadata_rpc() {
    let addr = serve_one(
        r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_hex":"72756e73","child":2,"child_type":"directory","attr_generation":1},"attr":{"inode":2,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":1,"mtime_ms":1,"ctime_ms":1},"body":null}}}"#,
    );
    let client = MetadataClient::connect(addr);
    let entry = client.mkdir("/runs", 0o755, 1000, 1000).unwrap();
    assert_eq!(entry.attr.inode.get(), 2);
    assert_eq!(entry.dentry.name.as_bytes(), b"runs");
}

#[test]
fn service_mkdirs_uses_single_batch_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        match request {
            MetadataRpcRequest::Batch { requests } => {
                assert_eq!(requests.len(), 2);
                assert_eq!(
                    requests[0],
                    MetadataRpcRequest::CreateDirPath {
                        path: "/runs/a".to_owned(),
                        mode: 0o755,
                        uid: 1000,
                        gid: 1000,
                    }
                );
                assert_eq!(
                    requests[1],
                    MetadataRpcRequest::CreateDirPath {
                        path: "/runs/b".to_owned(),
                        mode: 0o755,
                        uid: 1000,
                        gid: 1000,
                    }
                );
            }
            other => panic!("unexpected request: {other:?}"),
        }
        let response = response_body(
            r#"{"ok":true,"result":{"type":"batch","results":[{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"61","child":40,"child_type":"directory","attr_generation":7},"attr":{"inode":40,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}},{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"62","child":41,"child_type":"directory","attr_generation":8},"attr":{"inode":41,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":null}}}]}}"#,
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = MetadataClient::connect(addr);
    let paths = vec!["/runs/a".to_owned(), "/runs/b".to_owned()];
    let entries = client.mkdirs(&paths, 0o755, 1000, 1000).unwrap();
    let entries = entries.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries[0].attr.inode.get(), 40);
    assert_eq!(entries[1].attr.inode.get(), 41);
}

#[test]
fn service_create_file_uses_single_path_rpc_for_nested_parent() {
    let addr = serve_one(
        r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"636865636b706f696e742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}}"#,
    );
    let client = MetadataClient::connect(addr);
    let entry = client
        .create_file("/runs/checkpoint.bin", 0o644, 1000, 1000)
        .unwrap();
    assert_eq!(entry.attr.inode.get(), 42);
    assert_eq!(entry.dentry.name.as_bytes(), b"checkpoint.bin");
}

#[test]
fn service_create_special_node_sends_typed_rpc() {
    let addr = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::CreateSpecialNode {
                parent: 1,
                name: "accelerator0".to_owned(),
                file_type: "char_device".to_owned(),
                mode: 0o660,
                rdev: 0x1234,
                uid: 0,
                gid: 44,
            }
        );
        response_body(
            r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_hex":"616363656c657261746f7230","child":42,"child_type":"char_device","attr_generation":7},"attr":{"inode":42,"file_type":"char_device","mode":432,"uid":0,"gid":44,"rdev":4660,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}}"#,
        )
    });
    let client = MetadataClient::connect(addr);
    let entry = client
        .create_special_node(
            InodeId::root(),
            DentryName::new(b"accelerator0".to_vec()).unwrap(),
            SpecialNodeSpec {
                file_type: FileType::CharDevice,
                mode: 0o660,
                rdev: 0x1234,
                uid: 0,
                gid: 44,
            },
        )
        .unwrap();
    assert_eq!(entry.attr.file_type, FileType::CharDevice);
    assert_eq!(entry.attr.rdev, 0x1234);
}

#[test]
fn service_advisory_lock_sends_typed_rpc_and_maps_conflict() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        for response in [
            response_body(r#"{"ok":true,"result":{"type":"unit"}}"#),
            response_body(
                r#"{"ok":false,"error":"lock conflict","error_kind":{"type":"lock_conflict","lock":{"inode":42,"owner":7,"start":0,"end":99,"kind":"write","pid":700}}}"#,
            ),
        ] {
            let (request_id, flags, request) = read_frame(&mut stream).unwrap();
            let request = decode_request(&request).unwrap();
            assert!(matches!(
                request,
                MetadataRpcRequest::SetAdvisoryLock {
                    inode: 42,
                    owner: 9,
                    start: 10,
                    end: 20,
                    kind,
                    pid: 900,
                    wait: false,
                } if kind == "read"
            ));
            write_frame(&mut stream, request_id, flags, &response).unwrap();
        }
    });
    let client = MetadataClient::connect(addr);
    let err = client
        .set_advisory_lock(AdvisoryLockRequest {
            inode: InodeId::new(42).unwrap(),
            owner: 9,
            start: 10,
            end: 20,
            kind: AdvisoryLockKind::Read,
            pid: 900,
            wait: false,
        })
        .and_then(|_| {
            client.set_advisory_lock(AdvisoryLockRequest {
                inode: InodeId::new(42).unwrap(),
                owner: 9,
                start: 10,
                end: 20,
                kind: AdvisoryLockKind::Read,
                pid: 900,
                wait: false,
            })
        })
        .unwrap_err();
    match err {
        ClientError::LockConflict(lock) => {
            assert_eq!(lock.inode, InodeId::new(42).unwrap());
            assert_eq!(lock.owner, 7);
            assert_eq!(lock.kind, AdvisoryLockKind::Write);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn service_client_carries_observed_metadata_position_to_live_reads() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::CreateFilePath { path, .. } if path == "/runs/a.bin"
        ));
        let position = WireMetadataPosition { term: 2, index: 5 };
        let response = dentry_response_with_position(2, "a.bin", 40, 7, Some(position));
        write_frame(&mut stream, request_id, flags, &response).unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::RequireApplied {
                position: observed,
                request,
            } if observed == position
                && matches!(*request, MetadataRpcRequest::StatPath { ref path } if path == "/runs/a.bin")
        ));
        let response = response_body(
            r#"{"ok":true,"result":{"type":"path_metadata","metadata":{"attr":{"inode":40,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}}"#,
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = MetadataClient::connect(addr);
    client
        .create_file("/runs/a.bin", 0o644, 1000, 1000)
        .unwrap();
    let metadata = client.stat_path("/runs/a.bin").unwrap().unwrap();
    assert_eq!(metadata.attr.inode.get(), 40);
}

#[test]
fn service_client_exports_observed_metadata_position_from_write() {
    let position = WireMetadataPosition { term: 7, index: 11 };
    let addr = serve_one_request(move |request| {
        assert!(matches!(
            request,
            MetadataRpcRequest::CreateFilePath { path, .. } if path == "/runs/a.bin"
        ));
        dentry_response_with_position(2, "a.bin", 40, 7, Some(position))
    });
    let client = MetadataClient::connect(addr);

    client
        .create_file("/runs/a.bin", 0o644, 1000, 1000)
        .unwrap();

    assert_eq!(
        client.observed_metadata_position(),
        Some(ClientMetadataPosition {
            term: position.term,
            index: position.index,
        })
    );
}

#[test]
fn service_client_retries_stale_single_endpoint_until_fresh() {
    let position = WireMetadataPosition { term: 7, index: 11 };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        for response in [
            read_not_fresh_response(
                position,
                Some(WireMetadataPosition {
                    term: position.term,
                    index: position.index - 1,
                }),
            ),
            response_body(
                r#"{"ok":true,"result":{"type":"path_metadata","metadata":{"attr":{"inode":40,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}}"#,
            ),
        ] {
            let (request_id, flags, request) = read_frame(&mut stream).unwrap();
            let request = decode_request(&request).unwrap();
            assert!(matches!(
                request,
                MetadataRpcRequest::RequireApplied {
                    position: observed,
                    request,
                } if observed == position
                    && matches!(*request, MetadataRpcRequest::StatPath { ref path } if path == "/runs/a.bin")
            ));
            write_frame(&mut stream, request_id, flags, &response).unwrap();
        }
    });

    let client = MetadataClient::connect(addr);
    client.observe_metadata_position(ClientMetadataPosition {
        term: position.term,
        index: position.index,
    });

    let metadata = client.stat_path("/runs/a.bin").unwrap().unwrap();

    assert_eq!(metadata.attr.inode.get(), 40);
}

#[test]
fn service_client_imports_observed_position_for_learner_reads() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::RequireApplied {
                position,
                request,
            } if position == WireMetadataPosition { term: 7, index: 11 }
                && matches!(*request, MetadataRpcRequest::StatPath { ref path } if path == "/runs/a.bin")
        ));
        let response = response_body(
            r#"{"ok":true,"result":{"type":"path_metadata","metadata":{"attr":{"inode":40,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}}"#,
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = MetadataClient::connect(addr);
    client.observe_metadata_position(ClientMetadataPosition { term: 7, index: 11 });

    let metadata = client.stat_path("/runs/a.bin").unwrap().unwrap();

    assert_eq!(metadata.attr.inode.get(), 40);
}

#[test]
fn service_client_imports_observed_position_for_write_planning() {
    let position = WireMetadataPosition { term: 7, index: 11 };
    let addr = serve_one_request(move |request| {
        assert!(matches!(
            request,
            MetadataRpcRequest::RequireApplied {
                position: observed,
                request,
            } if observed == position
                && matches!(*request, MetadataRpcRequest::CreateFilePath { ref path, .. } if path == "/runs/a.bin")
        ));
        dentry_response_with_position(2, "a.bin", 40, 7, Some(position))
    });
    let client = MetadataClient::connect(addr);
    client.observe_metadata_position(ClientMetadataPosition {
        term: position.term,
        index: position.index,
    });

    let entry = client
        .create_file("/runs/a.bin", 0o644, 1000, 1000)
        .unwrap();

    assert_eq!(entry.attr.inode.get(), 40);
}

#[test]
fn service_client_routes_live_reads_to_learner_and_falls_back_on_stale() {
    let position = WireMetadataPosition { term: 7, index: 11 };

    let leader_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let leader_addr = leader_listener.local_addr().unwrap();
    let learner_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let learner_addr = learner_listener.local_addr().unwrap();

    let leader = thread::spawn(move || {
        let (mut stream, _) = leader_listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::CreateFilePath { path, .. } if path == "/runs/a.bin"
        ));
        let response = dentry_response_with_position(2, "a.bin", 40, 7, Some(position));
        write_frame(&mut stream, request_id, flags, &response).unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::RequireApplied {
                position: observed,
                request,
            } if observed == position
                && matches!(*request, MetadataRpcRequest::StatPath { ref path } if path == "/runs/a.bin")
        ));
        let response = response_body(
            r#"{"ok":true,"result":{"type":"path_metadata","metadata":{"attr":{"inode":40,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}}"#,
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });

    let learner = thread::spawn(move || {
        let (mut stream, _) = learner_listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::RequireApplied {
                position: observed,
                request,
            } if observed == position
                && matches!(*request, MetadataRpcRequest::StatPath { ref path } if path == "/runs/a.bin")
        ));
        let response = read_not_fresh_response(
            position,
            Some(WireMetadataPosition {
                term: position.term,
                index: position.index - 1,
            }),
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });

    let client = MetadataClient::new(
        MetadataClientOptions::new(leader_addr).with_read_endpoints(vec![learner_addr]),
    );

    client
        .create_file("/runs/a.bin", 0o644, 1000, 1000)
        .unwrap();
    let metadata = client.stat_path("/runs/a.bin").unwrap().unwrap();

    assert_eq!(metadata.attr.inode.get(), 40);
    leader.join().unwrap();
    learner.join().unwrap();
}

#[test]
fn service_create_files_uses_single_coalesced_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        match request {
            MetadataRpcRequest::CreateFilesInDirPath {
                parent_path, names, ..
            } => {
                assert_eq!(parent_path, "/runs");
                assert_eq!(names, vec!["a.bin".to_owned(), "b.bin".to_owned()]);
            }
            other => panic!("unexpected request: {other:?}"),
        }
        let response = response_body(
            r#"{"ok":true,"result":{"type":"batch","results":[{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"612e62696e","child":40,"child_type":"file","attr_generation":7},"attr":{"inode":40,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}},{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"622e62696e","child":41,"child_type":"file","attr_generation":8},"attr":{"inode":41,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":null}}}]}}"#,
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = MetadataClient::connect(addr);
    let paths = vec!["/runs/a.bin".to_owned(), "/runs/b.bin".to_owned()];
    let entries = client.create_files(&paths, 0o644, 1000, 1000).unwrap();
    let entries = entries.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries[0].attr.inode.get(), 40);
    assert_eq!(entries[1].attr.inode.get(), 41);
}

#[test]
fn service_create_files_splits_large_coalesced_batches() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        let first_names = match request {
            MetadataRpcRequest::CreateFilesInDirPath {
                parent_path, names, ..
            } => {
                assert_eq!(parent_path, "/runs");
                assert_eq!(names.len(), MAX_BATCH_RPC_REQUESTS);
                names
            }
            other => panic!("unexpected first request: {other:?}"),
        };
        let response = dentry_batch_response(&first_names, 40);
        write_frame(&mut stream, request_id, flags, &response).unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        let second_names = match request {
            MetadataRpcRequest::CreateFilesInDirPath {
                parent_path, names, ..
            } => {
                assert_eq!(parent_path, "/runs");
                assert_eq!(names.len(), 1);
                names
            }
            other => panic!("unexpected second request: {other:?}"),
        };
        let response = dentry_batch_response(
            &second_names,
            40 + u64::try_from(first_names.len()).expect("test batch fits u64"),
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = MetadataClient::connect(addr);
    let paths = (0..=MAX_BATCH_RPC_REQUESTS)
        .map(|idx| format!("/runs/file-{idx:03}.bin"))
        .collect::<Vec<_>>();
    let entries = client.create_files(&paths, 0o644, 1000, 1000).unwrap();
    let entries = entries.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), MAX_BATCH_RPC_REQUESTS + 1);
    assert_eq!(entries[0].attr.inode.get(), 40);
    assert_eq!(
        entries[MAX_BATCH_RPC_REQUESTS].attr.inode.get(),
        40 + u64::try_from(MAX_BATCH_RPC_REQUESTS).expect("test batch fits u64")
    );
}

#[test]
fn service_remove_many_uses_single_batch_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        match request {
            MetadataRpcRequest::Batch { requests } => {
                assert_eq!(requests.len(), 2);
                assert!(matches!(
                    &requests[0],
                    MetadataRpcRequest::RemoveFilePath { path } if path == "/runs/a.bin"
                ));
                assert!(matches!(
                    &requests[1],
                    MetadataRpcRequest::RemoveFilePath { path } if path == "/runs/b.bin"
                ));
            }
            other => panic!("unexpected request: {other:?}"),
        }
        let response = response_body(
            r#"{"ok":true,"result":{"type":"batch","results":[{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"612e62696e","child":40,"child_type":"file","attr_generation":7},"attr":{"inode":40,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}},{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"622e62696e","child":41,"child_type":"file","attr_generation":8},"attr":{"inode":41,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":null}}}]}}"#,
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = MetadataClient::connect(addr);
    let paths = vec!["/runs/a.bin".to_owned(), "/runs/b.bin".to_owned()];
    let entries = client.remove_many(&paths).unwrap();
    let entries = entries.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries[0].attr.inode.get(), 40);
    assert_eq!(entries[1].attr.inode.get(), 41);
}

#[test]
fn service_rmdir_many_uses_single_batch_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        match request {
            MetadataRpcRequest::Batch { requests } => {
                assert_eq!(requests.len(), 2);
                assert!(matches!(
                    &requests[0],
                    MetadataRpcRequest::RemoveEmptyDirPath { path } if path == "/runs/a"
                ));
                assert!(matches!(
                    &requests[1],
                    MetadataRpcRequest::RemoveEmptyDirPath { path } if path == "/runs/b"
                ));
            }
            other => panic!("unexpected request: {other:?}"),
        }
        let response = response_body(
            r#"{"ok":true,"result":{"type":"batch","results":[{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"61","child":40,"child_type":"directory","attr_generation":7},"attr":{"inode":40,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}},{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"62","child":41,"child_type":"directory","attr_generation":8},"attr":{"inode":41,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":null}}}]}}"#,
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = MetadataClient::connect(addr);
    let paths = vec!["/runs/a".to_owned(), "/runs/b".to_owned()];
    let entries = client.rmdir_many(&paths).unwrap();
    let entries = entries.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries[0].attr.inode.get(), 40);
    assert_eq!(entries[1].attr.inode.get(), 41);
}

#[test]
fn service_framed_rpc_accepts_out_of_order_responses() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let first = read_frame(&mut stream).unwrap();
        let second = read_frame(&mut stream).unwrap();
        let first_request = decode_request(&first.2).unwrap();
        let second_request = decode_request(&second.2).unwrap();
        let first_response = dentry_response_for_request(&first_request);
        let second_response = dentry_response_for_request(&second_request);

        write_frame(&mut stream, second.0, second.1, &second_response).unwrap();
        write_frame(&mut stream, first.0, first.1, &first_response).unwrap();
    });

    let client = Arc::new(MetadataClient::connect(addr));
    let first = {
        let client = Arc::clone(&client);
        thread::spawn(move || client.create_file("/runs/a.bin", 0o644, 1000, 1000))
    };
    let second = {
        let client = Arc::clone(&client);
        thread::spawn(move || client.create_file("/runs/b.bin", 0o644, 1000, 1000))
    };

    let first = first.join().unwrap().unwrap();
    let second = second.join().unwrap().unwrap();
    assert_eq!(first.dentry.name.as_bytes(), b"a.bin");
    assert_eq!(second.dentry.name.as_bytes(), b"b.bin");
}

#[test]
fn service_xattr_rpc_round_trips_bytes_names() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        assert_eq!(
            decode_request(&request).unwrap(),
            MetadataRpcRequest::SetXattr {
                inode: 42,
                name_hex: "757365722e636f6d6d656e74".to_owned(),
                value: b"training checkpoint".to_vec(),
                mode: WireXattrSetMode::Create,
            }
        );
        write_frame(
            &mut stream,
            request_id,
            flags,
            &encode_envelope(&MetadataRpcEnvelope {
                ok: true,
                result: Some(MetadataRpcResult::Unit),
                error: None,
                error_kind: None,
                metadata_position: None,
            })
            .unwrap(),
        )
        .unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        assert_eq!(
            decode_request(&request).unwrap(),
            MetadataRpcRequest::GetXattr {
                inode: 42,
                name_hex: "757365722e636f6d6d656e74".to_owned(),
            }
        );
        write_frame(
            &mut stream,
            request_id,
            flags,
            &encode_envelope(&MetadataRpcEnvelope {
                ok: true,
                result: Some(MetadataRpcResult::XattrValue {
                    value: Some(b"training checkpoint".to_vec()),
                }),
                error: None,
                error_kind: None,
                metadata_position: None,
            })
            .unwrap(),
        )
        .unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        assert_eq!(
            decode_request(&request).unwrap(),
            MetadataRpcRequest::ListXattr { inode: 42 }
        );
        write_frame(
            &mut stream,
            request_id,
            flags,
            &encode_envelope(&MetadataRpcEnvelope {
                ok: true,
                result: Some(MetadataRpcResult::XattrNames {
                    names_hex: vec!["757365722e636f6d6d656e74".to_owned()],
                }),
                error: None,
                error_kind: None,
                metadata_position: None,
            })
            .unwrap(),
        )
        .unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        assert_eq!(
            decode_request(&request).unwrap(),
            MetadataRpcRequest::RemoveXattr {
                inode: 42,
                name_hex: "757365722e636f6d6d656e74".to_owned(),
            }
        );
        write_frame(
            &mut stream,
            request_id,
            flags,
            &encode_envelope(&MetadataRpcEnvelope {
                ok: true,
                result: Some(MetadataRpcResult::Unit),
                error: None,
                error_kind: None,
                metadata_position: None,
            })
            .unwrap(),
        )
        .unwrap();
    });

    let client = MetadataClient::connect(addr);
    let inode = InodeId::new(42).unwrap();
    client
        .set_xattr(
            inode,
            b"user.comment",
            b"training checkpoint".to_vec(),
            XattrSetMode::Create,
        )
        .unwrap();
    assert_eq!(
        client.get_xattr(inode, b"user.comment").unwrap(),
        Some(b"training checkpoint".to_vec())
    );
    assert_eq!(
        client.list_xattr(inode).unwrap(),
        vec![b"user.comment".to_vec()]
    );
    client.remove_xattr(inode, b"user.comment").unwrap();
}

#[test]
fn service_error_without_error_kind_is_protocol_error() {
    let addr = serve_one(r#"{"ok":false,"error":"metadata command predicate failed"}"#);
    let client = MetadataClient::connect(addr);
    let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
    assert!(
        matches!(
            err,
            ClientError::Protocol(ref err)
                if err.contains("missing typed error_kind")
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn service_typed_error_maps_predicate_failed_to_metadata_error() {
    let addr = serve_one(
        r#"{"ok":false,"error":"metadata command predicate failed","error_kind":{"type":"predicate_failed"}}"#,
    );
    let client = MetadataClient::connect(addr);
    let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::Metadata(
            nokv_meta::MetadataError::PredicateFailed
        ))
    ));
}

#[test]
fn service_typed_error_maps_stale_generation_to_metadata_error() {
    let addr = serve_one(
        r#"{"ok":false,"error":"body generation 7 is stale; current generation is 8","error_kind":{"type":"stale_body_generation","expected":7,"current":8}}"#,
    );
    let client = MetadataClient::connect(addr);
    let err = client
        .read_body_plan(InodeId::new(42).unwrap(), 7, 0, 1)
        .unwrap_err();
    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::StaleBodyGeneration {
            expected: 7,
            current: 8
        })
    ));
}

#[test]
fn service_client_retries_forward_to_leader_endpoint() {
    let leader_addr = serve_one_request(|request| {
        assert!(matches!(
            request,
            MetadataRpcRequest::BootstrapRoot {
                mode: 0o755,
                uid: 1000,
                gid: 1000
            }
        ));
        response_body(
            r#"{"ok":true,"result":{"type":"inode_attr","attr":null},"metadata_position":{"term":3,"index":9}}"#,
        )
    });
    let follower_addr = serve_one_request(move |request| {
        assert!(matches!(request, MetadataRpcRequest::BootstrapRoot { .. }));
        encode_envelope(&MetadataRpcEnvelope {
            ok: false,
            result: None,
            error: Some("forward to metadata leader".to_owned()),
            error_kind: Some(WireMetadataError::ForwardToLeader {
                leader_id: Some(2),
                address: Some(leader_addr.to_string()),
            }),
            metadata_position: None,
        })
        .unwrap()
    });
    let client = MetadataClient::connect(follower_addr);

    client.bootstrap_root(0o755, 1000, 1000).unwrap();

    assert_eq!(
        client.observed_metadata_position(),
        Some(ClientMetadataPosition { term: 3, index: 9 })
    );
}

#[test]
fn service_typed_error_maps_read_not_fresh() {
    let err = client_error_from_wire_error(WireMetadataError::ReadNotFresh {
        required: WireMetadataPosition { term: 2, index: 8 },
        applied: Some(WireMetadataPosition { term: 2, index: 5 }),
    });
    assert!(matches!(
        err,
        ClientError::ReadNotFresh {
            required_term: 2,
            required_index: 8,
            applied_term: Some(2),
            applied_index: Some(5),
        }
    ));
}

#[test]
fn service_typed_error_maps_forward_to_leader() {
    let err = client_error_from_wire_error(WireMetadataError::ForwardToLeader {
        leader_id: Some(2),
        address: Some("127.0.0.1:9922".to_owned()),
    });
    assert!(matches!(
        err,
        ClientError::ForwardToLeader {
            leader_id: Some(2),
            address: Some(address),
        } if address.to_string() == "127.0.0.1:9922"
    ));
}

#[test]
fn service_typed_error_maps_backend_metadata_error() {
    let addr = serve_one(
        r#"{"ok":false,"error":"metadata backend unavailable","error_kind":{"type":"metadata","message":"metadata backend unavailable"}}"#,
    );
    let client = MetadataClient::connect(addr);
    let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::Metadata(
            nokv_meta::MetadataError::Backend(ref message)
        )) if message == "metadata backend unavailable"
    ));
}

#[test]
fn service_typed_error_maps_backend_object_error() {
    let addr = serve_one(
        r#"{"ok":false,"error":"object backend unavailable","error_kind":{"type":"object","message":"object backend unavailable"}}"#,
    );
    let client = MetadataClient::connect(addr);
    let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
    assert!(matches!(
        err,
        ClientError::Object(nokv_object::ObjectError::Backend(ref message))
            if message == "object backend unavailable"
    ));
}

#[test]
fn service_snapshot_cat_uses_snapshot_file_rpc() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadArtifactPathAtSnapshot { snapshot_id, path }
                if snapshot_id == 9 && path == "/runs/checkpoint"
        ));
        let response =
            response_body(r#"{"ok":true,"result":{"type":"file_bytes","bytes":[111,108,100]}}"#);
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    assert_eq!(client.cat_snapshot(9, "/runs/checkpoint").unwrap(), b"old");
}

#[test]
fn service_snapshot_namespace_methods_use_snapshot_rooted_rpcs() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::StatPathAtSnapshot { snapshot_id, path }
                if snapshot_id == 9 && path == "/"
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"path_metadata","metadata":{"attr":{"inode":2,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":2,"mtime_ms":2,"ctime_ms":2},"body":null}}}"#,
                ),
            )
            .unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadDirPlusPathAtSnapshot { snapshot_id, path }
                if snapshot_id == 9 && path == "/"
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"dentries","entries":[{"dentry":{"parent":2,"name_hex":"6e6573746564","child":3,"child_type":"directory","attr_generation":3},"attr":{"inode":3,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":3,"mtime_ms":3,"ctime_ms":3},"body":null}]}}"#,
                ),
            )
            .unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadFilePathAtSnapshot {
                snapshot_id,
                path,
                offset,
                len
            } if snapshot_id == 9 && path == "/nested/model.bin" && offset == 7 && len == 3
        ));
        write_frame(
            &mut stream,
            request_id,
            flags,
            &response_body(r#"{"ok":true,"result":{"type":"file_bytes","bytes":[111,108,100]}}"#),
        )
        .unwrap();
    });

    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    let root = client
        .metadata()
        .stat_path_at_snapshot(9, "/")
        .unwrap()
        .unwrap();
    assert_eq!(root.attr.inode.get(), 2);
    let entries = client.metadata().list_path_at_snapshot(9, "/").unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name.as_bytes(), b"nested");
    assert_eq!(
        client.read_snapshot(9, "/nested/model.bin", 7, 3).unwrap(),
        b"old"
    );
}

#[test]
fn service_list_page_uses_cursor_rpc() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadDirPlusPathPage {
                path,
                after_name_hex,
                limit
            } if path == "/runs" && after_name_hex.as_deref() == Some("612e62696e") && limit == 2
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"dentries_page","entries":[{"dentry":{"parent":2,"name_hex":"622e62696e","child":3,"child_type":"file","attr_generation":3},"attr":{"inode":3,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":3,"mtime_ms":3,"ctime_ms":3},"body":null}],"next_name_hex":"622e62696e"}}"#,
                ),
            )
            .unwrap();
    });
    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    let after = DentryName::new(b"a.bin".to_vec()).unwrap();
    let page = client
        .metadata()
        .list_page("/runs", Some(&after), 2)
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].dentry.name.as_bytes(), b"b.bin");
    assert_eq!(
        page.next_cursor.as_ref().map(DentryName::as_bytes),
        Some(b"b.bin".as_slice())
    );
}

#[test]
fn service_indexed_list_page_uses_indexed_cursor_rpc() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadIndexedPathPage {
                path,
                after_name_hex,
                limit
            } if path == "/runs" && after_name_hex.as_deref() == Some("612e62696e") && limit == 2
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"dentries_page","entries":[{"dentry":{"parent":2,"name_hex":"622e62696e","child":3,"child_type":"file","attr_generation":3},"attr":{"inode":3,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":3,"mtime_ms":3,"ctime_ms":3},"body":null}],"next_name_hex":"622e62696e"}}"#,
                ),
            )
            .unwrap();
    });
    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    let after = DentryName::new(b"a.bin".to_vec()).unwrap();
    let page = client
        .metadata()
        .list_indexed_page("/runs", Some(&after), 2)
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].dentry.name.as_bytes(), b"b.bin");
    assert_eq!(
        page.next_cursor.as_ref().map(DentryName::as_bytes),
        Some(b"b.bin".as_slice())
    );
}

#[test]
fn service_list_uses_paged_rpc_until_cursor_is_exhausted() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadDirPlusPathPage {
                path,
                after_name_hex,
                limit
            } if path == "/runs" && after_name_hex.is_none() && limit == DEFAULT_LIST_PAGE_SIZE
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"dentries_page","entries":[{"dentry":{"parent":2,"name_hex":"612e62696e","child":3,"child_type":"file","attr_generation":3},"attr":{"inode":3,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":3,"mtime_ms":3,"ctime_ms":3},"body":null}],"next_name_hex":"612e62696e"}}"#,
                ),
            )
            .unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadDirPlusPathPage {
                path,
                after_name_hex,
                limit
            } if path == "/runs"
                && after_name_hex.as_deref() == Some("612e62696e")
                && limit == DEFAULT_LIST_PAGE_SIZE
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"dentries_page","entries":[{"dentry":{"parent":2,"name_hex":"622e62696e","child":4,"child_type":"file","attr_generation":4},"attr":{"inode":4,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":0,"generation":4,"mtime_ms":4,"ctime_ms":4},"body":null}],"next_name_hex":null}}"#,
                ),
            )
            .unwrap();
    });
    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    let entries = client.metadata().list("/runs").unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.dentry.name.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"a.bin".as_slice(), b"b.bin".as_slice()]
    );
}

#[test]
fn service_metadata_stat_path_uses_path_metadata_rpc() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::StatPath { path } if path == "/artifact.bin"
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"path_metadata","metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":12,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":12,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"chunk_size":67108864,"block_size":4194304}}}}"#,
                ),
            )
            .unwrap();
    });

    let client = MetadataClient::connect(addr);
    let metadata = client.stat_path("/artifact.bin").unwrap().unwrap();
    assert_eq!(metadata.attr.inode.get(), 42);
    assert_eq!(metadata.body.unwrap().digest_uri, "sha256:demo");
}

#[test]
fn service_file_client_read_path_returns_metadata_and_checks_expected_generation() {
    let store = MemoryObjectStore::new();
    store
        .put(&ObjectKey::new("blocks/demo").unwrap(), b"hello server")
        .unwrap();
    let addr = serve_one(
        r#"{"ok":true,"result":{"type":"path_read_plan","metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":12,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":12,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"chunk_size":67108864,"block_size":4194304}},"plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","object_offset":6,"len":6,"output_offset":0}]}}}"#,
    );
    let client = NoKvFsClient::connect(addr, store);
    let read = client.read_path("/artifact.bin", 6, 6, Some(7)).unwrap();
    assert_eq!(read.bytes, b"server");
    assert_eq!(read.metadata.attr.generation, 7);
    assert_eq!(read.metadata.body.unwrap().digest_uri, "sha256:demo");

    let addr = serve_one(
        r#"{"ok":false,"error":"stale body generation","error_kind":{"type":"stale_body_generation","expected":7,"current":8}}"#,
    );
    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    let err = client
        .read_path("/artifact.bin", 0, 6, Some(7))
        .unwrap_err();
    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::StaleBodyGeneration {
            expected: 7,
            current: 8
        })
    ));
}

#[test]
fn service_file_client_reads_body_from_object_store() {
    let store = MemoryObjectStore::new();
    store
        .put(&ObjectKey::new("blocks/demo").unwrap(), b"hello server")
        .unwrap();
    let addr = serve_many(vec![
        response_body(
            r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_hex":"61727469666163742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":12,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":12,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"chunk_size":67108864,"block_size":4194304}}}}"#,
        ),
        response_body(
            r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","object_offset":6,"len":6,"output_offset":0}]}}}"#,
        ),
    ]);
    let client = NoKvFsClient::connect(addr, store);
    let bytes = client.read("/artifact.bin", 6, 6).unwrap();
    assert_eq!(bytes, b"server");
}

#[test]
fn service_file_client_uploads_blocks_then_publishes_metadata() {
    let store = MemoryObjectStore::new();
    let addr = serve_many(vec![
        response_body(
            r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":false,"dentry_version":null,"old_generation":null}}}"#,
        ),
        response_body(
            r#"{"ok":true,"result":{"type":"rename_replace","entry":{"dentry":{"parent":1,"name_hex":"61727469666163742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"size":11,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":11,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"chunk_size":67108864,"block_size":4194304}},"replaced":null}}"#,
        ),
    ]);
    let client = NoKvFsClient::connect(addr, store.clone());
    let entry = client
        .put_artifact(
            "/artifact.bin",
            b"hello world".to_vec(),
            artifact_metadata("artifact.bin"),
        )
        .unwrap();
    assert_eq!(entry.attr.inode.get(), 42);
    assert!(
        store
            .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
            .unwrap()
            .is_some(),
        "metadata publish should upload object block before metadata commit"
    );
}

#[test]
fn service_file_client_cleans_staged_blocks_after_publish_failure() {
    let store = MemoryObjectStore::new();
    let addr = serve_many(vec![
        response_body(
            r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":false,"dentry_version":null,"old_generation":null}}}"#,
        ),
        response_body(
            r#"{"ok":false,"error":"metadata command predicate failed","error_kind":{"type":"predicate_failed"}}"#,
        ),
    ]);
    let client = NoKvFsClient::connect(addr, store.clone());
    let err = client
        .put_artifact(
            "/artifact.bin",
            b"hello world".to_vec(),
            artifact_metadata("artifact.bin"),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::Metadata(
            nokv_meta::MetadataError::PredicateFailed
        ))
    ));
    assert!(
        store
            .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
            .unwrap()
            .is_none(),
        "failed metadata publish should clean staged object block"
    );
}
