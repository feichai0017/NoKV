use nokvfs_client::{ArtifactRepository, ClientError};
use nokvfs_meta::{HoltMetadataStore, NoKvFs};
use nokvfs_object::MemoryObjectStore;
use nokvfs_types::MountId;

type TestRepository = ArtifactRepository<NoKvFs<HoltMetadataStore, MemoryObjectStore>>;

fn repository() -> TestRepository {
    let service = NoKvFs::new(
        MountId::new(1).unwrap(),
        HoltMetadataStore::open_memory().unwrap(),
        MemoryObjectStore::new(),
    );
    service.bootstrap_root(0o755, 1000, 1000).unwrap();
    ArtifactRepository::new(service)
}

#[test]
fn artifact_repository_put_get_list_stat_and_overwrite() {
    let repo = repository();

    let info = repo
        .put_bytes("runs/run-1/metrics.json", br#"{"accuracy":0.99}"#.to_vec())
        .unwrap();
    assert_eq!(info.path, "runs/run-1/metrics.json");
    assert!(!info.is_dir);
    assert_eq!(info.size, Some(17));
    assert_eq!(
        info.metadata.as_ref().unwrap().digest_uri,
        "sha256:55c4e51d56c0a443e9e4d18476f25e3421b4e951888042330b26313793d14a20"
    );

    let root = repo.list("").unwrap();
    assert_eq!(root.len(), 1);
    assert_eq!(root[0].path, "runs");
    assert!(root[0].is_dir);

    let run_dir = repo.list("runs/run-1").unwrap();
    assert_eq!(run_dir.len(), 1);
    assert_eq!(run_dir[0].path, "runs/run-1/metrics.json");
    assert_eq!(run_dir[0].size, Some(17));
    repo.backend()
        .create_file_path("/runs/run-1/plain.txt", 0o644, 1000, 1000)
        .unwrap();
    let run_dir = repo.list("runs/run-1").unwrap();
    assert_eq!(run_dir.len(), 1);
    assert_eq!(run_dir[0].path, "runs/run-1/metrics.json");

    assert!(repo.list("runs/run-1/metrics.json").unwrap().is_empty());
    assert_eq!(repo.stat("runs/run-1/metrics.json").unwrap(), info);
    assert_eq!(
        repo.get_bytes("runs/run-1/metrics.json").unwrap(),
        br#"{"accuracy":0.99}"#
    );

    let overwritten = repo
        .put_bytes("runs/run-1/metrics.json", b"second".to_vec())
        .unwrap();
    assert_eq!(overwritten.path, "runs/run-1/metrics.json");
    assert_eq!(overwritten.size, Some(6));
    assert_eq!(
        overwritten.metadata.as_ref().unwrap().digest_uri,
        "sha256:16367aacb67a4a017c8da8ab95682ccb390863780f7114dda0a0e0c55644c7c4"
    );
    assert_eq!(
        repo.get_bytes("runs/run-1/metrics.json").unwrap(),
        b"second"
    );
}

#[test]
fn artifact_repository_delete_recursively_and_keeps_root_usable() {
    let repo = repository();
    repo.put_bytes("dir/file.txt", b"first".to_vec()).unwrap();
    repo.put_bytes("dir/nested/child.txt", b"second".to_vec())
        .unwrap();

    repo.delete("dir").unwrap();
    assert!(matches!(
        repo.stat("dir"),
        Err(ClientError::NotFound(ref path)) if path == "dir"
    ));
    assert!(repo.list("").unwrap().is_empty());

    repo.put_bytes("a.txt", b"a".to_vec()).unwrap();
    repo.put_bytes("nested/b.txt", b"b".to_vec()).unwrap();
    repo.delete("").unwrap();
    assert!(repo.list("").unwrap().is_empty());

    repo.put_bytes("next.txt", b"next".to_vec()).unwrap();
    let root = repo.list("").unwrap();
    assert_eq!(root.len(), 1);
    assert_eq!(root[0].path, "next.txt");
}

#[test]
fn artifact_repository_delete_missing_path_is_noop() {
    let repo = repository();

    repo.delete("missing/path.txt").unwrap();
    assert!(repo.list("").unwrap().is_empty());
}

#[test]
fn artifact_repository_rejects_noncanonical_paths() {
    let repo = repository();

    for path in [
        "",
        ".",
        "/absolute",
        "a//b",
        "a/../b",
        "../b",
        "a\\b",
        "a\0b",
        "nested/",
    ] {
        let err = repo.put_bytes(path, b"payload".to_vec()).unwrap_err();
        assert!(
            matches!(err, ClientError::InvalidArtifactPath(_)),
            "unexpected error for {path:?}: {err:?}"
        );
    }
}
