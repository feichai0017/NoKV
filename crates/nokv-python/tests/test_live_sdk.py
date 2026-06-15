"""End-to-end SDK tests against a live NoKV server + S3/RustFS object store.

Skipped unless a server is reachable. Configure via env (defaults match
scripts/run-multishard-fleet-smoke.sh-style single-server bringup):

    NOKV_TEST_ADDR=127.0.0.1:7750
    NOKV_TEST_BUCKET=nokv-live
    NOKV_TEST_ENDPOINT=http://127.0.0.1:9050
    NOKV_TEST_ACCESS_KEY=rustfsadmin
    NOKV_TEST_SECRET_KEY=rustfsadmin
    NOKV_TEST_REGION=auto

Exercises the write/namespace/snapshot bindings, the fsspec read+write surface,
and the checkpoint publish/resolve flow — all against real metadata RPC + real
object I/O.
"""

import os
import socket
import sys
import uuid

ADDR = os.environ.get("NOKV_TEST_ADDR", "127.0.0.1:7750")
OPTS = dict(
    bucket=os.environ.get("NOKV_TEST_BUCKET", "nokv-live"),
    endpoint=os.environ.get("NOKV_TEST_ENDPOINT", "http://127.0.0.1:9050"),
    access_key_id=os.environ.get("NOKV_TEST_ACCESS_KEY", "rustfsadmin"),
    secret_access_key=os.environ.get("NOKV_TEST_SECRET_KEY", "rustfsadmin"),
    region=os.environ.get("NOKV_TEST_REGION", "auto"),
)


def _server_up() -> bool:
    host, _, port = ADDR.partition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=1.0):
            return True
    except OSError:
        return False


def _client():
    from nokv import Client

    return Client(ADDR, **OPTS)


def test_write_read_namespace_roundtrip():
    c = _client()
    root = f"/live_{uuid.uuid4().hex[:8]}"
    c.mkdir(root)
    assert c.exists(root)

    payload = b"the quick brown fox" * 100
    entry = c.put_artifact(f"{root}/a.bin", payload)
    assert entry["type"] == "file" and entry["size"] == len(payload)

    assert c.cat(f"{root}/a.bin") == payload
    st = c.stat(f"{root}/a.bin")
    assert st["size"] == len(payload)

    names = {e["name"] for e in c.list_dir(root)}
    assert "a.bin" in names

    # rename + remove
    c.rename(f"{root}/a.bin", f"{root}/b.bin")
    assert c.exists(f"{root}/b.bin") and not c.exists(f"{root}/a.bin")
    c.remove_file(f"{root}/b.bin")
    assert not c.exists(f"{root}/b.bin")
    c.rmdir(root)


def test_replace_publishes_new_generation():
    c = _client()
    root = f"/live_{uuid.uuid4().hex[:8]}"
    c.mkdir(root)
    p = f"{root}/m.bin"
    g1 = c.put_artifact(p, b"v1")["generation"]
    g2 = c.put_artifact(p, b"v2-bigger", replace=True)["generation"]
    assert c.cat(p) == b"v2-bigger"
    assert g2 != g1
    c.remove_file(p)
    c.rmdir(root)


def test_snapshot_reproducible_read():
    c = _client()
    root = f"/live_{uuid.uuid4().hex[:8]}"
    c.mkdir(root)
    c.put_artifact(f"{root}/x.bin", b"original")
    pin = c.snapshot(root)
    sid = pin["snapshot_id"]
    # Supersede the live generation; the snapshot still sees the old bytes.
    c.put_artifact(f"{root}/x.bin", b"updated!!", replace=True)
    assert c.cat(f"{root}/x.bin") == b"updated!!"
    # A subtree snapshot RE-ROOTS: at the snapshot, the snapshotted dir is "/",
    # so reads are addressed relative to it ("/x.bin", not f"{root}/x.bin").
    assert c.cat("/x.bin", snapshot_id=sid) == b"original"
    c.retire_snapshot(sid)


def test_fsspec_surface():
    from nokv import NoKVFileSystem

    fs = NoKVFileSystem(client=_client())
    root = f"/fsx_{uuid.uuid4().hex[:8]}"
    fs.makedirs(f"{root}/sub", exist_ok=True)
    assert fs.exists(root)

    data = b"fsspec-write-path" * 64
    with fs.open(f"{root}/sub/data.bin", "wb") as handle:
        handle.write(data)
    assert fs.cat_file(f"{root}/sub/data.bin") == data
    # partial range
    assert fs.cat_file(f"{root}/sub/data.bin", 0, 6) == data[:6]

    info = fs.info(f"{root}/sub/data.bin")
    assert info["type"] == "file" and info["size"] == len(data)
    listing = fs.ls(f"{root}/sub")
    assert any(item["name"].endswith("data.bin") for item in listing)

    with fs.open(f"{root}/sub/data.bin", "rb") as handle:
        assert handle.read() == data

    fs.rm_file(f"{root}/sub/data.bin")
    assert not fs.exists(f"{root}/sub/data.bin")


def test_checkpoint_publish_resolve():
    from nokv import checkpoint

    c = _client()
    run = f"run_{uuid.uuid4().hex[:8]}"
    shards = {f"rank{r}": bytes([r]) * (1000 + r) for r in range(4)}
    checkpoint.publish_checkpoint(c, run, 100, shards)

    assert checkpoint.latest_step(c, run) == 100
    manifest = checkpoint.resolve_checkpoint(c, run)
    assert manifest["step"] == 100
    assert len(manifest["shards"]) == 4
    assert checkpoint.load_shard(c, manifest, "rank3") == bytes([3]) * 1003

    # distributed shape: per-rank publish, single commit
    entries = [
        checkpoint.publish_shard(c, run, 200, f"rank{r}", bytes([r + 10]) * 500)
        for r in range(2)
    ]
    assert checkpoint.latest_step(c, run) == 100  # not committed yet
    checkpoint.commit_checkpoint(c, run, 200, entries)
    assert checkpoint.latest_step(c, run) == 200


if __name__ == "__main__":
    if not _server_up():
        print(f"SKIP: no NoKV server at {ADDR}")
        sys.exit(0)
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                import traceback

                print(f"FAIL {name}: {exc!r}")
                traceback.print_exc()
    print(f"\n{'OK' if failures == 0 else 'FAILED'}: {failures} failure(s)")
    sys.exit(1 if failures else 0)
