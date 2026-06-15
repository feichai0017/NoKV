"""Unit tests for nokv.checkpoint against an in-memory fake client.

These exercise the checkpoint commit/resolve semantics (manifest-as-commit-point,
latest-committed-step selection, partial-checkpoint invisibility) without the
native extension or a live metadata server.
"""

import os
import sys

try:
    import pytest
except ImportError:  # allow standalone execution without pytest installed
    import contextlib

    class _PytestShim:
        @staticmethod
        @contextlib.contextmanager
        def raises(exc):
            try:
                yield
            except exc:
                return
            raise AssertionError(f"expected {exc.__name__} to be raised")

    pytest = _PytestShim()

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "python")
)

# Import the module directly to avoid importing the package __init__ (which pulls
# in the native _native extension that may not be built in a pure-unit context).
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "nokv_checkpoint",
    os.path.join(os.path.dirname(__file__), "..", "python", "nokv", "checkpoint.py"),
)
checkpoint = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(checkpoint)


class FakeClient:
    """Minimal in-memory namespace + immutable-artifact store."""

    def __init__(self):
        self.files = {}  # path -> bytes
        self.dirs = {"/"}

    def mkdir(self, path, mode, uid, gid):
        if path in self.dirs or path in self.files:
            raise RuntimeError(f"exists: {path}")
        self.dirs.add(path)
        return {"name": path.rsplit("/", 1)[-1], "type": "directory", "size": 0}

    def exists(self, path):
        return path in self.files or path in self.dirs

    def put_artifact(self, path, data, producer, digest_uri, content_type,
                     manifest_id, mode, uid, gid, replace):
        parent = path.rsplit("/", 1)[0] or "/"
        if parent not in self.dirs:
            raise RuntimeError(f"parent missing: {parent}")
        if path in self.dirs:
            raise RuntimeError(f"is a directory: {path}")
        if path in self.files and not replace:
            raise RuntimeError(f"exists: {path}")
        self.files[path] = bytes(data)
        return {"name": path.rsplit("/", 1)[-1], "type": "file", "size": len(data)}

    def cat(self, path, snapshot_id=None):
        if path not in self.files:
            raise RuntimeError(f"not found: {path}")
        return self.files[path]

    def list_dir(self, path, page_limit=1024):
        prefix = "" if path == "/" else path
        out = []
        seen = set()
        for entry, kind in (
            [(f, "file") for f in self.files] + [(d, "directory") for d in self.dirs]
        ):
            if entry == path or not entry.startswith(prefix + "/"):
                continue
            rest = entry[len(prefix) + 1 :]
            name = rest.split("/", 1)[0]
            if name in seen:
                continue
            seen.add(name)
            child = f"{prefix}/{name}"
            ckind = "directory" if child in self.dirs else "file"
            size = len(self.files.get(child, b"")) if ckind == "file" else 0
            out.append({"name": name, "type": ckind, "size": size})
        return out


def test_publish_then_resolve_roundtrip():
    client = FakeClient()
    manifest = checkpoint.publish_checkpoint(
        client, "run1", 10, {"rank0": b"aaaa", "rank1": b"bbbbbb"}
    )
    assert manifest["step"] == 10
    assert {s["name"] for s in manifest["shards"]} == {"rank0", "rank1"}

    resolved = checkpoint.resolve_checkpoint(client, "run1")
    assert resolved["step"] == 10
    assert checkpoint.load_shard(client, resolved, "rank1") == b"bbbbbb"

    loaded = checkpoint.load_checkpoint(client, "run1")
    assert loaded["shards"]["rank0"] == b"aaaa"


def test_latest_step_picks_highest_committed():
    client = FakeClient()
    checkpoint.publish_checkpoint(client, "run", 1, {"s": b"x"})
    checkpoint.publish_checkpoint(client, "run", 5, {"s": b"y"})
    checkpoint.publish_checkpoint(client, "run", 3, {"s": b"z"})
    assert checkpoint.latest_step(client, "run") == 5


def test_partial_checkpoint_is_invisible():
    """A step whose shards exist but whose manifest was never written must not be
    selected by latest_step (the manifest is the commit point)."""
    client = FakeClient()
    checkpoint.publish_checkpoint(client, "run", 1, {"s": b"committed"})
    # Simulate a crash mid-publish at step 2: shards present, no manifest.
    checkpoint.publish_shard(client, "run", 2, "s", b"orphan")
    assert checkpoint.latest_step(client, "run") == 1
    assert checkpoint.resolve_checkpoint(client, "run")["step"] == 1


def test_distributed_publish_then_commit():
    """Per-rank publish_shard + a single commit_checkpoint (the barrier path)."""
    client = FakeClient()
    entries = [
        checkpoint.publish_shard(client, "run", 7, f"rank{r}", bytes([r]) * (r + 1))
        for r in range(4)
    ]
    # Not committed yet -> invisible.
    assert checkpoint.latest_step(client, "run") is None
    checkpoint.commit_checkpoint(client, "run", 7, entries)
    resolved = checkpoint.resolve_checkpoint(client, "run", 7)
    assert len(resolved["shards"]) == 4
    assert checkpoint.load_shard(client, resolved, "rank3") == bytes([3]) * 4


def test_resolve_missing_raises():
    client = FakeClient()
    with pytest.raises(FileNotFoundError):
        checkpoint.resolve_checkpoint(client, "nope")


def test_publish_shard_rejects_manifest_and_escaping_names():
    """A shard name must be a single component and must not be the manifest, so
    a malicious/buggy name cannot clobber the commit point or escape the step."""
    client = FakeClient()
    for bad in ("_manifest.json", "../escape", "a/b", "..", "", "x\x00y"):
        with pytest.raises(ValueError):
            checkpoint.publish_shard(client, "run", 1, bad, b"data")
    # publish_checkpoint funnels through the same validation.
    with pytest.raises(ValueError):
        checkpoint.publish_checkpoint(client, "run", 1, {"_manifest.json": b"x"})
    # Nothing was written for the rejected names: the step has no manifest.
    assert checkpoint.latest_step(client, "run") is None


if __name__ == "__main__":
    failures = 0
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            try:
                _fn()
                print(f"PASS {_name}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"FAIL {_name}: {exc!r}")
    print(f"\n{'OK' if failures == 0 else 'FAILED'}: {failures} failure(s)")
    sys.exit(1 if failures else 0)
