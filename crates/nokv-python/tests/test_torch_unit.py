"""Unit tests for nokv.torch helpers that don't need a real PyTorch.

torch.py imports ``torch`` (and torch DCP types) at module load, so we install
lightweight stubs in ``sys.modules`` before importing it directly. This lets us
test two correctness-sensitive behaviors in a pure-unit context:

* ``_global_rank`` returns 0 only when no process group is initialized, and lets
  a real ``get_rank()`` failure propagate (never silently collapsing ranks).
* ``read_data`` raises when the batch read returns fewer blobs than requested
  ranges, instead of silently truncating via ``zip``.
"""

import os
import sys
import types

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


class _FakeDistributed:
    def __init__(self):
        self._available = True
        self._initialized = False
        self._rank = 0
        self._raise = None

    def is_available(self):
        return self._available

    def is_initialized(self):
        return self._initialized

    def get_rank(self):
        if self._raise is not None:
            raise self._raise
        return self._rank


def _install_torch_stub():
    torch = types.ModuleType("torch")
    torch.distributed = _FakeDistributed()

    def _load(buf, map_location=None):  # pragma: no cover - unused in these tests
        raise AssertionError("torch.load should not be called in these tests")

    torch.load = _load

    class _Future:
        def set_result(self, value):
            self._value = value

    torch.futures = types.SimpleNamespace(Future=_Future)
    sys.modules["torch"] = torch

    # torch.utils.data names imported at module top.
    utils = types.ModuleType("torch.utils")
    data = types.ModuleType("torch.utils.data")
    data.Dataset = type("Dataset", (), {})
    data.IterableDataset = type("IterableDataset", (), {})
    data.get_worker_info = lambda: None
    utils.data = data
    sys.modules["torch.utils"] = utils
    sys.modules["torch.utils.data"] = data

    # DCP types: StorageReader/StorageWriter are imported at module load (their
    # presence sets _HAS_DCP), and WriteItemType is resolved lazily in read_data.
    dcp = types.ModuleType("torch.distributed.checkpoint")
    dcp.StorageReader = type("StorageReader", (), {})
    dcp.StorageWriter = type("StorageWriter", (), {})
    dcp.WriteItemType = types.SimpleNamespace(BYTE_IO="BYTE_IO")
    sys.modules["torch.distributed.checkpoint"] = dcp
    return torch


def _load_torch_module():
    _install_torch_stub()
    pkg_dir = os.path.join(os.path.dirname(__file__), "..", "python", "nokv")
    nokv_pkg = types.ModuleType("nokv")
    nokv_pkg.__path__ = [pkg_dir]
    sys.modules.setdefault("nokv", nokv_pkg)

    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "nokv.torch", os.path.join(pkg_dir, "torch.py")
    )
    module = importlib.util.module_from_spec(spec)
    # Register before exec so @dataclass can resolve cls.__module__ in sys.modules
    # (Python 3.12+ dataclass machinery looks the module up there during class
    # creation).
    sys.modules["nokv.torch"] = module
    spec.loader.exec_module(module)
    return module


nokv_torch = _load_torch_module()


def test_global_rank_zero_when_not_initialized():
    import torch

    torch.distributed._initialized = False
    assert nokv_torch._global_rank() == 0


def test_global_rank_returns_get_rank_when_initialized():
    import torch

    torch.distributed._initialized = True
    torch.distributed._rank = 3
    try:
        assert nokv_torch._global_rank() == 3
    finally:
        torch.distributed._initialized = False


def test_global_rank_propagates_get_rank_failure():
    import torch

    torch.distributed._initialized = True
    torch.distributed._raise = RuntimeError("process group broken")
    try:
        with pytest.raises(RuntimeError):
            nokv_torch._global_rank()
    finally:
        torch.distributed._initialized = False
        torch.distributed._raise = None


class _FakeInfo:
    def __init__(self, path, offset, length):
        self.path = path
        self.offset = offset
        self.length = length


class _FakeReadItem:
    def __init__(self, storage_index):
        self.storage_index = storage_index
        self.type = "BYTE_IO"


class _FakePlan:
    def __init__(self, items):
        self.items = items


class _FakeClient:
    def __init__(self, blobs_per_batch):
        self._blobs = blobs_per_batch

    def read_ranges_batch(self, requests):
        # Return a short blob list to trigger the length guard.
        return [self._blobs]


def test_read_data_raises_on_blob_count_mismatch():
    reader = nokv_torch.NoKVStorageReader(
        _FakeClient(blobs_per_batch=[b"only-one"]), "run", 1
    )
    # Two read items mapped to the same shard, but the client returns one blob.
    reader.storage_data = {
        0: _FakeInfo("shard.bin", 0, 8),
        1: _FakeInfo("shard.bin", 8, 8),
    }
    plan = _FakePlan([_FakeReadItem(0), _FakeReadItem(1)])
    with pytest.raises(RuntimeError):
        reader.read_data(plan, planner=None)


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
