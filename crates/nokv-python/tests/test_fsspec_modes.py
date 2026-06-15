"""Unit tests for nokv.fsspec open-mode validation.

These exercise the immutability-driven open-mode policy (whole-object read/write
only; no append or read-update) without the native extension or a live server.
The module imports ``._native`` at top level, so we install a stub in
``sys.modules`` before loading ``fsspec.py`` directly.
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


def _load_fsspec_module():
    # Stub the native extension so the module imports in a pure-unit context.
    stub = types.ModuleType("nokv._native")
    for name in (
        "Client",
        "RangeBatchEpochReader",
        "RangeBatchPlan",
        "RangeBatchReader",
        "ReadBuffer",
    ):
        setattr(stub, name, type(name, (), {}))
    # The relative import `from ._native import ...` resolves via the parent
    # package, so register a minimal `nokv` package pointing at the source dir.
    pkg_dir = os.path.join(os.path.dirname(__file__), "..", "python", "nokv")
    nokv_pkg = types.ModuleType("nokv")
    nokv_pkg.__path__ = [pkg_dir]
    sys.modules.setdefault("nokv", nokv_pkg)
    sys.modules["nokv._native"] = stub

    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "nokv.fsspec", os.path.join(pkg_dir, "fsspec.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fsspec_mod = _load_fsspec_module()


def test_read_and_write_modes_are_accepted():
    for mode in ("rb", "wb", "r", "w", "xb"):
        fsspec_mod._validate_open_mode(mode)  # must not raise


def test_append_and_update_modes_are_rejected():
    for mode in ("ab", "a", "r+", "rb+", "w+", "wb+", "a+", "rt+"):
        with pytest.raises(ValueError):
            fsspec_mod._validate_open_mode(mode)


def test_unknown_mode_is_rejected():
    with pytest.raises(ValueError):
        fsspec_mod._validate_open_mode("zb")


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
