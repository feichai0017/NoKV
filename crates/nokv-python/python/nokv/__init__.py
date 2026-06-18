"""NoKV Python SDK for AI training data paths."""

from . import checkpoint
from ._native import (
    Client,
    RangeBatchEpochReader,
    RangeBatchPlan,
    RangeBatchReader,
    ReadBuffer,
    ReadBufferView,
)
from .checkpoint import (
    commit_checkpoint,
    latest_step,
    load_checkpoint,
    load_shard,
    publish_checkpoint,
    publish_shard,
    resolve_checkpoint,
)
from .fsspec import NoKVBufferedFile, NoKVFileSystem

__all__ = [
    "Client",
    "NoKVBufferedFile",
    "NoKVFileSystem",
    "RangeBatchEpochReader",
    "RangeBatchPlan",
    "RangeBatchReader",
    "ReadBuffer",
    "ReadBufferView",
    "checkpoint",
    "commit_checkpoint",
    "latest_step",
    "load_checkpoint",
    "load_shard",
    "publish_checkpoint",
    "publish_shard",
    "resolve_checkpoint",
]


def __getattr__(name):
    # Lazily expose the torch integration so importing `nokv` never hard-requires
    # torch. `nokv.torch` is available only when PyTorch is installed.
    if name == "torch":
        from . import torch as _torch

        return _torch
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
