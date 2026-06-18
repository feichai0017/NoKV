"""AI-native checkpoint publish/resolve over NoKV immutable artifacts.

A training checkpoint is a set of per-rank shard files plus a manifest. Each
shard is published as an immutable artifact generation (atomic per file). The
**checkpoint** becomes visible only when its ``_manifest.json`` is written, and
the manifest is always written *last* — so a reader resolving a checkpoint never
observes a partial set of shards. The manifest is the commit point.

Two publish shapes are supported:

* ``publish_checkpoint`` — one process holds all shards and writes them plus the
  manifest in a single call (single-process, or a rank-0 that gathered shards).
* ``publish_shard`` (per rank) + ``commit_checkpoint`` (one writer, after a
  ``torch.distributed.barrier()``) — thousands of ranks each stream their own
  shard, then a single manifest write atomically commits the whole set.

Resolution (``resolve_checkpoint`` / ``latest_step``) only ever returns a step
whose manifest exists, so resume/reshard always loads a consistent checkpoint.
"""

from __future__ import annotations

import json
from typing import Iterable, Mapping, Optional

MANIFEST_NAME = "_manifest.json"
MANIFEST_VERSION = 1
_DEFAULT_BASE = "checkpoints"


def _validate_shard_name(name: str) -> None:
    """Reject a shard ``name`` that would escape the step dir or clobber the
    commit manifest.

    A shard is written at ``{step_dir}/{name}``. A ``name`` equal to
    ``_manifest.json`` would overwrite the commit point (making a partial
    checkpoint look committed), and a ``name`` containing a path separator or
    ``..`` would write outside the step dir entirely. Both must be rejected
    before any artifact is published.
    """
    if not isinstance(name, str) or name == "":
        raise ValueError("shard name must be a non-empty string")
    if name == MANIFEST_NAME:
        raise ValueError(
            f"shard name {name!r} is reserved for the checkpoint manifest"
        )
    if "/" in name or name in (".", "..") or "\x00" in name:
        raise ValueError(
            f"shard name {name!r} must be a single path component "
            "(no '/', '..', or NUL)"
        )


def step_dir(base: str, run: str, step: int) -> str:
    return f"/{base}/{run}/step_{int(step)}"


def shard_path(base: str, run: str, step: int, name: str) -> str:
    return f"{step_dir(base, run, step)}/{name}"


def ensure_dirs(client, path: str) -> None:
    """mkdir -p ``path`` using the SDK, tolerating already-present components."""
    current = ""
    for part in [p for p in path.split("/") if p]:
        current = f"{current}/{part}"
        try:
            client.mkdir(current, 0o755, 0, 0)
        except RuntimeError:
            if not client.exists(current):
                raise


def publish_shard(
    client,
    run: str,
    step: int,
    name: str,
    data: bytes,
    *,
    base: str = _DEFAULT_BASE,
    producer: str = "nokv-checkpoint",
    meta: Optional[Mapping] = None,
) -> dict:
    """Publish one shard as an immutable artifact and return its manifest entry."""
    _validate_shard_name(name)
    ensure_dirs(client, step_dir(base, run, step))
    data = bytes(data)
    path = shard_path(base, run, step, name)
    client.put_artifact(
        path,
        data,
        producer,
        "",
        "application/octet-stream",
        f"{run}/step_{step}/{name}",
        0o644,
        0,
        0,
        True,  # replace: re-publishing a shard supersedes the prior generation
    )
    entry = {"name": name, "path": path, "size": len(data)}
    if meta is not None:
        entry["meta"] = dict(meta)
    return entry


def commit_checkpoint(
    client,
    run: str,
    step: int,
    shards: Iterable[dict],
    *,
    base: str = _DEFAULT_BASE,
    extra: Optional[Mapping] = None,
    producer: str = "nokv-checkpoint",
) -> dict:
    """Write the manifest — the atomic commit point for a checkpoint.

    Call this once (e.g. on rank 0 after a distributed barrier) with the
    manifest entries returned by every rank's ``publish_shard``.
    """
    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "run": run,
        "step": int(step),
        "shards": [dict(shard) for shard in shards],
        "extra": dict(extra) if extra else {},
    }
    body = json.dumps(manifest, sort_keys=True).encode("utf-8")
    client.put_artifact(
        f"{step_dir(base, run, step)}/{MANIFEST_NAME}",
        body,
        producer,
        "",
        "application/json",
        f"{run}/step_{step}/manifest",
        0o644,
        0,
        0,
        True,
    )
    return manifest


def publish_checkpoint(
    client,
    run: str,
    step: int,
    shards: Mapping[str, bytes],
    *,
    base: str = _DEFAULT_BASE,
    extra: Optional[Mapping] = None,
    producer: str = "nokv-checkpoint",
) -> dict:
    """Publish all shards then commit the manifest, in one call."""
    entries = [
        publish_shard(client, run, step, name, data, base=base, producer=producer)
        for name, data in shards.items()
    ]
    return commit_checkpoint(
        client, run, step, entries, base=base, extra=extra, producer=producer
    )


def _has_manifest(client, base: str, run: str, step: int) -> bool:
    return client.exists(f"{step_dir(base, run, step)}/{MANIFEST_NAME}")


def latest_step(client, run: str, *, base: str = _DEFAULT_BASE) -> Optional[int]:
    """The highest step under ``run`` that has a committed manifest, or None."""
    run_dir = f"/{base}/{run}"
    if not client.exists(run_dir):
        return None
    steps = []
    for entry in client.list_dir(run_dir):
        name = entry.get("name", "")
        if name.startswith("step_") and entry.get("type") == "directory":
            try:
                steps.append(int(name[len("step_") :]))
            except ValueError:
                continue
    for step in sorted(steps, reverse=True):
        if _has_manifest(client, base, run, step):
            return step
    return None


def resolve_checkpoint(
    client, run: str, step: Optional[int] = None, *, base: str = _DEFAULT_BASE
) -> dict:
    """Return the manifest for ``step`` (or the latest committed step)."""
    if step is None:
        step = latest_step(client, run, base=base)
        if step is None:
            raise FileNotFoundError(f"no committed checkpoint for run {run!r}")
    manifest_path = f"{step_dir(base, run, step)}/{MANIFEST_NAME}"
    if not client.exists(manifest_path):
        raise FileNotFoundError(manifest_path)
    return json.loads(client.cat(manifest_path).decode("utf-8"))


def load_shard(client, manifest: dict, name: str) -> bytes:
    """Read one shard's full bytes given a resolved manifest."""
    for shard in manifest.get("shards", []):
        if shard["name"] == name:
            return client.cat(shard["path"])
    raise KeyError(name)


def load_checkpoint(
    client, run: str, step: Optional[int] = None, *, base: str = _DEFAULT_BASE
) -> dict:
    """Resolve and read every shard. Returns ``{name: bytes}`` plus the manifest.

    For large checkpoints prefer ``resolve_checkpoint`` + the batch range-read
    path (``Client.prepare_range_batch*``) so loads stream into page-locked
    staging instead of materializing every shard in the heap at once.
    """
    manifest = resolve_checkpoint(client, run, step, base=base)
    data = {shard["name"]: client.cat(shard["path"]) for shard in manifest["shards"]}
    return {"manifest": manifest, "shards": data}
