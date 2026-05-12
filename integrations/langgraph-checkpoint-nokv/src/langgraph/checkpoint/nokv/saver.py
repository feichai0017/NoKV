from __future__ import annotations

import time
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from copy import deepcopy
from threading import RLock
from typing import Any

import grpc
from langchain_core.runnables import RunnableConfig

from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    DeltaChannelHistory,
    PendingWrite,
    get_checkpoint_id,
    get_serializable_checkpoint_metadata,
)
from langgraph.checkpoint.nokv.body_store import CheckpointBodyStore, TypedBodyRef
from langgraph.checkpoint.nokv.fsmeta_client import (
    DentryAttrPair,
    Inode,
    InodeType,
    NoKVFsMetaClient,
)
from langgraph.checkpoint.nokv.layout import (
    ChannelBlobEntryAttrs,
    CheckpointEntryAttrs,
    DeltaWriteEntryAttrs,
    EntryPath,
    FsMetaLayout,
    HeadEntryAttrs,
    WriteEntryAttrs,
    directory_attrs,
    thread_tombstone_attrs,
)

_ROOT_INODE = 1
_DIR_PAGE_LIMIT = 1024
_LOOKUP_CONFLICT_RETRIES = 8
_LOOKUP_CONFLICT_INITIAL_BACKOFF_S = 0.005


class NoKVCheckpointSaver(BaseCheckpointSaver[str]):
    """LangGraph checkpoint saver backed by NoKV fsmeta and an external body store."""

    def __init__(
        self,
        *,
        fsmeta_client: NoKVFsMetaClient,
        mount: str,
        body_store: CheckpointBodyStore,
        root_inode: int = _ROOT_INODE,
        layout: FsMetaLayout | None = None,
        enable_delta_index: bool = True,
    ) -> None:
        super().__init__()
        self.fsmeta = fsmeta_client
        self.mount = mount
        self.body_store = body_store
        self.root_inode = root_inode
        self.layout = layout or FsMetaLayout()
        self.enable_delta_index = enable_delta_index
        self._dir_cache_lock = RLock()
        self._dir_inode_cache: dict[tuple[str, ...], int] = {(): root_inode}

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        configurable = config["configurable"]
        thread_id = configurable["thread_id"]
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        parent_checkpoint_id = get_checkpoint_id(config)
        checkpoint_id = checkpoint["id"]

        self._ensure_dir(self.layout.checkpoints_dir(thread_id, checkpoint_ns))
        self._ensure_dir(self.layout.writes_dir(thread_id, checkpoint_ns))
        seed_body_refs_by_channel = self._write_channel_blobs(
            thread_id, checkpoint_ns, checkpoint, new_versions
        )

        checkpoint_body = deepcopy(checkpoint)
        checkpoint_body["channel_values"] = {}
        envelope = {
            "checkpoint": checkpoint_body,
            "metadata": get_serializable_checkpoint_metadata(config, metadata),
        }
        body = self._put_typed_body(envelope)
        checkpoint_attrs = CheckpointEntryAttrs(
            checkpoint_id=checkpoint_id,
            parent_checkpoint_id=parent_checkpoint_id,
            body=body,
            seed_body_refs_by_channel=seed_body_refs_by_channel,
        )
        self._put_file(
            self.layout.checkpoint_entry(thread_id, checkpoint_ns, checkpoint_id),
            checkpoint_attrs.to_opaque_attrs(),
        )
        self._put_file(
            self.layout.head_entry(thread_id, checkpoint_ns),
            HeadEntryAttrs(checkpoint_id=checkpoint_id).to_opaque_attrs(),
        )
        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return self.put(config, checkpoint, metadata, new_versions)

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        if self._thread_is_deleted(thread_id):
            return None

        checkpoint_id = get_checkpoint_id(config)
        if checkpoint_id is None:
            checkpoint_id = self._latest_checkpoint_id(thread_id, checkpoint_ns)
            if checkpoint_id is None:
                return None

        return self._load_tuple(thread_id, checkpoint_ns, checkpoint_id)

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return self.get_tuple(config)

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        if config is None:
            return
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        if self._thread_is_deleted(thread_id):
            return

        before_id = get_checkpoint_id(before) if before else None
        entries = self._list_dir(self.layout.checkpoints_dir(thread_id, checkpoint_ns))
        checkpoint_ids: list[str] = []
        for pair in entries:
            try:
                attrs = CheckpointEntryAttrs.from_opaque_attrs(pair.inode.opaque_attrs)
            except (KeyError, ValueError):
                continue
            checkpoint_ids.append(attrs.checkpoint_id)

        yielded = 0
        for checkpoint_id in sorted(set(checkpoint_ids), reverse=True):
            if before_id is not None and checkpoint_id >= before_id:
                continue
            tup = self._load_tuple(thread_id, checkpoint_ns, checkpoint_id)
            if tup is None:
                continue
            if filter and not _metadata_matches(tup.metadata, filter):
                continue
            yield tup
            yielded += 1
            if limit is not None and yielded >= limit:
                break

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        for item in self.list(config, filter=filter, before=before, limit=limit):
            yield item

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        configurable = config["configurable"]
        thread_id = configurable["thread_id"]
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        checkpoint_id = configurable["checkpoint_id"]
        self._ensure_dir(
            self.layout.checkpoint_writes_dir(thread_id, checkpoint_ns, checkpoint_id)
        )

        for ordinal, (channel, value) in enumerate(writes):
            idx = WRITES_IDX_MAP.get(channel, ordinal)
            entry = self.layout.write_entry(
                thread_id, checkpoint_ns, checkpoint_id, task_id, idx
            )
            if channel not in WRITES_IDX_MAP and self._read_file(entry) is not None:
                continue
            body = self._put_typed_body(value)
            attrs = WriteEntryAttrs(
                task_id=task_id,
                task_path=task_path,
                idx=idx,
                channel=channel,
                body=body,
            )
            self._put_file(entry, attrs.to_opaque_attrs())
            if self.enable_delta_index:
                self._put_delta_write_index(
                    thread_id=thread_id,
                    checkpoint_ns=checkpoint_ns,
                    checkpoint_id=checkpoint_id,
                    attrs=attrs,
                )

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        self.put_writes(config, writes, task_id, task_path)

    def delete_thread(self, thread_id: str) -> None:
        self._put_file(
            self.layout.thread_tombstone_entry(thread_id),
            thread_tombstone_attrs(time.time_ns()),
        )

    async def adelete_thread(self, thread_id: str) -> None:
        self.delete_thread(thread_id)

    def get_delta_channel_history(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, DeltaChannelHistory]:
        """Return DeltaChannel replay history using the fsmeta delta index.

        This follows the same two-stage shape as LangGraph's official saver
        fast paths:

        * Stage 1 walks checkpoint parent metadata once for all requested
          channels to identify each channel's on-path replay window and nearest
          seed checkpoint.
        * Stage 2 reads materialized `delta_channels/<channel>/...` entries for
          those channels, avoiding a `writes/<checkpoint_id>` directory scan for
          every ancestor checkpoint.

        `writes/<checkpoint_id>/...` remains the canonical source of truth. The
        delta tree is a derived acceleration index maintained by `put_writes()`.
        The current public fsmeta API does not expose a cross-file batch mutate,
        so the canonical write and index write are not crash-atomic yet. If the
        index is absent or unreadable, this method falls back to the canonical
        parent-chain reader. A future generic fsmeta batch-mutate API can remove
        that crash window without changing this saver contract.
        """
        if not channels:
            return {}

        if self.enable_delta_index:
            indexed = self._get_delta_channel_history_from_index(
                config=config, channels=channels
            )
            if indexed is not None:
                return indexed

        return self._get_delta_channel_history_by_parent_chain(
            config=config, channels=channels
        )

    def _get_delta_channel_history_from_index(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, DeltaChannelHistory] | None:
        """Read replay writes from the per-channel materialized index.

        The index is scoped by channel rather than by checkpoint. We still walk
        the target checkpoint's parent chain first, then filter every indexed
        write through that on-path checkpoint set. That preserves fork
        correctness while reducing the expensive fsmeta reads from
        O(ancestor checkpoints) write-directory scans to O(requested channels)
        delta-index directory scans.

        Returning `None` means the derived index should not be trusted for this
        read and the caller must use the canonical parent-chain path.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        if self._thread_is_deleted(thread_id):
            return _empty_delta_result(channels)

        checkpoint_id = self._resolve_target_checkpoint_id(
            config, thread_id, checkpoint_ns
        )
        if checkpoint_id is None:
            return _empty_delta_result(channels)

        window = self._delta_replay_window(
            thread_id, checkpoint_ns, checkpoint_id, channels
        )
        if window is None:
            return None
        eligible_by_ch, seed_ref_by_ch, ancestor_rank = window
        seed_by_ch = {
            channel: self._get_typed_body(seed_ref)
            for channel, seed_ref in seed_ref_by_ch.items()
        }

        collected_by_ch: dict[str, list[PendingWrite]] = {}
        for channel in channels:
            indexed_writes = self._load_delta_index_writes(
                thread_id=thread_id,
                checkpoint_ns=checkpoint_ns,
                channel=channel,
                eligible_checkpoints=eligible_by_ch[channel],
                ancestor_rank=ancestor_rank,
            )
            if indexed_writes is None:
                return None
            collected_by_ch[channel] = indexed_writes

        return _delta_result_presorted(channels, collected_by_ch, seed_by_ch)

    def _get_delta_channel_history_by_parent_chain(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, DeltaChannelHistory]:
        if not channels:
            return {}

        channel_set = set(channels)
        collected_by_ch: dict[str, list[PendingWrite]] = {ch: [] for ch in channels}
        seed_by_ch: dict[str, Any] = {}
        remaining = set(channel_set)

        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        if self._thread_is_deleted(thread_id):
            return _delta_result(channels, collected_by_ch, seed_by_ch)

        checkpoint_id = get_checkpoint_id(config)
        if checkpoint_id is None:
            checkpoint_id = self._latest_checkpoint_id(thread_id, checkpoint_ns)
            if checkpoint_id is None:
                return _delta_result(channels, collected_by_ch, seed_by_ch)

        target = self._load_checkpoint_attrs(thread_id, checkpoint_ns, checkpoint_id)
        cursor_id = target.parent_checkpoint_id if target is not None else None
        while cursor_id is not None and remaining:
            record = self._load_checkpoint_record(thread_id, checkpoint_ns, cursor_id)
            if record is None:
                break

            attrs, envelope = record
            seed_here: dict[str, Any] = {}
            checkpoint = envelope["checkpoint"]
            channel_versions = checkpoint["channel_versions"]
            for channel in list(remaining):
                found, seed = self._load_channel_value(
                    thread_id,
                    checkpoint_ns,
                    channel,
                    channel_versions.get(channel),
                )
                if found:
                    seed_here[channel] = seed

            for write in reversed(
                self._load_pending_writes(
                    thread_id, checkpoint_ns, cursor_id, channels=remaining
                )
            ):
                collected_by_ch[write[1]].append(write)

            for channel, seed in seed_here.items():
                seed_by_ch[channel] = seed
                remaining.discard(channel)

            cursor_id = attrs.parent_checkpoint_id

        return _delta_result(channels, collected_by_ch, seed_by_ch)

    async def aget_delta_channel_history(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, DeltaChannelHistory]:
        return self.get_delta_channel_history(config=config, channels=channels)

    def _put_delta_write_index(
        self,
        *,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        attrs: WriteEntryAttrs,
    ) -> None:
        """Maintain the derived channel-first index for DeltaChannel reads.

        This index is intentionally not the source of truth. The canonical
        `writes/<checkpoint_id>/...` record is written before this method is
        called. Duplicate ordinary writes therefore keep LangGraph's normal
        no-op behavior, while special negative-index writes update the same
        deterministic index entry.
        """
        delta_attrs = DeltaWriteEntryAttrs(
            checkpoint_id=checkpoint_id,
            task_id=attrs.task_id,
            task_path=attrs.task_path,
            idx=attrs.idx,
            channel=attrs.channel,
            body=attrs.body,
        )
        self._put_file(
            self.layout.delta_write_entry(
                thread_id,
                checkpoint_ns,
                attrs.channel,
                checkpoint_id,
                attrs.task_id,
                attrs.idx,
            ),
            delta_attrs.to_opaque_attrs(),
        )

    def _resolve_target_checkpoint_id(
        self, config: RunnableConfig, thread_id: str, checkpoint_ns: str
    ) -> str | None:
        checkpoint_id = get_checkpoint_id(config)
        if checkpoint_id is not None:
            return checkpoint_id
        return self._latest_checkpoint_id(thread_id, checkpoint_ns)

    def _delta_replay_window(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        channels: Sequence[str],
    ) -> tuple[dict[str, set[str]], dict[str, TypedBodyRef], dict[str, int]] | None:
        """Build per-channel replay windows from checkpoint parent metadata.

        The returned `eligible_by_ch` contains the ancestor checkpoint ids whose
        pending writes can contribute to each channel. Checkpoint attrs carry
        seed body refs, so this stage does not hydrate checkpoint envelopes or
        read channel blob inodes just to discover where each channel's seed is.
        """
        target = self._load_checkpoint_attrs(thread_id, checkpoint_ns, checkpoint_id)
        if target is None:
            empty = {channel: set() for channel in channels}
            return empty, {}, {}

        remaining = set(channels)
        eligible_by_ch: dict[str, set[str]] = {channel: set() for channel in channels}
        seed_ref_by_ch: dict[str, TypedBodyRef] = {}
        ancestor_rank: dict[str, int] = {}
        cursor_id = target.parent_checkpoint_id
        rank = 0

        while cursor_id is not None and remaining:
            attrs = self._load_checkpoint_attrs(thread_id, checkpoint_ns, cursor_id)
            if attrs is None:
                break
            if attrs.seed_body_refs_by_channel is None:
                return None

            ancestor_rank[cursor_id] = rank
            seed_refs = attrs.seed_body_refs_by_channel
            for channel in list(remaining):
                eligible_by_ch[channel].add(cursor_id)
                seed_ref = seed_refs.get(channel)
                if seed_ref is not None:
                    seed_ref_by_ch[channel] = seed_ref
                    remaining.discard(channel)

            cursor_id = attrs.parent_checkpoint_id
            rank += 1

        return eligible_by_ch, seed_ref_by_ch, ancestor_rank

    def _load_delta_index_writes(
        self,
        *,
        thread_id: str,
        checkpoint_ns: str,
        channel: str,
        eligible_checkpoints: set[str],
        ancestor_rank: Mapping[str, int],
    ) -> list[PendingWrite] | None:
        if not eligible_checkpoints:
            return []

        delta_dir = self.layout.delta_channel_dir(thread_id, checkpoint_ns, channel)
        try:
            self._resolve_path(delta_dir)
        except FileNotFoundError:
            return None

        writes: list[tuple[int, str, int, DeltaWriteEntryAttrs]] = []
        for pair in self._list_dir(delta_dir):
            try:
                attrs = DeltaWriteEntryAttrs.from_opaque_attrs(pair.inode.opaque_attrs)
            except (KeyError, ValueError):
                return None
            if attrs.channel != channel:
                return None
            if attrs.checkpoint_id not in eligible_checkpoints:
                continue
            rank = ancestor_rank.get(attrs.checkpoint_id)
            if rank is None:
                return None
            writes.append(
                (
                    rank,
                    attrs.task_id,
                    attrs.idx,
                    attrs,
                )
            )

        writes.sort(key=lambda item: (-item[0], item[1], item[2]))
        return [
            (attrs.task_id, attrs.channel, self._get_typed_body(attrs.body))
            for _, _, _, attrs in writes
        ]

    def _write_channel_blobs(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint: Checkpoint,
        new_versions: ChannelVersions,
    ) -> dict[str, TypedBodyRef]:
        values = checkpoint["channel_values"]
        seed_body_refs_by_channel: dict[str, TypedBodyRef] = {}
        for channel, version in new_versions.items():
            if channel in values:
                body = self._put_typed_body(values[channel])
                seed_body_refs_by_channel[channel] = body
            else:
                body = TypedBodyRef(type=CheckpointBodyStore.empty_type)
            attrs = ChannelBlobEntryAttrs(
                channel=channel,
                version=str(version),
                body=body,
            )
            self._put_file(
                self.layout.blob_entry(thread_id, checkpoint_ns, channel, version),
                attrs.to_opaque_attrs(),
            )
        return seed_body_refs_by_channel

    def _load_tuple(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> CheckpointTuple | None:
        record = self._load_checkpoint_record(thread_id, checkpoint_ns, checkpoint_id)
        if record is None:
            return None
        checkpoint_attrs, envelope = record
        checkpoint = envelope["checkpoint"]
        metadata = envelope["metadata"]
        checkpoint["channel_values"] = self._load_channel_values(
            thread_id, checkpoint_ns, checkpoint["channel_versions"]
        )
        parent_config = (
            {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_attrs.parent_checkpoint_id,
                }
            }
            if checkpoint_attrs.parent_checkpoint_id is not None
            else None
        )
        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            },
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=parent_config,
            pending_writes=self._load_pending_writes(
                thread_id, checkpoint_ns, checkpoint_id
            ),
        )

    def _load_checkpoint_record(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> tuple[CheckpointEntryAttrs, dict[str, Any]] | None:
        checkpoint_attrs = self._load_checkpoint_attrs(
            thread_id, checkpoint_ns, checkpoint_id
        )
        if checkpoint_attrs is None:
            return None
        return checkpoint_attrs, self._get_typed_body(checkpoint_attrs.body)

    def _load_checkpoint_attrs(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> CheckpointEntryAttrs | None:
        entry = self._read_file(
            self.layout.checkpoint_entry(thread_id, checkpoint_ns, checkpoint_id)
        )
        if entry is None:
            return None
        return CheckpointEntryAttrs.from_opaque_attrs(entry.inode.opaque_attrs)

    def _load_channel_values(
        self,
        thread_id: str,
        checkpoint_ns: str,
        channel_versions: dict[str, Any],
    ) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for channel, version in channel_versions.items():
            found, value = self._load_channel_value(
                thread_id, checkpoint_ns, channel, version
            )
            if found:
                values[channel] = value
        return values

    def _load_channel_value(
        self,
        thread_id: str,
        checkpoint_ns: str,
        channel: str,
        version: Any | None,
    ) -> tuple[bool, Any]:
        if version is None:
            return False, None
        entry = self._read_file(
            self.layout.blob_entry(thread_id, checkpoint_ns, channel, version)
        )
        if entry is None:
            return False, None
        attrs = ChannelBlobEntryAttrs.from_opaque_attrs(entry.inode.opaque_attrs)
        if attrs.body.type == CheckpointBodyStore.empty_type:
            return False, None
        return True, self._get_typed_body(attrs.body)

    def _load_pending_writes(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        *,
        channels: set[str] | None = None,
    ) -> list[tuple[str, str, Any]]:
        writes: list[tuple[str, int, str, Any]] = []
        for pair in self._list_dir(
            self.layout.checkpoint_writes_dir(thread_id, checkpoint_ns, checkpoint_id)
        ):
            try:
                attrs = WriteEntryAttrs.from_opaque_attrs(pair.inode.opaque_attrs)
            except (KeyError, ValueError):
                continue
            if channels is not None and attrs.channel not in channels:
                continue
            writes.append(
                (
                    attrs.task_id,
                    attrs.idx,
                    attrs.channel,
                    self._get_typed_body(attrs.body),
                )
            )
        writes.sort(key=lambda item: (item[0], item[1]))
        return [(task_id, channel, value) for task_id, _, channel, value in writes]

    def _latest_checkpoint_id(self, thread_id: str, checkpoint_ns: str) -> str | None:
        entry = self._read_file(self.layout.head_entry(thread_id, checkpoint_ns))
        if entry is None:
            return None
        return HeadEntryAttrs.from_opaque_attrs(entry.inode.opaque_attrs).checkpoint_id

    def _thread_is_deleted(self, thread_id: str) -> bool:
        return self._read_file(self.layout.thread_tombstone_entry(thread_id)) is not None

    def _put_typed_body(self, value: Any) -> TypedBodyRef:
        type_tag, data = self.serde.dumps_typed(value)
        return self.body_store.put_typed(type_tag, data)

    def _get_typed_body(self, body: TypedBodyRef) -> Any:
        type_tag, data = self.body_store.get_typed(body)
        return self.serde.loads_typed((type_tag, data))

    def _ensure_dir(self, path: tuple[str, ...]) -> int:
        cached = self._cached_dir_inode(path)
        if cached is not None:
            return cached

        parent = self.root_inode
        current_path: tuple[str, ...] = ()
        for name in path:
            current_path = (*current_path, name)
            cached = self._cached_dir_inode(current_path)
            if cached is not None:
                parent = cached
                continue
            try:
                dentry = self.fsmeta.lookup(mount=self.mount, parent=parent, name=name)
            except Exception as exc:
                if not _is_not_found(exc):
                    raise
                try:
                    created = self.fsmeta.create(
                        mount=self.mount,
                        parent=parent,
                        name=name,
                        inode_type=InodeType.DIRECTORY,
                        mode=0o755,
                        opaque_attrs=directory_attrs(name),
                    )
                    parent = created.inode.inode
                    continue
                except Exception as create_exc:
                    if not _is_already_exists(create_exc):
                        raise
                    dentry = self._lookup_after_create_conflict(parent, name)
            parent = dentry.inode
            self._cache_dir_inode(current_path, parent)
        return parent

    def _put_file(self, entry: EntryPath, opaque_attrs: bytes) -> Inode:
        parent = self._ensure_dir(entry.parent)
        existing = self._find_child(parent, entry.name)
        if existing is None:
            try:
                return self.fsmeta.create(
                    mount=self.mount,
                    parent=parent,
                    name=entry.name,
                    inode_type=InodeType.FILE,
                    size=len(opaque_attrs),
                    mode=0o600,
                    opaque_attrs=opaque_attrs,
                ).inode
            except Exception as exc:
                if not _is_already_exists(exc):
                    raise
                existing = self._find_child_after_create_conflict(parent, entry.name)
                if existing is None:
                    raise
        return self.fsmeta.update_inode(
            mount=self.mount,
            parent=parent,
            inode=existing.inode.inode,
            name=entry.name,
            size=len(opaque_attrs),
            updated_unix_ns=time.time_ns(),
            opaque_attrs=opaque_attrs,
        )

    def _read_file(self, entry: EntryPath) -> DentryAttrPair | None:
        try:
            parent = self._resolve_path(entry.parent)
        except FileNotFoundError:
            return None
        return self._find_child(parent, entry.name)

    def _find_child(self, parent: int, name: str) -> DentryAttrPair | None:
        try:
            return self.fsmeta.lookup_plus(
                mount=self.mount,
                parent=parent,
                name=name,
            )
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise

    def _list_dir(self, path: tuple[str, ...]) -> list[DentryAttrPair]:
        try:
            parent = self._resolve_path(path)
        except FileNotFoundError:
            return []
        entries: list[DentryAttrPair] = []
        start_after = ""
        while True:
            page = self.fsmeta.read_dir_plus(
                mount=self.mount,
                parent=parent,
                start_after=start_after,
                limit=_DIR_PAGE_LIMIT,
            )
            entries.extend(page)
            if len(page) < _DIR_PAGE_LIMIT:
                return entries
            start_after = page[-1].dentry.name

    def _resolve_path(self, path: tuple[str, ...]) -> int:
        cached = self._cached_dir_inode(path)
        if cached is not None:
            return cached

        parent = self.root_inode
        current_path: tuple[str, ...] = ()
        for name in path:
            current_path = (*current_path, name)
            cached = self._cached_dir_inode(current_path)
            if cached is not None:
                parent = cached
                continue
            try:
                dentry = self.fsmeta.lookup(mount=self.mount, parent=parent, name=name)
            except Exception as exc:
                if _is_not_found(exc):
                    raise FileNotFoundError(name) from exc
                raise
            parent = dentry.inode
            self._cache_dir_inode(current_path, parent)
        return parent

    def _cached_dir_inode(self, path: tuple[str, ...]) -> int | None:
        with self._dir_cache_lock:
            return self._dir_inode_cache.get(path)

    def _cache_dir_inode(self, path: tuple[str, ...], inode: int) -> None:
        with self._dir_cache_lock:
            self._dir_inode_cache[path] = inode

    def _lookup_after_create_conflict(self, parent: int, name: str) -> Any:
        backoff = _LOOKUP_CONFLICT_INITIAL_BACKOFF_S
        last_error: BaseException | None = None
        for _ in range(_LOOKUP_CONFLICT_RETRIES):
            try:
                return self.fsmeta.lookup(mount=self.mount, parent=parent, name=name)
            except Exception as exc:
                if not _is_not_found(exc):
                    raise
                last_error = exc
                time.sleep(backoff)
                backoff *= 2
        if last_error is not None:
            raise last_error
        raise FileNotFoundError(name)

    def _find_child_after_create_conflict(
        self, parent: int, name: str
    ) -> DentryAttrPair | None:
        backoff = _LOOKUP_CONFLICT_INITIAL_BACKOFF_S
        for _ in range(_LOOKUP_CONFLICT_RETRIES):
            found = self._find_child(parent, name)
            if found is not None:
                return found
            time.sleep(backoff)
            backoff *= 2
        return None


def _metadata_matches(metadata: CheckpointMetadata, expected: dict[str, Any]) -> bool:
    return all(metadata.get(key) == value for key, value in expected.items())


def _delta_result(
    channels: Sequence[str],
    collected_by_ch: dict[str, list[PendingWrite]],
    seed_by_ch: dict[str, Any],
) -> dict[str, DeltaChannelHistory]:
    result: dict[str, DeltaChannelHistory] = {}
    for channel in channels:
        entry: DeltaChannelHistory = {"writes": list(reversed(collected_by_ch[channel]))}
        if channel in seed_by_ch:
            entry["seed"] = seed_by_ch[channel]
        result[channel] = entry
    return result


def _delta_result_presorted(
    channels: Sequence[str],
    collected_by_ch: dict[str, list[PendingWrite]],
    seed_by_ch: dict[str, Any],
) -> dict[str, DeltaChannelHistory]:
    result: dict[str, DeltaChannelHistory] = {}
    for channel in channels:
        entry: DeltaChannelHistory = {"writes": list(collected_by_ch[channel])}
        if channel in seed_by_ch:
            entry["seed"] = seed_by_ch[channel]
        result[channel] = entry
    return result


def _empty_delta_result(channels: Sequence[str]) -> dict[str, DeltaChannelHistory]:
    return {channel: {"writes": []} for channel in channels}


def _is_not_found(exc: BaseException) -> bool:
    if isinstance(exc, FileNotFoundError):
        return True
    if isinstance(exc, grpc.RpcError):
        try:
            return exc.code() == grpc.StatusCode.NOT_FOUND
        except Exception:
            return False
    return False


def _is_already_exists(exc: BaseException) -> bool:
    if isinstance(exc, FileExistsError):
        return True
    if isinstance(exc, grpc.RpcError):
        try:
            return exc.code() == grpc.StatusCode.ALREADY_EXISTS
        except Exception:
            return False
    return False
