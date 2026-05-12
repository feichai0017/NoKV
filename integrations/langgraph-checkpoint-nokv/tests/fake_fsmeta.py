from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from langgraph.checkpoint.nokv import (
    BatchLookupPlusResult,
    CreateResult,
    Dentry,
    DentryAttrPair,
    Inode,
    InodeType,
    LookupKey,
)


class FakeFsMetaClient:
    """Small in-memory fsmeta client for saver conformance tests."""

    def __init__(self) -> None:
        self._next_inode = 2
        self._dentries: dict[tuple[str, int, str], Dentry] = {}
        self._inodes: dict[tuple[str, int], Inode] = {
            ("vol", 1): Inode(
                inode=1,
                type=InodeType.DIRECTORY,
                size=0,
                mode=0o755,
                link_count=1,
                created_unix_ns=0,
                updated_unix_ns=0,
                opaque_attrs=b"",
            )
        }

    def create(
        self,
        *,
        mount: str,
        parent: int,
        name: str,
        inode_type: InodeType,
        size: int = 0,
        mode: int = 0,
        created_unix_ns: int = 0,
        updated_unix_ns: int = 0,
        opaque_attrs: bytes = b"",
        timeout: float | None = None,
    ) -> CreateResult:
        key = (mount, parent, name)
        if key in self._dentries:
            raise FileExistsError(name)
        inode_id = self._next_inode
        self._next_inode += 1
        dentry = Dentry(parent=parent, name=name, inode=inode_id, type=inode_type)
        inode = Inode(
            inode=inode_id,
            type=inode_type,
            size=size,
            mode=mode,
            link_count=1,
            created_unix_ns=created_unix_ns,
            updated_unix_ns=updated_unix_ns,
            opaque_attrs=opaque_attrs,
        )
        self._dentries[key] = dentry
        self._inodes[(mount, inode_id)] = inode
        return CreateResult(dentry=dentry, inode=inode)

    def update_inode(
        self,
        *,
        mount: str,
        parent: int,
        inode: int,
        name: str,
        size: int | None = None,
        mode: int | None = None,
        updated_unix_ns: int | None = None,
        opaque_attrs: bytes | None = None,
        timeout: float | None = None,
    ) -> Inode:
        dentry = self.lookup(mount=mount, parent=parent, name=name)
        if dentry.inode != inode:
            raise FileNotFoundError(name)
        current = self._inodes[(mount, inode)]
        updated = replace(
            current,
            size=current.size if size is None else size,
            mode=current.mode if mode is None else mode,
            updated_unix_ns=(
                current.updated_unix_ns
                if updated_unix_ns is None
                else updated_unix_ns
            ),
            opaque_attrs=current.opaque_attrs if opaque_attrs is None else opaque_attrs,
        )
        self._inodes[(mount, inode)] = updated
        return updated

    def lookup(
        self,
        *,
        mount: str,
        parent: int,
        name: str,
        timeout: float | None = None,
    ) -> Dentry:
        try:
            return self._dentries[(mount, parent, name)]
        except KeyError as exc:
            raise FileNotFoundError(name) from exc

    def lookup_plus(
        self,
        *,
        mount: str,
        parent: int,
        name: str,
        timeout: float | None = None,
    ) -> DentryAttrPair:
        dentry = self.lookup(mount=mount, parent=parent, name=name, timeout=timeout)
        return DentryAttrPair(dentry=dentry, inode=self._inodes[(mount, dentry.inode)])

    def batch_lookup_plus(
        self,
        *,
        mount: str,
        lookups: Sequence[LookupKey],
        snapshot_version: int = 0,
        timeout: float | None = None,
    ) -> list[BatchLookupPlusResult]:
        results: list[BatchLookupPlusResult] = []
        for lookup in lookups:
            try:
                dentry = self._dentries[(mount, lookup.parent, lookup.name)]
            except KeyError:
                results.append(BatchLookupPlusResult(found=False, entry=None))
                continue
            pair = DentryAttrPair(
                dentry=dentry,
                inode=self._inodes[(mount, dentry.inode)],
            )
            results.append(BatchLookupPlusResult(found=True, entry=pair))
        return results

    def read_dir_plus(
        self,
        *,
        mount: str,
        parent: int,
        start_after: str = "",
        limit: int = 0,
        snapshot_version: int = 0,
        timeout: float | None = None,
    ) -> list[DentryAttrPair]:
        entries = [
            dentry
            for (entry_mount, entry_parent, _), dentry in self._dentries.items()
            if entry_mount == mount and entry_parent == parent and dentry.name > start_after
        ]
        entries.sort(key=lambda dentry: dentry.name)
        if limit:
            entries = entries[:limit]
        return [
            DentryAttrPair(dentry=dentry, inode=self._inodes[(mount, dentry.inode)])
            for dentry in entries
        ]

    def rename(
        self,
        *,
        mount: str,
        from_parent: int,
        from_name: str,
        to_parent: int,
        to_name: str,
        timeout: float | None = None,
    ) -> None:
        from_key = (mount, from_parent, from_name)
        to_key = (mount, to_parent, to_name)
        if to_key in self._dentries:
            raise FileExistsError(to_name)
        try:
            dentry = self._dentries.pop(from_key)
        except KeyError as exc:
            raise FileNotFoundError(from_name) from exc
        self._dentries[to_key] = replace(dentry, parent=to_parent, name=to_name)

    def unlink(
        self,
        *,
        mount: str,
        parent: int,
        name: str,
        timeout: float | None = None,
    ) -> None:
        key = (mount, parent, name)
        try:
            dentry = self._dentries.pop(key)
        except KeyError as exc:
            raise FileNotFoundError(name) from exc
        self._inodes.pop((mount, dentry.inode), None)
