from __future__ import annotations

import json
import os
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Protocol


@dataclass(frozen=True)
class BodyRef:
    """Compact reference to an external checkpoint body."""

    kind: str
    digest: str
    size: int

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.to_json_obj(), separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )

    @classmethod
    def from_json_bytes(cls, data: bytes) -> BodyRef:
        raw = json.loads(data.decode("utf-8"))
        return cls.from_json_obj(raw)

    def to_json_obj(self) -> dict[str, int | str]:
        return {"kind": self.kind, "digest": self.digest, "size": self.size}

    @classmethod
    def from_json_obj(cls, raw: object) -> BodyRef:
        if not isinstance(raw, dict):
            raise ValueError("body ref must be a JSON object")
        return cls(
            kind=str(raw["kind"]),
            digest=str(raw["digest"]),
            size=int(raw["size"]),
        )


@dataclass(frozen=True)
class TypedBodyRef:
    """Serializer type tag plus an optional external body reference."""

    type: str
    body_ref: BodyRef | None = None

    def to_json_obj(self) -> dict[str, object]:
        obj: dict[str, object] = {"type": self.type}
        if self.body_ref is not None:
            obj["ref"] = self.body_ref.to_json_obj()
        return obj

    @classmethod
    def from_json_obj(cls, raw: object) -> TypedBodyRef:
        if not isinstance(raw, dict):
            raise ValueError("typed body ref must be a JSON object")
        ref = raw.get("ref")
        return cls(
            type=str(raw["type"]),
            body_ref=BodyRef.from_json_obj(ref) if ref is not None else None,
        )


class BodyStore(Protocol):
    def put(self, data: bytes) -> BodyRef: ...

    def get(self, ref: BodyRef) -> bytes: ...

    def delete(self, ref: BodyRef) -> None: ...


class BodyNotFoundError(FileNotFoundError):
    pass


class BodyIntegrityError(ValueError):
    pass


class CheckpointBodyStore:
    """Stores serialized LangGraph checkpoint payloads outside fsmeta."""

    empty_type = "empty"

    def __init__(self, body_store: BodyStore) -> None:
        self.body_store = body_store

    @classmethod
    def from_local_path(
        cls, root: str | os.PathLike[str], *, shard_prefix_len: int = 2
    ) -> CheckpointBodyStore:
        return cls(FileBodyStore(root, shard_prefix_len=shard_prefix_len))

    def put_typed(self, type_tag: str, data: bytes | None) -> TypedBodyRef:
        if data is None:
            if type_tag != self.empty_type:
                raise ValueError("non-empty body types require bytes")
            return TypedBodyRef(type=type_tag)
        return TypedBodyRef(type=type_tag, body_ref=self.body_store.put(data))

    def get_typed(self, body: TypedBodyRef) -> tuple[str, bytes | None]:
        if body.body_ref is None:
            if body.type != self.empty_type:
                raise ValueError("typed body is missing a body ref")
            return body.type, None
        return body.type, self.body_store.get(body.body_ref)


class FileBodyStore:
    """Content-addressed body store backed by the local filesystem."""

    kind = "file-sha256"

    def __init__(self, root: str | os.PathLike[str], *, shard_prefix_len: int = 2) -> None:
        if shard_prefix_len < 0:
            raise ValueError("shard_prefix_len must be non-negative")
        self.root = Path(root)
        self.shard_prefix_len = shard_prefix_len
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, data: bytes) -> BodyRef:
        digest = sha256(data).hexdigest()
        ref = BodyRef(kind=self.kind, digest=digest, size=len(data))
        path = self._path_for(ref)
        if path.exists():
            return ref

        path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
            tmp.write(data)
            tmp_path = Path(tmp.name)
        try:
            os.replace(tmp_path, path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
        return ref

    def get(self, ref: BodyRef) -> bytes:
        self._validate_ref(ref)
        path = self._path_for(ref)
        try:
            data = path.read_bytes()
        except FileNotFoundError as exc:
            raise BodyNotFoundError(str(path)) from exc
        if len(data) != ref.size:
            raise BodyIntegrityError(
                f"body size mismatch for {ref.digest}: expected {ref.size}, got {len(data)}"
            )
        digest = sha256(data).hexdigest()
        if digest != ref.digest:
            raise BodyIntegrityError(
                f"body digest mismatch for {ref.digest}: got {digest}"
            )
        return data

    def delete(self, ref: BodyRef) -> None:
        self._validate_ref(ref)
        try:
            self._path_for(ref).unlink()
        except FileNotFoundError:
            return

    def _path_for(self, ref: BodyRef) -> Path:
        self._validate_ref(ref)
        if self.shard_prefix_len == 0:
            return self.root / ref.digest
        return self.root / ref.digest[: self.shard_prefix_len] / ref.digest

    def _validate_ref(self, ref: BodyRef) -> None:
        if ref.kind != self.kind:
            raise ValueError(f"unsupported body kind: {ref.kind!r}")
        if len(ref.digest) != 64:
            raise ValueError("sha256 digest must be 64 hex characters")
        try:
            int(ref.digest, 16)
        except ValueError as exc:
            raise ValueError("sha256 digest must be hex") from exc
        if ref.size < 0:
            raise ValueError("body size must be non-negative")
