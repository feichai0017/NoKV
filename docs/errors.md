# Error Handling Guide

This document defines how NoKV should own, define, and propagate errors.

---

## 1. Ownership Rules

1. Domain errors stay in domain packages.
2. Cross-cutting runtime errors may live in `utils` only when shared by multiple subsystems.
3. Command-local/business-flow errors should be unexported (`errXxx`) and stay in command/service packages.

Examples:

- `kv`: entry codec/read-decode errors.
- `vfs`: filesystem contract errors.
- `pd/core`: control-plane validation/conflict errors.

---

## 2. Propagation Rules

1. Wrap with `%w` when crossing package boundaries.
2. Match via `errors.Is`, not string compare.
3. Keep stable sentinel values for retryable / control-flow decisions.
4. Add context in upper layers; do not lose original cause.

---

## 3. Naming Rules

1. Exported sentinels use `ErrXxx`.
2. Error text should be lowercase and package-scoped when useful (for example `pd/core: ...`, `vfs: ...`).
3. Avoid duplicate sentinels with identical semantics in different packages.

---

## 4. Current Error Map

### Shared runtime sentinels

- `utils/error.go`: common cross-package sentinels such as invalid request,
  key/value validation errors, throttling, and lifecycle guards.

### Domain-specific sentinels

- `kv/entry_codec.go`: `ErrBadChecksum`, `ErrPartialEntry`
- `vfs/vfs.go`: `ErrRenameNoReplaceUnsupported`
- `lsm/compaction.go`: compaction planner/runtime domain errors
- `raftstore/peer/errors.go`: peer lifecycle/state errors
- `pb/errorpb.proto`: region/store routing protobuf errors (`RegionError`,
  `StoreNotMatch`, `RegionNotFound`, `KeyNotInRegion`, ...)
- `wal/errors.go`: WAL encode/decode and segment errors
- `pd/core/errors.go`: PD metadata and range validation errors

---

## 5. Propagation in Hot Paths

1. Embedded write path (`DB.Set*` -> commit worker -> LSM/WAL):
   - validation returns direct sentinel (`ErrEmptyKey`, `ErrNilValue`, `ErrInvalidRequest`);
   - storage boundary errors are wrapped with context and preserved via `%w`.
2. Distributed command path (`kv.Service` -> `Store.*Command` -> `kv.Apply`):
   - region/leader/store/range failures are mapped to `errorpb` messages in protobuf responses;
   - execution failures return Go errors to RPC layer and are translated to gRPC status.
3. Recovery/replay path (WAL/Vlog/Manifest):
   - partial/corrupt records return domain sentinels and are handled by truncation or
     restart logic in upper layers.
