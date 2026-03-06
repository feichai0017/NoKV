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

## 4. Current Project Status (2026-03-06)

The project is mostly aligned with this policy:

- Shared/common sentinels are in `utils/error.go`.
- Domain-specific sentinels exist in domain packages:
  - `kv/entry_codec.go`: `ErrBadChecksum`, `ErrPartialEntry`
  - `vfs/vfs.go`: `ErrRenameNoReplaceUnsupported`
  - `pd/core/errors.go`: PD validation/range errors

Recent cleanup already removed several unused legacy sentinels from `utils/error.go` and removed duplicated checksum sentinel in `utils`.

---

## 5. Remaining Cleanup Candidates

No high-priority domain-leak sentinels remain in `utils` after recent cleanup.

Follow-up opportunities (optional):

1. Continue reducing legacy-style names/messages for consistency.
2. Audit command-level `errXxx` values and keep them local/unexported.
3. Keep adding `%w` wrappers at package boundaries where context is still sparse.
