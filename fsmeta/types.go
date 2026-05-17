// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package fsmeta is NoKV's workspace namespace metadata plane.
//
// fsmeta sits on top of NoKV as a consumer. It exposes filesystem-shaped
// metadata semantics for AI agent workspaces: inodes, dentries, xattrs, atomic
// cross-directory rename, multi-mount namespace routing, snapshots, and watch
// streams. The same contract can also serve distributed filesystems that need a
// standalone metadata service.
//
// Scope boundary:
//
//   - fsmeta is not a filesystem. It is the namespace metadata kernel that an
//     agent workspace layer, a DFS, or a FUSE frontend consumes. NoKV's pitch is
//     "workspace metadata engine", not "another distributed filesystem".
//
//   - fsmeta does not live under meta/. meta/root is NoKV's own rooted cluster
//     truth: region descriptors, authority grants, and allocator fences.
//     User-facing filesystem metadata is application data: a consumer of NoKV,
//     not part of NoKV's internal truth.
//
//   - fsmeta uses runtime adapters for local and distributed storage. It may
//     reuse meta/root's rooted-event substrate only for namespace-level
//     authority where Eunomia semantics apply. Per-inode and per-dentry
//     mutations are data-plane writes, never rooted events.
package fsmeta

// MountID identifies one filesystem namespace hosted inside fsmeta.
type MountID string

// MountKeyID is the compact rooted storage identity for one mount. Public
// APIs keep using MountID; fsmeta storage keys use MountKeyID so every key does
// not repeat the human-readable mount name.
type MountKeyID uint64

// MountIdentity is the boundary object returned by rooted mount admission.
// Code that encodes storage keys must use this identity instead of trusting a
// caller-supplied string mount name.
type MountIdentity struct {
	MountID    MountID
	MountKeyID MountKeyID
}

// InodeID identifies one inode. ID 0 is reserved as invalid.
type InodeID uint64

// ChunkIndex identifies one logical file chunk.
type ChunkIndex uint64

// SessionID identifies one client/session lease record.
type SessionID string

// DentryAttrPair is the fused result returned by ReadDirPlus-style operations.
type DentryAttrPair struct {
	Dentry DentryRecord
	Inode  InodeRecord
}

// SnapshotEvidenceRef identifies one runtime-visible artifact retained by a
// SnapshotSubtreeToken. The payload bytes are intentionally not part of the
// public token; another gateway can use the runtime evidence to prove that the
// visible snapshot frontier is still recoverable.
type SnapshotEvidenceRef struct {
	EpochID       uint64
	EvidenceRoot  [32]byte
	PayloadDigest [32]byte
}

// VisibleSnapshotCapture is the runtime-internal evidence returned when a
// visible snapshot has been made durable without forcing immediate install.
type VisibleSnapshotCapture struct {
	Evidence []SnapshotEvidenceRef
}

// Valid reports whether ref can address durable runtime snapshot evidence.
func (r SnapshotEvidenceRef) Valid() bool {
	return r.EpochID != 0 &&
		r.EvidenceRoot != ([32]byte{}) &&
		r.PayloadDigest != ([32]byte{})
}

func cloneSnapshotEvidenceRefs(refs []SnapshotEvidenceRef) []SnapshotEvidenceRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]SnapshotEvidenceRef, len(refs))
	copy(out, refs)
	return out
}

// ReadVersionRequest asks for an ephemeral MVCC read version. It provides a
// consistent read timestamp only; it does not publish a snapshot epoch or pin
// GC state.
type ReadVersionRequest struct {
	Mount MountID
}

// SnapshotSubtreeToken identifies a durable subtree snapshot epoch. The token
// is published into rooted truth by the fsmeta service boundary and must be
// retired by callers when its GC-retention contract is no longer needed.
type SnapshotSubtreeToken struct {
	Mount           MountID
	MountKeyID      MountKeyID
	RootInode       InodeID
	ReadVersion     uint64
	RuntimeEvidence []SnapshotEvidenceRef
}

// Clone returns a detached snapshot token.
func (t SnapshotSubtreeToken) Clone() SnapshotSubtreeToken {
	t.RuntimeEvidence = cloneSnapshotEvidenceRefs(t.RuntimeEvidence)
	return t
}

// QuotaUsageRequest addresses one usage counter. Scope 0 is mount-wide;
// non-zero scopes are direct quota accounting roots.
type QuotaUsageRequest struct {
	Mount MountID
	Scope InodeID
}

const (
	// RootInode is the conventional root inode for one mount.
	RootInode InodeID = 1

	// MaxInodeOpaqueAttrsBytes bounds the application-owned inode payload.
	// It is for compact body references and custom attributes, not object bodies.
	MaxInodeOpaqueAttrsBytes = 16 * 1024

	// DefaultReadDirLimit bounds one ReadDir planning request when callers do not
	// supply an explicit page size.
	DefaultReadDirLimit uint32 = 1024

	// MaxReadDirLimit keeps one directory page bounded before fsmeta grows a
	// streaming API.
	MaxReadDirLimit uint32 = 16 * 1024

	// DefaultSessionExpireLimit bounds one stale-session cleanup pass when the
	// caller does not provide an explicit page size.
	DefaultSessionExpireLimit uint32 = 1024

	// MaxSessionExpireLimit keeps one session cleanup pass bounded.
	MaxSessionExpireLimit uint32 = 16 * 1024
)

func validateMountID(id MountID) error {
	if id == "" {
		return ErrInvalidMountID
	}
	return nil
}

func validateMountKeyID(id MountKeyID) error {
	if id == 0 {
		return ErrInvalidMountID
	}
	return nil
}

func validateMountIdentity(identity MountIdentity) error {
	if err := validateMountID(identity.MountID); err != nil {
		return err
	}
	return validateMountKeyID(identity.MountKeyID)
}

func validateMountIdentityForRequest(identity MountIdentity, mount MountID) error {
	if err := validateMountIdentity(identity); err != nil {
		return err
	}
	if mount != "" && mount != identity.MountID {
		return ErrInvalidMountID
	}
	return nil
}

func validateInodeID(id InodeID) error {
	if id == 0 {
		return ErrInvalidInodeID
	}
	return nil
}

func validateName(name string) error {
	if name == "" || name == "." || name == ".." {
		return ErrInvalidName
	}
	for i := 0; i < len(name); i++ {
		switch name[i] {
		case '/', 0:
			return ErrInvalidName
		}
	}
	return nil
}

func validateSessionID(id SessionID) error {
	if id == "" {
		return ErrInvalidSession
	}
	for i := 0; i < len(id); i++ {
		if id[i] == 0 {
			return ErrInvalidSession
		}
	}
	return nil
}

func normalizeReadDirLimit(limit uint32) (uint32, error) {
	if limit == 0 {
		return DefaultReadDirLimit, nil
	}
	if limit > MaxReadDirLimit {
		return 0, ErrInvalidPageSize
	}
	return limit, nil
}

func normalizeSessionExpireLimit(limit uint32) (uint32, error) {
	if limit == 0 {
		return DefaultSessionExpireLimit, nil
	}
	if limit > MaxSessionExpireLimit {
		return 0, ErrInvalidPageSize
	}
	return limit, nil
}
