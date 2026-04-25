// Package fsmeta is NoKV's filesystem-metadata plane.
//
// fsmeta sits on top of NoKV as a consumer. It uses the DB, percolator, and
// raftstore-client surfaces to host the schema and semantics a distributed
// filesystem needs: inodes, dentries, xattrs, atomic cross-directory rename,
// multi-mount namespace routing, and subtree-scoped operations.
//
// Scope boundary:
//
//   - fsmeta is not a filesystem. It is the metadata kernel that a DFS
//     (JuiceFS, SeaweedFS, or a native FUSE frontend) consumes. NoKV's pitch is
//     "metadata backend", not "another distributed filesystem".
//
//   - fsmeta does not live under meta/. meta/root is NoKV's own rooted cluster
//     truth: region descriptors, tenure, handover, and allocator fences.
//     User-facing filesystem metadata is application data: a consumer of NoKV,
//     not part of NoKV's internal truth.
//
//   - fsmeta reuses percolator for cross-row atomicity, and may reuse
//     meta/root's rooted-event substrate only for namespace-level authority
//     where Succession semantics apply. Per-inode and per-dentry mutations are
//     data-plane writes, never rooted events.
//
// See docs/notes/2026-04-24-fsmeta-positioning.md for the market gap analysis
// and phased roadmap.
package fsmeta

import "errors"

var (
	ErrInvalidMountID     = errors.New("fsmeta: invalid mount id")
	ErrInvalidInodeID     = errors.New("fsmeta: invalid inode id")
	ErrInvalidName        = errors.New("fsmeta: invalid name")
	ErrInvalidSession     = errors.New("fsmeta: invalid session id")
	ErrInvalidRequest     = errors.New("fsmeta: invalid request")
	ErrInvalidKey         = errors.New("fsmeta: invalid key")
	ErrInvalidKeyKind     = errors.New("fsmeta: invalid key kind")
	ErrInvalidValue       = errors.New("fsmeta: invalid value")
	ErrInvalidValueKind   = errors.New("fsmeta: invalid value kind")
	ErrInvalidPageSize    = errors.New("fsmeta: invalid page size")
	ErrExists             = errors.New("fsmeta: entry exists")
	ErrNotFound           = errors.New("fsmeta: entry not found")
	ErrMountNotRegistered = errors.New("fsmeta: mount is not registered")
	ErrMountRetired       = errors.New("fsmeta: mount is retired")
	ErrQuotaExceeded      = errors.New("fsmeta: quota exceeded")
)

// MountID identifies one filesystem namespace hosted inside fsmeta.
type MountID string

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

// SnapshotSubtreeToken identifies one MVCC read epoch for a direct subtree
// page. V0 uses the token as a stable read version; recursive subtree
// materialization and GC retention enforcement are later layers.
type SnapshotSubtreeToken struct {
	Mount       MountID
	RootInode   InodeID
	ReadVersion uint64
}

const (
	// RootInode is the conventional root inode for one mount.
	RootInode InodeID = 1

	// DefaultReadDirLimit bounds one ReadDir planning request when callers do not
	// supply an explicit page size.
	DefaultReadDirLimit uint32 = 1024

	// MaxReadDirLimit keeps one directory page bounded before fsmeta grows a
	// streaming API.
	MaxReadDirLimit uint32 = 16 * 1024
)

func validateMountID(id MountID) error {
	if id == "" {
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
