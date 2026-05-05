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
//     truth: region descriptors, authority grants, and allocator fences.
//     User-facing filesystem metadata is application data: a consumer of NoKV,
//     not part of NoKV's internal truth.
//
//   - fsmeta reuses percolator for cross-row atomicity, and may reuse
//     meta/root's rooted-event substrate only for namespace-level authority
//     where Eunomia semantics apply. Per-inode and per-dentry mutations are
//     data-plane writes, never rooted events.
package fsmeta

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
