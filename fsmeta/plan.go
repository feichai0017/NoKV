package fsmeta

// V0 scope:
//   - PlanUnlink only touches the dentry. Hardlink ref-count and inode GC are
//     intentionally out of this contract slice.
//   - PlanRenameSubtree v0 only moves the subtree root dentry. Descendants refer
//     to parent inode IDs, so they follow the moved root without key rewrites.
//     POSIX overwrite and file-type checks belong to the executor that
//     interprets current values.
//   - mkdir, link, and setxattr are left for later slices after the base
//     transaction contract is stable.

// OperationKind identifies one metadata operation contract.
type OperationKind string

const (
	OperationCreate           OperationKind = "create"
	OperationLookup           OperationKind = "lookup"
	OperationReadDir          OperationKind = "readdir"
	OperationSnapshotSubtree  OperationKind = "snapshot_subtree"
	OperationRenameSubtree    OperationKind = "rename_subtree"
	OperationUnlink           OperationKind = "unlink"
	OperationOpenWriteSession OperationKind = "open_write_session"
)

// OperationPlan describes the key set one metadata operation will touch.
//
// It is intentionally value-only. The transaction runner decides timestamps,
// retries, and mutation encoding; fsmeta only defines the semantic key boundary.
type OperationPlan struct {
	Kind         OperationKind
	Mount        MountID
	PrimaryKey   []byte
	StartKey     []byte
	Limit        uint32
	ReadKeys     [][]byte
	ReadPrefixes [][]byte
	MutateKeys   [][]byte
}

type CreateRequest struct {
	Mount  MountID
	Parent InodeID
	Name   string
	Inode  InodeID
}

type LookupRequest struct {
	Mount  MountID
	Parent InodeID
	Name   string
}

type ReadDirRequest struct {
	Mount           MountID
	Parent          InodeID
	StartAfter      string
	Limit           uint32
	SnapshotVersion uint64
}

type SnapshotSubtreeRequest struct {
	Mount     MountID
	RootInode InodeID
}

type RenameSubtreeRequest struct {
	Mount      MountID
	FromParent InodeID
	FromName   string
	ToParent   InodeID
	ToName     string
}

type UnlinkRequest struct {
	Mount  MountID
	Parent InodeID
	Name   string
}

type OpenWriteSessionRequest struct {
	Mount   MountID
	Inode   InodeID
	Session SessionID
}

func PlanCreate(req CreateRequest) (OperationPlan, error) {
	if err := validateMountID(req.Mount); err != nil {
		return OperationPlan{}, err
	}
	if err := validateInodeID(req.Parent); err != nil {
		return OperationPlan{}, err
	}
	if err := validateInodeID(req.Inode); err != nil {
		return OperationPlan{}, err
	}
	dentry, err := EncodeDentryKey(req.Mount, req.Parent, req.Name)
	if err != nil {
		return OperationPlan{}, err
	}
	inode, err := EncodeInodeKey(req.Mount, req.Inode)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       OperationCreate,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(dentry),
		ReadKeys:   cloneKeySet(dentry),
		MutateKeys: cloneKeySet(dentry, inode),
	}, nil
}

func PlanLookup(req LookupRequest) (OperationPlan, error) {
	dentry, err := EncodeDentryKey(req.Mount, req.Parent, req.Name)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       OperationLookup,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(dentry),
		ReadKeys:   cloneKeySet(dentry),
	}, nil
}

func PlanReadDir(req ReadDirRequest) (OperationPlan, error) {
	limit, err := normalizeReadDirLimit(req.Limit)
	if err != nil {
		return OperationPlan{}, err
	}
	var startKey []byte
	if req.StartAfter == "" {
		prefix, err := EncodeDentryPrefix(req.Mount, req.Parent)
		if err != nil {
			return OperationPlan{}, err
		}
		startKey = prefix
	} else {
		if err := validateName(req.StartAfter); err != nil {
			return OperationPlan{}, err
		}
		cursor, err := EncodeDentryKey(req.Mount, req.Parent, req.StartAfter)
		if err != nil {
			return OperationPlan{}, err
		}
		// Names cannot contain NUL, so cursor+"\x00" is the first inclusive
		// seek key after the cursor while still staying inside the dentry range.
		startKey = append(cursor, 0)
	}
	prefix, err := EncodeDentryPrefix(req.Mount, req.Parent)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:         OperationReadDir,
		Mount:        req.Mount,
		PrimaryKey:   cloneBytes(prefix),
		StartKey:     cloneBytes(startKey),
		Limit:        limit,
		ReadPrefixes: cloneKeySet(prefix),
	}, nil
}

func PlanSnapshotSubtree(req SnapshotSubtreeRequest) (OperationPlan, error) {
	if err := validateMountID(req.Mount); err != nil {
		return OperationPlan{}, err
	}
	if err := validateInodeID(req.RootInode); err != nil {
		return OperationPlan{}, err
	}
	prefix, err := EncodeDentryPrefix(req.Mount, req.RootInode)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:         OperationSnapshotSubtree,
		Mount:        req.Mount,
		PrimaryKey:   cloneBytes(prefix),
		ReadPrefixes: cloneKeySet(prefix),
	}, nil
}

func PlanRenameSubtree(req RenameSubtreeRequest) (OperationPlan, error) {
	if req.FromParent == req.ToParent && req.FromName == req.ToName {
		return OperationPlan{}, ErrInvalidRequest
	}
	from, err := EncodeDentryKey(req.Mount, req.FromParent, req.FromName)
	if err != nil {
		return OperationPlan{}, err
	}
	to, err := EncodeDentryKey(req.Mount, req.ToParent, req.ToName)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       OperationRenameSubtree,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(from),
		ReadKeys:   cloneKeySet(from, to),
		MutateKeys: cloneKeySet(from, to),
	}, nil
}

func PlanUnlink(req UnlinkRequest) (OperationPlan, error) {
	dentry, err := EncodeDentryKey(req.Mount, req.Parent, req.Name)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       OperationUnlink,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(dentry),
		ReadKeys:   cloneKeySet(dentry),
		MutateKeys: cloneKeySet(dentry),
	}, nil
}

func PlanOpenWriteSession(req OpenWriteSessionRequest) (OperationPlan, error) {
	inode, err := EncodeInodeKey(req.Mount, req.Inode)
	if err != nil {
		return OperationPlan{}, err
	}
	session, err := EncodeSessionKey(req.Mount, req.Session)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       OperationOpenWriteSession,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(session),
		ReadKeys:   cloneKeySet(inode),
		MutateKeys: cloneKeySet(session),
	}, nil
}

func cloneKeySet(keys ...[]byte) [][]byte {
	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		out = append(out, cloneBytes(key))
	}
	return out
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}
