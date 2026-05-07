package fsmeta

// fsmeta operation plans define semantic key boundaries only. The executor
// owns value interpretation, conflict handling, and operation-specific checks;
// the transaction runner owns timestamps, retries, and MVCC mutation encoding.
//
// RenameSubtree moves only the subtree-root dentry. Descendants reference
// parent inode IDs, so they remain reachable without descendant key rewrites.

// OperationKind identifies one metadata operation contract.
type OperationKind string

const (
	OperationCreate           OperationKind = "create"
	OperationUpdateInode      OperationKind = "update_inode"
	OperationLookup           OperationKind = "lookup"
	OperationReadDir          OperationKind = "readdir"
	OperationSnapshotSubtree  OperationKind = "snapshot_subtree"
	OperationRename           OperationKind = "rename"
	OperationRenameSubtree    OperationKind = "rename_subtree"
	OperationLink             OperationKind = "link"
	OperationUnlink           OperationKind = "unlink"
	OperationOpenWriteSession OperationKind = "open_write_session"
	OperationHeartbeatSession OperationKind = "heartbeat_write_session"
	OperationCloseSession     OperationKind = "close_write_session"
	OperationExpireSessions   OperationKind = "expire_write_sessions"
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
	Attrs  CreateAttrs
}

type CreateAttrs struct {
	Type          InodeType
	Size          uint64
	Mode          uint32
	CreatedUnixNs int64
	UpdatedUnixNs int64
	OpaqueAttrs   []byte
}

type CreateResult struct {
	Dentry DentryRecord
	Inode  InodeRecord
}

type UpdateInodeRequest struct {
	Mount            MountID
	Parent           InodeID
	Inode            InodeID
	Name             string
	SetSize          bool
	Size             uint64
	SetMode          bool
	Mode             uint32
	SetUpdatedUnixNs bool
	UpdatedUnixNs    int64
	SetOpaqueAttrs   bool
	OpaqueAttrs      []byte
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

type RenameRequest struct {
	Mount      MountID
	FromParent InodeID
	FromName   string
	ToParent   InodeID
	ToName     string
}

type RenameSubtreeRequest struct {
	Mount      MountID
	FromParent InodeID
	FromName   string
	ToParent   InodeID
	ToName     string
}

type LinkRequest struct {
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
	Mount         MountID
	Inode         InodeID
	Session       SessionID
	ExpiresUnixNs int64
}

type HeartbeatWriteSessionRequest struct {
	Mount         MountID
	Inode         InodeID
	Session       SessionID
	ExpiresUnixNs int64
}

type CloseWriteSessionRequest struct {
	Mount   MountID
	Session SessionID
}

type ExpireWriteSessionsRequest struct {
	Mount MountID
	Limit uint32
}

type ExpireWriteSessionsResult struct {
	Expired uint64
}

func PlanCreate(req CreateRequest, inodeID InodeID) (OperationPlan, error) {
	if err := validateMountID(req.Mount); err != nil {
		return OperationPlan{}, err
	}
	if err := validateInodeID(req.Parent); err != nil {
		return OperationPlan{}, err
	}
	if err := validateInodeID(inodeID); err != nil {
		return OperationPlan{}, err
	}
	dentry, err := EncodeDentryKey(req.Mount, req.Parent, req.Name)
	if err != nil {
		return OperationPlan{}, err
	}
	inode, err := EncodeInodeKey(req.Mount, inodeID)
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

func PlanUpdateInode(req UpdateInodeRequest) (OperationPlan, error) {
	if err := validateMountID(req.Mount); err != nil {
		return OperationPlan{}, err
	}
	if err := validateInodeID(req.Parent); err != nil {
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
		Kind:       OperationUpdateInode,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(inode),
		ReadKeys:   cloneKeySet(dentry, inode),
		MutateKeys: cloneKeySet(inode),
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

func PlanRename(req RenameRequest) (OperationPlan, error) {
	if req.FromParent == req.ToParent && req.FromName == req.ToName {
		return OperationPlan{}, ErrInvalidRequest
	}
	source, err := EncodeDentryKey(req.Mount, req.FromParent, req.FromName)
	if err != nil {
		return OperationPlan{}, err
	}
	dest, err := EncodeDentryKey(req.Mount, req.ToParent, req.ToName)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       OperationRename,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(source),
		ReadKeys:   cloneKeySet(source, dest),
		MutateKeys: cloneKeySet(source, dest),
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

func PlanLink(req LinkRequest) (OperationPlan, error) {
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
		Kind:       OperationLink,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(to),
		ReadKeys:   cloneKeySet(from, to),
		MutateKeys: cloneKeySet(to),
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
	owner, err := EncodeInodeSessionKey(req.Mount, req.Inode)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       OperationOpenWriteSession,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(session),
		ReadKeys:   cloneKeySet(inode, session, owner),
		MutateKeys: cloneKeySet(session, owner),
	}, nil
}

func PlanHeartbeatWriteSession(req HeartbeatWriteSessionRequest) (OperationPlan, error) {
	session, err := EncodeSessionKey(req.Mount, req.Session)
	if err != nil {
		return OperationPlan{}, err
	}
	owner, err := EncodeInodeSessionKey(req.Mount, req.Inode)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       OperationHeartbeatSession,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(session),
		ReadKeys:   cloneKeySet(session, owner),
		MutateKeys: cloneKeySet(session, owner),
	}, nil
}

func PlanCloseWriteSession(req CloseWriteSessionRequest) (OperationPlan, error) {
	session, err := EncodeSessionKey(req.Mount, req.Session)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:       OperationCloseSession,
		Mount:      req.Mount,
		PrimaryKey: cloneBytes(session),
		ReadKeys:   cloneKeySet(session),
		MutateKeys: cloneKeySet(session),
	}, nil
}

func PlanExpireWriteSessions(req ExpireWriteSessionsRequest) (OperationPlan, error) {
	limit, err := normalizeSessionExpireLimit(req.Limit)
	if err != nil {
		return OperationPlan{}, err
	}
	prefix, err := EncodeSessionPrefix(req.Mount)
	if err != nil {
		return OperationPlan{}, err
	}
	return OperationPlan{
		Kind:         OperationExpireSessions,
		Mount:        req.Mount,
		PrimaryKey:   cloneBytes(prefix),
		StartKey:     cloneBytes(prefix),
		Limit:        limit,
		ReadPrefixes: cloneKeySet(prefix),
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
