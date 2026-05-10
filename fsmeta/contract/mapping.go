package contract

import (
	"context"
	"sync"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
)

// NewInodeMappingExecutor adapts an external fsmeta service to the contract
// harness. Generated histories use deterministic inode ids so later operations
// can refer to objects before the real service allocates storage ids; the
// adapter translates those planned ids to server-assigned ids at the API
// boundary and translates returned records back before the model checker sees
// them.
func NewInodeMappingExecutor(base Executor) (Executor, error) {
	if base == nil {
		return nil, errMappingRequired
	}
	m := &inodeMappingExecutor{
		base:            base,
		plannedToActual: make(map[fsmeta.InodeID]fsmeta.InodeID),
		actualToPlanned: make(map[fsmeta.InodeID]fsmeta.InodeID),
	}
	m.rememberLocked(fsmeta.RootInode, fsmeta.RootInode)
	return m, nil
}

type inodeMappingExecutor struct {
	base Executor

	mu              sync.RWMutex
	plannedToActual map[fsmeta.InodeID]fsmeta.InodeID
	actualToPlanned map[fsmeta.InodeID]fsmeta.InodeID
}

func (m *inodeMappingExecutor) Create(ctx context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	planned, hasPlanned := plannedCreateInode(ctx)
	req.Parent = m.actualInode(req.Parent)
	result, err := m.base.Create(ctx, req)
	if err != nil {
		if hasPlanned && createOutcomeAmbiguous(err) {
			if recovered, ok := m.recoverCreate(ctx, req, planned); ok {
				return recovered, nil
			}
		}
		return fsmeta.CreateResult{}, err
	}
	if !hasPlanned {
		planned = result.Inode.Inode
	}
	m.remember(planned, result.Inode.Inode)
	return m.translateCreateResult(result), nil
}

func createOutcomeAmbiguous(err error) bool {
	return nokverrors.Retryable(err)
}

func (m *inodeMappingExecutor) UpdateInode(ctx context.Context, req fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error) {
	req.Parent = m.actualInode(req.Parent)
	req.Inode = m.actualInode(req.Inode)
	record, err := m.base.UpdateInode(ctx, req)
	if err != nil {
		return fsmeta.InodeRecord{}, err
	}
	return m.translateInodeRecord(record), nil
}

func (m *inodeMappingExecutor) Lookup(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	req.Parent = m.actualInode(req.Parent)
	record, err := m.base.Lookup(ctx, req)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	return m.translateDentryRecord(record), nil
}

func (m *inodeMappingExecutor) ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	req.Parent = m.actualInode(req.Parent)
	pairs, err := m.base.ReadDirPlus(ctx, req)
	if err != nil {
		return nil, err
	}
	out := make([]fsmeta.DentryAttrPair, len(pairs))
	for i, pair := range pairs {
		out[i] = fsmeta.DentryAttrPair{
			Dentry: m.translateDentryRecord(pair.Dentry),
			Inode:  m.translateInodeRecord(pair.Inode),
		}
	}
	return out, nil
}

func (m *inodeMappingExecutor) SnapshotSubtree(ctx context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	req.RootInode = m.actualInode(req.RootInode)
	token, err := m.base.SnapshotSubtree(ctx, req)
	if err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	token.RootInode = m.plannedInode(token.RootInode)
	return token, nil
}

func (m *inodeMappingExecutor) Rename(ctx context.Context, req fsmeta.RenameRequest) error {
	req.FromParent = m.actualInode(req.FromParent)
	req.ToParent = m.actualInode(req.ToParent)
	return m.base.Rename(ctx, req)
}

func (m *inodeMappingExecutor) RenameSubtree(ctx context.Context, req fsmeta.RenameSubtreeRequest) error {
	req.FromParent = m.actualInode(req.FromParent)
	req.ToParent = m.actualInode(req.ToParent)
	return m.base.RenameSubtree(ctx, req)
}

func (m *inodeMappingExecutor) Link(ctx context.Context, req fsmeta.LinkRequest) error {
	req.FromParent = m.actualInode(req.FromParent)
	req.ToParent = m.actualInode(req.ToParent)
	return m.base.Link(ctx, req)
}

func (m *inodeMappingExecutor) Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error {
	req.Parent = m.actualInode(req.Parent)
	return m.base.Unlink(ctx, req)
}

func (m *inodeMappingExecutor) OpenWriteSession(ctx context.Context, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	req.Inode = m.actualInode(req.Inode)
	record, err := m.base.OpenWriteSession(ctx, req)
	if err != nil {
		return fsmeta.SessionRecord{}, err
	}
	return m.translateSessionRecord(record), nil
}

func (m *inodeMappingExecutor) HeartbeatWriteSession(ctx context.Context, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error) {
	req.Inode = m.actualInode(req.Inode)
	record, err := m.base.HeartbeatWriteSession(ctx, req)
	if err != nil {
		return fsmeta.SessionRecord{}, err
	}
	return m.translateSessionRecord(record), nil
}

func (m *inodeMappingExecutor) CloseWriteSession(ctx context.Context, req fsmeta.CloseWriteSessionRequest) error {
	req.Inode = m.actualInode(req.Inode)
	return m.base.CloseWriteSession(ctx, req)
}

func (m *inodeMappingExecutor) ExpireWriteSessions(ctx context.Context, req fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error) {
	return m.base.ExpireWriteSessions(ctx, req)
}

func (m *inodeMappingExecutor) recoverCreate(ctx context.Context, req fsmeta.CreateRequest, planned fsmeta.InodeID) (fsmeta.CreateResult, bool) {
	dentry, err := m.base.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Name:   req.Name,
	})
	if err != nil {
		return fsmeta.CreateResult{}, false
	}
	m.remember(planned, dentry.Inode)
	return m.translateCreateResult(fsmeta.CreateResult{
		Dentry: dentry,
		Inode:  req.Attrs.InodeRecord(dentry.Inode),
	}), true
}

func (m *inodeMappingExecutor) translateCreateResult(result fsmeta.CreateResult) fsmeta.CreateResult {
	return fsmeta.CreateResult{
		Dentry: m.translateDentryRecord(result.Dentry),
		Inode:  m.translateInodeRecord(result.Inode),
	}
}

func (m *inodeMappingExecutor) translateDentryRecord(record fsmeta.DentryRecord) fsmeta.DentryRecord {
	record.Parent = m.plannedInode(record.Parent)
	record.Inode = m.plannedInode(record.Inode)
	return record
}

func (m *inodeMappingExecutor) translateInodeRecord(record fsmeta.InodeRecord) fsmeta.InodeRecord {
	record.Inode = m.plannedInode(record.Inode)
	return record
}

func (m *inodeMappingExecutor) translateSessionRecord(record fsmeta.SessionRecord) fsmeta.SessionRecord {
	record.Inode = m.plannedInode(record.Inode)
	return record
}

func (m *inodeMappingExecutor) actualInode(planned fsmeta.InodeID) fsmeta.InodeID {
	if planned == 0 {
		return 0
	}
	m.mu.RLock()
	actual, ok := m.plannedToActual[planned]
	m.mu.RUnlock()
	if ok {
		return actual
	}
	return planned
}

func (m *inodeMappingExecutor) plannedInode(actual fsmeta.InodeID) fsmeta.InodeID {
	if actual == 0 {
		return 0
	}
	m.mu.RLock()
	planned, ok := m.actualToPlanned[actual]
	m.mu.RUnlock()
	if ok {
		return planned
	}
	return actual
}

func (m *inodeMappingExecutor) remember(planned, actual fsmeta.InodeID) {
	if planned == 0 || actual == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rememberLocked(planned, actual)
}

func (m *inodeMappingExecutor) rememberLocked(planned, actual fsmeta.InodeID) {
	m.plannedToActual[planned] = actual
	m.actualToPlanned[actual] = planned
}
