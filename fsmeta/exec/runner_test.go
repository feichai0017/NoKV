package exec

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/feichai0017/NoKV/engine/slab/dirpage"
	"github.com/feichai0017/NoKV/engine/slab/negativecache"
	"github.com/feichai0017/NoKV/fsmeta"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

type fakeRunner struct {
	nextTS        uint64
	data          map[string][]byte
	mutations     [][]*kvrpcpb.Mutation
	getCalls      int
	scanVersions  []uint64
	batchVersions []uint64
	mutateErr     error
	mutateErrs    []error
}

type fakeMountResolver struct {
	records map[fsmeta.MountID]MountAdmission
	err     error
	calls   int
}

type fakeSubtreePublisher struct {
	starts      []subtreePublishCall
	completes   []subtreePublishCall
	err         error
	startErr    error
	completeErr error
}

type fakeQuotaResolver struct {
	err      error
	changes  [][]QuotaChange
	mutation *kvrpcpb.Mutation
}

type subtreePublishCall struct {
	mount    fsmeta.MountID
	root     fsmeta.InodeID
	frontier uint64
}

func (q *fakeQuotaResolver) ReserveQuota(_ context.Context, _ TxnRunner, changes []QuotaChange, _ uint64) ([]*kvrpcpb.Mutation, error) {
	q.changes = append(q.changes, append([]QuotaChange(nil), changes...))
	if q.err != nil {
		return nil, q.err
	}
	if q.mutation != nil {
		return []*kvrpcpb.Mutation{cloneMutation(q.mutation)}, nil
	}
	return nil, nil
}

func (p *fakeSubtreePublisher) StartSubtreeHandoff(_ context.Context, mount fsmeta.MountID, root fsmeta.InodeID, frontier uint64) error {
	if p.startErr != nil {
		return p.startErr
	}
	if p.err != nil {
		return p.err
	}
	p.starts = append(p.starts, subtreePublishCall{mount: mount, root: root, frontier: frontier})
	return nil
}

func (p *fakeSubtreePublisher) CompleteSubtreeHandoff(_ context.Context, mount fsmeta.MountID, root fsmeta.InodeID, frontier uint64) error {
	if p.completeErr != nil {
		return p.completeErr
	}
	if p.err != nil {
		return p.err
	}
	p.completes = append(p.completes, subtreePublishCall{mount: mount, root: root, frontier: frontier})
	return nil
}

func (r *fakeMountResolver) ResolveMount(_ context.Context, mount fsmeta.MountID) (MountAdmission, error) {
	r.calls++
	if r.err != nil {
		return MountAdmission{}, r.err
	}
	record, ok := r.records[mount]
	if !ok {
		return MountAdmission{}, fsmeta.ErrMountNotRegistered
	}
	return record, nil
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{
		nextTS: 1,
		data:   make(map[string][]byte),
	}
}

func (r *fakeRunner) ReserveTimestamp(_ context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errors.New("zero timestamp reservation")
	}
	first := r.nextTS
	r.nextTS += count
	return first, nil
}

func (r *fakeRunner) Get(_ context.Context, key []byte, _ uint64) ([]byte, bool, error) {
	r.getCalls++
	value, ok := r.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (r *fakeRunner) BatchGet(_ context.Context, keys [][]byte, version uint64) (map[string][]byte, error) {
	r.batchVersions = append(r.batchVersions, version)
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if value, ok := r.data[string(key)]; ok {
			out[string(key)] = append([]byte(nil), value...)
		}
	}
	return out, nil
}

func (r *fakeRunner) Scan(_ context.Context, startKey []byte, limit uint32, version uint64) ([]KV, error) {
	r.scanVersions = append(r.scanVersions, version)
	keys := make([][]byte, 0, len(r.data))
	for key := range r.data {
		if bytes.Compare([]byte(key), startKey) >= 0 {
			keys = append(keys, []byte(key))
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	out := make([]KV, 0, limit)
	for _, key := range keys {
		if uint32(len(out)) >= limit {
			break
		}
		out = append(out, KV{
			Key:   append([]byte(nil), key...),
			Value: append([]byte(nil), r.data[string(key)]...),
		})
	}
	return out, nil
}

func TestExecutorSnapshotSubtreeTokenDrivesReadVersion(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 21, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	executor, err := New(runner)
	require.NoError(t, err)

	token, err := executor.SnapshotSubtree(context.Background(), fsmeta.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 7,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.SnapshotSubtreeToken{Mount: "vol", RootInode: 7, ReadVersion: 1}, token)

	_, err = executor.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount:           "vol",
		Parent:          7,
		Limit:           8,
		SnapshotVersion: token.ReadVersion,
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{token.ReadVersion}, runner.scanVersions)
	require.Equal(t, []uint64{token.ReadVersion}, runner.batchVersions)
}

func TestExecutorGetQuotaUsage(t *testing.T) {
	runner := newFakeRunner()
	key, err := fsmeta.EncodeUsageKey("vol", 7)
	require.NoError(t, err)
	value, err := fsmeta.EncodeUsageValue(fsmeta.UsageRecord{Bytes: 4096, Inodes: 2})
	require.NoError(t, err)
	runner.data[string(key)] = value
	executor, err := New(runner)
	require.NoError(t, err)

	usage, err := executor.GetQuotaUsage(context.Background(), fsmeta.QuotaUsageRequest{Mount: "vol", Scope: 7})
	require.NoError(t, err)
	require.Equal(t, fsmeta.UsageRecord{Bytes: 4096, Inodes: 2}, usage)
}

func TestExecutorGetQuotaUsageReturnsZeroForMissingCounter(t *testing.T) {
	runner := newFakeRunner()
	executor, err := New(runner)
	require.NoError(t, err)

	usage, err := executor.GetQuotaUsage(context.Background(), fsmeta.QuotaUsageRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.UsageRecord{}, usage)
}

func (r *fakeRunner) Mutate(_ context.Context, _ []byte, mutations []*kvrpcpb.Mutation, _, _, _ uint64) error {
	if len(r.mutateErrs) > 0 {
		err := r.mutateErrs[0]
		r.mutateErrs = r.mutateErrs[1:]
		if err != nil {
			return err
		}
	}
	if r.mutateErr != nil {
		return r.mutateErr
	}
	cloned := make([]*kvrpcpb.Mutation, 0, len(mutations))
	for _, mut := range mutations {
		if mut.GetAssertionNotExist() {
			if _, ok := r.data[string(mut.GetKey())]; ok {
				return fsmeta.ErrExists
			}
		}
		cloned = append(cloned, cloneMutation(mut))
	}
	for _, mut := range cloned {
		switch mut.GetOp() {
		case kvrpcpb.Mutation_Put:
			r.data[string(mut.GetKey())] = append([]byte(nil), mut.GetValue()...)
		case kvrpcpb.Mutation_Delete:
			delete(r.data, string(mut.GetKey()))
		}
	}
	r.mutations = append(r.mutations, cloned)
	return nil
}

func TestExecutorCreateAndLookup(t *testing.T) {
	runner := newFakeRunner()
	executor, err := New(runner)
	require.NoError(t, err)

	err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Inode:  22,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.NoError(t, err)

	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{
		Parent: fsmeta.RootInode,
		Name:   "file",
		Inode:  22,
		Type:   fsmeta.InodeTypeFile,
	}, record)

	require.Len(t, runner.mutations, 1)
	require.Len(t, runner.mutations[0], 2)
	require.True(t, runner.mutations[0][0].GetAssertionNotExist())
	require.True(t, runner.mutations[0][1].GetAssertionNotExist())
}

func TestExecutorCreateRejectsExistingDentry(t *testing.T) {
	runner := newFakeRunner()
	executor, err := New(runner)
	require.NoError(t, err)

	req := fsmeta.CreateRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "file", Inode: 22}
	err = executor.Create(context.Background(), req, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.NoError(t, err)

	err = executor.Create(context.Background(), req, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.ErrorIs(t, err, fsmeta.ErrExists)
	require.Len(t, runner.mutations, 1)
	require.Zero(t, runner.getCalls)
}

func TestExecutorCreateRequiresActiveMountWhenResolverConfigured(t *testing.T) {
	t.Run("active mount", func(t *testing.T) {
		runner := newFakeRunner()
		resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
			"vol": {MountID: "vol", RootInode: fsmeta.RootInode, SchemaVersion: 1},
		}}
		executor, err := New(runner, WithMountResolver(resolver))
		require.NoError(t, err)

		err = executor.Create(context.Background(), fsmeta.CreateRequest{
			Mount:  "vol",
			Parent: fsmeta.RootInode,
			Name:   "file",
			Inode:  22,
		}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
		require.NoError(t, err)
		require.Equal(t, 1, resolver.calls)
		require.Len(t, runner.mutations, 1)
	})

	t.Run("missing mount", func(t *testing.T) {
		runner := newFakeRunner()
		resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{}}
		executor, err := New(runner, WithMountResolver(resolver))
		require.NoError(t, err)

		err = executor.Create(context.Background(), fsmeta.CreateRequest{
			Mount:  "missing",
			Parent: fsmeta.RootInode,
			Name:   "file",
			Inode:  22,
		}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
		require.ErrorIs(t, err, fsmeta.ErrMountNotRegistered)
		require.Equal(t, 1, resolver.calls)
		require.Empty(t, runner.mutations)
	})

	t.Run("retired mount", func(t *testing.T) {
		runner := newFakeRunner()
		resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
			"vol": {MountID: "vol", RootInode: fsmeta.RootInode, SchemaVersion: 1, Retired: true},
		}}
		executor, err := New(runner, WithMountResolver(resolver))
		require.NoError(t, err)

		err = executor.Create(context.Background(), fsmeta.CreateRequest{
			Mount:  "vol",
			Parent: fsmeta.RootInode,
			Name:   "file",
			Inode:  22,
		}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
		require.ErrorIs(t, err, fsmeta.ErrMountRetired)
		require.Equal(t, 1, resolver.calls)
		require.Empty(t, runner.mutations)
	})
}

func TestExecutorCreateReservesQuotaInsideMutation(t *testing.T) {
	runner := newFakeRunner()
	quotaKey, err := fsmeta.EncodeUsageKey("vol", 0)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: quotaKey, Value: []byte("usage")}}
	executor, err := New(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Inode:  22,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, Size: 4096})
	require.NoError(t, err)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.changes)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, quotaKey, runner.mutations[0][2].GetKey())
}

func TestExecutorCreateRejectsQuotaExceededBeforeMutation(t *testing.T) {
	runner := newFakeRunner()
	quota := &fakeQuotaResolver{err: fsmeta.ErrQuotaExceeded}
	executor, err := New(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Inode:  22,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, Size: 4096})
	require.ErrorIs(t, err, fsmeta.ErrQuotaExceeded)
	require.Empty(t, runner.mutations)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.changes)
}

func TestExecutorUnlinkReservesNegativeQuotaWhenInodeExists(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 1})
	quota := &fakeQuotaResolver{}
	executor, err := New(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), fsmeta.UnlinkRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.NoError(t, err)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", Scope: 7, Bytes: -4096, Inodes: -1}}}, quota.changes)
}

func TestExecutorLinkCreatesDentryAndIncrementsLinkCount(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 1})
	quota := &fakeQuotaResolver{}
	executor, err := New(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	err = executor.Link(context.Background(), fsmeta.LinkRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "file",
		ToParent:   8,
		ToName:     "alias",
	})
	require.NoError(t, err)

	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 8, Name: "alias"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), record.Inode)
	inode, ok, err := executor.readInode(context.Background(), "vol", 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(2), inode.LinkCount)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", Scope: 8, Bytes: 4096, Inodes: 1}}}, quota.changes)
}

func TestExecutorLinkRejectsDirectory(t *testing.T) {
	runner := newFakeRunner()
	seedDentryType(t, runner, "vol", 7, "dir", 22, fsmeta.InodeTypeDirectory)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeDirectory, LinkCount: 1})
	executor, err := New(runner)
	require.NoError(t, err)

	err = executor.Link(context.Background(), fsmeta.LinkRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "dir",
		ToParent:   8,
		ToName:     "alias",
	})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
	require.Empty(t, runner.mutations)
}

func TestExecutorCreateTranslatesAlreadyExistsConflict(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErr = fakeKeyConflictError{errors: []*kvrpcpb.KeyError{{
		AlreadyExists: &kvrpcpb.KeyAlreadyExists{Key: []byte("dentry")},
	}}}
	executor, err := New(runner)
	require.NoError(t, err)

	err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Inode:  22,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.ErrorIs(t, err, fsmeta.ErrExists)
	require.Zero(t, runner.getCalls)
}

func TestExecutorRetriesCommitTsExpired(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		fakeKeyConflictError{errors: []*kvrpcpb.KeyError{{
			CommitTsExpired: &kvrpcpb.CommitTsExpired{
				Key:         []byte("dentry"),
				CommitTs:    2,
				MinCommitTs: 5,
			},
		}}},
		nil,
	}
	executor, err := New(runner)
	require.NoError(t, err)

	err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Inode:  22,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, uint64(5), runner.nextTS)
	require.Equal(t, uint64(1), executor.Stats()["txn_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["txn_retry_exhausted_total"])
}

func TestExecutorLookupReturnsNotFound(t *testing.T) {
	executor, err := New(newFakeRunner())
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "missing",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
}

func TestExecutorReadDirConsumesPlanCursorAndLimit(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedDentry(t, runner, "vol", 7, "b", 22)
	seedDentry(t, runner, "vol", 7, "c", 23)
	seedDentry(t, runner, "vol", 8, "outside", 99)

	executor, err := New(runner)
	require.NoError(t, err)

	records, err := executor.ReadDir(context.Background(), fsmeta.ReadDirRequest{
		Mount:      "vol",
		Parent:     7,
		StartAfter: "a",
		Limit:      1,
	})
	require.NoError(t, err)
	require.Equal(t, []fsmeta.DentryRecord{{
		Parent: 7,
		Name:   "b",
		Inode:  22,
		Type:   fsmeta.InodeTypeFile,
	}}, records)
}

func TestExecutorReadDirPlusReturnsDentriesAndAttrs(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{
		Inode:     21,
		Type:      fsmeta.InodeTypeFile,
		Size:      4096,
		Mode:      0o644,
		LinkCount: 1,
	})
	seedDentryType(t, runner, "vol", 7, "b", 22, fsmeta.InodeTypeDirectory)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{
		Inode:     22,
		Type:      fsmeta.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 2,
	})

	executor, err := New(runner)
	require.NoError(t, err)

	pairs, err := executor.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []fsmeta.DentryAttrPair{
		{
			Dentry: fsmeta.DentryRecord{Parent: 7, Name: "a", Inode: 21, Type: fsmeta.InodeTypeFile},
			Inode: fsmeta.InodeRecord{
				Inode:     21,
				Type:      fsmeta.InodeTypeFile,
				Size:      4096,
				Mode:      0o644,
				LinkCount: 1,
			},
		},
		{
			Dentry: fsmeta.DentryRecord{Parent: 7, Name: "b", Inode: 22, Type: fsmeta.InodeTypeDirectory},
			Inode: fsmeta.InodeRecord{
				Inode:     22,
				Type:      fsmeta.InodeTypeDirectory,
				Mode:      0o755,
				LinkCount: 2,
			},
		},
	}, pairs)
}

func TestExecutorReadDirPlusMissingInodeReturnsNotFound(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	executor, err := New(runner)
	require.NoError(t, err)

	_, err = executor.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  8,
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
}

func TestExecutorUnlinkRemovesDentry(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	executor, err := New(runner)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), fsmeta.UnlinkRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
	})
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	require.Len(t, runner.mutations, 1)
	require.Len(t, runner.mutations[0], 2)
	require.Equal(t, kvrpcpb.Mutation_Delete, runner.mutations[0][0].GetOp())
	require.Equal(t, kvrpcpb.Mutation_Delete, runner.mutations[0][1].GetOp())
	_, ok, err := executor.readInode(context.Background(), "vol", 22, 99)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestExecutorUnlinkDecrementsMultiLinkInode(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 2})
	executor, err := New(runner)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), fsmeta.UnlinkRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.NoError(t, err)

	inode, ok, err := executor.readInode(context.Background(), "vol", 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(1), inode.LinkCount)
}

func TestExecutorUnlinkMissingDentry(t *testing.T) {
	runner := newFakeRunner()
	executor, err := New(runner)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), fsmeta.UnlinkRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "missing",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	require.Empty(t, runner.mutations)
}

func TestExecutorRenameSubtreeMovesDentry(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	publisher := &fakeSubtreePublisher{}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := New(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), fsmeta.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	})
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "old"})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 8, Name: "new"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{
		Parent: 8,
		Name:   "new",
		Inode:  22,
		Type:   fsmeta.InodeTypeFile,
	}, record)
	require.Len(t, runner.mutations, 1)
	require.Len(t, runner.mutations[0], 2)
	require.Equal(t, kvrpcpb.Mutation_Delete, runner.mutations[0][0].GetOp())
	require.Equal(t, kvrpcpb.Mutation_Put, runner.mutations[0][1].GetOp())
	require.True(t, runner.mutations[0][1].GetAssertionNotExist())
	require.Equal(t, []subtreePublishCall{{mount: "vol", root: fsmeta.RootInode, frontier: 2}}, publisher.starts)
	require.Equal(t, []subtreePublishCall{{mount: "vol", root: fsmeta.RootInode, frontier: 2}}, publisher.completes)
}

func TestExecutorRenameSubtreeBlocksMutationWhenStartHandoffFails(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	publisher := &fakeSubtreePublisher{startErr: errors.New("publish failed")}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := New(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), fsmeta.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	})
	require.ErrorContains(t, err, "publish failed")
	require.Empty(t, runner.mutations)

	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "old"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), record.Inode)
}

func TestExecutorRenameSubtreeReportsCompleteHandoffFailureAfterMutation(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	publisher := &fakeSubtreePublisher{completeErr: errors.New("complete failed")}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := New(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), fsmeta.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	})
	require.ErrorContains(t, err, "complete failed")
	require.Len(t, runner.mutations, 1)
	require.Equal(t, []subtreePublishCall{{mount: "vol", root: fsmeta.RootInode, frontier: 2}}, publisher.starts)

	_, err = executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "old"})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 8, Name: "new"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), record.Inode)
}

func TestExecutorRenameSubtreeRejectsMissingSource(t *testing.T) {
	runner := newFakeRunner()
	executor, err := New(runner)
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), fsmeta.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "missing",
		ToParent:   8,
		ToName:     "new",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	require.Empty(t, runner.mutations)
}

func TestExecutorRenameSubtreeRejectsExistingDestination(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	seedDentry(t, runner, "vol", 8, "existing", 23)
	publisher := &fakeSubtreePublisher{}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := New(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), fsmeta.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "existing",
	})
	require.ErrorIs(t, err, fsmeta.ErrExists)
	require.Empty(t, runner.mutations)
	require.Empty(t, publisher.starts)
	require.Empty(t, publisher.completes)
}

func seedDentry(t *testing.T, runner *fakeRunner, mount fsmeta.MountID, parent fsmeta.InodeID, name string, inode fsmeta.InodeID) {
	t.Helper()
	seedDentryType(t, runner, mount, parent, name, inode, fsmeta.InodeTypeFile)
}

func seedDentryType(t *testing.T, runner *fakeRunner, mount fsmeta.MountID, parent fsmeta.InodeID, name string, inode fsmeta.InodeID, typ fsmeta.InodeType) {
	t.Helper()
	key, err := fsmeta.EncodeDentryKey(mount, parent, name)
	require.NoError(t, err)
	value, err := fsmeta.EncodeDentryValue(fsmeta.DentryRecord{
		Parent: parent,
		Name:   name,
		Inode:  inode,
		Type:   typ,
	})
	require.NoError(t, err)
	runner.data[string(key)] = value
}

func seedInode(t *testing.T, runner *fakeRunner, mount fsmeta.MountID, record fsmeta.InodeRecord) {
	t.Helper()
	key, err := fsmeta.EncodeInodeKey(mount, record.Inode)
	require.NoError(t, err)
	value, err := fsmeta.EncodeInodeValue(record)
	require.NoError(t, err)
	runner.data[string(key)] = value
}

func cloneMutation(mut *kvrpcpb.Mutation) *kvrpcpb.Mutation {
	if mut == nil {
		return nil
	}
	return &kvrpcpb.Mutation{
		Op:                mut.GetOp(),
		Key:               append([]byte(nil), mut.GetKey()...),
		Value:             append([]byte(nil), mut.GetValue()...),
		AssertionNotExist: mut.GetAssertionNotExist(),
		ExpiresAt:         mut.GetExpiresAt(),
	}
}

type fakeKeyConflictError struct {
	errors []*kvrpcpb.KeyError
}

func (e fakeKeyConflictError) Error() string {
	return "fake key conflict"
}

func (e fakeKeyConflictError) KeyErrors() []*kvrpcpb.KeyError {
	return e.errors
}

// -----------------------------------------------------------------------------
// Slab-consumer integration tests: NegativeSlab + DirPageSlab wiring.
// These exercise the executor's negCache / dirPages hooks end-to-end
// against the fake runner; the underlying cache logic is tested in
// engine/slab/{negativecache,dirpage}/_test.go.
// -----------------------------------------------------------------------------

func TestExecutorNegativeCacheLookupShortCircuit(t *testing.T) {
	runner := newFakeRunner()
	cache := negativecache.New(negativecache.Config{
		GroupKeyFn: func(k []byte) []byte { return k },
	})
	executor, err := New(runner, WithNegativeCache(cache))
	require.NoError(t, err)

	req := fsmeta.LookupRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "missing"}

	// First lookup: real LSM probe (runner.Get), records the miss.
	_, err = executor.Lookup(context.Background(), req)
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	firstGetCalls := runner.getCalls

	// Second lookup: served by cache, no runner round-trip.
	_, err = executor.Lookup(context.Background(), req)
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	require.Equal(t, firstGetCalls, runner.getCalls,
		"runner.Get must not be called when negative cache memo is fresh")
}

func TestExecutorNegativeCacheInvalidatedByCreate(t *testing.T) {
	runner := newFakeRunner()
	cache := negativecache.New(negativecache.Config{
		GroupKeyFn: func(k []byte) []byte { return k },
	})
	executor, err := New(runner, WithNegativeCache(cache))
	require.NoError(t, err)

	req := fsmeta.LookupRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "novel"}
	_, err = executor.Lookup(context.Background(), req)
	require.ErrorIs(t, err, fsmeta.ErrNotFound)

	// Create the dentry. After commit the cache must drop the memo so the
	// next Lookup re-issues against the runner and observes the new entry.
	err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount: "vol", Parent: fsmeta.RootInode, Name: "novel", Inode: 100,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.NoError(t, err)

	record, err := executor.Lookup(context.Background(), req)
	require.NoError(t, err, "create must invalidate the prior negative memo")
	require.Equal(t, fsmeta.InodeID(100), record.Inode)
}

func TestExecutorDirPageReadDirPlusCacheHit(t *testing.T) {
	runner := newFakeRunner()
	cache, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = cache.Close() }()
	executor, err := New(runner, WithDirPageCache(cache))
	require.NoError(t, err)

	mount := fsmeta.MountID("vol")
	parent := fsmeta.RootInode
	for i, name := range []string{"a", "b", "c"} {
		err := executor.Create(context.Background(), fsmeta.CreateRequest{
			Mount: mount, Parent: parent, Name: name, Inode: fsmeta.InodeID(10 + i),
		}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
		require.NoError(t, err)
	}

	req := fsmeta.ReadDirRequest{Mount: mount, Parent: parent, Limit: 100}

	// First call: runner Scan + BatchGet, then async materialize.
	first, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, first, 3)
	scansAfterFirst := len(runner.scanVersions)

	// Second call: cache hit → no new Scan / BatchGet against the runner.
	second, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, second, 3)
	require.Equal(t, scansAfterFirst, len(runner.scanVersions),
		"runner.Scan must not be called when dirpage cache hits")
}

func TestExecutorDirPageInvalidatedByCreate(t *testing.T) {
	runner := newFakeRunner()
	cache, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = cache.Close() }()
	executor, err := New(runner, WithDirPageCache(cache))
	require.NoError(t, err)

	mount := fsmeta.MountID("vol")
	parent := fsmeta.RootInode

	// Materialize an initial empty page set under frontier 0.
	_, err = executor.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount: mount, Parent: parent, Limit: 10,
	})
	require.NoError(t, err)

	// Create a dentry; this must bump the dirpage epoch.
	err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount: mount, Parent: parent, Name: "fresh", Inode: 99,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.NoError(t, err)

	// Next ReadDirPlus must miss the cache (epoch advanced) and re-scan.
	scansBefore := len(runner.scanVersions)
	out, err := executor.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount: mount, Parent: parent, Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, out, 1, "create must invalidate the cached empty page set")
	require.Greater(t, len(runner.scanVersions), scansBefore,
		"epoch bump must force a runner scan on the next ReadDirPlus")
}
