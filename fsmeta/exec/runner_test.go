package exec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/slab/dirpage"
	"github.com/feichai0017/NoKV/engine/slab/negativecache"
	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

type fakeRunner struct {
	nextTS              uint64
	data                map[string][]byte
	mutations           [][]*kvrpcpb.Mutation
	getCalls            int
	scanVersions        []uint64
	batchVersions       []uint64
	scanErrs            []error
	batchErrs           []error
	mutateErr           error
	mutateErrs          []error
	actualCommitVersion uint64
}

type atomicMutateCall struct {
	primary       []byte
	predicates    []*kvrpcpb.AtomicPredicate
	mutations     []*kvrpcpb.Mutation
	startVersion  uint64
	commitVersion uint64
}

type fakeAtomicRunner struct {
	*fakeRunner
	handled     bool
	err         error
	atomicCalls []atomicMutateCall
}

func requireStatUint(t *testing.T, stats map[string]any, key string, want uint64) {
	t.Helper()
	raw, ok := stats[key]
	require.Truef(t, ok, "missing stat %s", key)
	got, ok := raw.(uint64)
	require.Truef(t, ok, "stat %s has type %T", key, raw)
	require.Equal(t, want, got)
}

func txnLockedError(mount fsmeta.MountID, parent fsmeta.InodeID, name string) error {
	key, err := fsmeta.EncodeDentryKey(mount, parent, name)
	if err != nil {
		panic(err)
	}
	return nokverrors.NewTxnKeyError(&kvrpcpb.KeyError{
		Locked: &kvrpcpb.Locked{
			PrimaryLock: key,
			Key:         key,
			LockVersion: 10,
			LockTtl:     defaultLockTTL,
		},
	})
}

type fakeMountResolver struct {
	records map[fsmeta.MountID]MountAdmission
	err     error
	calls   int
}

type fakeAuthorityResolver struct {
	same  bool
	err   error
	calls int
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

type fakeInodeAllocator struct {
	next  fsmeta.InodeID
	ids   []fsmeta.InodeID
	err   error
	calls int
}

func (a *fakeInodeAllocator) AllocateCreateInode(context.Context, fsmeta.MountID, fsmeta.InodeID, string) (fsmeta.InodeID, error) {
	a.calls++
	if a.err != nil {
		return 0, a.err
	}
	if len(a.ids) > 0 {
		id := a.ids[0]
		a.ids = a.ids[1:]
		return id, nil
	}
	if a.next == 0 {
		a.next = 22
	}
	id := a.next
	a.next++
	return id, nil
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

func (r *fakeAuthorityResolver) SameAuthority(context.Context, fsmeta.MountID, fsmeta.InodeID, fsmeta.InodeID) (bool, error) {
	r.calls++
	if r.err != nil {
		return false, r.err
	}
	return r.same, nil
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
	if len(r.batchErrs) > 0 {
		err := r.batchErrs[0]
		r.batchErrs = r.batchErrs[1:]
		if err != nil {
			return nil, err
		}
	}
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
	if len(r.scanErrs) > 0 {
		err := r.scanErrs[0]
		r.scanErrs = r.scanErrs[1:]
		if err != nil {
			return nil, err
		}
	}
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

func (r *fakeRunner) Mutate(_ context.Context, primary []byte, mutations []*kvrpcpb.Mutation, _, commitVersion, _ uint64) (uint64, error) {
	return r.applyMutations(primary, mutations, commitVersion, r.actualCommitVersion)
}

func (r *fakeRunner) MutateAtCommit(_ context.Context, primary []byte, mutations []*kvrpcpb.Mutation, _, commitVersion, _ uint64) (uint64, error) {
	return r.applyMutations(primary, mutations, commitVersion, 0)
}

func (r *fakeRunner) applyMutations(primary []byte, mutations []*kvrpcpb.Mutation, commitVersion, overrideCommitVersion uint64) (uint64, error) {
	if len(r.mutateErrs) > 0 {
		err := r.mutateErrs[0]
		r.mutateErrs = r.mutateErrs[1:]
		if err != nil {
			return 0, err
		}
	}
	if r.mutateErr != nil {
		return 0, r.mutateErr
	}
	cloned := make([]*kvrpcpb.Mutation, 0, len(mutations))
	hasPrimary := len(mutations) == 0
	for _, mut := range mutations {
		if mut.GetAssertionNotExist() {
			if _, ok := r.data[string(mut.GetKey())]; ok {
				return 0, fsmeta.ErrExists
			}
		}
		if bytes.Equal(mut.GetKey(), primary) {
			hasPrimary = true
		}
		cloned = append(cloned, cloneMutation(mut))
	}
	if !hasPrimary {
		return 0, fmt.Errorf("primary key %q not present in mutations", primary)
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
	if overrideCommitVersion != 0 {
		return overrideCommitVersion, nil
	}
	return commitVersion, nil
}

func (r *fakeAtomicRunner) TryAtomicMutate(_ context.Context, primary []byte, predicates []*kvrpcpb.AtomicPredicate, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (bool, error) {
	r.atomicCalls = append(r.atomicCalls, atomicMutateCall{
		primary:       cloneBytes(primary),
		predicates:    cloneAtomicPredicates(predicates),
		mutations:     cloneMutations(mutations),
		startVersion:  startVersion,
		commitVersion: commitVersion,
	})
	if r.err != nil {
		return true, r.err
	}
	if !r.handled {
		return false, nil
	}
	for _, pred := range predicates {
		if pred == nil {
			continue
		}
		_, exists := r.data[string(pred.GetKey())]
		switch pred.GetKind() {
		case kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS:
			if exists {
				return true, fsmeta.ErrExists
			}
		case kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_EXISTS:
			if !exists {
				return true, fsmeta.ErrNotFound
			}
		default:
			return true, fsmeta.ErrInvalidRequest
		}
	}
	cloned := make([]*kvrpcpb.Mutation, 0, len(mutations))
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		if mut.GetAssertionNotExist() {
			if _, exists := r.data[string(mut.GetKey())]; exists {
				return true, fsmeta.ErrExists
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
	return true, nil
}

func TestExecutorCreateAndLookup(t *testing.T) {
	runner := newFakeRunner()
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}))
	require.NoError(t, err)

	result, err := executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), result.Inode.Inode)

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

func TestExecutorCreateRequiresInodeAllocator(t *testing.T) {
	executor, err := New(newFakeRunner())
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.ErrorIs(t, err, errInodeAllocatorRequired)
}

func TestExecutorCreateUsesAtomicMutateFastPathWhenHandled(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}))
	require.NoError(t, err)

	req := fsmeta.CreateRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "file", Attrs: fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile}}
	_, err = executor.Create(context.Background(), req)
	require.NoError(t, err)

	plan, err := fsmeta.PlanCreate(req, 22)
	require.NoError(t, err)
	stats := executor.Stats()
	requireStatUint(t, stats, "create_total", 1)
	requireStatUint(t, stats, "create_fastpath_attempt_total", 1)
	requireStatUint(t, stats, "create_fastpath_success_total", 1)
	requireStatUint(t, stats, "create_fastpath_fallback_total", 0)
	requireStatUint(t, stats, "create_fastpath_skip_quota_total", 0)
	requireStatUint(t, stats, "create_fastpath_runner_unsupported_total", 0)
	require.Len(t, runner.atomicCalls, 1)
	call := runner.atomicCalls[0]
	require.Equal(t, plan.PrimaryKey, call.primary)
	require.Equal(t, uint64(1), call.startVersion)
	require.Equal(t, uint64(2), call.commitVersion)
	require.Len(t, call.predicates, 2)
	require.Equal(t, plan.MutateKeys[0], call.predicates[0].GetKey())
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS, call.predicates[0].GetKind())
	require.Equal(t, plan.MutateKeys[1], call.predicates[1].GetKey())
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS, call.predicates[1].GetKind())
	require.Len(t, call.mutations, 2)
	require.True(t, call.mutations[0].GetAssertionNotExist())
	require.True(t, call.mutations[1].GetAssertionNotExist())
	require.Empty(t, base.mutations)

	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), record.Inode)
}

func TestExecutorCreateFallsBackWhenAtomicMutateNotHandled(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: false}
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	require.Len(t, runner.atomicCalls, 1)
	stats := executor.Stats()
	requireStatUint(t, stats, "create_total", 1)
	requireStatUint(t, stats, "create_fastpath_attempt_total", 1)
	requireStatUint(t, stats, "create_fastpath_success_total", 0)
	requireStatUint(t, stats, "create_fastpath_fallback_total", 1)
	requireStatUint(t, stats, "create_fastpath_skip_quota_total", 0)
	requireStatUint(t, stats, "create_fastpath_runner_unsupported_total", 0)
	require.Len(t, base.mutations, 1)
	require.Len(t, base.mutations[0], 2)
}

func TestExecutorCreateRecordsUnsupportedAtomicRunner(t *testing.T) {
	runner := newFakeRunner()
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	stats := executor.Stats()
	requireStatUint(t, stats, "create_total", 1)
	requireStatUint(t, stats, "create_fastpath_attempt_total", 0)
	requireStatUint(t, stats, "create_fastpath_success_total", 0)
	requireStatUint(t, stats, "create_fastpath_fallback_total", 0)
	requireStatUint(t, stats, "create_fastpath_skip_quota_total", 0)
	requireStatUint(t, stats, "create_fastpath_runner_unsupported_total", 1)
	require.Len(t, runner.mutations, 1)
}

func TestExecutorCreateSkipsAtomicMutateWhenQuotaMutates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	quotaKey, err := fsmeta.EncodeUsageKey("vol", 0)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: quotaKey, Value: []byte("usage")}}
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}), WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)

	// Quota reservation adds a third key, so Create must use the full 2PC
	// path until AtomicMutate can prove all fsmeta and quota keys share one
	// atomic local apply group.
	stats := executor.Stats()
	requireStatUint(t, stats, "create_total", 1)
	requireStatUint(t, stats, "create_fastpath_attempt_total", 0)
	requireStatUint(t, stats, "create_fastpath_success_total", 0)
	requireStatUint(t, stats, "create_fastpath_fallback_total", 0)
	requireStatUint(t, stats, "create_fastpath_skip_quota_total", 1)
	requireStatUint(t, stats, "create_fastpath_runner_unsupported_total", 0)
	require.Empty(t, runner.atomicCalls)
	require.Len(t, base.mutations, 1)
	require.Len(t, base.mutations[0], 3)
	require.Equal(t, quotaKey, base.mutations[0][2].GetKey())
}

func TestExecutorCreateRejectsExistingDentry(t *testing.T) {
	runner := newFakeRunner()
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22, 23}}))
	require.NoError(t, err)

	req := fsmeta.CreateRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "file", Attrs: fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile}}
	_, err = executor.Create(context.Background(), req)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), req)
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
		executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}), WithMountResolver(resolver))
		require.NoError(t, err)

		_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
			Mount:  "vol",
			Parent: fsmeta.RootInode,
			Name:   "file",
			Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
		})
		require.NoError(t, err)
		require.Equal(t, 1, resolver.calls)
		require.Len(t, runner.mutations, 1)
	})

	t.Run("missing mount", func(t *testing.T) {
		runner := newFakeRunner()
		resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{}}
		executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}), WithMountResolver(resolver))
		require.NoError(t, err)

		_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
			Mount:  "missing",
			Parent: fsmeta.RootInode,
			Name:   "file",
			Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
		})
		require.ErrorIs(t, err, fsmeta.ErrMountNotRegistered)
		require.Equal(t, 1, resolver.calls)
		require.Empty(t, runner.mutations)
	})

	t.Run("retired mount", func(t *testing.T) {
		runner := newFakeRunner()
		resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
			"vol": {MountID: "vol", RootInode: fsmeta.RootInode, SchemaVersion: 1, Retired: true},
		}}
		executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}), WithMountResolver(resolver))
		require.NoError(t, err)

		_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
			Mount:  "vol",
			Parent: fsmeta.RootInode,
			Name:   "file",
			Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
		})
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
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}), WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.changes)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, quotaKey, runner.mutations[0][2].GetKey())
}

func TestExecutorCreateRejectsQuotaExceededBeforeMutation(t *testing.T) {
	runner := newFakeRunner()
	quota := &fakeQuotaResolver{err: fsmeta.ErrQuotaExceeded}
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}), WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 4096},
	})
	require.ErrorIs(t, err, fsmeta.ErrQuotaExceeded)
	require.Empty(t, runner.mutations)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.changes)
}

func TestExecutorUpdateInodeUpdatesMutableFieldsAndQuota(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{
		Inode:         22,
		Type:          fsmeta.InodeTypeFile,
		Size:          4096,
		Mode:          0o644,
		LinkCount:     1,
		CreatedUnixNs: 10,
		UpdatedUnixNs: 20,
	})
	quotaKey, err := fsmeta.EncodeUsageKey("vol", 7)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: quotaKey, Value: []byte("usage")}}
	executor, err := New(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	updated, err := executor.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:            "vol",
		Parent:           7,
		Inode:            22,
		Name:             "file",
		SetSize:          true,
		Size:             8192,
		SetMode:          true,
		Mode:             0o600,
		SetUpdatedUnixNs: true,
		UpdatedUnixNs:    30,
		SetOpaqueAttrs:   true,
		OpaqueAttrs:      []byte("body=cas://1"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(8192), updated.Size)
	require.Equal(t, uint32(0o600), updated.Mode)
	require.Equal(t, int64(30), updated.UpdatedUnixNs)
	require.Equal(t, []byte("body=cas://1"), updated.OpaqueAttrs)
	require.Equal(t, int64(10), updated.CreatedUnixNs)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", Scope: 7, Bytes: 4096}}}, quota.changes)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, quotaKey, runner.mutations[0][1].GetKey())

	stored, ok, err := executor.readInode(context.Background(), "vol", 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, updated, stored)
}

func TestExecutorUpdateInodeRejectsHardLinkedInode(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 2})
	executor, err := New(runner)
	require.NoError(t, err)

	_, err = executor.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  7,
		Inode:   22,
		Name:    "file",
		SetSize: true,
		Size:    8192,
	})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
	require.Empty(t, runner.mutations)
}

func TestExecutorUpdateInodeRejectsDentryTypeMismatch(t *testing.T) {
	runner := newFakeRunner()
	seedDentryType(t, runner, "vol", 7, "file", 22, fsmeta.InodeTypeDirectory)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 1})
	executor, err := New(runner)
	require.NoError(t, err)

	_, err = executor.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  7,
		Inode:   22,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	})
	require.ErrorIs(t, err, fsmeta.ErrInvalidValue)
	require.Empty(t, runner.mutations)
}

func TestExecutorWriteSessionLifecycle(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	now := time.Unix(0, 100)
	executor, err := New(runner, WithClock(func() time.Time { return now }))
	require.NoError(t, err)

	opened, err := executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:         "vol",
		Inode:         22,
		Session:       "writer-1",
		ExpiresUnixNs: 200,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.SessionRecord{Session: "writer-1", Inode: 22, ExpiresUnixNs: 200}, opened)

	sessionKey, err := fsmeta.EncodeSessionKey("vol", "writer-1")
	require.NoError(t, err)
	ownerKey, err := fsmeta.EncodeInodeSessionKey("vol", 22)
	require.NoError(t, err)
	require.Contains(t, runner.data, string(sessionKey))
	require.Contains(t, runner.data, string(ownerKey))

	_, err = executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:         "vol",
		Inode:         22,
		Session:       "writer-2",
		ExpiresUnixNs: 250,
	})
	require.ErrorIs(t, err, fsmeta.ErrExists)

	heartbeat, err := executor.HeartbeatWriteSession(context.Background(), fsmeta.HeartbeatWriteSessionRequest{
		Mount:         "vol",
		Inode:         22,
		Session:       "writer-1",
		ExpiresUnixNs: 300,
	})
	require.NoError(t, err)
	require.Equal(t, int64(300), heartbeat.ExpiresUnixNs)
	stored, ok, err := executor.readSessionByKey(context.Background(), ownerKey, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(300), stored.ExpiresUnixNs)

	err = executor.CloseWriteSession(context.Background(), fsmeta.CloseWriteSessionRequest{Mount: "vol", Session: "writer-1"})
	require.NoError(t, err)
	require.NotContains(t, runner.data, string(sessionKey))
	require.NotContains(t, runner.data, string(ownerKey))
}

func TestExecutorOpenWriteSessionReclaimsExpiredOwner(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	oldRecord := fsmeta.SessionRecord{Session: "writer-old", Inode: 22, ExpiresUnixNs: 50}
	oldValue, err := fsmeta.EncodeSessionValue(oldRecord)
	require.NoError(t, err)
	oldSessionKey, err := fsmeta.EncodeSessionKey("vol", "writer-old")
	require.NoError(t, err)
	ownerKey, err := fsmeta.EncodeInodeSessionKey("vol", 22)
	require.NoError(t, err)
	runner.data[string(oldSessionKey)] = oldValue
	runner.data[string(ownerKey)] = oldValue

	executor, err := New(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)
	_, err = executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:         "vol",
		Inode:         22,
		Session:       "writer-new",
		ExpiresUnixNs: 200,
	})
	require.NoError(t, err)
	require.NotContains(t, runner.data, string(oldSessionKey))
	newSessionKey, err := fsmeta.EncodeSessionKey("vol", "writer-new")
	require.NoError(t, err)
	require.Contains(t, runner.data, string(newSessionKey))
	require.Contains(t, runner.data, string(ownerKey))
}

func TestExecutorOpenWriteSessionDoesNotDeleteReusedLiveSession(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 23, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	live := fsmeta.SessionRecord{Session: "writer-reused", Inode: 23, ExpiresUnixNs: 500}
	seedSession(t, runner, "vol", live)
	expired := fsmeta.SessionRecord{Session: "writer-reused", Inode: 22, ExpiresUnixNs: 50}
	expiredValue, err := fsmeta.EncodeSessionValue(expired)
	require.NoError(t, err)
	expiredOwnerKey, err := fsmeta.EncodeInodeSessionKey("vol", expired.Inode)
	require.NoError(t, err)
	runner.data[string(expiredOwnerKey)] = expiredValue
	liveSessionKey, err := fsmeta.EncodeSessionKey("vol", live.Session)
	require.NoError(t, err)
	liveOwnerKey, err := fsmeta.EncodeInodeSessionKey("vol", live.Inode)
	require.NoError(t, err)
	executor, err := New(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	_, err = executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:         "vol",
		Inode:         22,
		Session:       "writer-new",
		ExpiresUnixNs: 200,
	})

	require.NoError(t, err)
	require.Contains(t, runner.data, string(liveSessionKey))
	require.Contains(t, runner.data, string(liveOwnerKey))
	owner, ok, err := executor.readSessionByKey(context.Background(), expiredOwnerKey, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, fsmeta.SessionID("writer-new"), owner.Session)
}

func TestExecutorOpenWriteSessionRejectsDirectory(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeDirectory, LinkCount: 1})
	executor, err := New(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	_, err = executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:         "vol",
		Inode:         22,
		Session:       "writer-1",
		ExpiresUnixNs: 200,
	})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
	require.Empty(t, runner.mutations)
}

func TestExecutorExpireWriteSessionsDeletesBothIndexes(t *testing.T) {
	runner := newFakeRunner()
	expired := fsmeta.SessionRecord{Session: "writer-old", Inode: 22, ExpiresUnixNs: 50}
	live := fsmeta.SessionRecord{Session: "writer-live", Inode: 23, ExpiresUnixNs: 500}
	seedSession(t, runner, "vol", expired)
	seedSession(t, runner, "vol", live)
	executor, err := New(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	result, err := executor.ExpireWriteSessions(context.Background(), fsmeta.ExpireWriteSessionsRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ExpireWriteSessionsResult{Expired: 1}, result)

	expiredSessionKey, err := fsmeta.EncodeSessionKey("vol", expired.Session)
	require.NoError(t, err)
	expiredOwnerKey, err := fsmeta.EncodeInodeSessionKey("vol", expired.Inode)
	require.NoError(t, err)
	liveSessionKey, err := fsmeta.EncodeSessionKey("vol", live.Session)
	require.NoError(t, err)
	liveOwnerKey, err := fsmeta.EncodeInodeSessionKey("vol", live.Inode)
	require.NoError(t, err)
	require.NotContains(t, runner.data, string(expiredSessionKey))
	require.NotContains(t, runner.data, string(expiredOwnerKey))
	require.Contains(t, runner.data, string(liveSessionKey))
	require.Contains(t, runner.data, string(liveOwnerKey))
}

func TestExecutorExpireWriteSessionsDoesNotDeleteReusedLiveSession(t *testing.T) {
	runner := newFakeRunner()
	expired := fsmeta.SessionRecord{Session: "writer-reused", Inode: 22, ExpiresUnixNs: 50}
	live := fsmeta.SessionRecord{Session: "writer-reused", Inode: 23, ExpiresUnixNs: 500}
	expiredValue, err := fsmeta.EncodeSessionValue(expired)
	require.NoError(t, err)
	liveValue, err := fsmeta.EncodeSessionValue(live)
	require.NoError(t, err)
	sessionKey, err := fsmeta.EncodeSessionKey("vol", live.Session)
	require.NoError(t, err)
	expiredOwnerKey, err := fsmeta.EncodeInodeSessionKey("vol", expired.Inode)
	require.NoError(t, err)
	liveOwnerKey, err := fsmeta.EncodeInodeSessionKey("vol", live.Inode)
	require.NoError(t, err)
	runner.data[string(expiredOwnerKey)] = expiredValue
	runner.data[string(sessionKey)] = liveValue
	runner.data[string(liveOwnerKey)] = liveValue
	executor, err := New(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	result, err := executor.ExpireWriteSessions(context.Background(), fsmeta.ExpireWriteSessionsRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ExpireWriteSessionsResult{}, result)
	require.NotContains(t, runner.data, string(expiredOwnerKey))
	require.Contains(t, runner.data, string(sessionKey))
	require.Contains(t, runner.data, string(liveOwnerKey))
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
	runner.mutateErr = fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
		AlreadyExists: &kvrpcpb.KeyAlreadyExists{Key: []byte("dentry")},
	}}}
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.ErrorIs(t, err, fsmeta.ErrExists)
	require.Zero(t, runner.getCalls)
}

func TestExecutorRetriesCommitTsExpired(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
			CommitTsExpired: &kvrpcpb.CommitTsExpired{
				Key:         []byte("dentry"),
				CommitTs:    2,
				MinCommitTs: 5,
			},
		}}},
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []fsmeta.InodeID{22}}
	executor, err := New(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(5), runner.nextTS)
	require.Equal(t, uint64(1), executor.Stats()["txn_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["txn_retry_exhausted_total"])
}

func TestExecutorRetriesLostTxnLock(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
			Retryable: "percolator: lock not found",
		}}},
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []fsmeta.InodeID{22}}
	executor, err := New(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(1), executor.Stats()["txn_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["txn_retry_exhausted_total"])
}

func TestExecutorRetriesLockedTxnContention(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
			Locked: &kvrpcpb.Locked{
				PrimaryLock: []byte("dentry"),
				Key:         []byte("dentry"),
				LockVersion: 2,
				LockTtl:     defaultLockTTL,
			},
		}}},
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []fsmeta.InodeID{22}}
	executor, err := New(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(5), runner.nextTS)
	require.Equal(t, uint64(1), executor.Stats()["txn_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["txn_retry_exhausted_total"])
}

func TestExecutorRetriesWriteConflict(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
			WriteConflict: &kvrpcpb.WriteConflict{
				Key:        []byte("dentry"),
				ConflictTs: 4,
				StartTs:    2,
			},
		}}},
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []fsmeta.InodeID{22}}
	executor, err := New(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
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

func TestExecutorReadDirRetriesLiveLock(t *testing.T) {
	runner := newFakeRunner()
	runner.scanErrs = []error{txnLockedError("vol", 7, "a")}
	seedDentry(t, runner, "vol", 7, "a", 21)
	executor, err := New(runner)
	require.NoError(t, err)

	records, err := executor.ReadDir(context.Background(), fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, []uint64{1, 2}, runner.scanVersions)
	requireStatUint(t, executor.Stats(), "read_retries_total", 1)
	requireStatUint(t, executor.Stats(), "read_retry_exhausted_total", 0)
}

func TestExecutorReadDirExhaustsRetriesOnLiveLock(t *testing.T) {
	runner := newFakeRunner()
	for range maxReadContentionRetries + 1 {
		runner.scanErrs = append(runner.scanErrs, txnLockedError("vol", 7, "a"))
	}
	seedDentry(t, runner, "vol", 7, "a", 21)
	executor, err := New(runner)
	require.NoError(t, err)

	_, err = executor.ReadDir(context.Background(), fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  8,
	})
	require.Error(t, err)
	require.Equal(t, []uint64{1, 2, 3, 4}, runner.scanVersions)
	requireStatUint(t, executor.Stats(), "read_retries_total", uint64(maxReadContentionRetries))
	requireStatUint(t, executor.Stats(), "read_retry_exhausted_total", 1)
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

func TestExecutorReadDirPlusRetriesLiveLockAtSnapshotVersion(t *testing.T) {
	runner := newFakeRunner()
	runner.scanErrs = []error{txnLockedError("vol", 7, "a")}
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{
		Inode:     21,
		Type:      fsmeta.InodeTypeFile,
		LinkCount: 1,
	})
	executor, err := New(runner)
	require.NoError(t, err)

	pairs, err := executor.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount:           "vol",
		Parent:          7,
		Limit:           8,
		SnapshotVersion: 100,
	})
	require.NoError(t, err)
	require.Len(t, pairs, 1)
	require.Equal(t, []uint64{100, 100}, runner.scanVersions)
	requireStatUint(t, executor.Stats(), "read_retries_total", 1)
	requireStatUint(t, executor.Stats(), "read_retry_exhausted_total", 0)
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

func TestExecutorRenameMovesDentryWithoutSubtreeHandoff(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	publisher := &fakeSubtreePublisher{}
	authority := &fakeAuthorityResolver{same: true}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := New(
		runner,
		WithMountResolver(resolver),
		WithSubtreeAuthorityResolver(authority),
		WithSubtreeHandoffPublisher(publisher),
	)
	require.NoError(t, err)

	err = executor.Rename(context.Background(), fsmeta.RenameRequest{
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
	require.Equal(t, fsmeta.InodeID(22), record.Inode)
	require.Len(t, runner.mutations, 1)
	require.Empty(t, publisher.starts)
	require.Empty(t, publisher.completes)
	require.Equal(t, 1, authority.calls)
}

func TestExecutorRenameRejectsCrossAuthority(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	authority := &fakeAuthorityResolver{same: false}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := New(runner, WithMountResolver(resolver), WithSubtreeAuthorityResolver(authority))
	require.NoError(t, err)

	err = executor.Rename(context.Background(), fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	})
	require.ErrorIs(t, err, fsmeta.ErrCrossAuthorityRename)
	require.Empty(t, runner.mutations)
	require.Equal(t, 1, authority.calls)
}

func TestExecutorRenameSubtreePinsCommitVersionToHandoffFrontier(t *testing.T) {
	runner := newFakeRunner()
	runner.actualCommitVersion = 99
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

func seedSession(t *testing.T, runner *fakeRunner, mount fsmeta.MountID, record fsmeta.SessionRecord) {
	t.Helper()
	value, err := fsmeta.EncodeSessionValue(record)
	require.NoError(t, err)
	sessionKey, err := fsmeta.EncodeSessionKey(mount, record.Session)
	require.NoError(t, err)
	ownerKey, err := fsmeta.EncodeInodeSessionKey(mount, record.Inode)
	require.NoError(t, err)
	runner.data[string(sessionKey)] = value
	runner.data[string(ownerKey)] = value
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

type fakeTxnKeyError struct {
	errors []*kvrpcpb.KeyError
}

func (e fakeTxnKeyError) Error() string {
	return "fake txn key error"
}

func (e fakeTxnKeyError) KeyErrors() []*kvrpcpb.KeyError {
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
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{100}}), WithNegativeCache(cache))
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
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{100}}), WithNegativeCache(cache))
	require.NoError(t, err)

	req := fsmeta.LookupRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "novel"}
	_, err = executor.Lookup(context.Background(), req)
	require.ErrorIs(t, err, fsmeta.ErrNotFound)

	// Create the dentry. After commit the cache must drop the memo so the
	// next Lookup re-issues against the runner and observes the new entry.
	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount: "vol", Parent: fsmeta.RootInode, Name: "novel", Attrs: fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
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
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{10, 11, 12}}), WithDirPageCache(cache))
	require.NoError(t, err)

	mount := fsmeta.MountID("vol")
	parent := fsmeta.RootInode
	for _, name := range []string{"a", "b", "c"} {
		_, err := executor.Create(context.Background(), fsmeta.CreateRequest{
			Mount: mount, Parent: parent, Name: name, Attrs: fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
		})
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
	require.Equal(t, first, second)
	require.Equal(t, scansAfterFirst, len(runner.scanVersions),
		"runner.Scan must not be called when dirpage cache hits")
}

func TestExecutorDirPageReadDirPlusCacheKeysPagination(t *testing.T) {
	runner := newFakeRunner()
	cache, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = cache.Close() }()
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{10, 11, 12}}), WithDirPageCache(cache))
	require.NoError(t, err)

	mount := fsmeta.MountID("vol")
	parent := fsmeta.RootInode
	for _, name := range []string{"a", "b", "c"} {
		_, err := executor.Create(context.Background(), fsmeta.CreateRequest{
			Mount: mount, Parent: parent, Name: name, Attrs: fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
		})
		require.NoError(t, err)
	}

	// Materialize a non-leading page first. A later first-page request must not
	// reuse it just because both requests target the same parent directory.
	pageAfterA, err := executor.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount: mount, Parent: parent, StartAfter: "a", Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, "b", pageAfterA[0].Dentry.Name)

	firstPage, err := executor.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount: mount, Parent: parent, Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, "a", firstPage[0].Dentry.Name)
	require.Equal(t, parent, firstPage[0].Dentry.Parent)
}

func TestExecutorDirPageDecodeFailureFallsBackToRunner(t *testing.T) {
	runner := newFakeRunner()
	cache := &corruptDirPageCache{}
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{10}}), WithDirPageCache(cache))
	require.NoError(t, err)

	mount := fsmeta.MountID("vol")
	parent := fsmeta.RootInode
	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount: mount, Parent: parent, Name: "a", Attrs: fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	out, err := executor.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount: mount, Parent: parent, Limit: 1,
	})
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, "a", out[0].Dentry.Name)
	require.NotEmpty(t, runner.scanVersions, "corrupt derived cache must fall back to the runner")
}

func TestEncodeDirPageEntriesRejectsPartialMaterialization(t *testing.T) {
	_, err := encodeDirPageEntries([]fsmeta.DentryAttrPair{{
		Dentry: fsmeta.DentryRecord{Parent: 1, Name: "bad", Inode: 10, Type: fsmeta.InodeTypeFile},
		Inode: fsmeta.InodeRecord{
			Inode:       10,
			Type:        fsmeta.InodeTypeFile,
			OpaqueAttrs: make([]byte, fsmeta.MaxInodeOpaqueAttrsBytes+1),
		},
	}})
	require.Error(t, err)
}

func TestExecutorDirPageInvalidatedByCreate(t *testing.T) {
	runner := newFakeRunner()
	cache, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = cache.Close() }()
	executor, err := New(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{99}}), WithDirPageCache(cache))
	require.NoError(t, err)

	mount := fsmeta.MountID("vol")
	parent := fsmeta.RootInode

	// Materialize an initial empty page set under frontier 0.
	_, err = executor.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount: mount, Parent: parent, Limit: 10,
	})
	require.NoError(t, err)

	// Create a dentry; this must bump the dirpage epoch.
	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount: mount, Parent: parent, Name: "fresh", Attrs: fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
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

type corruptDirPageCache struct{}

func (c *corruptDirPageCache) CurrentEpoch(dirpage.DirectoryKey) uint64 { return 0 }

func (c *corruptDirPageCache) Lookup(dirpage.PageKey, uint64) ([]dirpage.Entry, bool) {
	return []dirpage.Entry{{Name: []byte("stale"), Inode: 999, AttrBlob: []byte("not-an-inode")}}, true
}

func (c *corruptDirPageCache) MaterializeAsync(dirpage.PageKey, uint64, []dirpage.Entry) error {
	return nil
}

func (c *corruptDirPageCache) Invalidate(dirpage.DirectoryKey) uint64 { return 1 }

func (c *corruptDirPageCache) Stats() dirpage.Stats { return dirpage.Stats{} }
