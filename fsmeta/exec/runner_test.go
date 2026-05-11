package exec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/slab/dirpage"
	"github.com/feichai0017/NoKV/engine/slab/negativecache"
	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
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
	timestampErrs       []error
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

var testMountIdentity = fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}

func testMountIdentityFor(mount fsmeta.MountID) fsmeta.MountIdentity {
	if mount == testMountIdentity.MountID {
		return testMountIdentity
	}
	return fsmeta.MountIdentity{MountID: mount, MountKeyID: 2}
}

func testMountAdmission() MountAdmission {
	return MountAdmission{
		MountID:       testMountIdentity.MountID,
		MountKeyID:    testMountIdentity.MountKeyID,
		RootInode:     fsmeta.RootInode,
		SchemaVersion: 1,
	}
}

func newTestExecutor(runner TxnRunner, opts ...Option) (*Executor, error) {
	defaults := []Option{WithMountResolver(&fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		testMountIdentity.MountID: testMountAdmission(),
	}})}
	return New(runner, append(defaults, opts...)...)
}

func requireStatUint(t *testing.T, stats map[string]any, key string, want uint64) {
	t.Helper()
	raw, ok := stats[key]
	require.Truef(t, ok, "missing stat %s", key)
	got, ok := raw.(uint64)
	require.Truef(t, ok, "stat %s has type %T", key, raw)
	require.Equal(t, want, got)
}

func requireAtomicStatUint(t *testing.T, stats map[string]any, kind fsmeta.OperationKind, key string, want uint64) {
	t.Helper()
	raw, ok := stats["atomic_one_phase"]
	require.True(t, ok, "missing atomic_one_phase stats")
	byOp, ok := raw.(map[string]any)
	require.Truef(t, ok, "atomic_one_phase has type %T", raw)
	rawOp, ok := byOp[string(kind)]
	require.Truef(t, ok, "missing atomic_one_phase stats for %s", kind)
	opStats, ok := rawOp.(map[string]uint64)
	require.Truef(t, ok, "atomic_one_phase[%s] has type %T", kind, rawOp)
	require.Equal(t, want, opStats[key])
}

func requirePerasStatUint(t *testing.T, stats map[string]any, key string, want uint64) {
	t.Helper()
	raw, ok := stats["peras_admission"]
	require.True(t, ok, "missing peras_admission stats")
	perasStats, ok := raw.(map[string]any)
	require.Truef(t, ok, "peras_admission has type %T", raw)
	got, ok := perasStats[key].(uint64)
	require.Truef(t, ok, "peras_admission[%s] has type %T", key, perasStats[key])
	require.Equal(t, want, got)
}

func requirePerasSlowReasonStatUint(t *testing.T, stats map[string]any, reason compile.SlowReason, want uint64) {
	t.Helper()
	raw, ok := stats["peras_admission"]
	require.True(t, ok, "missing peras_admission stats")
	perasStats, ok := raw.(map[string]any)
	require.Truef(t, ok, "peras_admission has type %T", raw)
	rawReasons, ok := perasStats["slow_by_reason"]
	require.True(t, ok, "missing peras slow reason stats")
	reasons, ok := rawReasons.(map[string]uint64)
	require.Truef(t, ok, "peras slow_by_reason has type %T", rawReasons)
	require.Equal(t, want, reasons[string(reason)])
}

func requirePerasStatBool(t *testing.T, stats map[string]any, key string, want bool) {
	t.Helper()
	raw, ok := stats["peras_admission"]
	require.True(t, ok, "missing peras_admission stats")
	perasStats, ok := raw.(map[string]any)
	require.Truef(t, ok, "peras_admission has type %T", raw)
	got, ok := perasStats[key].(bool)
	require.Truef(t, ok, "peras_admission[%s] has type %T", key, perasStats[key])
	require.Equal(t, want, got)
}

func requirePerasShadowStatUint(t *testing.T, stats map[string]any, key string, want uint64) {
	t.Helper()
	raw, ok := stats["peras_shadow"]
	require.True(t, ok, "missing peras_shadow stats")
	perasStats, ok := raw.(map[string]any)
	require.Truef(t, ok, "peras_shadow has type %T", raw)
	got, ok := perasStats[key].(uint64)
	require.Truef(t, ok, "peras_shadow[%s] has type %T", key, perasStats[key])
	require.Equal(t, want, got)
}

func requirePerasShadowStatBool(t *testing.T, stats map[string]any, key string, want bool) {
	t.Helper()
	raw, ok := stats["peras_shadow"]
	require.True(t, ok, "missing peras_shadow stats")
	perasStats, ok := raw.(map[string]any)
	require.Truef(t, ok, "peras_shadow has type %T", raw)
	got, ok := perasStats[key].(bool)
	require.Truef(t, ok, "peras_shadow[%s] has type %T", key, perasStats[key])
	require.Equal(t, want, got)
}

func requirePerasVisibleStatUint(t *testing.T, stats map[string]any, key string, want uint64) {
	t.Helper()
	raw, ok := stats["peras_visible_commit"]
	require.True(t, ok, "missing peras_visible_commit stats")
	perasStats, ok := raw.(map[string]any)
	require.Truef(t, ok, "peras_visible_commit has type %T", raw)
	got, ok := perasStats[key].(uint64)
	require.Truef(t, ok, "peras_visible_commit[%s] has type %T", key, perasStats[key])
	require.Equal(t, want, got)
}

func requirePerasVisibleStatBool(t *testing.T, stats map[string]any, key string, want bool) {
	t.Helper()
	raw, ok := stats["peras_visible_commit"]
	require.True(t, ok, "missing peras_visible_commit stats")
	perasStats, ok := raw.(map[string]any)
	require.Truef(t, ok, "peras_visible_commit has type %T", raw)
	got, ok := perasStats[key].(bool)
	require.Truef(t, ok, "peras_visible_commit[%s] has type %T", key, perasStats[key])
	require.Equal(t, want, got)
}

func txnLockedError(mount fsmeta.MountID, parent fsmeta.InodeID, name string) error {
	key, err := fsmeta.EncodeDentryKey(testMountIdentityFor(mount), parent, name)
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

type fakePerasAdmitter struct {
	owned  bool
	err    error
	calls  int
	scopes []compile.AuthorityScope
}

type fakePerasSubmitter struct {
	err    error
	calls  int
	ids    []fsperas.OperationID
	deltas []compile.SemanticDelta
}

type fakePerasCommitter struct {
	err    error
	calls  int
	ids    []fsperas.OperationID
	deltas []compile.SemanticDelta
}

type fakePerasWatchFence struct {
	watched bool
	calls   int
	effects [][]compile.WriteEffect
}

type fakePerasAuthorityFlusher struct {
	fakePerasCommitter
	flushCalls  int
	flushScopes []compile.AuthorityScope
}

type noopPerasSubmitter struct{}

type noopPerasCommitter struct{}

type flushFailingPerasCommitter struct{}

type ownedPerasAdmitter struct{}

type fakeSubtreePublisher struct {
	starts      []subtreePublishCall
	completes   []subtreePublishCall
	err         error
	startErr    error
	completeErr error
}

type fakeQuotaResolver struct {
	err               error
	changes           [][]QuotaChange
	mutation          *kvrpcpb.Mutation
	allowPerasVisible bool
	perasChecks       [][]QuotaChange
}

type fakeInodeAllocator struct {
	next  fsmeta.InodeID
	ids   []fsmeta.InodeID
	err   error
	calls int
}

func (a *fakeInodeAllocator) AllocateCreateInode(context.Context, fsmeta.MountIdentity, fsmeta.InodeID, string) (fsmeta.InodeID, error) {
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

func (q *fakeQuotaResolver) AllowPerasVisibleQuota(_ context.Context, changes []QuotaChange) (bool, error) {
	q.perasChecks = append(q.perasChecks, append([]QuotaChange(nil), changes...))
	if q.err != nil {
		return false, q.err
	}
	return q.allowPerasVisible, nil
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

func (a *fakePerasAdmitter) AcquirePerasAuthority(_ context.Context, scope compile.AuthorityScope) (bool, error) {
	a.calls++
	a.scopes = append(a.scopes, compile.AuthorityScope{
		Mount:      scope.Mount,
		MountKeyID: scope.MountKeyID,
		Buckets:    append([]fsmeta.AffinityBucket(nil), scope.Buckets...),
		Parents:    append([]fsmeta.InodeID(nil), scope.Parents...),
		Inodes:     append([]fsmeta.InodeID(nil), scope.Inodes...),
	})
	if a.err != nil {
		return false, a.err
	}
	return a.owned, nil
}

func (s *fakePerasSubmitter) SubmitPeras(_ context.Context, id fsperas.OperationID, delta compile.SemanticDelta) (fsperas.VisibleAck, error) {
	s.calls++
	s.ids = append(s.ids, id)
	s.deltas = append(s.deltas, delta)
	if s.err != nil {
		return fsperas.VisibleAck{}, s.err
	}
	return fsperas.VisibleAck{
		EpochID:  1,
		OpID:     id,
		HolderID: "holder-a",
	}, nil
}

func (c *fakePerasCommitter) CommitPeras(_ context.Context, id fsperas.OperationID, delta compile.SemanticDelta) (fsperas.VisibleAck, error) {
	c.calls++
	c.ids = append(c.ids, id)
	c.deltas = append(c.deltas, delta)
	if c.err != nil {
		return fsperas.VisibleAck{}, c.err
	}
	return fsperas.VisibleAck{EpochID: 1, OpID: id, HolderID: "holder-a"}, nil
}

func (f *fakePerasWatchFence) HasPerasWatchedWrite(effects []compile.WriteEffect) bool {
	f.calls++
	f.effects = append(f.effects, append([]compile.WriteEffect(nil), effects...))
	return f.watched
}

func (f *fakePerasAuthorityFlusher) FlushAuthority(_ context.Context, scope compile.AuthorityScope) error {
	f.flushCalls++
	f.flushScopes = append(f.flushScopes, compile.AuthorityScope{
		Mount:      scope.Mount,
		MountKeyID: scope.MountKeyID,
		Buckets:    append([]fsmeta.AffinityBucket(nil), scope.Buckets...),
		Parents:    append([]fsmeta.InodeID(nil), scope.Parents...),
		Inodes:     append([]fsmeta.InodeID(nil), scope.Inodes...),
	})
	return nil
}

func (noopPerasSubmitter) SubmitPeras(context.Context, fsperas.OperationID, compile.SemanticDelta) (fsperas.VisibleAck, error) {
	return fsperas.VisibleAck{EpochID: 1, HolderID: "holder-a"}, nil
}

func (noopPerasCommitter) CommitPeras(_ context.Context, id fsperas.OperationID, _ compile.SemanticDelta) (fsperas.VisibleAck, error) {
	return fsperas.VisibleAck{EpochID: 1, OpID: id, HolderID: "holder-a"}, nil
}

func (flushFailingPerasCommitter) CommitPeras(_ context.Context, id fsperas.OperationID, _ compile.SemanticDelta) (fsperas.VisibleAck, error) {
	return fsperas.VisibleAck{EpochID: 1, OpID: id, HolderID: "holder-a"}, nil
}

func (flushFailingPerasCommitter) Flush(context.Context) error {
	return errors.New("unexpected peras flush")
}

func (ownedPerasAdmitter) AcquirePerasAuthority(context.Context, compile.AuthorityScope) (bool, error) {
	return true, nil
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
	if len(r.timestampErrs) > 0 {
		err := r.timestampErrs[0]
		r.timestampErrs = r.timestampErrs[1:]
		if err != nil {
			return 0, err
		}
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
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	token, err := executor.SnapshotSubtree(context.Background(), fsmeta.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 7,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.SnapshotSubtreeToken{Mount: "vol", MountKeyID: 1, RootInode: 7, ReadVersion: 1}, token)

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

func TestExecutorSnapshotSubtreeFlushesPerasAuthorityBeforeToken(t *testing.T) {
	runner := newFakeRunner()
	flusher := &fakePerasAuthorityFlusher{}
	executor, err := newTestExecutor(runner,
		WithPerasCommitter(flusher),
		WithPerasAuthorityAdmitter(&fakePerasAdmitter{owned: true}),
	)
	require.NoError(t, err)

	token, err := executor.SnapshotSubtree(context.Background(), fsmeta.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 7,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), token.ReadVersion)
	require.Equal(t, 1, flusher.flushCalls)
	require.Len(t, flusher.flushScopes, 1)
	require.Equal(t, fsmeta.MountID("vol"), flusher.flushScopes[0].Mount)
	require.Equal(t, fsmeta.MountKeyID(1), flusher.flushScopes[0].MountKeyID)
	require.Equal(t, []fsmeta.InodeID{7}, flusher.flushScopes[0].Parents)
}

func TestExecutorResolveSnapshotSubtreeTokenAllowsRetiredMount(t *testing.T) {
	runner := newFakeRunner()
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 9, RootInode: fsmeta.RootInode, SchemaVersion: 1, Retired: true},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver))
	require.NoError(t, err)

	token, err := executor.ResolveSnapshotSubtreeToken(context.Background(), fsmeta.SnapshotSubtreeToken{
		Mount:       "vol",
		RootInode:   7,
		ReadVersion: 42,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.SnapshotSubtreeToken{Mount: "vol", MountKeyID: 9, RootInode: 7, ReadVersion: 42}, token)
}

func TestExecutorGetReadVersionReservesEphemeralTimestamp(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	version, err := executor.GetReadVersion(context.Background(), fsmeta.ReadVersionRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
	require.Equal(t, uint64(2), runner.nextTS)
}

func TestExecutorGetQuotaUsage(t *testing.T) {
	runner := newFakeRunner()
	key, err := fsmeta.EncodeUsageKey(testMountIdentity, 7)
	require.NoError(t, err)
	value, err := fsmeta.EncodeUsageValue(fsmeta.UsageRecord{Bytes: 4096, Inodes: 2})
	require.NoError(t, err)
	runner.data[string(key)] = value
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	usage, err := executor.GetQuotaUsage(context.Background(), fsmeta.QuotaUsageRequest{Mount: "vol", Scope: 7})
	require.NoError(t, err)
	require.Equal(t, fsmeta.UsageRecord{Bytes: 4096, Inodes: 2}, usage)
}

func TestExecutorGetQuotaUsageReturnsZeroForMissingCounter(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
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
		case kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS:
			if !exists || !bytes.Equal(r.data[string(pred.GetKey())], pred.GetExpectedValue()) {
				return true, fsmeta.ErrInvalidValue
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
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}))
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

func TestExecutorCreateAdmitsPerasAuthority(t *testing.T) {
	runner := newFakeRunner()
	admitter := &fakePerasAdmitter{owned: true}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithPerasAuthorityAdmitter(admitter),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	require.Equal(t, 1, admitter.calls)
	require.Len(t, admitter.scopes, 1)
	require.Equal(t, fsmeta.MountID("vol"), admitter.scopes[0].Mount)
	require.Equal(t, fsmeta.MountKeyID(1), admitter.scopes[0].MountKeyID)
	require.Equal(t, []fsmeta.InodeID{fsmeta.RootInode}, admitter.scopes[0].Parents)
	require.Equal(t, []fsmeta.InodeID{22}, admitter.scopes[0].Inodes)
	require.Len(t, runner.mutations, 1)

	stats := executor.Stats()
	requirePerasStatBool(t, stats, "enabled", true)
	requirePerasStatUint(t, stats, "eligible_total", 1)
	requirePerasStatUint(t, stats, "acquire_total", 1)
	requirePerasStatUint(t, stats, "owned_total", 1)
	requirePerasStatUint(t, stats, "held_total", 0)
	requirePerasStatUint(t, stats, "slow_total", 0)
}

func TestExecutorCreateSubmitsPerasShadowAndKeepsRaftCommit(t *testing.T) {
	runner := newFakeRunner()
	admitter := &fakePerasAdmitter{owned: true}
	submitter := &fakePerasSubmitter{}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithPerasAuthorityAdmitter(admitter),
		WithPerasShadowSubmitter(submitter),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	require.Equal(t, 1, submitter.calls)
	require.Len(t, submitter.ids, 1)
	require.Equal(t, "fsmeta-exec/create", submitter.ids[0].ClientID)
	require.Equal(t, uint64(1), submitter.ids[0].Seq)
	require.Len(t, submitter.deltas, 1)
	require.Equal(t, compile.EligibilityVisibleCommit, submitter.deltas[0].Eligibility)
	require.Len(t, runner.mutations, 1, "shadow submit must not replace the current Raft commit")

	stats := executor.Stats()
	requirePerasShadowStatBool(t, stats, "enabled", true)
	requirePerasShadowStatUint(t, stats, "submit_total", 1)
	requirePerasShadowStatUint(t, stats, "success_total", 1)
	requirePerasShadowStatUint(t, stats, "error_total", 0)
	requirePerasShadowStatUint(t, stats, "skip_no_authority_total", 0)
}

func TestExecutorCreatePerasShadowErrorStillUsesRaftCommit(t *testing.T) {
	runner := newFakeRunner()
	submitter := &fakePerasSubmitter{err: errors.New("shadow submit failed")}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithPerasAuthorityAdmitter(&fakePerasAdmitter{owned: true}),
		WithPerasShadowSubmitter(submitter),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)

	stats := executor.Stats()
	requirePerasShadowStatUint(t, stats, "submit_total", 1)
	requirePerasShadowStatUint(t, stats, "success_total", 0)
	requirePerasShadowStatUint(t, stats, "error_total", 1)
}

func TestExecutorCreatePerasShadowRequiresAuthorityAdmission(t *testing.T) {
	runner := newFakeRunner()
	submitter := &fakePerasSubmitter{}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithPerasShadowSubmitter(submitter),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Zero(t, submitter.calls)
	require.Len(t, runner.mutations, 1)

	stats := executor.Stats()
	requirePerasShadowStatUint(t, stats, "submit_total", 0)
	requirePerasShadowStatUint(t, stats, "skip_no_authority_total", 1)
}

func TestExecutorCreatePerasVisibleCommitBypassesRaftCommit(t *testing.T) {
	runner := newFakeRunner()
	committer := &fakePerasCommitter{}
	submitter := &fakePerasSubmitter{}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithPerasAuthorityAdmitter(&fakePerasAdmitter{owned: true}),
		WithPerasCommitter(committer),
		WithPerasShadowSubmitter(submitter),
	)
	require.NoError(t, err)

	result, err := executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	require.Equal(t, fsmeta.InodeID(22), result.Inode.Inode)
	require.Equal(t, fsmeta.InodeID(22), result.Dentry.Inode)
	require.Equal(t, 1, committer.calls)
	require.Len(t, committer.ids, 1)
	require.Equal(t, "fsmeta-exec/create", committer.ids[0].ClientID)
	require.Equal(t, uint64(1), committer.ids[0].Seq)
	require.Len(t, committer.deltas, 1)
	require.Equal(t, compile.EligibilityVisibleCommit, committer.deltas[0].Eligibility)
	require.Zero(t, submitter.calls, "visible commit success must not also submit shadow evidence")
	require.Empty(t, runner.mutations, "visible commit success must bypass the current Raft commit")

	stats := executor.Stats()
	requirePerasVisibleStatBool(t, stats, "enabled", true)
	requirePerasVisibleStatUint(t, stats, "attempt_total", 1)
	requirePerasVisibleStatUint(t, stats, "success_total", 1)
	requirePerasVisibleStatUint(t, stats, "error_total", 0)
	requirePerasVisibleStatUint(t, stats, "skip_no_authority_total", 0)
}

func TestExecutorCreatePerasVisibleCommitRejectsExistingDentry(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", fsmeta.RootInode, "file", 21)
	committer := &fakePerasCommitter{}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithPerasAuthorityAdmitter(&fakePerasAdmitter{owned: true}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.ErrorIs(t, err, fsmeta.ErrExists)
	require.Zero(t, committer.calls, "failed not-exists predicate must not enter Peras visible commit")
	require.Empty(t, runner.mutations)

	stats := executor.Stats()
	requirePerasVisibleStatUint(t, stats, "attempt_total", 0)
	requirePerasVisibleStatUint(t, stats, "skip_predicate_total", 1)
}

func TestExecutorCreatePerasVisibleCommitFallsBackWhenWatched(t *testing.T) {
	runner := newFakeRunner()
	committer := &fakePerasCommitter{}
	fence := &fakePerasWatchFence{watched: true}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithPerasAuthorityAdmitter(&fakePerasAdmitter{owned: true}),
		WithPerasCommitter(committer),
		WithPerasWatchFence(fence),
	)
	require.NoError(t, err)

	result, err := executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), result.Inode.Inode)
	require.Equal(t, 1, fence.calls)
	require.Len(t, fence.effects, 1)
	require.Zero(t, committer.calls)
	require.Len(t, runner.mutations, 1)

	stats := executor.Stats()
	requirePerasVisibleStatUint(t, stats, "attempt_total", 0)
	requirePerasVisibleStatUint(t, stats, "skip_watched_total", 1)
}

func TestExecutorCreatePerasVisibleCommitErrorDoesNotFallback(t *testing.T) {
	runner := newFakeRunner()
	fastErr := errors.New("peras commit failed")
	committer := &fakePerasCommitter{err: fastErr}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithPerasAuthorityAdmitter(&fakePerasAdmitter{owned: true}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.ErrorIs(t, err, fastErr)
	require.Equal(t, 1, committer.calls)
	require.Empty(t, runner.mutations, "ambiguous Peras evidence must not fall back into a second commit path")

	stats := executor.Stats()
	requirePerasVisibleStatUint(t, stats, "attempt_total", 1)
	requirePerasVisibleStatUint(t, stats, "success_total", 0)
	requirePerasVisibleStatUint(t, stats, "error_total", 1)
}

func TestExecutorCreatePerasVisibleCommitRequiresAuthorityAdmission(t *testing.T) {
	runner := newFakeRunner()
	committer := &fakePerasCommitter{}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Zero(t, committer.calls)
	require.Len(t, runner.mutations, 1)

	stats := executor.Stats()
	requirePerasVisibleStatUint(t, stats, "attempt_total", 0)
	requirePerasVisibleStatUint(t, stats, "skip_no_authority_total", 1)
}

func TestExecutorCreatePerasVisibleCommitSkipsSharedQuota(t *testing.T) {
	runner := newFakeRunner()
	quotaKey, err := fsmeta.EncodeUsageKey(testMountIdentity, 7)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: quotaKey, Value: []byte("usage")}}
	committer := &fakePerasCommitter{}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithQuotaResolver(quota),
		WithPerasAuthorityAdmitter(&fakePerasAdmitter{owned: true}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)

	require.Zero(t, committer.calls, "shared quota must remain on the slow path until quota credits exist")
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.changes)
	require.Len(t, runner.mutations, 1)
	require.Len(t, runner.mutations[0], 3)

	stats := executor.Stats()
	requirePerasVisibleStatUint(t, stats, "attempt_total", 0)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.perasChecks)
}

func TestExecutorCreatePerasVisibleCommitAllowsQuotaResolverWithoutFence(t *testing.T) {
	runner := newFakeRunner()
	quota := &fakeQuotaResolver{allowPerasVisible: true}
	committer := &fakePerasCommitter{}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithQuotaResolver(quota),
		WithPerasAuthorityAdmitter(&fakePerasAdmitter{owned: true}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)

	require.Equal(t, 1, committer.calls)
	require.Empty(t, quota.changes)
	require.Empty(t, runner.mutations)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.perasChecks)

	stats := executor.Stats()
	requirePerasVisibleStatUint(t, stats, "attempt_total", 1)
	requirePerasVisibleStatUint(t, stats, "success_total", 1)
}

func TestExecutorOpenWriteSessionPerasVisibleCommitBypassesRaftCommit(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	committer := &fakePerasCommitter{}
	executor, err := newTestExecutor(
		runner,
		WithClock(func() time.Time { return time.Unix(0, 100) }),
		WithPerasAuthorityAdmitter(&fakePerasAdmitter{owned: true}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	record, err := executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)

	require.Equal(t, fsmeta.SessionRecord{Session: "writer-1", Inode: 22, ExpiresUnixNs: 200}, record)
	require.Equal(t, 1, committer.calls)
	require.Len(t, committer.deltas, 1)
	require.Len(t, committer.deltas[0].WriteEffects, 2)
	for _, effect := range committer.deltas[0].WriteEffects {
		require.Equal(t, compile.EffectPut, effect.Kind)
		require.NotEmpty(t, effect.Key)
		require.NotEmpty(t, effect.Value)
		decoded, err := fsmeta.DecodeSessionValue(effect.Value)
		require.NoError(t, err)
		require.Equal(t, record, decoded)
	}
	require.Empty(t, runner.mutations, "session visible commit must bypass the current Raft commit")

	stats := executor.Stats()
	requirePerasVisibleStatUint(t, stats, "attempt_total", 1)
	requirePerasVisibleStatUint(t, stats, "success_total", 1)
	requirePerasVisibleStatUint(t, stats, "skip_non_concrete_total", 0)
}

func TestExecutorCreatePerasBufferedVisibleCommitServesLookupOverlay(t *testing.T) {
	runner := newFakeRunner()
	committer := newTestBufferedPerasCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	created, err := executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Empty(t, runner.mutations)

	lookedUp, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
	})
	require.NoError(t, err)
	require.Equal(t, created.Dentry, lookedUp)
}

func TestExecutorCreatePerasBufferedVisibleCommitUsesEmptyDirectoryFact(t *testing.T) {
	runner := newFakeRunner()
	committer := newTestBufferedPerasCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22, 23}}),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "run",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeDirectory},
	})
	require.NoError(t, err)
	getsAfterDir := runner.getCalls

	created, err := executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 22,
		Name:   "part-000",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	require.Equal(t, fsmeta.InodeID(23), created.Inode.Inode)
	require.Equal(t, getsAfterDir, runner.getCalls, "empty-directory admission should avoid per-child predicate reads")
	require.Empty(t, runner.mutations)
	require.Equal(t, uint64(2), committer.Stats()["commit_total"])
}

func TestExecutorWriteSessionLifecyclePerasBufferedVisibleCommitServesOverlay(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	committer := newTestBufferedPerasCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithClock(func() time.Time { return time.Unix(0, 100) }),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	opened, err := executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)
	require.Equal(t, int64(200), opened.ExpiresUnixNs)

	heartbeat, err := executor.HeartbeatWriteSession(context.Background(), fsmeta.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     200 * time.Nanosecond,
	})
	require.NoError(t, err)
	require.Equal(t, int64(300), heartbeat.ExpiresUnixNs)

	sessionKey, err := fsmeta.EncodeSessionKey(testMountIdentity, 22, "writer-1")
	require.NoError(t, err)
	ownerKey, err := fsmeta.EncodeInodeSessionKey(testMountIdentity, 22)
	require.NoError(t, err)
	owner, ok, err := executor.readSessionByKey(context.Background(), ownerKey, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, heartbeat, owner)

	err = executor.CloseWriteSession(context.Background(), fsmeta.CloseWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
	})
	require.NoError(t, err)

	_, deleted, ok := committer.GetPerasOverlay(sessionKey)
	require.True(t, ok)
	require.True(t, deleted)
	_, deleted, ok = committer.GetPerasOverlay(ownerKey)
	require.True(t, ok)
	require.True(t, deleted)
	require.Empty(t, runner.mutations, "buffered session lifecycle should stay entirely inside Peras overlay")

	stats := executor.Stats()
	requirePerasVisibleStatUint(t, stats, "attempt_total", 3)
	requirePerasVisibleStatUint(t, stats, "success_total", 3)
}

func TestExecutorCreatePerasBufferedVisibleCommitServesReadDirPlusOverlay(t *testing.T) {
	runner := newFakeRunner()
	committer := newTestBufferedPerasCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	created, err := executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	pairs, err := executor.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Limit:  16,
	})
	require.NoError(t, err)
	require.Equal(t, []fsmeta.DentryAttrPair{{
		Dentry: created.Dentry,
		Inode:  created.Inode,
	}}, pairs)
}

func TestExecutorCreateStopsWhenPerasAuthorityHeldElsewhere(t *testing.T) {
	runner := newFakeRunner()
	admitter := &fakePerasAdmitter{owned: false}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithPerasAuthorityAdmitter(admitter),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.ErrorIs(t, err, errPerasAuthorityNotHeld)
	require.Equal(t, 1, admitter.calls)
	require.Empty(t, runner.mutations)

	stats := executor.Stats()
	requirePerasStatUint(t, stats, "eligible_total", 1)
	requirePerasStatUint(t, stats, "acquire_total", 1)
	requirePerasStatUint(t, stats, "owned_total", 0)
	requirePerasStatUint(t, stats, "held_total", 1)
}

func TestExecutorCreateWithSharedQuotaSkipsPerasAuthorityAdmission(t *testing.T) {
	runner := newFakeRunner()
	admitter := &fakePerasAdmitter{owned: true}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithQuotaResolver(&fakeQuotaResolver{}),
		WithPerasAuthorityAdmitter(admitter),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)
	require.Zero(t, admitter.calls)
	require.Len(t, runner.mutations, 1)

	stats := executor.Stats()
	requirePerasStatUint(t, stats, "eligible_total", 0)
	requirePerasStatUint(t, stats, "acquire_total", 0)
	requirePerasStatUint(t, stats, "owned_total", 0)
	requirePerasStatUint(t, stats, "slow_total", 1)
	requirePerasSlowReasonStatUint(t, stats, compile.SlowReasonSharedQuota, 1)
}

func TestExecutorRetriesTimestampAuthorityRefreshBeforeMutate(t *testing.T) {
	runner := newFakeRunner()
	runner.timestampErrs = []error{nokverrors.New(nokverrors.KindStaleEpoch, "coordinator client: stale witness era")}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})

	require.NoError(t, err)
	require.Empty(t, runner.timestampErrs)
	require.Len(t, runner.mutations, 1)
	requireStatUint(t, executor.Stats(), "txn_retries_total", 1)
}

func TestExecutorRetriesReadTimestampAuthorityRefresh(t *testing.T) {
	runner := newFakeRunner()
	runner.timestampErrs = []error{nokverrors.New(nokverrors.KindStaleEpoch, "coordinator client: stale witness era")}
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	version, err := executor.GetReadVersion(context.Background(), fsmeta.ReadVersionRequest{Mount: "vol"})

	require.NoError(t, err)
	require.NotZero(t, version)
	require.Empty(t, runner.timestampErrs)
	requireStatUint(t, executor.Stats(), "read_retries_total", 1)
}

func TestExecutorCreateRequiresInodeAllocator(t *testing.T) {
	executor, err := newTestExecutor(newFakeRunner())
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.ErrorIs(t, err, errInodeAllocatorRequired)
}

func TestExecutorCreateUsesAtomicMutateOnePhaseWhenHandled(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}))
	require.NoError(t, err)

	req := fsmeta.CreateRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "file", Attrs: fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile}}
	_, err = executor.Create(context.Background(), req)
	require.NoError(t, err)

	plan, err := fsmeta.PlanCreate(req, testMountIdentity, 22)
	require.NoError(t, err)
	stats := executor.Stats()
	requireStatUint(t, stats, "create_total", 1)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "attempt_total", 1)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "success_total", 1)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "fallback_total", 0)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "skip_total", 0)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "runner_unsupported_total", 0)
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
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}))
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
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "attempt_total", 1)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "success_total", 0)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "fallback_total", 1)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "skip_total", 0)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "runner_unsupported_total", 0)
	require.Len(t, base.mutations, 1)
	require.Len(t, base.mutations[0], 2)
}

func TestExecutorCreateRecordsUnsupportedAtomicRunner(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}))
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
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "attempt_total", 0)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "success_total", 0)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "fallback_total", 0)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "skip_total", 0)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "runner_unsupported_total", 1)
	require.Len(t, runner.mutations, 1)
}

func TestExecutorCreateSkipsAtomicMutateWhenQuotaMutates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	quotaKey, err := fsmeta.EncodeUsageKey(testMountIdentity, 0)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: quotaKey, Value: []byte("usage")}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}), WithQuotaResolver(quota))
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
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "attempt_total", 0)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "success_total", 0)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "fallback_total", 0)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "skip_total", 1)
	requireAtomicStatUint(t, stats, fsmeta.OperationCreate, "runner_unsupported_total", 0)
	require.Empty(t, runner.atomicCalls)
	require.Len(t, base.mutations, 1)
	require.Len(t, base.mutations[0], 3)
	require.Equal(t, quotaKey, base.mutations[0][2].GetKey())
}

func TestExecutorUpdateInodeUsesAtomicMutateWithValuePredicates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedDentry(t, runner.fakeRunner, "vol", 7, "file", 22)
	seedInode(t, runner.fakeRunner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Mode: 0o644, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	updated, err := executor.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  7,
		Inode:   22,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(0o600), updated.Mode)
	require.Len(t, runner.atomicCalls, 1)
	require.Empty(t, base.mutations)
	requireAtomicStatUint(t, executor.Stats(), fsmeta.OperationUpdateInode, "success_total", 1)
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[0].predicates[0].GetKind())
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[0].predicates[1].GetKind())

	stored, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(0o600), stored.Mode)
}

func TestExecutorUpdateInodeSkipsAtomicMutateWhenQuotaMutates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedDentry(t, runner.fakeRunner, "vol", 7, "file", 22)
	seedInode(t, runner.fakeRunner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 1024, LinkCount: 1})
	quotaKey, err := fsmeta.EncodeUsageKey(testMountIdentity, 7)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: quotaKey, Value: []byte("usage")}}
	executor, err := newTestExecutor(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  7,
		Inode:   22,
		Name:    "file",
		SetSize: true,
		Size:    2048,
	})
	require.NoError(t, err)
	require.Empty(t, runner.atomicCalls)
	require.Len(t, base.mutations, 1)
	requireAtomicStatUint(t, executor.Stats(), fsmeta.OperationUpdateInode, "skip_total", 1)
}

func TestExecutorUpdateInodePerasBufferedVisibleCommitReadsCreateOverlay(t *testing.T) {
	runner := newFakeRunner()
	committer := newTestBufferedPerasCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	created, err := executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	})
	require.NoError(t, err)

	updated, err := executor.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:            "vol",
		Parent:           7,
		Inode:            created.Inode.Inode,
		Name:             "file",
		SetSize:          true,
		Size:             8192,
		SetMode:          true,
		Mode:             0o600,
		SetUpdatedUnixNs: true,
		UpdatedUnixNs:    42,
	})
	require.NoError(t, err)

	require.Equal(t, uint32(0o600), updated.Mode)
	require.Equal(t, uint64(8192), updated.Size)
	require.Equal(t, int64(42), updated.UpdatedUnixNs)
	stored, ok, err := executor.readInode(context.Background(), testMountIdentity, created.Inode.Inode, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, updated, stored)
	require.Empty(t, runner.mutations, "create+update should stay inside Peras overlay")

	stats := executor.Stats()
	requirePerasVisibleStatUint(t, stats, "attempt_total", 2)
	requirePerasVisibleStatUint(t, stats, "success_total", 2)
}

func TestExecutorCreateRejectsExistingDentry(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22, 23}}))
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
			"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
		}}
		executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}), WithMountResolver(resolver))
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
		executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}), WithMountResolver(resolver))
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
			"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1, Retired: true},
		}}
		executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}), WithMountResolver(resolver))
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
	quotaKey, err := fsmeta.EncodeUsageKey(testMountIdentity, 0)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: quotaKey, Value: []byte("usage")}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}), WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.changes)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, quotaKey, runner.mutations[0][2].GetKey())
}

func TestExecutorCreateRejectsQuotaExceededBeforeMutation(t *testing.T) {
	runner := newFakeRunner()
	quota := &fakeQuotaResolver{err: fsmeta.ErrQuotaExceeded}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}), WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 4096},
	})
	require.ErrorIs(t, err, fsmeta.ErrQuotaExceeded)
	require.Empty(t, runner.mutations)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.changes)
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
	quotaKey, err := fsmeta.EncodeUsageKey(testMountIdentity, 7)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: quotaKey, Value: []byte("usage")}}
	executor, err := newTestExecutor(runner, WithQuotaResolver(quota))
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
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096}}}, quota.changes)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, quotaKey, runner.mutations[0][1].GetKey())

	stored, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, updated, stored)
}

func TestExecutorUpdateInodeRejectsHardLinkedInode(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 2})
	executor, err := newTestExecutor(runner)
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
	executor, err := newTestExecutor(runner)
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
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return now }))
	require.NoError(t, err)

	opened, err := executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.SessionRecord{Session: "writer-1", Inode: 22, ExpiresUnixNs: 200}, opened)

	sessionKey, err := fsmeta.EncodeSessionKey(testMountIdentity, 22, "writer-1")
	require.NoError(t, err)
	ownerKey, err := fsmeta.EncodeInodeSessionKey(testMountIdentity, 22)
	require.NoError(t, err)
	require.Contains(t, runner.data, string(sessionKey))
	require.Contains(t, runner.data, string(ownerKey))

	_, err = executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-2",
		TTL:     150 * time.Nanosecond,
	})
	require.ErrorIs(t, err, fsmeta.ErrExists)

	heartbeat, err := executor.HeartbeatWriteSession(context.Background(), fsmeta.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     200 * time.Nanosecond,
	})
	require.NoError(t, err)
	require.Equal(t, int64(300), heartbeat.ExpiresUnixNs)
	stored, ok, err := executor.readSessionByKey(context.Background(), ownerKey, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(300), stored.ExpiresUnixNs)

	err = executor.CloseWriteSession(context.Background(), fsmeta.CloseWriteSessionRequest{Mount: "vol", Inode: 22, Session: "writer-1"})
	require.NoError(t, err)
	require.NotContains(t, runner.data, string(sessionKey))
	require.NotContains(t, runner.data, string(ownerKey))
}

func TestExecutorWriteSessionLifecycleUsesAtomicMutateWithValuePredicates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedInode(t, runner.fakeRunner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	now := time.Unix(0, 100)
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return now }))
	require.NoError(t, err)

	_, err = executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)
	_, err = executor.HeartbeatWriteSession(context.Background(), fsmeta.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     200 * time.Nanosecond,
	})
	require.NoError(t, err)
	err = executor.CloseWriteSession(context.Background(), fsmeta.CloseWriteSessionRequest{Mount: "vol", Inode: 22, Session: "writer-1"})
	require.NoError(t, err)

	require.Len(t, runner.atomicCalls, 3)
	require.Empty(t, base.mutations)
	stats := executor.Stats()
	requireAtomicStatUint(t, stats, fsmeta.OperationOpenWriteSession, "success_total", 1)
	requireAtomicStatUint(t, stats, fsmeta.OperationHeartbeatSession, "success_total", 1)
	requireAtomicStatUint(t, stats, fsmeta.OperationCloseSession, "success_total", 1)
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[1].predicates[0].GetKind())
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[2].predicates[0].GetKind())
}

func TestExecutorOpenWriteSessionUsesAtomicMutateForStaleSessionCleanup(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedInode(t, runner.fakeRunner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	oldRecord := fsmeta.SessionRecord{Session: "writer-old", Inode: 22, ExpiresUnixNs: 50}
	oldValue, err := fsmeta.EncodeSessionValue(oldRecord)
	require.NoError(t, err)
	oldSessionKey, err := fsmeta.EncodeSessionKey(testMountIdentity, 22, "writer-old")
	require.NoError(t, err)
	ownerKey, err := fsmeta.EncodeInodeSessionKey(testMountIdentity, 22)
	require.NoError(t, err)
	runner.data[string(oldSessionKey)] = oldValue
	runner.data[string(ownerKey)] = oldValue
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	_, err = executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-new",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)

	require.Len(t, runner.atomicCalls, 1)
	require.Empty(t, base.mutations)
	require.NotContains(t, runner.data, string(oldSessionKey))
	requireAtomicStatUint(t, executor.Stats(), fsmeta.OperationOpenWriteSession, "success_total", 1)
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[0].predicates[2].GetKind())
}

func TestExecutorWriteSessionRejectsNonPositiveTTL(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	_, err = executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
	})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
	require.Empty(t, runner.mutations)

	seedSession(t, runner, "vol", fsmeta.SessionRecord{Session: "writer-live", Inode: 22, ExpiresUnixNs: 500})
	_, err = executor.HeartbeatWriteSession(context.Background(), fsmeta.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-live",
		TTL:     -time.Nanosecond,
	})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
}

func TestExecutorOpenWriteSessionComputesExpiryInsideRetryAttempt(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	sessionKey, err := fsmeta.EncodeSessionKey(testMountIdentity, 22, "writer-1")
	require.NoError(t, err)
	runner.mutateErrs = []error{
		nokverrors.NewTxnKeyError(&kvrpcpb.KeyError{
			CommitTsExpired: &kvrpcpb.CommitTsExpired{
				Key:         sessionKey,
				CommitTs:    2,
				MinCommitTs: 4,
			},
		}),
		nil,
	}
	clockCalls := 0
	executor, err := newTestExecutor(runner, WithClock(func() time.Time {
		clockCalls++
		if clockCalls == 1 {
			return time.Unix(0, 100)
		}
		return time.Unix(0, 500)
	}))
	require.NoError(t, err)

	opened, err := executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)
	require.Equal(t, int64(600), opened.ExpiresUnixNs)
	stored, ok, err := executor.readSessionByKey(context.Background(), sessionKey, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(600), stored.ExpiresUnixNs)
}

func TestExecutorOpenWriteSessionReclaimsExpiredOwner(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	oldRecord := fsmeta.SessionRecord{Session: "writer-old", Inode: 22, ExpiresUnixNs: 50}
	oldValue, err := fsmeta.EncodeSessionValue(oldRecord)
	require.NoError(t, err)
	oldSessionKey, err := fsmeta.EncodeSessionKey(testMountIdentity, 22, "writer-old")
	require.NoError(t, err)
	ownerKey, err := fsmeta.EncodeInodeSessionKey(testMountIdentity, 22)
	require.NoError(t, err)
	runner.data[string(oldSessionKey)] = oldValue
	runner.data[string(ownerKey)] = oldValue

	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)
	_, err = executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-new",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)
	require.NotContains(t, runner.data, string(oldSessionKey))
	newSessionKey, err := fsmeta.EncodeSessionKey(testMountIdentity, 22, "writer-new")
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
	expiredOwnerKey, err := fsmeta.EncodeInodeSessionKey(testMountIdentity, expired.Inode)
	require.NoError(t, err)
	runner.data[string(expiredOwnerKey)] = expiredValue
	liveSessionKey, err := fsmeta.EncodeSessionKey(testMountIdentity, live.Inode, live.Session)
	require.NoError(t, err)
	liveOwnerKey, err := fsmeta.EncodeInodeSessionKey(testMountIdentity, live.Inode)
	require.NoError(t, err)
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	_, err = executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-new",
		TTL:     100 * time.Nanosecond,
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
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	_, err = executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   22,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
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
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	result, err := executor.ExpireWriteSessions(context.Background(), fsmeta.ExpireWriteSessionsRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ExpireWriteSessionsResult{Expired: 1}, result)

	expiredSessionKey, err := fsmeta.EncodeSessionKey(testMountIdentity, expired.Inode, expired.Session)
	require.NoError(t, err)
	expiredOwnerKey, err := fsmeta.EncodeInodeSessionKey(testMountIdentity, expired.Inode)
	require.NoError(t, err)
	liveSessionKey, err := fsmeta.EncodeSessionKey(testMountIdentity, live.Inode, live.Session)
	require.NoError(t, err)
	liveOwnerKey, err := fsmeta.EncodeInodeSessionKey(testMountIdentity, live.Inode)
	require.NoError(t, err)
	require.NotContains(t, runner.data, string(expiredSessionKey))
	require.NotContains(t, runner.data, string(expiredOwnerKey))
	require.Contains(t, runner.data, string(liveSessionKey))
	require.Contains(t, runner.data, string(liveOwnerKey))
}

func TestExecutorExpireWriteSessionsCountsSessionPerInode(t *testing.T) {
	runner := newFakeRunner()
	first := fsmeta.SessionRecord{Session: "writer-reused", Inode: 22, ExpiresUnixNs: 50}
	second := fsmeta.SessionRecord{Session: "writer-reused", Inode: 23, ExpiresUnixNs: 50}
	seedSession(t, runner, "vol", first)
	seedSession(t, runner, "vol", second)
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	result, err := executor.ExpireWriteSessions(context.Background(), fsmeta.ExpireWriteSessionsRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ExpireWriteSessionsResult{Expired: 2}, result)
}

func TestExecutorExpireWriteSessionsDoesNotDeleteReusedLiveSession(t *testing.T) {
	runner := newFakeRunner()
	expired := fsmeta.SessionRecord{Session: "writer-reused", Inode: 22, ExpiresUnixNs: 50}
	live := fsmeta.SessionRecord{Session: "writer-reused", Inode: 23, ExpiresUnixNs: 500}
	expiredValue, err := fsmeta.EncodeSessionValue(expired)
	require.NoError(t, err)
	liveValue, err := fsmeta.EncodeSessionValue(live)
	require.NoError(t, err)
	sessionKey, err := fsmeta.EncodeSessionKey(testMountIdentity, live.Inode, live.Session)
	require.NoError(t, err)
	expiredOwnerKey, err := fsmeta.EncodeInodeSessionKey(testMountIdentity, expired.Inode)
	require.NoError(t, err)
	liveOwnerKey, err := fsmeta.EncodeInodeSessionKey(testMountIdentity, live.Inode)
	require.NoError(t, err)
	runner.data[string(expiredOwnerKey)] = expiredValue
	runner.data[string(sessionKey)] = liveValue
	runner.data[string(liveOwnerKey)] = liveValue
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	require.NoError(t, err)

	result, err := executor.ExpireWriteSessions(context.Background(), fsmeta.ExpireWriteSessionsRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ExpireWriteSessionsResult{}, result)
	require.NotContains(t, runner.data, string(expiredOwnerKey))
	require.Contains(t, runner.data, string(sessionKey))
	require.Contains(t, runner.data, string(liveOwnerKey))
}

func TestExecutorExpireWriteSessionsDoesNotFlushPerasOverlay(t *testing.T) {
	runner := newFakeRunner()
	expired := fsmeta.SessionRecord{Session: "writer-old", Inode: 22, ExpiresUnixNs: 50}
	seedSession(t, runner, "vol", expired)
	executor, err := newTestExecutor(
		runner,
		WithClock(func() time.Time { return time.Unix(0, 100) }),
		WithPerasCommitter(flushFailingPerasCommitter{}),
	)
	require.NoError(t, err)

	result, err := executor.ExpireWriteSessions(context.Background(), fsmeta.ExpireWriteSessionsRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ExpireWriteSessionsResult{Expired: 1}, result)
}

func TestExecutorUnlinkReservesNegativeQuotaWhenInodeExists(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 1})
	quota := &fakeQuotaResolver{}
	executor, err := newTestExecutor(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), fsmeta.UnlinkRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.NoError(t, err)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: -4096, Inodes: -1}}}, quota.changes)
}

func TestExecutorLinkCreatesDentryAndIncrementsLinkCount(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 1})
	quota := &fakeQuotaResolver{}
	executor, err := newTestExecutor(runner, WithQuotaResolver(quota))
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
	inode, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(2), inode.LinkCount)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 8, Bytes: 4096, Inodes: 1}}}, quota.changes)
}

func TestExecutorLinkPerasBufferedVisibleCommitServesOverlay(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 1})
	committer := newTestBufferedPerasCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	err = executor.Link(context.Background(), fsmeta.LinkRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "file",
		ToParent:   8,
		ToName:     "alias",
	})
	require.NoError(t, err)

	require.Empty(t, runner.mutations)
	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 8, Name: "alias"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), record.Inode)
	inode, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(2), inode.LinkCount)
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])
}

func TestExecutorLinkUsesAtomicMutateWithValuePredicates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedDentry(t, runner.fakeRunner, "vol", 7, "file", 22)
	seedInode(t, runner.fakeRunner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.Link(context.Background(), fsmeta.LinkRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "file",
		ToParent:   8,
		ToName:     "alias",
	})
	require.NoError(t, err)

	require.Len(t, runner.atomicCalls, 1)
	require.Empty(t, base.mutations)
	requireAtomicStatUint(t, executor.Stats(), fsmeta.OperationLink, "success_total", 1)
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[0].predicates[0].GetKind())
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS, runner.atomicCalls[0].predicates[1].GetKind())
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[0].predicates[2].GetKind())
	inode, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(2), inode.LinkCount)
}

func TestExecutorLinkRejectsDirectory(t *testing.T) {
	runner := newFakeRunner()
	seedDentryType(t, runner, "vol", 7, "dir", 22, fsmeta.InodeTypeDirectory)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeDirectory, LinkCount: 1})
	executor, err := newTestExecutor(runner)
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
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{22}}))
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
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
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
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
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

func TestExecutorRetriesRouteUnavailable(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		nokverrors.New(nokverrors.KindRouteUnavailable, "route lookup refreshing"),
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []fsmeta.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
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
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
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

func TestExecutorRetriesSustainedLiveTxnContention(t *testing.T) {
	runner := newFakeRunner()
	for range 9 {
		runner.mutateErrs = append(runner.mutateErrs, fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
			Locked: &kvrpcpb.Locked{
				PrimaryLock: []byte("dentry"),
				Key:         []byte("dentry"),
				LockVersion: 2,
				LockTtl:     defaultLockTTL,
			},
		}}})
	}
	runner.mutateErrs = append(runner.mutateErrs, nil)
	allocator := &fakeInodeAllocator{ids: []fsmeta.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
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
	require.Equal(t, uint64(21), runner.nextTS)
	require.Equal(t, uint64(9), executor.Stats()["txn_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["txn_retry_exhausted_total"])
}

func TestTxnContentionRetryPolicyUsesLockTTLAfterFixedAttempts(t *testing.T) {
	lockErr := fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
		Locked: &kvrpcpb.Locked{
			PrimaryLock: []byte("dentry"),
			Key:         []byte("dentry"),
			LockVersion: 2,
			LockTtl:     uint64(5 * time.Second / time.Millisecond),
		},
	}}}
	budget := txnRetryBudget(lockErr, defaultLockTTL)

	require.Equal(t, 5*time.Second+txnContentionRetryMaxBackoff, budget)
	require.True(t, canRetryTxnAttempt(maxTxnContentionRetries+8, time.Now(), lockErr, defaultLockTTL))
	require.False(t, canRetryTxnAttempt(maxTxnContentionRetries+8, time.Now().Add(-budget-time.Millisecond), lockErr, defaultLockTTL))
}

func TestTxnContentionRetryPolicyKeepsCountBoundForNonLockConflicts(t *testing.T) {
	writeConflictErr := fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
		WriteConflict: &kvrpcpb.WriteConflict{
			Key:        []byte("dentry"),
			ConflictTs: 4,
			StartTs:    2,
		},
	}}}

	require.Zero(t, txnRetryBudget(writeConflictErr, defaultLockTTL))
	require.True(t, canRetryTxnAttempt(maxTxnContentionRetries-1, time.Now(), writeConflictErr, defaultLockTTL))
	require.False(t, canRetryTxnAttempt(maxTxnContentionRetries, time.Now(), writeConflictErr, defaultLockTTL))
}

func TestTxnRetryBudgetFallsBackWhenLockDetailsAreUnavailable(t *testing.T) {
	err := nokverrors.New(nokverrors.KindLockConflict, "lock conflict translated across rpc boundary")

	require.Equal(t, 25*time.Millisecond+txnContentionRetryMaxBackoff, txnRetryBudget(err, 25))
}

func TestTxnRetryBudgetCoversPercolatorRetryableStartTSLoss(t *testing.T) {
	err := fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
		Retryable: "percolator: lock not found",
	}}}

	require.Equal(t, 50*time.Millisecond+txnContentionRetryMaxBackoff, txnRetryBudget(err, 50))
	require.True(t, canRetryTxnAttempt(maxTxnContentionRetries+1, time.Now(), err, 50))
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
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
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
	executor, err := newTestExecutor(newFakeRunner())
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

	executor, err := newTestExecutor(runner)
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
	executor, err := newTestExecutor(runner)
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
	executor, err := newTestExecutor(runner)
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

	executor, err := newTestExecutor(runner)
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
	executor, err := newTestExecutor(runner)
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
	executor, err := newTestExecutor(runner)
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
	executor, err := newTestExecutor(runner)
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
	_, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestExecutorUnlinkUsesAtomicMutateWithValuePredicates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedDentry(t, runner.fakeRunner, "vol", 7, "file", 22)
	seedInode(t, runner.fakeRunner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), fsmeta.UnlinkRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.NoError(t, err)

	require.Len(t, runner.atomicCalls, 1)
	require.Empty(t, base.mutations)
	requireAtomicStatUint(t, executor.Stats(), fsmeta.OperationUnlink, "success_total", 1)
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[0].predicates[0].GetKind())
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[0].predicates[1].GetKind())
	_, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestExecutorUnlinkDecrementsMultiLinkInode(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 2})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), fsmeta.UnlinkRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.NoError(t, err)

	inode, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(1), inode.LinkCount)
}

func TestExecutorUnlinkPerasBufferedVisibleCommitServesOverlay(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 2})
	committer := newTestBufferedPerasCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), fsmeta.UnlinkRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.NoError(t, err)

	require.Empty(t, runner.mutations)
	_, err = executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	inode, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(1), inode.LinkCount)
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])
}

func TestExecutorUnlinkMissingDentry(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
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
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
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
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(
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

func TestExecutorRenameUsesAtomicMutateWithoutSubtreeHandoff(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedDentry(t, runner.fakeRunner, "vol", 7, "old", 22)
	seedInode(t, runner.fakeRunner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	publisher := &fakeSubtreePublisher{}
	authority := &fakeAuthorityResolver{same: true}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(
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

	require.Len(t, runner.atomicCalls, 1)
	require.Empty(t, base.mutations)
	require.Empty(t, publisher.starts)
	require.Empty(t, publisher.completes)
	requireAtomicStatUint(t, executor.Stats(), fsmeta.OperationRename, "success_total", 1)
	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 8, Name: "new"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), record.Inode)
}

func TestExecutorRenamePerasBufferedVisibleCommitServesOverlay(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	committer := newTestBufferedPerasCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	err = executor.Rename(context.Background(), fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   7,
		ToName:     "new",
	})
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "old"})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "new"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{Parent: 7, Name: "new", Inode: 22, Type: fsmeta.InodeTypeFile}, record)
	require.Empty(t, runner.mutations, "same-parent rename should stay inside Peras overlay")

	stats := executor.Stats()
	requirePerasVisibleStatUint(t, stats, "attempt_total", 1)
	requirePerasVisibleStatUint(t, stats, "success_total", 1)
}

func TestExecutorCrossParentRenamePerasBufferedVisibleCommitServesOverlay(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 1})
	committer := newTestBufferedPerasCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithSubtreeAuthorityResolver(&fakeAuthorityResolver{same: true}),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(committer),
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
	require.Equal(t, fsmeta.DentryRecord{Parent: 8, Name: "new", Inode: 22, Type: fsmeta.InodeTypeFile}, record)
	require.Empty(t, runner.mutations, "same-authority cross-parent rename should stay inside Peras overlay")
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])
}

func TestExecutorAtomicOnePhaseBacksOffAfterRepeatedFallback(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: false}
	authority := &fakeAuthorityResolver{same: true}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeAuthorityResolver(authority))
	require.NoError(t, err)

	total := atomicOnePhaseBackoffAfter + 3
	for i := range total {
		oldName := fmt.Sprintf("old-%d", i)
		newName := fmt.Sprintf("new-%d", i)
		seedDentry(t, runner.fakeRunner, "vol", 7, oldName, fsmeta.InodeID(100+i))
		err := executor.Rename(context.Background(), fsmeta.RenameRequest{
			Mount:      "vol",
			FromParent: 7,
			FromName:   oldName,
			ToParent:   8,
			ToName:     newName,
		})
		require.NoError(t, err)
	}

	require.Len(t, runner.atomicCalls, atomicOnePhaseBackoffAfter)
	stats := executor.Stats()
	requireAtomicStatUint(t, stats, fsmeta.OperationRename, "fallback_total", atomicOnePhaseBackoffAfter)
	requireAtomicStatUint(t, stats, fsmeta.OperationRename, "skip_total", 3)
	requireAtomicStatUint(t, stats, fsmeta.OperationRename, "backoff_skip_total", 3)
}

func TestExecutorAtomicOnePhaseBackoffIsAffinityScoped(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: false}
	authority := &fakeAuthorityResolver{same: true}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeAuthorityResolver(authority))
	require.NoError(t, err)

	for i := range atomicOnePhaseBackoffAfter {
		oldName := fmt.Sprintf("old-%d", i)
		newName := fmt.Sprintf("new-%d", i)
		seedDentry(t, runner.fakeRunner, "vol", 7, oldName, fsmeta.InodeID(100+i))
		err := executor.Rename(context.Background(), fsmeta.RenameRequest{
			Mount:      "vol",
			FromParent: 7,
			FromName:   oldName,
			ToParent:   8,
			ToName:     newName,
		})
		require.NoError(t, err)
	}

	from, to := findDifferentRenameAffinity(t, 7, 8)
	seedDentry(t, runner.fakeRunner, "vol", from, "other-old", 999)
	err = executor.Rename(context.Background(), fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: from,
		FromName:   "other-old",
		ToParent:   to,
		ToName:     "other-new",
	})
	require.NoError(t, err)

	require.Len(t, runner.atomicCalls, atomicOnePhaseBackoffAfter+1)
	requireAtomicStatUint(t, executor.Stats(), fsmeta.OperationRename, "skip_total", 0)
}

func TestExecutorRenameRejectsCrossAuthority(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	authority := &fakeAuthorityResolver{same: false}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeAuthorityResolver(authority))
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
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
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
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
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
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
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
	executor, err := newTestExecutor(runner)
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
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
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
	key, err := fsmeta.EncodeDentryKey(testMountIdentityFor(mount), parent, name)
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
	key, err := fsmeta.EncodeInodeKey(testMountIdentityFor(mount), record.Inode)
	require.NoError(t, err)
	value, err := fsmeta.EncodeInodeValue(record)
	require.NoError(t, err)
	runner.data[string(key)] = value
}

func findDifferentRenameAffinity(t *testing.T, baseFrom, baseTo fsmeta.InodeID) (fsmeta.InodeID, fsmeta.InodeID) {
	t.Helper()
	base := renameAffinity(t, baseFrom, baseTo)
	for from := fsmeta.InodeID(9); from < 256; from++ {
		for to := fsmeta.InodeID(9); to < 256; to++ {
			if from == to {
				continue
			}
			if renameAffinity(t, from, to) != base {
				return from, to
			}
		}
	}
	t.Fatalf("no distinct rename affinity found")
	return 0, 0
}

func renameAffinity(t *testing.T, from, to fsmeta.InodeID) string {
	t.Helper()
	source, err := fsmeta.EncodeDentryKey(testMountIdentity, from, "old")
	require.NoError(t, err)
	destination, err := fsmeta.EncodeDentryKey(testMountIdentity, to, "new")
	require.NoError(t, err)
	return atomicOnePhaseAffinity(source, []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Delete, Key: source},
		{Op: kvrpcpb.Mutation_Put, Key: destination},
	})
}

func seedSession(t *testing.T, runner *fakeRunner, mount fsmeta.MountID, record fsmeta.SessionRecord) {
	t.Helper()
	value, err := fsmeta.EncodeSessionValue(record)
	require.NoError(t, err)
	sessionKey, err := fsmeta.EncodeSessionKey(testMountIdentityFor(mount), record.Inode, record.Session)
	require.NoError(t, err)
	ownerKey, err := fsmeta.EncodeInodeSessionKey(testMountIdentityFor(mount), record.Inode)
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
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{100}}), WithNegativeCache(cache))
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
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{100}}), WithNegativeCache(cache))
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
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{10, 11, 12}}), WithDirPageCache(cache))
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
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{10, 11, 12}}), WithDirPageCache(cache))
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
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{10}}), WithDirPageCache(cache))
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
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{99}}), WithDirPageCache(cache))
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

func BenchmarkExecutorAdmitPerasAuthorityOwned(b *testing.B) {
	executor, err := New(newFakeRunner(), WithPerasAuthorityAdmitter(ownedPerasAdmitter{}))
	if err != nil {
		b.Fatal(err)
	}
	delta := compile.SemanticDelta{
		Eligibility: compile.EligibilityVisibleCommit,
		Authority: compile.AuthorityScope{
			Mount:      "vol",
			MountKeyID: 1,
			Buckets:    []fsmeta.AffinityBucket{fsmeta.BucketForInodeID(fsmeta.RootInode)},
			Parents:    []fsmeta.InodeID{fsmeta.RootInode},
			Inodes:     []fsmeta.InodeID{22},
		},
	}
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		if err := executor.admitPerasAuthority(ctx, delta); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecutorPerasShadowSubmitCreate(b *testing.B) {
	executor, err := New(
		newFakeRunner(),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasShadowSubmitter(noopPerasSubmitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	delta, err := compile.Create(fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMountIdentity, 22)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		executor.submitPerasShadow(ctx, delta)
	}
}

func BenchmarkExecutorCreateDefaultPath(b *testing.B) {
	executor, err := newTestExecutor(newFakeRunner(), WithInodeAllocator(&fakeInodeAllocator{next: 22}))
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorCreate(b, executor)
}

func BenchmarkExecutorCreatePerasVisibleCommit(b *testing.B) {
	executor, err := newTestExecutor(
		newFakeRunner(),
		WithInodeAllocator(&fakeInodeAllocator{next: 22}),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(noopPerasCommitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorCreate(b, executor)
}

func BenchmarkExecutorCreatePerasDirectVisibleCommit(b *testing.B) {
	runner := newFakeRunner()
	holder, err := fsperas.NewHolder(fsperas.HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
	})
	if err != nil {
		b.Fatal(err)
	}
	committer, err := fsperas.NewDirectCommitter(fsperas.DirectCommitterConfig{
		Holder:   holder,
		Versions: runner,
		ReplayDB: benchmarkPerasApplier{},
	})
	if err != nil {
		b.Fatal(err)
	}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{next: 22}),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(committer),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorCreate(b, executor)
}

func BenchmarkExecutorOpenWriteSessionDefaultPath(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner, WithClock(func() time.Time { return time.Unix(0, 100) }))
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorOpenWriteSession(b, runner, executor)
}

func BenchmarkExecutorOpenWriteSessionPerasVisibleCommit(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(
		runner,
		WithClock(func() time.Time { return time.Unix(0, 100) }),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(noopPerasCommitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorOpenWriteSession(b, runner, executor)
}

func BenchmarkExecutorUpdateInodeDefaultPath(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorUpdateInode(b, runner, executor)
}

func BenchmarkExecutorUpdateInodePerasVisibleCommit(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(
		runner,
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(noopPerasCommitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorUpdateInode(b, runner, executor)
}

func BenchmarkExecutorRenameDefaultPath(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorRename(b, runner, executor)
}

func BenchmarkExecutorRenamePerasVisibleCommit(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(
		runner,
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(noopPerasCommitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorRename(b, runner, executor)
}

func BenchmarkExecutorLinkDefaultPath(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorLink(b, runner, executor)
}

func BenchmarkExecutorLinkPerasVisibleCommit(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(
		runner,
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(noopPerasCommitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorLink(b, runner, executor)
}

func BenchmarkExecutorUnlinkDefaultPath(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorUnlink(b, runner, executor)
}

func BenchmarkExecutorUnlinkPerasVisibleCommit(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(
		runner,
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(noopPerasCommitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorUnlink(b, runner, executor)
}

func newTestBufferedPerasCommitter(t *testing.T, versions fsperas.VersionAllocator) *fsperas.BufferedCommitter {
	t.Helper()
	holder, err := fsperas.NewHolder(fsperas.HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
	})
	require.NoError(t, err)
	committer, err := fsperas.NewBufferedCommitter(fsperas.BufferedCommitterConfig{
		Holder:   holder,
		Versions: versions,
		ReplayDB: benchmarkPerasApplier{},
	})
	require.NoError(t, err)
	return committer
}

func benchmarkExecutorCreate(b *testing.B, executor *Executor) {
	ctx := context.Background()
	var seq uint64
	b.ReportAllocs()
	for b.Loop() {
		name := "file-" + strconv.FormatUint(seq, 10)
		seq++
		if _, err := executor.Create(ctx, fsmeta.CreateRequest{
			Mount:  "vol",
			Parent: fsmeta.RootInode,
			Name:   name,
			Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorUpdateInode(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := fsmeta.InodeID(i + 1000)
		name := "file-" + strconv.Itoa(i)
		benchmarkSeedDentry(b, runner, 7, name, inode)
		benchmarkSeedInode(b, runner, inode)
		if _, err := executor.UpdateInode(ctx, fsmeta.UpdateInodeRequest{
			Mount:            "vol",
			Parent:           7,
			Inode:            inode,
			Name:             name,
			SetSize:          true,
			Size:             uint64(i + 4096),
			SetMode:          true,
			Mode:             0o600,
			SetUpdatedUnixNs: true,
			UpdatedUnixNs:    int64(i + 1),
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorRename(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := fsmeta.InodeID(i + 1000)
		oldName := "old-" + strconv.Itoa(i)
		newName := "new-" + strconv.Itoa(i)
		benchmarkSeedDentry(b, runner, 7, oldName, inode)
		if err := executor.Rename(ctx, fsmeta.RenameRequest{
			Mount:      "vol",
			FromParent: 7,
			FromName:   oldName,
			ToParent:   7,
			ToName:     newName,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorOpenWriteSession(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := fsmeta.InodeID(i + 1000)
		benchmarkSeedInode(b, runner, inode)
		if _, err := executor.OpenWriteSession(ctx, fsmeta.OpenWriteSessionRequest{
			Mount:   "vol",
			Inode:   inode,
			Session: fsmeta.SessionID("writer-" + strconv.Itoa(i)),
			TTL:     100 * time.Nanosecond,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorLink(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := fsmeta.InodeID(i + 1000)
		fromName := "file-" + strconv.Itoa(i)
		toName := "alias-" + strconv.Itoa(i)
		benchmarkSeedDentry(b, runner, 7, fromName, inode)
		benchmarkSeedInode(b, runner, inode)
		if err := executor.Link(ctx, fsmeta.LinkRequest{
			Mount:      "vol",
			FromParent: 7,
			FromName:   fromName,
			ToParent:   8,
			ToName:     toName,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorUnlink(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := fsmeta.InodeID(i + 1000)
		name := "file-" + strconv.Itoa(i)
		benchmarkSeedDentry(b, runner, 7, name, inode)
		benchmarkSeedInodeRecord(b, runner, fsmeta.InodeRecord{Inode: inode, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 2})
		if err := executor.Unlink(ctx, fsmeta.UnlinkRequest{
			Mount:  "vol",
			Parent: 7,
			Name:   name,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSeedDentry(b *testing.B, runner *fakeRunner, parent fsmeta.InodeID, name string, inode fsmeta.InodeID) {
	b.Helper()
	key, err := fsmeta.EncodeDentryKey(testMountIdentity, parent, name)
	if err != nil {
		b.Fatal(err)
	}
	value, err := fsmeta.EncodeDentryValue(fsmeta.DentryRecord{
		Parent: parent,
		Name:   name,
		Inode:  inode,
		Type:   fsmeta.InodeTypeFile,
	})
	if err != nil {
		b.Fatal(err)
	}
	runner.data[string(key)] = value
}

func benchmarkSeedInode(b *testing.B, runner *fakeRunner, inode fsmeta.InodeID) {
	b.Helper()
	benchmarkSeedInodeRecord(b, runner, fsmeta.InodeRecord{Inode: inode, Type: fsmeta.InodeTypeFile, LinkCount: 1})
}

func benchmarkSeedInodeRecord(b *testing.B, runner *fakeRunner, record fsmeta.InodeRecord) {
	b.Helper()
	key, err := fsmeta.EncodeInodeKey(testMountIdentity, record.Inode)
	if err != nil {
		b.Fatal(err)
	}
	value, err := fsmeta.EncodeInodeValue(record)
	if err != nil {
		b.Fatal(err)
	}
	runner.data[string(key)] = value
}

type benchmarkPerasApplier struct{}

func (benchmarkPerasApplier) ApplyInternalEntries([]*entrykv.Entry) error {
	return nil
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
