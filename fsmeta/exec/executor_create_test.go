package exec

import (
	"context"
	"errors"
	"github.com/feichai0017/NoKV/engine/slab/negativecache"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExecutorCreateAdmitsPerasAuthority(t *testing.T) {
	runner := newFakeRunner()
	admitter := &fakePerasAdmitter{owned: true}
	inode := testInodeForParentBucket(t, fsmeta.RootInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{inode}}),
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
	require.Equal(t, []fsmeta.InodeID{inode}, admitter.scopes[0].Inodes)
	require.Len(t, runner.mutations, 1)

	stats := executor.Stats()
	requirePerasStatBool(t, stats, "enabled", true)
	requirePerasStatUint(t, stats, "eligible_total", 1)
	requirePerasStatUint(t, stats, "acquire_total", 1)
	requirePerasStatUint(t, stats, "owned_total", 1)
	requirePerasStatUint(t, stats, "held_total", 0)
	requirePerasStatUint(t, stats, "slow_total", 0)
}

func TestExecutorCreatePerasVisibleCommitBypassesRaftCommit(t *testing.T) {
	runner := newFakeRunner()
	committer := &fakePerasCommitter{}
	inode := testInodeForParentBucket(t, fsmeta.RootInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{inode}}),
		WithPerasAuthorityAdmitter(&fakePerasAdmitter{owned: true}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	result, err := executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	require.Equal(t, inode, result.Inode.Inode)
	require.Equal(t, inode, result.Dentry.Inode)
	require.Equal(t, 1, committer.calls)
	require.Len(t, committer.ids, 1)
	require.Contains(t, committer.ids[0].ClientID, "fsmeta-exec/create")
	require.Equal(t, uint64(1), committer.ids[0].Seq)
	require.Len(t, committer.deltas, 1)
	require.Equal(t, compile.EligibilityVisibleCommit, committer.deltas[0].Eligibility)
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
	inode := testInodeForParentBucket(t, fsmeta.RootInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{inode}}),
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
	requirePerasVisibleStatUint(t, stats, "attempt_total", 1)
	requirePerasVisibleStatUint(t, stats, "skip_predicate_total", 1)
}

func TestExecutorCreatePerasVisibleCommitErrorDoesNotFallback(t *testing.T) {
	runner := newFakeRunner()
	commitErr := errors.New("peras commit failed")
	committer := &fakePerasCommitter{err: commitErr}
	inode := testInodeForParentBucket(t, fsmeta.RootInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{inode}}),
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
	require.ErrorIs(t, err, commitErr)
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

	require.Zero(t, committer.calls, "shared quota must remain on the transaction runner until quota credits exist")
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
	inode := testInodeForParentBucket(t, 7)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{inode}}),
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

func TestExecutorCreatePerasVisibleCommitRejectsOverlayDuplicate(t *testing.T) {
	runner := newFakeRunner()
	committer := newTestPerasCommitter(t, runner)
	firstInode := testInodeForParentBucket(t, fsmeta.RootInode)
	secondInode := testInodeForParentBucket(t, fsmeta.RootInode, firstInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{firstInode, secondInode}}),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
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

	_, err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.ErrorIs(t, err, fsmeta.ErrExists)
	require.Empty(t, runner.mutations, "overlay predicate failure must not fall back into ordinary mutation")
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])

	stats := executor.Stats()
	requirePerasVisibleStatUint(t, stats, "attempt_total", 2)
	requirePerasVisibleStatUint(t, stats, "success_total", 1)
	requirePerasVisibleStatUint(t, stats, "skip_predicate_total", 1)
	requirePerasVisibleStatUint(t, stats, "error_total", 0)
}

func TestExecutorCreatePerasVisibleCommitUsesEmptyDirectoryFact(t *testing.T) {
	runner := newFakeRunner()
	committer := newTestPerasCommitter(t, runner)
	dirInode := testInodeForParentBucket(t, fsmeta.RootInode)
	childInode := testInodeForParentBucket(t, dirInode, dirInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{dirInode, childInode}}),
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
		Parent: dirInode,
		Name:   "part-000",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	require.Equal(t, childInode, created.Inode.Inode)
	require.Equal(t, getsAfterDir, runner.getCalls, "empty-directory admission should avoid per-child predicate reads")
	require.Empty(t, runner.mutations)
	require.Equal(t, uint64(2), committer.Stats()["commit_total"])
}

func TestExecutorCreateFallsBackWhenPerasAuthorityHeldElsewhere(t *testing.T) {
	runner := newFakeRunner()
	admitter := &fakePerasAdmitter{owned: false}
	inode := testInodeForParentBucket(t, fsmeta.RootInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{inode}}),
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
	require.NotEmpty(t, runner.mutations)

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

func BenchmarkExecutorCheckpointStormDefaultPath100(b *testing.B) {
	executor, err := newTestExecutor(newFakeRunner(), WithInodeAllocator(&fakeInodeAllocator{next: 22}))
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorCheckpointStorm(b, executor, nil, 100)
}

func BenchmarkExecutorCheckpointStormPerasSegment100(b *testing.B) {
	runner := newFakeRunner()
	committer := newTestPerasCommitter(b, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{next: 22}),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(committer),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorCheckpointStorm(b, executor, committer, 100)
}

func BenchmarkExecutorCheckpointStormPerasVisible100(b *testing.B) {
	executor, err := newTestExecutor(
		newFakeRunner(),
		WithInodeAllocator(&fakeInodeAllocator{next: 22}),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(noopPerasCommitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorCheckpointStorm(b, executor, nil, 100)
}
