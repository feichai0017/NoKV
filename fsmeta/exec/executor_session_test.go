package exec

import (
	"context"
	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

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

func TestExecutorOpenWriteSessionPerasUsesCreateSessionOwnerFact(t *testing.T) {
	runner := newFakeRunner()
	committer := newTestPerasCommitter(t, runner)
	inode := testInodeForParentBucket(t, fsmeta.RootInode)
	executor, err := newTestExecutor(
		runner,
		WithClock(func() time.Time { return time.Unix(0, 100) }),
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{inode}}),
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
	runner.getCalls = 0

	opened, err := executor.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   created.Inode.Inode,
		Session: "writer-1",
		TTL:     100 * time.Nanosecond,
	})
	require.NoError(t, err)

	require.Equal(t, fsmeta.SessionRecord{Session: "writer-1", Inode: created.Inode.Inode, ExpiresUnixNs: 200}, opened)
	require.Equal(t, 0, runner.getCalls, "create facts prove both the inode owner key and the per-inode session namespace are absent")
	require.Empty(t, runner.mutations)
	require.Equal(t, uint64(2), committer.Stats()["commit_total"])
}

func TestExecutorWriteSessionLifecyclePerasVisibleCommitServesOverlay(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	committer := newTestPerasCommitter(t, runner)
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
	require.Empty(t, runner.mutations, "Peras session lifecycle should stay entirely inside Peras overlay")

	stats := executor.Stats()
	requirePerasVisibleStatUint(t, stats, "attempt_total", 3)
	requirePerasVisibleStatUint(t, stats, "success_total", 3)
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

func TestExecutorExpireWriteSessionsFlushesPerasAuthority(t *testing.T) {
	runner := newFakeRunner()
	expired := fsmeta.SessionRecord{Session: "writer-old", Inode: 22, ExpiresUnixNs: 50}
	seedSession(t, runner, "vol", expired)
	flusher := &fakePerasAuthorityFlusher{}
	executor, err := newTestExecutor(
		runner,
		WithClock(func() time.Time { return time.Unix(0, 100) }),
		WithPerasCommitter(flusher),
	)
	require.NoError(t, err)

	result, err := executor.ExpireWriteSessions(context.Background(), fsmeta.ExpireWriteSessionsRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ExpireWriteSessionsResult{Expired: 1}, result)
	require.Equal(t, 1, flusher.flushCalls)
	require.Len(t, flusher.flushScopes, 1)
	require.Equal(t, fsmeta.MountID("vol"), flusher.flushScopes[0].Mount)
}

func TestExecutorExpireWriteSessionsUsesPerasVisibleDelete(t *testing.T) {
	runner := newFakeRunner()
	expired := fsmeta.SessionRecord{Session: "writer-visible-old", Inode: 22, ExpiresUnixNs: 50}
	seedSession(t, runner, "vol", expired)
	committer := newTestPerasCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithClock(func() time.Time { return time.Unix(0, 100) }),
		WithPerasAuthorityAdmitter(ownedPerasAdmitter{}),
		WithPerasCommitter(committer),
	)
	require.NoError(t, err)

	result, err := executor.ExpireWriteSessions(context.Background(), fsmeta.ExpireWriteSessionsRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ExpireWriteSessionsResult{Expired: 1}, result)
	require.Empty(t, runner.mutations)

	sessionKey, err := fsmeta.EncodeSessionKey(testMountIdentity, expired.Inode, expired.Session)
	require.NoError(t, err)
	_, deleted, ok := committer.GetPerasOverlay(sessionKey)
	require.True(t, ok)
	require.True(t, deleted)
	ownerKey, err := fsmeta.EncodeInodeSessionKey(testMountIdentity, expired.Inode)
	require.NoError(t, err)
	_, deleted, ok = committer.GetPerasOverlay(ownerKey)
	require.True(t, ok)
	require.True(t, deleted)
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
