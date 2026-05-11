package raftstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	perasauth "github.com/feichai0017/NoKV/fsmeta/runtime/perasauth"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestRemotePerasCommitterCommitsAndServesOverlay(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnessesWithDurability(t, 3, wal.DurabilityFsync),
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	delta := testRuntimePerasDelta([]byte("dentry/a"), []byte("inode/a"))
	_, err = committer.CommitPeras(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta, nil)
	require.NoError(t, err)

	value, deleted, ok := committer.GetPerasOverlay([]byte("dentry/a"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])
}

func TestRemotePerasCommitterFlushesSegmentAndKeepsReadsVisible(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.NoError(t, commitRuntimePeras(ctx, committer, 2, []byte("dentry/b"), []byte("inode/b")))
	require.NoError(t, committer.Flush(ctx))

	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
	require.Equal(t, uint64(2), stats["segment_operations_total"])
	require.Equal(t, 0, stats["overlay_keys"])
	require.Equal(t, 4, stats["segment_keys"])
	require.Equal(t, 0, stats["pending"])
	require.Equal(t, 1, installer.calls)
	require.Equal(t, testRuntimePerasDelta([]byte("dentry/a"), []byte("inode/a")).Authority, installer.scope)
	require.NotZero(t, installer.segment.Root)
	require.NotEmpty(t, installer.payload)
	require.NotZero(t, installer.digest)
	decoded, err := fsperas.VerifyPerasSegmentPayload(installer.payload, installer.segment.Root, installer.digest)
	require.NoError(t, err)
	require.Equal(t, installer.segment.Root, decoded.Root)

	value, deleted, ok := committer.GetPerasOverlay([]byte("dentry/a"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)

	scan := committer.ScanPerasOverlay([]byte("dentry/"), 2)
	require.Len(t, scan, 2)
	require.Equal(t, []byte("dentry/a"), scan[0].Key)
	require.Equal(t, []byte("dentry/b"), scan[1].Key)

}

func TestRemotePerasCommitterShutdownFlushesPendingSegment(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.Equal(t, 1, committer.Stats()["pending"])
	require.NoError(t, committer.Shutdown(ctx))
	require.Equal(t, 0, committer.Stats()["pending"])
	require.Equal(t, 1, installer.calls)

	err = commitRuntimePeras(ctx, committer, 2, []byte("dentry/b"), []byte("inode/b"))
	require.ErrorIs(t, err, errPerasCommitterClosed)
}

func TestRemotePerasCommitterFlushSplitsFSMetaBuckets(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	leftA, leftB := testRuntimeBucketKeys(t, mount, 1)
	rightA, rightB := testRuntimeBucketKeys(t, mount, 2)
	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, leftA, leftB))
	require.NoError(t, commitRuntimePeras(ctx, committer, 2, rightA, rightB))
	require.NoError(t, committer.Flush(ctx))

	stats := committer.Stats()
	require.Equal(t, uint64(2), stats["flush_total"])
	require.Equal(t, uint64(2), stats["segment_total"])
	require.Equal(t, uint64(2), stats["segment_operations_total"])
	require.Equal(t, 2, installer.calls)
}

func TestRemotePerasCommitterFlushHonorsReplayMutationBudget(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:                 provider,
		Witnesses:                 testRuntimePerasWitnesses(t, 3),
		Installer:                 installer,
		SegmentBatchSize:          1024,
		SegmentMaxReplayMutations: 4,
		SegmentFlushEvery:         time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.NoError(t, commitRuntimePeras(ctx, committer, 2, []byte("dentry/b"), []byte("inode/b")))
	require.NoError(t, commitRuntimePeras(ctx, committer, 3, []byte("dentry/c"), []byte("inode/c")))
	require.NoError(t, committer.Flush(ctx))

	stats := committer.Stats()
	require.Equal(t, uint64(2), stats["flush_total"])
	require.Equal(t, uint64(2), stats["segment_total"])
	require.Equal(t, uint64(3), stats["segment_operations_total"])
	require.Equal(t, 2, installer.calls)
}

func TestRemotePerasCommitterFlushRequiresInstaller(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.ErrorIs(t, committer.Flush(ctx), errPerasCommitterInvalid)

	stats := committer.Stats()
	require.Equal(t, uint64(0), stats["flush_total"])
	require.Equal(t, uint64(0), stats["segment_total"])
	require.Equal(t, 1, stats["pending"])
	value, deleted, ok := committer.GetPerasOverlay([]byte("dentry/a"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestRemotePerasCommitterRecoversWitnessSegment(t *testing.T) {
	witnesses := testRuntimePerasWitnessesWithDurability(t, 3, wal.DurabilityFsync)
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         witnesses,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{EpochID: 1, HolderID: "holder-a"})
	require.NoError(t, err)
	delta := testRuntimePerasDelta([]byte("dentry/recovered"), []byte("inode/recovered"))
	_, err = holder.Submit(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta)
	require.NoError(t, err)
	plan, scope, err := holder.BuildPendingReplayPlan(10)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	require.NoError(t, committer.appendSegmentWitnesses(context.Background(), scope, holder, segment, payload, digest))

	installer := &fakeRuntimePerasSegmentInstaller{}
	recoverer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         witnesses,
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer recoverer.Close()

	require.NoError(t, recoverer.RecoverWitnessSegments(context.Background(), scope, holder.EpochID()))
	require.Equal(t, 1, installer.calls)
	require.Equal(t, segment.Root, installer.segment.Root)
	value, deleted, ok := recoverer.GetPerasOverlay([]byte("dentry/recovered"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestRemotePerasCommitterFlushAuthorityFlushesOnlyOverlappingPendingOps(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	scopeA := compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{1},
		Inodes:     []fsmeta.InodeID{2},
	}
	scopeB := compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{2},
		Inodes:     []fsmeta.InodeID{3},
	}
	deltaA := testRuntimePerasDelta([]byte("dentry/a"), []byte("inode/a"))
	deltaA.Authority = scopeA
	deltaB := testRuntimePerasDelta([]byte("dentry/b"), []byte("inode/b"))
	deltaB.Authority = scopeB

	ctx := context.Background()
	_, err = committer.CommitPeras(ctx, fsperas.OperationID{ClientID: "client", Seq: 1}, deltaA, nil)
	require.NoError(t, err)
	_, err = committer.CommitPeras(ctx, fsperas.OperationID{ClientID: "client", Seq: 2}, deltaB, nil)
	require.NoError(t, err)

	require.NoError(t, committer.FlushAuthority(ctx, scopeA))
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
	require.Equal(t, uint64(1), stats["segment_operations_total"])
	require.Equal(t, 1, stats["pending"])
	require.Equal(t, 2, stats["overlay_keys"])
	require.Equal(t, 2, stats["segment_keys"])
	require.Equal(t, 1, installer.calls)
	require.Equal(t, scopeA, installer.scope)
	require.Equal(t, uint64(1), installer.segment.Stats().OperationCount)

	value, deleted, ok := committer.GetPerasOverlay([]byte("dentry/a"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
	value, deleted, ok = committer.GetPerasOverlay([]byte("dentry/b"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)

	require.NoError(t, committer.Flush(ctx))
	stats = committer.Stats()
	require.Equal(t, uint64(2), stats["flush_total"])
	require.Equal(t, uint64(2), stats["segment_total"])
	require.Equal(t, uint64(2), stats["segment_operations_total"])
	require.Equal(t, 0, stats["pending"])
	require.Equal(t, 0, stats["overlay_keys"])
	require.Equal(t, 2, installer.calls)
}

func TestRemotePerasCommitterDrainAuthorityFlushesAndRetires(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	scope := compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{1},
		Inodes:     []fsmeta.InodeID{2},
	}
	delta := testRuntimePerasDelta([]byte("dentry/a"), []byte("inode/a"))
	delta.Authority = scope
	otherScope := compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{9},
		Inodes:     []fsmeta.InodeID{10},
	}
	otherDelta := testRuntimePerasDelta([]byte("dentry/b"), []byte("inode/b"))
	otherDelta.Authority = otherScope

	ctx := context.Background()
	_, err = committer.CommitPeras(ctx, fsperas.OperationID{ClientID: "client", Seq: 1}, delta, nil)
	require.NoError(t, err)
	_, err = committer.CommitPeras(ctx, fsperas.OperationID{ClientID: "client", Seq: 2}, otherDelta, nil)
	require.NoError(t, err)
	retirer := &fakeRuntimePerasRetirer{}
	require.NoError(t, committer.DrainAuthority(ctx, retirer, scope))

	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
	require.Equal(t, uint64(2), stats["segment_operations_total"])
	require.Equal(t, 0, stats["pending"])
	require.Equal(t, 1, installer.calls)
	require.Equal(t, 1, retirer.calls)
	require.Equal(t, []compile.AuthorityScope{scope}, retirer.scopes)
}

func TestRemotePerasCommitterBackgroundFlushTimesOutAndBacksOff(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &blockingRuntimePerasSegmentInstaller{}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:              provider,
		Witnesses:              testRuntimePerasWitnesses(t, 3),
		Installer:              installer,
		SegmentBatchSize:       1,
		SegmentFlushEvery:      time.Hour,
		BackgroundFlushTimeout: 10 * time.Millisecond,
		BackgroundErrorBackoff: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.Eventually(t, func() bool {
		stats := committer.Stats()
		return stats["background_error_total"] == uint64(1) && installer.calls.Load() == 1
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, commitRuntimePeras(ctx, committer, 2, []byte("dentry/b"), []byte("inode/b")))
	require.Eventually(t, func() bool {
		return committer.Stats()["background_skip_total"].(uint64) > 0
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), installer.calls.Load())
	require.Equal(t, 2, committer.Stats()["pending"])
}

func TestRemotePerasCommitterFlushAllowsConcurrentCommitsDuringInstall(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &blockingRuntimePerasSegmentInstaller{}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, commitRuntimePeras(context.Background(), committer, 1, []byte("dentry/a"), []byte("inode/a")))

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- committer.Flush(ctx)
	}()
	require.Eventually(t, func() bool {
		return installer.calls.Load() == 1
	}, time.Second, 10*time.Millisecond)

	commitDone := make(chan error, 1)
	go func() {
		commitDone <- commitRuntimePeras(context.Background(), committer, 2, []byte("dentry/b"), []byte("inode/b"))
	}()
	select {
	case err := <-commitDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("concurrent commit did not complete while background segment install was in progress")
	}

	cancel()
	require.ErrorIs(t, <-flushDone, context.Canceled)
	require.Equal(t, 2, committer.Stats()["pending"])
}

func TestRemotePerasCommitterDrainAuthorityBlocksConcurrentCommitsUntilInstallFinishes(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &blockingRuntimePerasSegmentInstaller{}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, commitRuntimePeras(context.Background(), committer, 1, []byte("dentry/a"), []byte("inode/a")))

	drainDone := make(chan error, 1)
	go func() {
		drainDone <- committer.DrainAuthority(ctx, &fakeRuntimePerasRetirer{})
	}()
	require.Eventually(t, func() bool {
		return installer.calls.Load() == 1
	}, time.Second, 10*time.Millisecond)

	commitDone := make(chan error, 1)
	go func() {
		commitDone <- commitRuntimePeras(context.Background(), committer, 2, []byte("dentry/b"), []byte("inode/b"))
	}()
	select {
	case err := <-commitDone:
		t.Fatalf("concurrent commit completed while authority drain install was still in progress: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	cancel()
	require.ErrorIs(t, <-drainDone, context.Canceled)
	require.NoError(t, <-commitDone)
}

func TestRemotePerasCommitterRollsBackHolderOnOverlayAdmissionFailure(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	delta := testRuntimePerasDelta([]byte("dentry/a"), []byte("inode/a"))
	delta.WriteEffects = []compile.WriteEffect{{Kind: compile.EffectDelete}}
	_, err = committer.CommitPeras(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta, nil)
	require.Error(t, err)
	require.Equal(t, 0, committer.Stats()["pending"])
}

func TestValidatePerasSegmentInstallResponseChecksRootAndCounts(t *testing.T) {
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID: fsperas.OperationID{ClientID: "client", Seq: 1},
			Kind: fsmeta.OperationCreate,
			Mutations: []fsperas.ReplayMutation{
				{Key: []byte("dentry/a"), Value: []byte("dentry-value")},
				{Key: []byte("inode/a"), Value: []byte("inode-value")},
			},
		}},
	})
	require.NoError(t, err)

	stats := segment.Stats()
	resp := &kvrpcpb.PerasInstallSegmentResponse{
		SegmentRoot:    append([]byte(nil), segment.Root[:]...),
		OperationCount: stats.OperationCount,
		EntryCount:     stats.EntryCount,
		AppliedEntries: 1,
	}
	require.NoError(t, validatePerasSegmentInstallResponse(segment, resp))

	resp.SegmentRoot[0] ^= 0xff
	require.ErrorIs(t, validatePerasSegmentInstallResponse(segment, resp), errPerasCommitterInvalid)

	resp.SegmentRoot = append([]byte(nil), segment.Root[:]...)
	resp.EntryCount++
	require.ErrorIs(t, validatePerasSegmentInstallResponse(segment, resp), errPerasCommitterInvalid)
}

func BenchmarkRemotePerasCommitterCreate(b *testing.B) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(b, 3),
		SegmentBatchSize:  1 << 30,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(b, err)
	defer committer.Close()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dentryKey := appendUvarintKey("dentry/", uint64(i))
		inodeKey := appendUvarintKey("inode/", uint64(i))
		_, err := committer.CommitPeras(ctx, fsperas.OperationID{ClientID: "bench", Seq: uint64(i + 1)}, testRuntimePerasDelta(dentryKey, inodeKey), nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRemotePerasCommitterCreateParallel(b *testing.B) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRemotePerasCommitter(RemotePerasCommitterConfig{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(b, 3),
		SegmentBatchSize:  1 << 30,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(b, err)
	defer committer.Close()

	ctx := context.Background()
	var seq atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			current := seq.Add(1)
			dentryKey := appendUvarintKey("dentry/", current)
			inodeKey := appendUvarintKey("inode/", current)
			_, err := committer.CommitPeras(ctx, fsperas.OperationID{ClientID: "bench", Seq: current}, testRuntimePerasDelta(dentryKey, inodeKey), nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func commitRuntimePeras(ctx context.Context, committer *RemotePerasCommitter, seq uint64, dentryKey, inodeKey []byte) error {
	_, err := committer.CommitPeras(ctx, fsperas.OperationID{ClientID: "client", Seq: seq}, testRuntimePerasDelta(dentryKey, inodeKey), nil)
	return err
}

type fakeRuntimePerasSegmentInstaller struct {
	calls   int
	scope   compile.AuthorityScope
	segment fsperas.PerasSegment
	payload []byte
	digest  [32]byte
}

func (i *fakeRuntimePerasSegmentInstaller) InstallPerasSegment(_ context.Context, scope compile.AuthorityScope, segment fsperas.PerasSegment, payload []byte, digest [32]byte) error {
	i.calls++
	i.scope = scope
	i.segment = segment
	i.payload = append([]byte(nil), payload...)
	i.digest = digest
	return nil
}

type blockingRuntimePerasSegmentInstaller struct {
	calls atomic.Int32
}

func (i *blockingRuntimePerasSegmentInstaller) InstallPerasSegment(ctx context.Context, _ compile.AuthorityScope, _ fsperas.PerasSegment, _ []byte, _ [32]byte) error {
	i.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

type fakeRuntimePerasGrantProvider struct {
	holderID string
	grant    perasauth.AuthorityGrant
	owned    bool
	err      error
}

func (p *fakeRuntimePerasGrantProvider) HolderID() string {
	return p.holderID
}

func (p *fakeRuntimePerasGrantProvider) Acquire(context.Context, compile.AuthorityScope) (perasauth.AuthorityGrant, bool, error) {
	owned := p.owned
	if !owned {
		owned = true
	}
	return p.grant, owned, p.err
}

type fakeRuntimePerasRetirer struct {
	calls  int
	scopes []compile.AuthorityScope
	err    error
}

func (r *fakeRuntimePerasRetirer) RetirePerasAuthority(_ context.Context, scopes ...compile.AuthorityScope) error {
	r.calls++
	r.scopes = append(r.scopes, scopes...)
	return r.err
}

func testRuntimePerasWitnesses(tb testing.TB, n int) []fsperas.WitnessReplica {
	return testRuntimePerasWitnessesWithDurability(tb, n, wal.DurabilityBuffered)
}

func testRuntimePerasWitnessesWithDurability(tb testing.TB, n int, durability wal.DurabilityPolicy) []fsperas.WitnessReplica {
	tb.Helper()
	witnesses := make([]fsperas.WitnessReplica, 0, n)
	for i := range n {
		manager, err := wal.Open(wal.Config{Dir: tb.TempDir()})
		require.NoError(tb, err)
		tb.Cleanup(func() { _ = manager.Close() })
		log, err := fsperas.NewWALWitnessLog(manager, durability)
		require.NoError(tb, err)
		witness, err := fsperas.NewLocalWitnessReplica(fmt.Sprintf("witness-%d", i), log)
		require.NoError(tb, err)
		witnesses = append(witnesses, witness)
	}
	return witnesses
}

func testRuntimeCommitterGrant() perasauth.AuthorityGrant {
	return perasauth.AuthorityGrant{
		GrantID:         "grant-1",
		EpochID:         1,
		HolderID:        "holder-a",
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
		Scope: rootproto.PerasAuthorityScope{
			MountID:    "vol",
			MountKeyID: 1,
			Parents:    []uint64{1},
			Inodes:     []uint64{2},
		},
	}
}

func testRuntimePerasDelta(dentryKey, inodeKey []byte) compile.SemanticDelta {
	return compile.SemanticDelta{
		Kind: fsmeta.OperationCreate,
		Authority: compile.AuthorityScope{
			Mount:      "vol",
			MountKeyID: 1,
			Parents:    []fsmeta.InodeID{1},
			Inodes:     []fsmeta.InodeID{2},
		},
		ReadPredicates: []compile.Predicate{
			{Kind: compile.PredicateNotExists, Key: dentryKey},
			{Kind: compile.PredicateNotExists, Key: inodeKey},
		},
		WriteEffects: []compile.WriteEffect{
			{Kind: compile.EffectPut, Key: dentryKey, Value: []byte("dentry-value")},
			{Kind: compile.EffectPut, Key: inodeKey, Value: []byte("inode-value")},
		},
		Eligibility: compile.EligibilityVisibleCommit,
	}
}

func appendUvarintKey(prefix string, v uint64) []byte {
	out := append([]byte(prefix), 0)
	return binary.AppendUvarint(out, v)
}

func testRuntimeBucketKeys(t *testing.T, mount fsmeta.MountIdentity, bucket fsmeta.AffinityBucket) ([]byte, []byte) {
	t.Helper()
	var first, second []byte
	for inode := fsmeta.InodeID(2); inode < 100_000; inode++ {
		if fsmeta.BucketForInodeID(inode) != bucket {
			continue
		}
		key, err := fsmeta.EncodeInodeKey(mount, inode)
		require.NoError(t, err)
		if first == nil {
			first = key
			continue
		}
		second = key
		break
	}
	require.NotNil(t, first)
	require.NotNil(t, second)
	return first, second
}
