package peras

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeCommitsAndServesOverlay(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	delta := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	_, err = committer.SubmitVisible(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta, nil)
	require.NoError(t, err)

	value, deleted, ok := committer.GetPerasOverlay(delta.Effects[0].Key)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])
}

func TestRuntimePublishesVisibleWatch(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	parent := fsmeta.InodeID(1)
	inode := testRuntimeInodeForBucket(t, fsmeta.BucketForInodeID(parent))
	dentryKey, err := fsmeta.EncodeDentryKey(mount, parent, "visible")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, inode)
	require.NoError(t, err)
	prefix, err := fsmeta.EncodeDentryPrefix(mount, parent)
	require.NoError(t, err)
	router := fsmetawatch.NewRouter()
	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{KeyPrefix: prefix})
	require.NoError(t, err)
	defer sub.Close()

	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		WatchPublisher:    router,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	delta := testRuntimePerasOp(dentryKey, inodeKey)
	delta = testRuntimePerasOpWithScope(delta, compile.AuthorityScope{
		Mount:      delta.Delta.Authority.Mount,
		MountKeyID: delta.Delta.Authority.MountKeyID,
		Parents:    []fsmeta.InodeID{parent},
		Inodes:     []fsmeta.InodeID{inode},
	})
	_, err = committer.SubmitVisible(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta, nil)
	require.NoError(t, err)

	got := <-sub.Events()
	require.Equal(t, fsmeta.WatchEventSourcePerasVisible, got.Source)
	require.Equal(t, dentryKey, got.Key)
	require.Zero(t, got.Cursor.RegionID)
	require.Equal(t, uint64(1), got.Cursor.Term)
	require.Equal(t, uint64(1), got.Cursor.Index)
}

func TestRuntimeFlushesSegmentAndKeepsReadsVisible(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
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
	require.NoError(t, committer.FlushDurable(ctx))

	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
	require.Equal(t, uint64(2), stats["segment_operations_total"])
	require.Equal(t, 0, stats["overlay_keys"])
	require.Equal(t, 4, stats["segment_keys"])
	require.Equal(t, 0, stats["pending"])
	require.Equal(t, 1, installer.calls)
	require.True(t, ScopesOverlap(installer.scope, testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a")).Delta.Authority))
	require.True(t, ScopesOverlap(installer.scope, testRuntimePerasOp([]byte("dentry/b"), []byte("inode/b")).Delta.Authority))
	require.NotZero(t, installer.segment.Root)
	require.NotEmpty(t, installer.payload)
	require.NotZero(t, installer.digest)
	require.False(t, installer.materialize)
	decoded, err := fsperas.VerifyPerasSegmentPayload(installer.payload, installer.segment.Root, installer.digest)
	require.NoError(t, err)
	require.Equal(t, installer.segment.Root, decoded.Root)

	dentryA := testRuntimeDentryKeyForLabel("a")
	dentryB := testRuntimeDentryKeyForLabel("b")
	value, deleted, ok := committer.GetPerasOverlay(dentryA)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)

	scan := committer.ScanPerasOverlay(testRuntimeRootDentryPrefix(), 2)
	require.Len(t, scan, 2)
	require.Equal(t, dentryA, scan[0].Key)
	require.Equal(t, dentryB, scan[1].Key)

}

func TestRuntimeScanPerasOverlayMergesViewsByLimit(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	keyA := testRuntimeDentryKeyForLabel("a")
	keyB := testRuntimeDentryKeyForLabel("b")
	keyC := testRuntimeDentryKeyForLabel("c")
	keyD := testRuntimeDentryKeyForLabel("d")
	require.NoError(t, committer.read.sealed.AddSegment(testRuntimePerasSegmentForOverlay(keyA, []byte("sealed-a"))))
	require.NoError(t, committer.read.sealed.AddSegment(testRuntimePerasSegmentForOverlay(keyB, []byte("sealed-b"))))
	require.NoError(t, committer.read.sealed.AddSegment(testRuntimePerasSegmentForOverlay(keyD, []byte("sealed-d"))))
	require.NoError(t, committer.read.overlay.Add(fsperas.OperationID{ClientID: "test", Seq: 1}, testRuntimeRenameDentryOp("b", "c", []byte("overlay-c"))))

	scan := committer.ScanPerasOverlay(testRuntimeRootDentryPrefix(), 4)
	require.Equal(t, []fsperas.OverlayKV{
		{Key: keyA, Value: []byte("sealed-a")},
		{Key: keyB, Delete: true},
		{Key: keyC, Value: []byte("overlay-c")},
		{Key: keyD, Value: []byte("sealed-d")},
	}, scan)

	scan = committer.ScanPerasOverlay(keyB, 2)
	require.Equal(t, []fsperas.OverlayKV{
		{Key: keyB, Delete: true},
		{Key: keyC, Value: []byte("overlay-c")},
	}, scan)
}

func TestRuntimePublishesRootSealAfterInstall(t *testing.T) {
	provider := &publishingRuntimePerasGrantProvider{
		fakeRuntimePerasGrantProvider: fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()},
	}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
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
	require.NoError(t, committer.FlushPublished(ctx))

	provider.mu.Lock()
	defer provider.mu.Unlock()
	require.Equal(t, 1, provider.sealCalls)
	require.Equal(t, provider.grant.GrantID, provider.sealedGrant.GrantID)
	require.Equal(t, installer.segment.Root, provider.sealedSegment.Root)
	require.Equal(t, installer.digest, provider.sealedDigest)
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["seal_total"])
	require.NotZero(t, stats["flush_latency_total_nanosecond"])
	require.NotZero(t, stats["witness_latency_total_nanosecond"])
	require.NotZero(t, stats["install_latency_total_nanosecond"])
	require.NotZero(t, stats["seal_latency_total_nanosecond"])
	require.NotZero(t, stats["flush_latency_max_nanosecond"])
	require.NotZero(t, stats["witness_latency_max_nanosecond"])
	require.NotZero(t, stats["install_latency_max_nanosecond"])
	require.NotZero(t, stats["seal_latency_max_nanosecond"])
	require.NotZero(t, stats["flush_latency_average_nanosecond"])
	require.NotZero(t, stats["witness_latency_average_nanosecond"])
	require.NotZero(t, stats["install_latency_average_nanosecond"])
	require.NotZero(t, stats["seal_latency_average_nanosecond"])
	require.Equal(t, uint64(1), stats["flush_batch_total"])
	require.Equal(t, uint64(1), stats["flush_jobs_total"])
	require.Equal(t, uint64(1), stats["flush_jobs_last"])
	require.Equal(t, uint64(1), stats["flush_jobs_max"])
}

func TestRuntimeCanStopAtDurablePersistence(t *testing.T) {
	provider := &publishingRuntimePerasGrantProvider{
		fakeRuntimePerasGrantProvider: fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()},
	}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
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
	require.NoError(t, committer.FlushTo(ctx, fsperas.SegmentPersistenceDurable))

	provider.mu.Lock()
	require.Equal(t, 0, provider.sealCalls)
	provider.mu.Unlock()
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(0), stats["seal_total"])
	require.NotZero(t, stats["witness_latency_total_nanosecond"])
	require.NotZero(t, stats["install_latency_total_nanosecond"])
	require.Zero(t, stats["seal_latency_total_nanosecond"])
	require.True(t, committer.segmentInstalled(installer.segment.Root))
	require.Equal(t, 0, stats["pending"])
}

func TestRuntimeAppendsBatchWitnessesBeforeParallelInstall(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	keys := make([][2][]byte, 0, 4)
	for bucket := fsmeta.AffinityBucket(0); len(keys) < cap(keys); bucket++ {
		first, second := testRuntimeBucketKeys(t, mount, bucket)
		keys = append(keys, [2][]byte{first, second})
	}
	witness := &countingRuntimePerasWitness{id: "witness-0"}
	installer := &witnessPhaseCheckingRuntimePerasSegmentInstaller{
		witness: witness,
		want:    len(keys),
	}
	committer, err := NewRuntime(Config{
		Authority:                 &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()},
		Witnesses:                 []fsperas.WitnessReplica{witness},
		Installer:                 installer,
		Quorum:                    1,
		SegmentBatchSize:          1024,
		SegmentMaxReplayMutations: 2,
		SegmentInstallParallelism: 2,
		SegmentFlushEvery:         time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	for idx, pair := range keys {
		require.NoError(t, commitRuntimePeras(ctx, committer, uint64(idx+1), pair[0], pair[1]))
	}
	require.NoError(t, committer.FlushDurable(ctx))
	require.Equal(t, len(keys), witness.Count())
	require.Equal(t, int32(len(keys)), installer.calls.Load())
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_batch_total"])
	require.Equal(t, uint64(len(keys)), stats["flush_jobs_total"])
	require.Equal(t, uint64(len(keys)), stats["flush_jobs_max"])
}

func TestRuntimeReturnsInstalledCompletionOnRetry(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	opID := fsperas.OperationID{ClientID: "client", Seq: 7}
	delta := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	ack, err := committer.SubmitVisible(ctx, opID, delta, nil)
	require.NoError(t, err)
	require.Equal(t, opID, ack.OpID)
	require.NoError(t, committer.FlushDurable(ctx))
	completion, ok := committer.Completion(opID)
	require.True(t, ok)
	require.Equal(t, opID, completion.OpID)

	retryAck, err := committer.SubmitVisible(ctx, opID, delta, nil)
	require.NoError(t, err)
	require.Equal(t, ack.OpID, retryAck.OpID)
	require.Equal(t, ack.EpochID, retryAck.EpochID)
	require.Equal(t, 1, installer.calls)
	require.Equal(t, 0, committer.Stats()["pending"])
}

func TestRuntimeReturnsPendingAckOnRetry(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	opID := fsperas.OperationID{ClientID: "client", Seq: 9}
	delta := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	first, err := committer.SubmitVisible(ctx, opID, delta, nil)
	require.NoError(t, err)
	second, err := committer.SubmitVisible(ctx, opID, delta, func(context.Context, compile.MaterializedOp) (fsperas.AdmissionResult, bool, error) {
		t.Fatal("pending retry should not re-run admission")
		return fsperas.AdmissionResult{}, false, nil
	})
	require.NoError(t, err)

	require.Equal(t, first, second)
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["commit_total"])
	require.Equal(t, 1, stats["pending"])
	require.Equal(t, 2, stats["overlay_keys"])
}

func TestRuntimeShutdownFlushesPendingSegment(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
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
	require.ErrorIs(t, err, ErrRuntimeClosed)
}

func TestRuntimeFlushPreservesCatalogSegmentAcrossFSMetaBuckets(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
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
	require.NoError(t, committer.FlushDurable(ctx))

	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
	require.Equal(t, uint64(2), stats["segment_operations_total"])
	require.Equal(t, 1, installer.calls)
}

func TestRuntimeAcceptsCrossBucketCreateForCatalogInstall(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	leftA, _ := testRuntimeBucketKeys(t, mount, 1)
	rightA, _ := testRuntimeBucketKeys(t, mount, 2)
	ctx := context.Background()
	op := testRuntimePerasOp(leftA, rightA)
	_, err = committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: 1}, op, nil)
	require.NoError(t, err)

	stats := committer.Stats()
	require.Equal(t, uint64(0), stats["flush_total"])
	require.Equal(t, uint64(0), stats["segment_total"])
	require.Equal(t, uint64(0), stats["segment_operations_total"])
	require.Equal(t, 0, installer.calls)
	require.Equal(t, 1, stats["pending"])
	_, _, ok := committer.GetPerasOverlay(op.Effects[0].Key)
	require.True(t, ok)
	_, _, ok = committer.GetPerasOverlay(op.Effects[1].Key)
	require.True(t, ok)

	require.NoError(t, committer.FlushDurable(ctx))
	stats = committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
	require.Equal(t, 1, installer.calls)
	require.Equal(t, 0, stats["pending"])
}

func TestRuntimeFlushHonorsReplayMutationBudget(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
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
	require.NoError(t, committer.FlushDurable(ctx))

	stats := committer.Stats()
	require.Equal(t, uint64(2), stats["flush_total"])
	require.Equal(t, uint64(2), stats["segment_total"])
	require.Equal(t, uint64(3), stats["segment_operations_total"])
	require.Equal(t, 2, installer.calls)
}

func TestRuntimeRetriesRetryableSegmentInstall(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &flakyRuntimePerasSegmentInstaller{failures: 2}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	require.NoError(t, commitRuntimePeras(context.Background(), committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.NoError(t, committer.FlushDurable(context.Background()))

	require.Equal(t, 3, installer.calls)
	stats := committer.Stats()
	require.Equal(t, uint64(2), stats["retry_total"])
	require.Equal(t, uint64(2), stats["retry_stale_epoch_total"])
	require.Equal(t, uint64(1), stats["flush_total"])
}

func TestPerasSegmentInstallRetryDelayKeepsStaleEpochShort(t *testing.T) {
	stale := nokverrors.New(nokverrors.KindStaleEpoch, "stale")
	require.Equal(t, defaultPerasSegmentInstallStaleBackoff, perasSegmentInstallRetryDelay(stale, 0))
	require.Equal(t, defaultPerasSegmentInstallStaleMaxBackoff, perasSegmentInstallRetryDelay(stale, 20))

	unavailable := nokverrors.New(nokverrors.KindUnavailable, "down")
	require.Equal(t, defaultPerasSegmentInstallRetryBackoff, perasSegmentInstallRetryDelay(unavailable, 0))
	require.Equal(t, defaultPerasSegmentInstallMaxBackoff, perasSegmentInstallRetryDelay(unavailable, 20))
}

func TestRuntimeFlushRequiresInstaller(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.ErrorIs(t, committer.FlushDurable(ctx), ErrRuntimeInvalid)

	stats := committer.Stats()
	require.Equal(t, uint64(0), stats["flush_total"])
	require.Equal(t, uint64(0), stats["segment_total"])
	require.Equal(t, 1, stats["pending"])
	value, deleted, ok := committer.GetPerasOverlay(testRuntimeDentryKeyForLabel("a"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestRuntimeRecoversWitnessSegment(t *testing.T) {
	witnesses := testRuntimePerasWitnesses(t, 3)
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{EpochID: 1, HolderID: "holder-a"})
	require.NoError(t, err)
	delta := testRuntimePerasOp([]byte("dentry/recovered"), []byte("inode/recovered"))
	recoveredKey := delta.Effects[0].Key
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
	recoverer, err := NewRuntime(Config{
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
	require.Equal(t, uint64(1), recoverer.Stats()["segment_recovery_install_total"])
	require.Equal(t, uint64(0), recoverer.Stats()["segment_recovery_skip_total"])
	value, deleted, ok := recoverer.GetPerasOverlay(recoveredKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestRuntimeRecoveryPrefersInstalledCatalog(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "restored")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 10)
	require.NoError(t, err)
	witnesses := testRuntimePerasWitnesses(t, 3)
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{EpochID: 1, HolderID: "holder-a"})
	require.NoError(t, err)
	delta := testRuntimePerasOp(dentryKey, inodeKey)
	delta = testRuntimePerasOpWithScope(delta, compile.AuthorityScope{
		Mount:      delta.Delta.Authority.Mount,
		MountKeyID: delta.Delta.Authority.MountKeyID,
		Parents:    []fsmeta.InodeID{fsmeta.RootInode},
		Inodes:     []fsmeta.InodeID{10},
	})
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

	seal := testRuntimePerasRootSeal("grant-1", "holder-a", scope, time.Now())
	seal.SegmentRoot = segment.Root
	seal.SegmentPayloadDigest = digest
	scanner := &fakeRuntimePerasCatalogScanner{rows: testRuntimePerasCatalogRows(t, segment, 99)}
	installer := &fakeRuntimePerasSegmentInstaller{}
	provider.seals = []rootproto.PerasAuthoritySeal{seal}
	recoverer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		Installer:         installer,
		CatalogScanner:    scanner,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer recoverer.Close()

	require.NoError(t, recoverer.RecoverWitnessSegments(context.Background(), scope, holder.EpochID()))
	require.Equal(t, 0, installer.calls)
	require.Positive(t, scanner.calls)
	require.Equal(t, uint64(1), recoverer.Stats()["segment_total"])
	require.Equal(t, uint64(1), recoverer.Stats()["segment_catalog_load_total"])
	require.Equal(t, uint64(0), recoverer.Stats()["segment_recovery_install_total"])
	require.Equal(t, uint64(1), recoverer.Stats()["segment_recovery_skip_total"])
	value, deleted, ok := recoverer.GetPerasOverlay(dentryKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestRuntimeRecoversRootSealedSegmentFromWitnessWhenCatalogMissing(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "rooted")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 11)
	require.NoError(t, err)
	witnesses := testRuntimePerasWitnesses(t, 3)
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{EpochID: 1, HolderID: "holder-a"})
	require.NoError(t, err)
	delta := testRuntimePerasOp(dentryKey, inodeKey)
	delta = testRuntimePerasOpWithScope(delta, compile.AuthorityScope{
		Mount:      delta.Delta.Authority.Mount,
		MountKeyID: delta.Delta.Authority.MountKeyID,
		Parents:    []fsmeta.InodeID{fsmeta.RootInode},
		Inodes:     []fsmeta.InodeID{11},
	})
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

	seal := testRuntimePerasRootSeal("grant-1", "holder-a", scope, time.Now())
	seal.SegmentRoot = segment.Root
	seal.SegmentPayloadDigest = digest
	seal.OperationCount = segment.Stats().OperationCount
	seal.EntryCount = segment.Stats().EntryCount
	provider.seals = []rootproto.PerasAuthoritySeal{seal}
	installer := &fakeRuntimePerasSegmentInstaller{}
	recoverer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		Installer:         installer,
		CatalogScanner:    &fakeRuntimePerasCatalogScanner{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer recoverer.Close()

	require.NoError(t, recoverer.RecoverWitnessSegments(context.Background(), scope, holder.EpochID()))
	require.Equal(t, 1, installer.calls)
	require.Equal(t, segment.Root, installer.segment.Root)
	stats := recoverer.Stats()
	require.Equal(t, uint64(1), stats["root_sealed_segment_total"])
	require.Equal(t, uint64(1), stats["root_sealed_segment_missing_total"])
	require.Equal(t, uint64(1), stats["segment_recovery_install_total"])
	value, deleted, ok := recoverer.GetPerasOverlay(dentryKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestRuntimeLoadsInstalledSegmentCatalog(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "restored")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 10)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID: fsperas.OperationID{ClientID: "client", Seq: 1},
			Kind: fsmeta.OperationCreate,
			Mutations: []fsperas.ReplayMutation{
				{Key: dentryKey, Value: []byte("dentry-value")},
				{Key: inodeKey, Value: []byte("inode-value")},
			},
		}},
	})
	require.NoError(t, err)

	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	scanner := &fakeRuntimePerasCatalogScanner{rows: testRuntimePerasCatalogRows(t, segment, 99)}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		CatalogScanner:    scanner,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	scope := compile.AuthorityScope{
		Mount:      mount.MountID,
		MountKeyID: mount.MountKeyID,
	}
	require.NoError(t, committer.LoadInstalledSegments(context.Background(), scope))
	value, deleted, ok := committer.GetPerasOverlay(dentryKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
	require.Equal(t, uint64(1), committer.Stats()["segment_total"])
	require.Equal(t, 2, committer.Stats()["segment_keys"])
	require.Equal(t, uint64(1), committer.Stats()["segment_catalog_load_total"])
	completion, ok := committer.Completion(fsperas.OperationID{ClientID: "client", Seq: 1})
	require.True(t, ok)
	require.Equal(t, fsmeta.OperationCreate, completion.Kind)

	require.NoError(t, committer.LoadInstalledSegments(context.Background(), scope))
	require.Equal(t, uint64(1), committer.Stats()["segment_total"])
	require.Equal(t, uint64(1), committer.Stats()["segment_catalog_load_total"])
}

func TestRuntimeLoadsRootSealedSegments(t *testing.T) {
	segment := testRuntimePerasRootSegment(t)
	scope := compile.AuthorityScope{Mount: "vol", MountKeyID: 7}
	seal := testRuntimePerasRootSeal("grant-1", "holder-a", scope, time.Now())
	seal.SegmentRoot = segment.Root
	provider := &fakeRuntimePerasGrantProvider{
		holderID: "holder-a",
		grant:    testRuntimeCommitterGrant(),
		seals:    []rootproto.PerasAuthoritySeal{seal},
	}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		CatalogScanner:    &fakeRuntimePerasCatalogScanner{rows: testRuntimePerasCatalogRows(t, segment, 99)},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	require.NoError(t, committer.LoadRootSealedSegments(context.Background(), scope))
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["segment_catalog_load_total"])
	require.Equal(t, uint64(1), stats["root_sealed_segment_total"])
	require.Equal(t, uint64(0), stats["root_sealed_segment_missing_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
}

func TestRuntimeLoadRootSealedSegmentsSkipsCatalogWithoutRootSeal(t *testing.T) {
	scope := compile.AuthorityScope{Mount: "vol", MountKeyID: 7}
	provider := &fakeRuntimePerasGrantProvider{
		holderID: "holder-a",
		grant:    testRuntimeCommitterGrant(),
	}
	scanner := &fakeRuntimePerasCatalogScanner{rows: testRuntimePerasCatalogRows(t, testRuntimePerasRootSegment(t), 99)}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		CatalogScanner:    scanner,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	require.NoError(t, committer.LoadRootSealedSegments(context.Background(), scope))
	require.Zero(t, scanner.calls)
	stats := committer.Stats()
	require.Equal(t, uint64(0), stats["segment_catalog_load_total"])
	require.Equal(t, uint64(0), stats["root_sealed_segment_total"])
}

func TestRuntimeRejectsMissingRootSealedSegmentCatalog(t *testing.T) {
	segment := testRuntimePerasRootSegment(t)
	scope := compile.AuthorityScope{Mount: "vol", MountKeyID: 7}
	seal := testRuntimePerasRootSeal("grant-1", "holder-a", scope, time.Now())
	seal.SegmentRoot = segment.Root
	provider := &fakeRuntimePerasGrantProvider{
		holderID: "holder-a",
		grant:    testRuntimeCommitterGrant(),
		seals:    []rootproto.PerasAuthoritySeal{seal},
	}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		CatalogScanner:    &fakeRuntimePerasCatalogScanner{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	err = committer.LoadRootSealedSegments(context.Background(), scope)
	require.ErrorIs(t, err, fsperas.ErrInvalidPerasSegment)
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["root_sealed_segment_total"])
	require.Equal(t, uint64(1), stats["root_sealed_segment_missing_total"])
}

func TestRuntimeRecoversPredecessorBeforeOpeningNewEpoch(t *testing.T) {
	witnesses := testRuntimePerasWitnesses(t, 3)
	predecessorProvider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	predecessor, err := NewRuntime(Config{
		Authority:         predecessorProvider,
		Witnesses:         witnesses,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer predecessor.Close()

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{EpochID: 1, HolderID: "holder-a"})
	require.NoError(t, err)
	recoveredDelta := testRuntimePerasOp([]byte("dentry/recovered"), []byte("inode/recovered"))
	recoveredDelta = testRuntimePerasOpWithScope(recoveredDelta, compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{1},
		Inodes:     []fsmeta.InodeID{2},
	})
	recoveredKey := recoveredDelta.Effects[0].Key
	_, err = holder.Submit(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, recoveredDelta)
	require.NoError(t, err)
	plan, scope, err := holder.BuildPendingReplayPlan(10)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	require.NoError(t, predecessor.appendSegmentWitnesses(context.Background(), scope, holder, segment, payload, digest))

	nextGrant := testRuntimeCommitterGrant()
	nextGrant.GrantID = "grant-2"
	nextGrant.EpochID = 2
	nextGrant.PredecessorDigest = segment.Root
	installer := &fakeRuntimePerasSegmentInstaller{}
	recoverer, err := NewRuntime(Config{
		Authority:         &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: nextGrant},
		Witnesses:         witnesses,
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer recoverer.Close()

	require.NoError(t, commitRuntimePeras(context.Background(), recoverer, 2, []byte("dentry/new"), []byte("inode/new")))
	require.Equal(t, 1, installer.calls)
	require.Equal(t, segment.Root, installer.segment.Root)

	value, deleted, ok := recoverer.GetPerasOverlay(recoveredKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
	value, deleted, ok = recoverer.GetPerasOverlay(testRuntimeDentryKeyForLabel("new"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestPerasSegmentWithinScopeRejectsDifferentBucket(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	leftA, leftB := testRuntimeBucketKeys(t, mount, 1)
	rightA, _ := testRuntimeBucketKeys(t, mount, 2)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID: fsperas.OperationID{ClientID: "client", Seq: 1},
			Kind: fsmeta.OperationUpdateInode,
			Mutations: []fsperas.ReplayMutation{
				{Key: leftA, Value: []byte("a")},
				{Key: leftB, Value: []byte("b")},
			},
		}},
	})
	require.NoError(t, err)

	require.True(t, SegmentWithinScope(segment, compile.AuthorityScope{
		Mount:      mount.MountID,
		MountKeyID: mount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	}))
	require.False(t, SegmentWithinScope(segment, compile.AuthorityScope{
		Mount:      mount.MountID,
		MountKeyID: mount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{2},
	}))
	require.NotEqual(t, leftA, rightA)
}

func TestRuntimeFlushAuthorityFlushesOnlyOverlappingPendingOps(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	requestedScopeA := compile.AuthorityScope{
		Mount:           "vol",
		MountKeyID:      1,
		Parents:         []fsmeta.InodeID{1},
		Inodes:          []fsmeta.InodeID{2},
		AllowOpaqueKeys: true,
	}
	requestedScopeB := compile.AuthorityScope{
		Mount:           "vol",
		MountKeyID:      1,
		Parents:         []fsmeta.InodeID{2},
		Inodes:          []fsmeta.InodeID{3},
		AllowOpaqueKeys: true,
	}
	deltaA := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	deltaA = testRuntimePerasOpWithScope(deltaA, requestedScopeA)
	scopeA := deltaA.Delta.Authority
	deltaB := testRuntimePerasOp([]byte("dentry/b"), []byte("inode/b"))
	deltaB = testRuntimePerasOpWithScope(deltaB, requestedScopeB)

	ctx := context.Background()
	_, err = committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: 1}, deltaA, nil)
	require.NoError(t, err)
	_, err = committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: 2}, deltaB, nil)
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

	value, deleted, ok := committer.GetPerasOverlay(deltaA.Effects[0].Key)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
	value, deleted, ok = committer.GetPerasOverlay(deltaB.Effects[0].Key)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)

	require.NoError(t, committer.FlushDurable(ctx))
	stats = committer.Stats()
	require.Equal(t, uint64(2), stats["flush_total"])
	require.Equal(t, uint64(2), stats["segment_total"])
	require.Equal(t, uint64(2), stats["segment_operations_total"])
	require.Equal(t, 0, stats["pending"])
	require.Equal(t, 0, stats["overlay_keys"])
	require.Equal(t, 2, installer.calls)
}

func TestRuntimeDrainAuthorityFlushesAndRetires(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	requestedScope := compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{1},
		Inodes:     []fsmeta.InodeID{testRuntimeInodeForBucketValue(fsmeta.BucketForInodeID(1))},
	}
	delta := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	delta = testRuntimePerasOpWithScope(delta, requestedScope)
	scope := delta.Delta.Authority
	requestedOtherScope := compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{9},
		Inodes:     []fsmeta.InodeID{10},
	}
	otherDelta := testRuntimePerasOp([]byte("dentry/b"), []byte("inode/b"))
	otherDelta = testRuntimePerasOpWithScope(otherDelta, requestedOtherScope)

	ctx := context.Background()
	_, err = committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: 1}, delta, nil)
	require.NoError(t, err)
	_, err = committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: 2}, otherDelta, nil)
	require.NoError(t, err)
	retirer := &fakeRuntimePerasRetirer{}
	require.NoError(t, committer.DrainAuthority(ctx, retirer, scope))

	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
	require.Equal(t, uint64(1), stats["segment_operations_total"])
	require.Equal(t, 1, stats["pending"])
	require.Equal(t, 1, installer.calls)
	require.True(t, installer.materialize)
	require.Equal(t, 1, retirer.calls)
	require.Equal(t, []compile.AuthorityScope{scope}, retirer.scopes)
}

func TestRuntimeDrainAuthorityUsesMaterializeBudget(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:                 provider,
		Witnesses:                 testRuntimePerasWitnesses(t, 3),
		Installer:                 installer,
		SegmentBatchSize:          1024,
		SegmentMaxReplayMutations: defaultPerasSegmentMaxReplayMutations,
		SegmentFlushEvery:         time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	for i := range 11 {
		seq := uint64(i + 1)
		require.NoError(t, commitRuntimePeras(ctx, committer, seq, appendUvarintKey("dentry/", seq), appendUvarintKey("inode/", seq)))
	}
	require.NoError(t, committer.DrainAuthority(ctx, &fakeRuntimePerasRetirer{}))

	require.Equal(t, 2, installer.calls)
	require.Equal(t, []bool{true, true}, installer.modes)
	require.Equal(t, uint64(2), committer.Stats()["segment_total"])
	require.Equal(t, uint64(11), committer.Stats()["segment_operations_total"])
}

func TestRuntimeBackgroundFlushTimesOutAndBacksOff(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &blockingRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
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

func TestRuntimeCloseCancelsInstallLane(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &blockingRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:                  provider,
		Witnesses:                  testRuntimePerasWitnesses(t, 3),
		Installer:                  installer,
		SegmentBatchSize:           1024,
		SegmentInstallParallelism:  1,
		SegmentFlushEvery:          time.Hour,
		BackgroundFlushTimeout:     time.Hour,
		BackgroundErrorBackoff:     time.Hour,
		SegmentMaxReplayMutations:  defaultPerasSegmentMaxReplayMutations,
		SegmentWitnessRetryBackoff: time.Millisecond,
	})
	require.NoError(t, err)

	require.NoError(t, commitRuntimePeras(context.Background(), committer, 1, []byte("dentry/a"), []byte("inode/a")))
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- committer.FlushDurable(context.Background())
	}()
	require.Eventually(t, func() bool {
		return installer.calls.Load() == 1
	}, time.Second, 10*time.Millisecond)
	stats := committer.Stats()
	require.Equal(t, 1, stats["segment_install_parallelism"])
	require.Equal(t, 4, stats["segment_install_queue_capacity"])

	closeDone := make(chan struct{})
	go func() {
		committer.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("committer close did not cancel blocked segment install")
	}
	require.Error(t, <-flushDone)
}

func TestRuntimeFlushChainsBoundedReplayWindows(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	for seq := uint64(1); seq <= 5; seq++ {
		require.NoError(t, commitRuntimePeras(context.Background(), committer, seq, appendUvarintKey("dentry/", seq), appendUvarintKey("inode/", seq)))
	}

	for committer.Stats()["pending"].(int) > 0 {
		committer.commitMu.Lock()
		batches, err := committer.freezeFlushBatchesLocked(nil, false, 2)
		committer.commitMu.Unlock()
		require.NoError(t, err)
		require.NotEmpty(t, batches)
		require.NoError(t, (flushPipeline{runtime: committer, level: fsperas.SegmentPersistencePublished}).run(context.Background(), batches))
	}
	require.Equal(t, uint64(3), committer.Stats()["segment_total"])
	require.Equal(t, uint64(5), committer.Stats()["segment_operations_total"])
}

func TestRuntimeFlushAllowsConcurrentCommitsDuringInstall(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &blockingRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
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
		flushDone <- committer.FlushDurable(ctx)
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

func TestRuntimeDrainAuthorityBlocksConcurrentCommitsUntilInstallFinishes(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &blockingRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
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

func TestRuntimeDrainAuthorityAllowsDisjointCommitsDuringInstall(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &blockingRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
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
	left := testRuntimePerasOpForBucket(leftA, leftB, 1)
	right := testRuntimePerasOpForBucket(rightA, rightB, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = committer.SubmitVisible(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, left, nil)
	require.NoError(t, err)

	drainDone := make(chan error, 1)
	go func() {
		drainDone <- committer.DrainAuthority(ctx, &fakeRuntimePerasRetirer{}, left.Delta.Authority)
	}()
	require.Eventually(t, func() bool {
		return installer.calls.Load() == 1
	}, time.Second, 10*time.Millisecond)

	disjointDone := make(chan error, 1)
	go func() {
		_, err := committer.SubmitVisible(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 2}, right, nil)
		disjointDone <- err
	}()
	select {
	case err := <-disjointDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("disjoint commit blocked behind scoped authority drain")
	}

	cancel()
	require.ErrorIs(t, <-drainDone, context.Canceled)
}

func TestRuntimeRollsBackHolderOnOverlayAdmissionFailure(t *testing.T) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimePerasWitnesses(t, 3),
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	delta := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	committer.read = nil
	_, err = committer.SubmitVisible(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta, nil)
	require.Error(t, err)
	require.Equal(t, 0, committer.Stats()["pending"])
}

func BenchmarkRuntimeCreate(b *testing.B) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
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
		_, err := committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "bench", Seq: uint64(i + 1)}, testRuntimePerasOp(dentryKey, inodeKey), nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRuntimeCreateParallel(b *testing.B) {
	provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
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
			_, err := committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "bench", Seq: current}, testRuntimePerasOp(dentryKey, inodeKey), nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRuntimeScanPerasOverlay(b *testing.B) {
	committer := &Runtime{
		read: newReadState(),
	}
	for i := range 100_000 {
		key, err := fsmeta.EncodeDentryKey(testRuntimeMount, fsmeta.RootInode, fmt.Sprintf("%08d", i))
		require.NoError(b, err)
		require.NoError(b, committer.read.sealed.AddSegment(testRuntimePerasSegmentForOverlay(key, []byte("sealed"))))
	}
	for i := range 1024 {
		name := fmt.Sprintf("%08d", i*16)
		op := testRuntimeCreateOp(testRuntimeMount, fsmeta.RootInode, name, fsmeta.InodeID(200_000+i), []byte("overlay"), []byte("inode-value"))
		require.NoError(b, committer.read.overlay.Add(fsperas.OperationID{ClientID: "bench", Seq: uint64(i + 1)}, op))
	}
	start, err := fsmeta.EncodeDentryKey(testRuntimeMount, fsmeta.RootInode, "00000000")
	require.NoError(b, err)
	require.Len(b, committer.ScanPerasOverlay(start, 128), 128)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out := committer.ScanPerasOverlay(start, 128)
		if len(out) != 128 {
			b.Fatalf("scan returned %d rows", len(out))
		}
	}
}

func BenchmarkRuntimeFlushInstallParallelism(b *testing.B) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	keys := make([][2][]byte, 0, 16)
	for bucket := fsmeta.AffinityBucket(0); len(keys) < cap(keys); bucket++ {
		first, second := testRuntimeBucketKeys(b, mount, bucket)
		keys = append(keys, [2][]byte{first, second})
	}
	for _, parallelism := range []int{1, 4} {
		b.Run(fmt.Sprintf("parallel_%d", parallelism), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				provider := &fakeRuntimePerasGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
				installer := &delayingRuntimePerasSegmentInstaller{delay: time.Millisecond}
				committer, err := NewRuntime(Config{
					Authority:                 provider,
					Witnesses:                 testRuntimePerasWitnesses(b, 3),
					Installer:                 installer,
					SegmentBatchSize:          1 << 30,
					SegmentMaxReplayMutations: 2,
					SegmentInstallParallelism: parallelism,
					SegmentFlushEvery:         time.Hour,
				})
				require.NoError(b, err)
				for idx, pair := range keys {
					require.NoError(b, commitRuntimePeras(context.Background(), committer, uint64(idx+1), pair[0], pair[1]))
				}
				require.NoError(b, committer.FlushDurable(context.Background()))
				committer.Close()
				if got := installer.calls.Load(); got != int32(len(keys)) {
					b.Fatalf("installed %d segments, want %d", got, len(keys))
				}
			}
		})
	}
}

func commitRuntimePeras(ctx context.Context, committer *Runtime, seq uint64, dentryKey, inodeKey []byte) error {
	_, err := committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: seq}, testRuntimePerasOp(dentryKey, inodeKey), nil)
	return err
}

func testRuntimePerasSegmentForOverlay(key, value []byte) fsperas.PerasSegment {
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID: fsperas.OperationID{ClientID: "overlay", Seq: 1},
			Kind: fsmeta.OperationUpdateInode,
			Mutations: []fsperas.ReplayMutation{
				{Key: key, Value: value},
			},
		}},
	})
	if err != nil {
		panic(err)
	}
	return segment
}

func testRuntimePerasCatalogRows(tb testing.TB, segment fsperas.PerasSegment, installVersion uint64) []KV {
	tb.Helper()
	catalogKeys, err := fsperas.PerasSegmentCatalogIndexKeys(segment)
	require.NoError(tb, err)
	objectKey, err := fsperas.PerasSegmentObjectKey(segment)
	require.NoError(tb, err)
	objectValue, err := fsperas.EncodePerasSegmentCatalogRecord(segment, installVersion)
	require.NoError(tb, err)
	objectRecord, err := fsperas.DecodePerasSegmentCatalogRecord(objectValue)
	require.NoError(tb, err)
	indexValue, err := fsperas.EncodePerasSegmentCatalogIndexRecord(objectRecord, objectKey)
	require.NoError(tb, err)
	rows := make([]KV, 0, len(catalogKeys)+1)
	for _, key := range catalogKeys {
		rows = append(rows, KV{Key: key, Value: indexValue})
	}
	return append(rows, KV{Key: objectKey, Value: objectValue})
}

type fakeRuntimePerasSegmentInstaller struct {
	mu          sync.Mutex
	calls       int
	scope       compile.AuthorityScope
	segment     fsperas.PerasSegment
	payload     []byte
	digest      [32]byte
	materialize bool
	modes       []bool
}

func (i *fakeRuntimePerasSegmentInstaller) InstallSegment(_ context.Context, req SegmentInstallRequest) (InstallCursor, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.calls++
	i.scope = req.Scope
	i.segment = req.Segment
	i.payload = append([]byte(nil), req.Payload...)
	i.digest = req.PayloadDigest
	i.materialize = req.MaterializeMVCC
	i.modes = append(i.modes, req.MaterializeMVCC)
	return testPerasInstallCursor(uint64(i.calls)), nil
}

type fakeRuntimePerasCatalogScanner struct {
	rows  []KV
	calls int
}

func (s *fakeRuntimePerasCatalogScanner) Scan(_ context.Context, startKey []byte, limit uint32, _ uint64) ([]KV, error) {
	s.calls++
	rows := append([]KV(nil), s.rows...)
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(rows[i].Key, rows[j].Key) < 0
	})
	out := make([]KV, 0, limit)
	for _, row := range rows {
		if bytes.Compare(row.Key, startKey) < 0 {
			continue
		}
		out = append(out, KV{
			Key:   append([]byte(nil), row.Key...),
			Value: append([]byte(nil), row.Value...),
		})
		if uint32(len(out)) >= limit {
			break
		}
	}
	return out, nil
}

type flakyRuntimePerasSegmentInstaller struct {
	mu       sync.Mutex
	calls    int
	failures int
}

func (i *flakyRuntimePerasSegmentInstaller) InstallSegment(context.Context, SegmentInstallRequest) (InstallCursor, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.calls++
	if i.calls <= i.failures {
		return InstallCursor{}, nokverrors.New(nokverrors.KindStaleEpoch, "stale install era")
	}
	return testPerasInstallCursor(uint64(i.calls)), nil
}

type blockingRuntimePerasSegmentInstaller struct {
	calls atomic.Int32
}

func (i *blockingRuntimePerasSegmentInstaller) InstallSegment(ctx context.Context, _ SegmentInstallRequest) (InstallCursor, error) {
	i.calls.Add(1)
	<-ctx.Done()
	return InstallCursor{}, ctx.Err()
}

type delayingRuntimePerasSegmentInstaller struct {
	calls atomic.Int32
	delay time.Duration
}

func (i *delayingRuntimePerasSegmentInstaller) InstallSegment(ctx context.Context, _ SegmentInstallRequest) (InstallCursor, error) {
	i.calls.Add(1)
	timer := time.NewTimer(i.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return InstallCursor{}, ctx.Err()
	case <-timer.C:
		return testPerasInstallCursor(uint64(i.calls.Load())), nil
	}
}

type countingRuntimePerasWitness struct {
	id string
	mu sync.Mutex
	n  int
}

func (w *countingRuntimePerasWitness) ID() string {
	return w.id
}

func (w *countingRuntimePerasWitness) AppendSegment(context.Context, compile.AuthorityScope, fsperas.SegmentWitnessRecord) error {
	w.mu.Lock()
	w.n++
	w.mu.Unlock()
	return nil
}

func (w *countingRuntimePerasWitness) Probe(context.Context, uint64) (fsperas.WitnessSnapshot, error) {
	return fsperas.WitnessSnapshot{}, nil
}

func (w *countingRuntimePerasWitness) Count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

type witnessPhaseCheckingRuntimePerasSegmentInstaller struct {
	witness *countingRuntimePerasWitness
	want    int
	calls   atomic.Int32
}

func (i *witnessPhaseCheckingRuntimePerasSegmentInstaller) InstallSegment(context.Context, SegmentInstallRequest) (InstallCursor, error) {
	i.calls.Add(1)
	if got := i.witness.Count(); got < i.want {
		return InstallCursor{}, fmt.Errorf("install started after %d witnessed segments, want %d", got, i.want)
	}
	return testPerasInstallCursor(uint64(i.calls.Load())), nil
}

type fakeRuntimePerasGrantProvider struct {
	holderID string
	grant    AuthorityGrant
	seals    []rootproto.PerasAuthoritySeal
	owned    bool
	err      error
	sealErr  error
}

func (p *fakeRuntimePerasGrantProvider) HolderID() string {
	return p.holderID
}

func (p *fakeRuntimePerasGrantProvider) Acquire(context.Context, compile.AuthorityScope) (AuthorityGrant, bool, error) {
	owned := p.owned
	if !owned {
		owned = true
	}
	return p.grant, owned, p.err
}

func (p *fakeRuntimePerasGrantProvider) ListPerasAuthoritySeals(context.Context, compile.AuthorityScope) ([]rootproto.PerasAuthoritySeal, error) {
	if p.sealErr != nil {
		return nil, p.sealErr
	}
	out := make([]rootproto.PerasAuthoritySeal, len(p.seals))
	for i, seal := range p.seals {
		out[i] = rootproto.ClonePerasAuthoritySeal(seal)
	}
	return out, nil
}

type publishingRuntimePerasGrantProvider struct {
	fakeRuntimePerasGrantProvider
	mu            sync.Mutex
	sealCalls     int
	sealedGrant   AuthorityGrant
	sealedSegment fsperas.PerasSegment
	sealedDigest  [32]byte
	sealedCursor  InstallCursor
	sealErr       error
}

func (p *publishingRuntimePerasGrantProvider) PublishSegmentSeal(_ context.Context, grant AuthorityGrant, segment fsperas.PerasSegment, digest [32]byte, cursor InstallCursor) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sealCalls++
	p.sealedGrant = grant
	p.sealedSegment = segment
	p.sealedDigest = digest
	p.sealedCursor = cursor
	return p.sealErr
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
	tb.Helper()
	witnesses := make([]fsperas.WitnessReplica, 0, n)
	for i := range n {
		witnesses = append(witnesses, &recordingRuntimePerasWitness{id: fmt.Sprintf("witness-%d", i)})
	}
	return witnesses
}

type recordingRuntimePerasWitness struct {
	id      string
	mu      sync.Mutex
	records []fsperas.SegmentWitnessRecord
}

func (w *recordingRuntimePerasWitness) ID() string { return w.id }

func (w *recordingRuntimePerasWitness) AppendSegment(_ context.Context, _ compile.AuthorityScope, record fsperas.SegmentWitnessRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.records = append(w.records, record)
	return nil
}

func (w *recordingRuntimePerasWitness) Probe(_ context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := fsperas.WitnessSnapshot{}
	for _, record := range w.records {
		if record.EpochID == epochID {
			out.Segments = append(out.Segments, record)
		}
	}
	return out, nil
}

func testRuntimeCommitterGrant() AuthorityGrant {
	return AuthorityGrant{
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

func testRuntimePerasRootSeal(id, holder string, scope compile.AuthorityScope, sealed time.Time) rootproto.PerasAuthoritySeal {
	return rootproto.PerasAuthoritySeal{
		GrantID:              id,
		EpochID:              1,
		HolderID:             holder,
		Scope:                AuthorityScopeFromDelta(scope),
		SegmentRoot:          [32]byte{1},
		SegmentPayloadDigest: [32]byte{2},
		OperationCount:       3,
		EntryCount:           4,
		SealedUnixNano:       sealed.UnixNano(),
		InstallRegionID:      5,
		InstallTerm:          6,
		InstallIndex:         7,
		InstallVersion:       8,
	}
}

func testRuntimePerasRootSegment(t *testing.T) fsperas.PerasSegment {
	t.Helper()
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 7}
	dentry, err := fsmeta.EncodeDentryKey(mount, 99, "a")
	require.NoError(t, err)
	inode, err := fsmeta.EncodeInodeKey(mount, 100)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{
			{
				OpID: fsperas.OperationID{ClientID: "client", Seq: 1},
				Mutations: []fsperas.ReplayMutation{
					{Key: dentry, Value: []byte("dentry-value")},
					{Key: inode, Value: []byte("inode-value")},
				},
			},
		},
	})
	require.NoError(t, err)
	return segment
}

func testPerasInstallCursor(offset uint64) InstallCursor {
	if offset == 0 {
		offset = 1
	}
	return InstallCursor{
		RegionID:       10 + offset,
		Term:           20 + offset,
		Index:          30 + offset,
		InstallVersion: 40 + offset,
	}
}

var testRuntimeMount = fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}

func testRuntimePerasOp(dentryKey, inodeKey []byte) compile.MaterializedOp {
	mount, parent, name, inode := testRuntimeCreateArgs(dentryKey, inodeKey)
	return testRuntimeCreateOp(mount, parent, name, inode, []byte("dentry-value"), []byte("inode-value"))
}

func testRuntimeCreateOp(mount fsmeta.MountIdentity, parent fsmeta.InodeID, name string, inode fsmeta.InodeID, dentryValue, inodeValue []byte) compile.MaterializedOp {
	program, err := compile.CompileCreateProgram(fsmeta.CreateRequest{
		Mount:  mount.MountID,
		Parent: parent,
		Name:   name,
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	}, mount, inode)
	if err != nil {
		panic(err)
	}
	if dentryValue == nil {
		dentryValue = program.Compiled.Delta.WriteEffects[0].Value
	}
	if _, err := fsmeta.DecodeInodeValue(inodeValue); inodeValue == nil || err != nil {
		inodeValue = program.Compiled.Delta.WriteEffects[1].Value
	}
	op, err := compile.MaterializeCreate(program, compile.CreateValues{
		DentryValue: dentryValue,
		InodeValue:  inodeValue,
	})
	if err != nil {
		panic(err)
	}
	return op
}

func testRuntimeCreateArgs(dentryKey, inodeKey []byte) (fsmeta.MountIdentity, fsmeta.InodeID, string, fsmeta.InodeID) {
	mount := testRuntimeMount
	parent := fsmeta.RootInode
	name := testRuntimeNameFromKey(dentryKey)
	inode := fsmeta.InodeID(0)
	inodeFromStorageKey := false
	if parts, ok := fsmeta.InspectKey(dentryKey); ok {
		mount.MountKeyID = parts.MountKeyID
		switch parts.Kind {
		case fsmeta.KeyKindDentry:
			parent = parts.Parent
			if dentryName, ok := fsmeta.DentryNameOfKey(dentryKey); ok {
				name = dentryName
			}
		case fsmeta.KeyKindInode, fsmeta.KeyKindChunk, fsmeta.KeyKindSession:
			parent = parts.Inode
		}
	}
	if parts, ok := fsmeta.InspectKey(inodeKey); ok {
		if mount.MountKeyID == 0 {
			mount.MountKeyID = parts.MountKeyID
		}
		switch parts.Kind {
		case fsmeta.KeyKindInode, fsmeta.KeyKindChunk, fsmeta.KeyKindSession:
			inode = parts.Inode
			inodeFromStorageKey = true
		}
	}
	if !inodeFromStorageKey {
		inode = testRuntimeInodeForBucketSeed(fsmeta.BucketForInodeID(parent), inodeKey)
	}
	if inode == 0 || inode == parent {
		inode = testRuntimeInodeForBucketSeed(fsmeta.BucketForInodeID(parent), append(append([]byte(nil), inodeKey...), '#'))
	}
	return mount, parent, name, inode
}

func testRuntimeNameFromKey(key []byte) string {
	if name, ok := fsmeta.DentryNameOfKey(key); ok {
		return name
	}
	label := string(key)
	if slash := strings.LastIndex(label, "/"); slash >= 0 {
		label = label[slash+1:]
	}
	if label == "" || label == "." || label == ".." || strings.ContainsAny(label, "/\x00") {
		return "k-" + hex.EncodeToString(key)
	}
	return label
}

func testRuntimeInodeFromKey(key []byte) fsmeta.InodeID {
	var hash uint64 = 1469598103934665603
	for _, b := range key {
		hash ^= uint64(b)
		hash *= 1099511628211
	}
	return fsmeta.InodeID(2 + hash%1_000_000)
}

func testRuntimeDentryKeyForLabel(label string) []byte {
	return testRuntimePerasOp([]byte("dentry/"+label), []byte("inode/"+label)).Effects[0].Key
}

func testRuntimeRootDentryPrefix() []byte {
	prefix, err := fsmeta.EncodeDentryPrefix(testRuntimeMount, fsmeta.RootInode)
	if err != nil {
		panic(err)
	}
	return prefix
}

func testRuntimeRenameDentryOp(fromName, toName string, toValue []byte) compile.MaterializedOp {
	program, err := compile.CompileRenameProgram(fsmeta.RenameRequest{
		Mount:      testRuntimeMount.MountID,
		FromParent: fsmeta.RootInode,
		FromName:   fromName,
		ToParent:   fsmeta.RootInode,
		ToName:     toName,
	}, testRuntimeMount)
	if err != nil {
		panic(err)
	}
	fromKey, err := fsmeta.EncodeDentryKey(testRuntimeMount, fsmeta.RootInode, fromName)
	if err != nil {
		panic(err)
	}
	toKey, err := fsmeta.EncodeDentryKey(testRuntimeMount, fsmeta.RootInode, toName)
	if err != nil {
		panic(err)
	}
	op, err := compile.MaterializeCompiledOpWithEvidence(program.Compiled, []compile.WriteEffect{
		{Kind: compile.EffectDelete, Key: fromKey},
		{Kind: compile.EffectPut, Key: toKey, Value: toValue},
	}, compile.PredicateEvidence{}, nil)
	if err != nil {
		panic(err)
	}
	return op
}

func testRuntimePerasOpForBucket(dentryKey, inodeKey []byte, bucket fsmeta.AffinityBucket) compile.MaterializedOp {
	parent := testRuntimeInodeForBucketValue(bucket)
	inode := parent + fsmeta.InodeID(fsmeta.DefaultAffinityBucketCount)
	name := testRuntimeNameFromKey(dentryKey)
	return testRuntimeCreateOp(testRuntimeMount, parent, name, inode, []byte("dentry-value"), []byte("inode-value"))
}

func testRuntimeInodeForBucketValue(bucket fsmeta.AffinityBucket) fsmeta.InodeID {
	for inode := fsmeta.InodeID(2); inode < 100_000; inode++ {
		if fsmeta.BucketForInodeID(inode) == bucket {
			return inode
		}
	}
	panic(fmt.Sprintf("no inode found for bucket %d", bucket))
}

func testRuntimeInodeForBucketSeed(bucket fsmeta.AffinityBucket, seed []byte) fsmeta.InodeID {
	start := uint64(testRuntimeInodeFromKey(seed))
	for offset := uint64(0); offset < 1_000_000; offset++ {
		inode := fsmeta.InodeID(2 + (start+offset)%1_000_000)
		if fsmeta.BucketForInodeID(inode) == bucket {
			return inode
		}
	}
	panic(fmt.Sprintf("no inode found for bucket %d", bucket))
}

func uniqueRuntimeBuckets(in []fsmeta.AffinityBucket) []fsmeta.AffinityBucket {
	if len(in) == 0 {
		return nil
	}
	out := make([]fsmeta.AffinityBucket, 0, len(in))
	seen := make(map[fsmeta.AffinityBucket]struct{}, len(in))
	for _, bucket := range in {
		if _, ok := seen[bucket]; ok {
			continue
		}
		seen[bucket] = struct{}{}
		out = append(out, bucket)
	}
	return out
}

func testRuntimePerasOpWithScope(op compile.MaterializedOp, scope compile.AuthorityScope) compile.MaterializedOp {
	mount, parent, name, inode := testRuntimeCreateArgs(op.Effects[0].Key, op.Effects[1].Key)
	if scope.Mount != "" {
		mount.MountID = scope.Mount
	}
	if scope.MountKeyID != 0 {
		mount.MountKeyID = scope.MountKeyID
	}
	if len(scope.Parents) > 0 {
		parent = scope.Parents[0]
	}
	if len(scope.Inodes) > 0 {
		inode = scope.Inodes[0]
	}
	if name == "" {
		name = "entry"
	}
	return testRuntimeCreateOp(mount, parent, name, inode, []byte("dentry-value"), []byte("inode-value"))
}

func appendUvarintKey(prefix string, v uint64) []byte {
	out := append([]byte(prefix), 0)
	return binary.AppendUvarint(out, v)
}

func testRuntimeBucketKeys(tb testing.TB, mount fsmeta.MountIdentity, bucket fsmeta.AffinityBucket) ([]byte, []byte) {
	tb.Helper()
	var first, second []byte
	for inode := fsmeta.InodeID(2); inode < 100_000; inode++ {
		if fsmeta.BucketForInodeID(inode) != bucket {
			continue
		}
		key, err := fsmeta.EncodeInodeKey(mount, inode)
		require.NoError(tb, err)
		if first == nil {
			first = key
			continue
		}
		second = key
		break
	}
	require.NotNil(tb, first)
	require.NotNil(tb, second)
	return first, second
}

func testRuntimeInodeForBucket(tb testing.TB, bucket fsmeta.AffinityBucket) fsmeta.InodeID {
	tb.Helper()
	for inode := fsmeta.InodeID(2); inode < 100_000; inode++ {
		if fsmeta.BucketForInodeID(inode) == bucket {
			return inode
		}
	}
	tb.Fatalf("no inode found for bucket %d", bucket)
	return 0
}
