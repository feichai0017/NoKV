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
	_, err = committer.CommitPeras(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta)
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
	_, err = committer.CommitPeras(ctx, fsperas.OperationID{ClientID: "client", Seq: 1}, deltaA)
	require.NoError(t, err)
	_, err = committer.CommitPeras(ctx, fsperas.OperationID{ClientID: "client", Seq: 2}, deltaB)
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
	_, err = committer.CommitPeras(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta)
	require.Error(t, err)
	require.Equal(t, 0, committer.Stats()["pending"])
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
		_, err := committer.CommitPeras(ctx, fsperas.OperationID{ClientID: "bench", Seq: uint64(i + 1)}, testRuntimePerasDelta(dentryKey, inodeKey))
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
			_, err := committer.CommitPeras(ctx, fsperas.OperationID{ClientID: "bench", Seq: current}, testRuntimePerasDelta(dentryKey, inodeKey))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func commitRuntimePeras(ctx context.Context, committer *RemotePerasCommitter, seq uint64, dentryKey, inodeKey []byte) error {
	_, err := committer.CommitPeras(ctx, fsperas.OperationID{ClientID: "client", Seq: seq}, testRuntimePerasDelta(dentryKey, inodeKey))
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
