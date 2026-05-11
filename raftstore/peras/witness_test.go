package peras

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/feichai0017/NoKV/fsmeta/runtime/perasauth"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
)

var _ fsperas.WitnessReplica = (*WitnessNode)(nil)

func TestWitnessNodeAppendSegmentAndProbe(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()
	record := testSegmentRecord()

	require.NoError(t, node.AppendSegment(context.Background(), scope, record))

	snapshot, err := node.Probe(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []fsperas.SegmentWitnessRecord{record}, snapshot.Segments)
}

func TestWitnessNodeRejectsMissingAuthority(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	require.NoError(t, node.authorities.Replace(nil))

	err := node.AppendSegment(context.Background(), testAuthorityScope(), testSegmentRecord())
	require.ErrorIs(t, err, ErrWitnessAuthorityMissing)
}

func TestWitnessNodeRefreshesAuthorityBeforeRejecting(t *testing.T) {
	now := time.Unix(100, 0)
	manager, err := wal.Open(wal.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { require.NoError(t, manager.Close()) }()
	log, err := fsperas.NewWALWitnessLog(manager, wal.DurabilityFsync)
	require.NoError(t, err)
	authorities := perasauth.NewActiveAuthorities()
	refreshed := false
	node, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:      "store-1",
		Log:         log,
		Authorities: authorities,
		AuthorityRefresh: func(context.Context) error {
			refreshed = true
			return authorities.Replace([]perasauth.AuthorityGrant{testAuthorityGrant()})
		},
		Now: func() time.Time { return now },
	})
	require.NoError(t, err)

	require.NoError(t, node.AppendSegment(context.Background(), testAuthorityScope(), testSegmentRecord()))
	require.True(t, refreshed)
}

func TestWitnessNodeRefreshFailureIsFatal(t *testing.T) {
	now := time.Unix(100, 0)
	manager, err := wal.Open(wal.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { require.NoError(t, manager.Close()) }()
	log, err := fsperas.NewWALWitnessLog(manager, wal.DurabilityFsync)
	require.NoError(t, err)
	authorities := perasauth.NewActiveAuthorities()
	refreshErr := errors.New("refresh failed")
	node, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:      "store-1",
		Log:         log,
		Authorities: authorities,
		AuthorityRefresh: func(context.Context) error {
			return refreshErr
		},
		Now: func() time.Time { return now },
	})
	require.NoError(t, err)

	err = node.AppendSegment(context.Background(), testAuthorityScope(), testSegmentRecord())
	require.ErrorIs(t, err, refreshErr)
}

func TestWitnessNodeRejectsWrongHolderAndEpoch(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()

	wrongHolder := testSegmentRecord()
	wrongHolder.HolderID = "holder-b"
	require.ErrorIs(t, node.AppendSegment(context.Background(), scope, wrongHolder), ErrWitnessAuthorityMismatch)

	wrongEpoch := testSegmentRecord()
	wrongEpoch.EpochID = 2
	require.ErrorIs(t, node.AppendSegment(context.Background(), scope, wrongEpoch), ErrWitnessAuthorityMismatch)
}

func TestWitnessNodeRejectsExpiredAuthority(t *testing.T) {
	now := time.Unix(100, 0)
	node, cleanup := openTestWitnessNodeWithNow(t, wal.DurabilityFsync, func() time.Time { return now })
	defer cleanup()
	expired := testAuthorityGrant()
	expired.ExpiresUnixNano = now.Add(-time.Nanosecond).UnixNano()
	require.NoError(t, node.authorities.Replace([]perasauth.AuthorityGrant{expired}))

	err := node.AppendSegment(context.Background(), testAuthorityScope(), testSegmentRecord())
	require.ErrorIs(t, err, ErrWitnessAuthorityMissing)
}

func TestWitnessNodeDuplicateSegmentIsIdempotent(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()
	record := testSegmentRecord()

	require.NoError(t, node.AppendSegment(context.Background(), scope, record))
	require.NoError(t, node.AppendSegment(context.Background(), scope, record))

	snapshot, err := node.Probe(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []fsperas.SegmentWitnessRecord{record}, snapshot.Segments)
}

func TestWitnessNodeLoadsSegmentsFromWAL(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(100, 0)
	manager, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	log, err := fsperas.NewWALWitnessLog(manager, wal.DurabilityFsync)
	require.NoError(t, err)
	authorities := testActiveAuthorities(t, now)
	node, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:      "store-1",
		Log:         log,
		Authorities: authorities,
		Now:         func() time.Time { return now },
	})
	require.NoError(t, err)
	record := testSegmentRecord()
	require.NoError(t, node.AppendSegment(context.Background(), testAuthorityScope(), record))
	require.NoError(t, manager.Close())

	reopened, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	reopenedLog, err := fsperas.NewWALWitnessLog(reopened, wal.DurabilityFsync)
	require.NoError(t, err)
	reopenedNode, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:      "store-1",
		Log:         reopenedLog,
		Authorities: authorities,
		Now:         func() time.Time { return now },
	})
	require.NoError(t, err)

	require.NoError(t, reopenedNode.AppendSegment(context.Background(), testAuthorityScope(), record))
	snapshot, err := reopenedNode.Probe(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []fsperas.SegmentWitnessRecord{record}, snapshot.Segments)
}

func TestWitnessNodeContextCancellationStopsAppend(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := node.AppendSegment(ctx, testAuthorityScope(), testSegmentRecord())
	require.True(t, errors.Is(err, context.Canceled))
}

func BenchmarkWitnessNodeAppendSegment(b *testing.B) {
	node, cleanup := openBenchWitnessNode(b)
	defer cleanup()
	scope := testAuthorityScope()
	record := testSegmentRecord()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		record.SegmentRoot[0]++
		if err := node.AppendSegment(context.Background(), scope, record); err != nil {
			b.Fatal(err)
		}
	}
}

func openTestWitnessNode(t *testing.T, durability wal.DurabilityPolicy) (*WitnessNode, func()) {
	t.Helper()
	now := time.Unix(100, 0)
	return openTestWitnessNodeWithNow(t, durability, func() time.Time { return now })
}

func openTestWitnessNodeWithNow(t *testing.T, durability wal.DurabilityPolicy, now func() time.Time) (*WitnessNode, func()) {
	t.Helper()
	manager, err := wal.Open(wal.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	log, err := fsperas.NewWALWitnessLog(manager, durability)
	require.NoError(t, err)
	node, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:      "store-1",
		Log:         log,
		Authorities: testActiveAuthorities(t, now()),
		Now:         now,
	})
	require.NoError(t, err)
	return node, func() { require.NoError(t, manager.Close()) }
}

func openBenchWitnessNode(b *testing.B) (*WitnessNode, func()) {
	b.Helper()
	now := time.Unix(100, 0)
	manager, err := wal.Open(wal.Config{Dir: b.TempDir()})
	require.NoError(b, err)
	log, err := fsperas.NewWALWitnessLog(manager, wal.DurabilityBuffered)
	require.NoError(b, err)
	node, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:      "store-1",
		Log:         log,
		Authorities: testActiveAuthorities(b, now),
		Now:         func() time.Time { return now },
	})
	require.NoError(b, err)
	return node, func() { require.NoError(b, manager.Close()) }
}

func testActiveAuthorities(tb testing.TB, now time.Time) *perasauth.ActiveAuthorities {
	tb.Helper()
	table := perasauth.NewActiveAuthorities()
	grant := testAuthorityGrant()
	grant.ExpiresUnixNano = now.Add(time.Hour).UnixNano()
	require.NoError(tb, table.Replace([]perasauth.AuthorityGrant{grant}))
	return table
}

func testAuthorityGrant() perasauth.AuthorityGrant {
	return rootproto.PerasAuthorityGrant{
		GrantID:         "grant-1",
		EpochID:         1,
		HolderID:        "holder-a",
		Scope:           perasauth.AuthorityScopeFromDelta(testAuthorityScope()),
		ExpiresUnixNano: time.Unix(101, 0).UnixNano(),
	}
}

func testAuthorityScope() compile.AuthorityScope {
	return compile.AuthorityScope{
		Mount:      fsmeta.MountID("vol"),
		MountKeyID: fsmeta.MountKeyID(7),
		Buckets:    []fsmeta.AffinityBucket{3},
		Parents:    []fsmeta.InodeID{11},
		Inodes:     []fsmeta.InodeID{29},
	}
}

func testSegmentRecord() fsperas.SegmentWitnessRecord {
	var root [32]byte
	root[0] = 1
	var digest [32]byte
	digest[0] = 2
	return fsperas.SegmentWitnessRecord{
		EpochID:              1,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		SegmentPayloadSize:   4096,
		SegmentPointer:       "inline",
		OperationCount:       64,
		EntryCount:           128,
		TimestampUnixNano:    100,
		HolderID:             "holder-a",
	}
}
