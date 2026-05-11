package capsule

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/local"
	"github.com/feichai0017/NoKV/txn/percolator"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestMVCCReplayStoreMaterializesReaderVisibleValues(t *testing.T) {
	db := openCapsuleReplayDB(t)
	plan := versionedReplayPlanForTest(t, 100)
	store, err := NewMVCCReplayStoreForPlan(db, plan)
	require.NoError(t, err)

	stats, err := ApplyReplayPlan(store, plan)
	require.NoError(t, err)
	require.Equal(t, ApplyStats{Waves: 2, Operations: 3, Mutations: 6}, stats)

	reader := percolator.NewReader(db)
	value, _, err := reader.GetValue([]byte("dentry/a"), 200)
	require.NoError(t, err)
	require.Equal(t, []byte("inode=7"), value)
	value, _, err = reader.GetValue([]byte("inode/7"), 200)
	require.NoError(t, err)
	require.Equal(t, []byte("attrs"), value)
}

func TestMVCCReplayStoreMaterializesDelete(t *testing.T) {
	db := openCapsuleReplayDB(t)
	putStore, err := NewMVCCReplayStore(db, 100)
	require.NoError(t, err)
	_, err = ApplyReplayPlan(putStore, replayPlanForTest(t))
	require.NoError(t, err)

	deleteStore, err := NewMVCCReplayStore(db, 200)
	require.NoError(t, err)
	_, err = ApplyReplayPlan(deleteStore, ReplayPlan{
		EpochID: 2,
		Waves: [][]ReplayOperation{{{
			OpID: opID("delete", 1),
			Mutations: []ReplayMutation{{
				Key:    []byte("dentry/a"),
				Delete: true,
			}},
		}}},
	})
	require.NoError(t, err)

	reader := percolator.NewReader(db)
	value, _, err := reader.GetValue([]byte("dentry/a"), 150)
	require.NoError(t, err)
	require.Equal(t, []byte("inode=7"), value)
	_, _, err = reader.GetValue([]byte("dentry/a"), 250)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

func TestBuildMVCCReplayEntriesUsesShortValueForSmallPuts(t *testing.T) {
	entries, err := BuildMVCCReplayEntries(ReplayOperation{
		OpID: opID("client-a", 1),
		Mutations: []ReplayMutation{{
			Key:   []byte("short"),
			Value: []byte("value"),
		}},
	}, 100)
	require.NoError(t, err)
	defer releaseMVCCReplayEntries(entries)

	require.Len(t, entries, 1)
	require.Equal(t, entrykv.CFWrite, entries[0].CF)
}

func TestBuildMVCCReplayEntriesKeepsLargeValuesInDefaultCF(t *testing.T) {
	entries, err := BuildMVCCReplayEntries(ReplayOperation{
		OpID: opID("client-a", 1),
		Mutations: []ReplayMutation{{
			Key:   []byte("large"),
			Value: bytes.Repeat([]byte("x"), 129),
		}},
	}, 100)
	require.NoError(t, err)
	defer releaseMVCCReplayEntries(entries)

	require.Len(t, entries, 3)
	require.Equal(t, entrykv.CFDefault, entries[0].CF)
	require.Equal(t, byte(entrykv.BitDelete), entries[0].Meta&entrykv.BitDelete)
	require.Equal(t, entrykv.CFDefault, entries[1].CF)
	require.Equal(t, entrykv.CFWrite, entries[2].CF)
}

func TestMVCCReplayStoreKeepsVersionOnApplyFailure(t *testing.T) {
	storeErr := errors.New("apply failed")
	failing := &failingInternalEntryApplier{err: storeErr}
	plan := versionedReplayPlanForTest(t, 100)
	store, err := NewMVCCReplayStoreForPlan(failing, plan)
	require.NoError(t, err)

	_, err = ApplyReplayPlan(store, plan)
	require.ErrorIs(t, err, storeErr)

	failing.err = nil
	_, err = ApplyReplayPlan(store, ReplayPlan{
		EpochID: 2,
		Waves: [][]ReplayOperation{{{
			OpID: opID("client-z", 1),
			Mutations: []ReplayMutation{{
				Key:   []byte("z"),
				Value: []byte("value"),
			}},
		}}},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(100), failing.lastVersion())
}

func TestNewMVCCReplayStoreForPlanRequiresExactVersionRange(t *testing.T) {
	plan := versionedReplayPlanForTest(t, 100)
	plan.Versions.Count--

	_, err := NewMVCCReplayStoreForPlan(noopInternalEntryApplier{}, plan)
	require.ErrorIs(t, err, ErrReplayVersionRequired)
}

func BenchmarkMVCCReplayStoreApply64(b *testing.B) {
	seal, err := BuildCapsuleSealWithVersions(1, 1, sealSnapshotForBench(b, 64))
	if err != nil {
		b.Fatal(err)
	}
	plan, err := BuildReplayPlan(seal)
	if err != nil {
		b.Fatal(err)
	}
	db := noopInternalEntryApplier{}

	b.ReportAllocs()
	for b.Loop() {
		store, err := NewMVCCReplayStoreForPlan(db, plan)
		if err != nil {
			b.Fatal(err)
		}
		stats, err := ApplyReplayPlan(store, plan)
		if err != nil {
			b.Fatal(err)
		}
		if stats.Operations != 64 {
			b.Fatalf("unexpected operation count %d", stats.Operations)
		}
	}
}

type failingInternalEntryApplier struct {
	err      error
	versions []uint64
}

func (a *failingInternalEntryApplier) ApplyInternalEntries(entries []*entrykv.Entry) error {
	if a.err != nil {
		return a.err
	}
	for _, entry := range entries {
		a.versions = append(a.versions, entry.Version)
	}
	return nil
}

func (a *failingInternalEntryApplier) lastVersion() uint64 {
	if len(a.versions) == 0 {
		return 0
	}
	return a.versions[len(a.versions)-1]
}

type noopInternalEntryApplier struct{}

func (noopInternalEntryApplier) ApplyInternalEntries([]*entrykv.Entry) error {
	return nil
}

func openCapsuleReplayDB(t *testing.T) *local.DB {
	t.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = filepath.Join(t.TempDir(), "db")
	opt.MemTableSize = 1 << 12
	opt.SSTableMaxSz = 1 << 20
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func versionedReplayPlanForTest(t *testing.T, firstVersion uint64) ReplayPlan {
	t.Helper()
	first := testSealPrepare()
	first.OpID = opID("client-a", 1)
	second := testSealPrepare()
	second.OpID = opID("client-b", 1)
	second.ConflictDAGFrontier = []OperationID{first.OpID}
	third := testSealPrepare()
	third.OpID = opID("client-c", 1)

	seal, err := BuildCapsuleSealWithVersions(1, firstVersion, WitnessSnapshot{
		Prepares: []PrepareRecord{second, third, first},
		Commits: []CommitCertificateRecord{
			testCommitForPrepare(t, second),
			testCommitForPrepare(t, third),
			testCommitForPrepare(t, first),
		},
	})
	require.NoError(t, err)
	plan, err := BuildReplayPlan(seal)
	require.NoError(t, err)
	return plan
}
