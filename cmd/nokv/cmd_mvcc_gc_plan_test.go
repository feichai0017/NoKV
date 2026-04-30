package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/fsmeta"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/percolator"
	storekv "github.com/feichai0017/NoKV/raftstore/kv"
	"github.com/stretchr/testify/require"
)

func prepareMVCCGCPlanWorkdir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	db, err := NoKV.Open(opt)
	require.NoError(t, err)

	volKey, err := fsmeta.EncodeInodeKey("vol", 10)
	require.NoError(t, err)
	otherKey, err := fsmeta.EncodeInodeKey("other", 10)
	require.NoError(t, err)
	for _, key := range [][]byte{volKey, otherKey} {
		applyMVCCGCPlanWrite(t, db, key, 150, 140)
		applyMVCCGCPlanWrite(t, db, key, 90, 80)
		applyMVCCGCPlanWrite(t, db, key, 40, 30)
	}
	require.NoError(t, db.Close())
	return dir
}

func applyMVCCGCPlanWrite(t *testing.T, db *NoKV.DB, key []byte, commitTs, startTs uint64) {
	t.Helper()
	write := percolator.EncodeWrite(percolator.Write{Kind: kvrpcpb.Mutation_Put, StartTs: startTs})
	entry := entrykv.NewInternalEntry(entrykv.CFWrite, key, commitTs, entrykv.SafeCopy(nil, write), 0, 0)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*entrykv.Entry{entry}))
}

func applyMVCCGCPlanPutVersion(t *testing.T, db *NoKV.DB, key []byte, commitTs, startTs uint64, value string) {
	t.Helper()
	defaultEntry := entrykv.NewInternalEntry(entrykv.CFDefault, key, startTs, []byte(value), 0, 0)
	defer defaultEntry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*entrykv.Entry{defaultEntry}))
	applyMVCCGCPlanWrite(t, db, key, commitTs, startTs)
}

func prepareMVCCGCApplyWorkdir(t *testing.T) (string, []byte) {
	t.Helper()
	dir := t.TempDir()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	db, err := NoKV.Open(opt)
	require.NoError(t, err)

	key, err := fsmeta.EncodeInodeKey("vol", 10)
	require.NoError(t, err)
	applyMVCCGCPlanPutVersion(t, db, key, 150, 140, "new")
	applyMVCCGCPlanPutVersion(t, db, key, 90, 80, "anchor")
	applyMVCCGCPlanPutVersion(t, db, key, 40, 30, "old")
	require.NoError(t, db.Close())
	return dir, key
}

func TestRunMVCCGCPlanCmdJSON(t *testing.T) {
	dir := prepareMVCCGCPlanWorkdir(t)
	var buf bytes.Buffer
	err := runMVCCGCPlanCmd(&buf, []string{
		"-workdir", dir,
		"-safe-point", "100",
		"-global-floor", "50",
		"-mount-floor", "vol=50",
		"-json",
	})
	require.NoError(t, err)

	var stats storekv.MVCCGCPlanStats
	require.NoError(t, json.Unmarshal(buf.Bytes(), &stats))
	require.Equal(t, uint64(2), stats.Keys)
	require.Equal(t, uint64(6), stats.WriteVersions)
	require.Equal(t, uint64(5), stats.RetainedWrites)
	require.Equal(t, uint64(1), stats.DroppableWrites)
	require.Equal(t, uint64(2), stats.AnchorWrites)
	require.Equal(t, uint64(5), stats.RetainedDefaultRefs)
	require.Equal(t, uint64(1), stats.SafePointClampedKeys)
	require.Equal(t, uint64(3), stats.MaxVersionsPerKey)
	require.Equal(t, uint64(50), stats.MinEffectiveSafePoint)
	require.Equal(t, uint64(100), stats.MaxEffectiveSafePoint)
}

func TestRunMVCCGCPlanCmdPlain(t *testing.T) {
	dir := prepareMVCCGCPlanWorkdir(t)
	var buf bytes.Buffer
	err := runMVCCGCPlanCmd(&buf, []string{"-workdir", dir, "-safe-point", "100"})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "MVCCGC.Keys")
	require.Contains(t, buf.String(), "MVCCGC.DroppableWrites")
}

func TestRunMVCCGCPlanCmdRejectsInvalidFlags(t *testing.T) {
	var buf bytes.Buffer
	require.ErrorContains(t, runMVCCGCPlanCmd(&buf, []string{"-safe-point", "100"}), "workdir is required")
	require.ErrorContains(t, runMVCCGCPlanCmd(&buf, []string{"-workdir", t.TempDir()}), "safe-point is required")
	require.ErrorContains(t, runMVCCGCPlanCmd(&buf, []string{"-workdir", t.TempDir(), "-safe-point", "100", "-mount-floor", "vol"}), "mount floor must be")
	require.ErrorContains(t, runMVCCGCPlanCmd(&buf, []string{"-workdir", t.TempDir(), "-safe-point", "100", "-mount-floor", "vol=abc"}), "parse mount floor")
	require.ErrorContains(t, runMVCCGCPlanCmd(&buf, []string{"-workdir", t.TempDir(), "-safe-point", "100", "-meta-root-timeout", "0"}), "meta-root-timeout")
}

func TestRunMVCCGCPlanCmdLoadsMetaRootRetention(t *testing.T) {
	dir := prepareMVCCGCPlanWorkdir(t)
	orig := loadMVCCGCRootRetention
	t.Cleanup(func() { loadMVCCGCRootRetention = orig })
	var gotAddr string
	loadMVCCGCRootRetention = func(_ context.Context, addr string) (rootstate.SnapshotRetentionIndex, error) {
		gotAddr = addr
		return rootstate.SnapshotRetentionIndex{
			GlobalFloor: 50,
			MountFloors: map[string]uint64{
				"vol": 50,
			},
		}, nil
	}

	var buf bytes.Buffer
	err := runMVCCGCPlanCmd(&buf, []string{
		"-workdir", dir,
		"-safe-point", "100",
		"-meta-root-addr", "127.0.0.1:2380",
		"-json",
	})
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:2380", gotAddr)

	var stats storekv.MVCCGCPlanStats
	require.NoError(t, json.Unmarshal(buf.Bytes(), &stats))
	require.Equal(t, uint64(5), stats.RetainedWrites)
	require.Equal(t, uint64(1), stats.DroppableWrites)
	require.Equal(t, uint64(1), stats.SafePointClampedKeys)
	require.Equal(t, uint64(3), stats.MaxVersionsPerKey)
	require.Equal(t, uint64(50), stats.MinEffectiveSafePoint)
	require.Equal(t, uint64(100), stats.MaxEffectiveSafePoint)
}

func TestMVCCGCPolicyMergesManualAndRootFloorsConservatively(t *testing.T) {
	orig := loadMVCCGCRootRetention
	t.Cleanup(func() { loadMVCCGCRootRetention = orig })
	loadMVCCGCRootRetention = func(_ context.Context, _ string) (rootstate.SnapshotRetentionIndex, error) {
		return rootstate.SnapshotRetentionIndex{
			GlobalFloor: 90,
			MountFloors: map[string]uint64{
				"vol":   80,
				"other": 70,
			},
		}, nil
	}

	var floors mountFloorFlags
	require.NoError(t, floors.Set("vol=50"))
	require.NoError(t, floors.Set("manual=40"))
	opt := mvccGCCommandOptions{
		requestedSafePoint: 100,
		globalFloor:        60,
		mountFloors:        floors,
		metaRootAddr:       "root",
		metaRootTimeout:    time.Second,
	}
	policy, err := opt.policy(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(60), policy.SnapshotRetention.GlobalFloor)
	require.Equal(t, uint64(50), policy.SnapshotRetention.MountFloors["vol"])
	require.Equal(t, uint64(70), policy.SnapshotRetention.MountFloors["other"])
	require.Equal(t, uint64(40), policy.SnapshotRetention.MountFloors["manual"])
}

func TestRunMVCCGCCmdRequiresApply(t *testing.T) {
	dir, _ := prepareMVCCGCApplyWorkdir(t)
	var buf bytes.Buffer
	err := runMVCCGCCmd(&buf, []string{"-workdir", dir, "-safe-point", "100"})
	require.ErrorContains(t, err, "requires --apply")
}

func TestRunMVCCGCCmdJSONAppliesTombstones(t *testing.T) {
	dir, key := prepareMVCCGCApplyWorkdir(t)
	var buf bytes.Buffer
	err := runMVCCGCCmd(&buf, []string{
		"-workdir", dir,
		"-safe-point", "100",
		"-batch-entries", "2",
		"-apply",
		"-json",
	})
	require.NoError(t, err)

	var stats storekv.MVCCGCApplyStats
	require.NoError(t, json.Unmarshal(buf.Bytes(), &stats))
	require.Equal(t, uint64(1), stats.AppliedWriteDeletes)
	require.Equal(t, uint64(1), stats.AppliedDefaultDeletes)

	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	write, err := db.GetInternalEntry(entrykv.CFWrite, key, 40)
	require.NoError(t, err)
	defer write.DecrRef()
	require.NotZero(t, write.Meta&entrykv.BitDelete)

	payload, err := db.GetInternalEntry(entrykv.CFDefault, key, 30)
	require.NoError(t, err)
	defer payload.DecrRef()
	require.NotZero(t, payload.Meta&entrykv.BitDelete)
}

func TestRunMVCCGCCmdRejectsInvalidBatch(t *testing.T) {
	var buf bytes.Buffer
	require.ErrorContains(t, runMVCCGCCmd(&buf, []string{"-workdir", t.TempDir(), "-safe-point", "100", "-batch-entries", "-1", "-apply"}), "batch-entries")
}
