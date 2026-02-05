package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/raftstore/scheduler"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/wal"
	"github.com/stretchr/testify/require"
)

func TestRunManifestCmd(t *testing.T) {
	dir := t.TempDir()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ValueThreshold = 0
	db := NoKV.Open(opt)
	if err := db.Set([]byte("cli-manifest"), []byte("value")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var buf bytes.Buffer
	if err := runManifestCmd(&buf, []string{"-workdir", dir, "-json"}); err != nil {
		t.Fatalf("runManifestCmd: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := payload["levels"]; !ok {
		t.Fatalf("expected levels in manifest output")
	}
	levels, _ := payload["levels"].([]any)
	if len(levels) > 0 {
		if lvl, ok := levels[0].(map[string]any); ok {
			if _, ok := lvl["value_bytes"]; !ok {
				t.Fatalf("expected value_bytes in manifest level entry")
			}
		}
	}
}

func TestRunStatsCmd(t *testing.T) {
	dir := t.TempDir()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ValueThreshold = 0
	db := NoKV.Open(opt)
	if err := db.Set([]byte("cli-stats"), []byte("value")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var buf bytes.Buffer
	if err := runStatsCmd(&buf, []string{"-workdir", dir, "-json"}); err != nil {
		t.Fatalf("runStatsCmd: %v", err)
	}
	var snap NoKV.StatsSnapshot
	if err := json.Unmarshal(buf.Bytes(), &snap); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if snap.Entries == 0 {
		t.Fatalf("expected entry count > 0")
	}
	if snap.ValueLogSegments == 0 {
		t.Fatalf("expected value log segments > 0")
	}
	if len(snap.LSMLevels) == 0 {
		t.Fatalf("expected LSM level metrics")
	}
	if snap.LSMValueBytesTotal < 0 {
		t.Fatalf("expected aggregated LSM value bytes to be non-negative")
	}
	if snap.CompactionValueWeight <= 0 {
		t.Fatalf("expected compaction value weight > 0")
	}
	if snap.LSMValueDensityMax < 0 {
		t.Fatalf("expected non-negative value density max")
	}
}

func TestRunVlogCmd(t *testing.T) {
	dir := t.TempDir()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ValueThreshold = 0
	db := NoKV.Open(opt)
	if err := db.Set([]byte("cli-vlog"), []byte("value")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var buf bytes.Buffer
	if err := runVlogCmd(&buf, []string{"-workdir", dir, "-json"}); err != nil {
		t.Fatalf("runVlogCmd: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal vlog output: %v", err)
	}
	if _, ok := payload["segments"]; ok {
		return
	}
	if _, ok := payload["buckets"]; !ok {
		t.Fatalf("expected segments or buckets array in vlog output")
	}
}

func TestRunVlogCmdPlain(t *testing.T) {
	dir := prepareDBWorkdir(t)
	var buf bytes.Buffer
	err := runVlogCmd(&buf, []string{"-workdir", dir})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "Active FID")
}

func TestRenderStatsWarnLine(t *testing.T) {
	var buf bytes.Buffer
	snap := NoKV.StatsSnapshot{
		Entries:              1,
		WALActiveSegment:     7,
		WALSegmentCount:      3,
		WALSegmentsRemoved:   1,
		WALActiveSize:        4096,
		RaftGroupCount:       2,
		RaftLaggingGroups:    1,
		RaftMaxLagSegments:   5,
		RaftLagWarnThreshold: 3,
		RaftLagWarning:       true,
	}
	if err := renderStats(&buf, snap, false); err != nil {
		t.Fatalf("renderStats: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Raft.Warning") {
		t.Fatalf("expected Raft.Warning line in output, got: %q", out)
	}
	if !strings.Contains(out, "WAL.ActiveSize") {
		t.Fatalf("expected WAL.ActiveSize line in output, got: %q", out)
	}
	if !strings.Contains(out, "Regions.Total") {
		t.Fatalf("expected Regions.Total line in output, got: %q", out)
	}
	if !strings.Contains(out, "Compaction.ValueWeight") {
		t.Fatalf("expected Compaction.ValueWeight line in output, got: %q", out)
	}
}

func TestRunManifestCmdPlain(t *testing.T) {
	dir := prepareDBWorkdir(t)
	var buf bytes.Buffer
	err := runManifestCmd(&buf, []string{"-workdir", dir})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "Manifest Log Pointer")
}
func TestRunRegionsCmd(t *testing.T) {
	dir := t.TempDir()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ValueThreshold = 0
	db := NoKV.Open(opt)
	if err := db.Set([]byte("cli-region"), []byte("value")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var buf bytes.Buffer
	if err := runRegionsCmd(&buf, []string{"-workdir", dir, "-json"}); err != nil {
		t.Fatalf("runRegionsCmd: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("decode regions output: %v", err)
	}
	regions, ok := payload["regions"].([]any)
	if !ok {
		t.Fatalf("expected regions array in output: %v", payload)
	}
	_ = len(regions)
}

func TestFetchExpvarSnapshot(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		payload := map[string]any{
			"NoKV.Stats.Entries":           float64(12),
			"NoKV.Stats.ValueLog.Segments": map[string]any{"value": float64(2)},
			"NoKV.Stats.HotKeys":           map[string]any{"k1": map[string]any{"value": float64(3)}},
			"NoKV.Stats.LSM.Levels": []any{
				map[string]any{"level": float64(0), "tables": float64(1)},
			},
		}
		_ = json.NewEncoder(w).Encode(payload)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	url := strings.TrimPrefix(server.URL, "http://")
	snap, err := fetchExpvarSnapshot(url)
	require.NoError(t, err)
	require.Equal(t, int64(12), snap.Entries)
	require.Equal(t, 2, snap.ValueLogSegments)
	require.Len(t, snap.HotKeys, 1)
	require.Equal(t, "k1", snap.HotKeys[0].Key)
	require.Len(t, snap.LSMLevels, 1)
	require.Equal(t, 0, snap.LSMLevels[0].Level)
}

func TestFetchExpvarSnapshotWithPath(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"NoKV.Stats.Entries": float64(2)})
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	snap, err := fetchExpvarSnapshot(server.URL + "/debug/vars")
	require.NoError(t, err)
	require.Equal(t, int64(2), snap.Entries)
}

func TestParseExpvarSnapshotHotKeysList(t *testing.T) {
	snap := parseExpvarSnapshot(map[string]any{
		"NoKV.Stats.HotKeys": []any{
			map[string]any{"key": "k2", "count": float64(4)},
		},
	})
	require.Len(t, snap.HotKeys, 1)
	require.Equal(t, "k2", snap.HotKeys[0].Key)
	require.Equal(t, int32(4), snap.HotKeys[0].Count)
}

func TestParseExpvarSnapshotHotKeysMap(t *testing.T) {
	snap := parseExpvarSnapshot(map[string]any{
		"NoKV.Stats.HotKeys": map[string]any{
			"k3": map[string]any{"value": float64(7)},
		},
	})
	require.Len(t, snap.HotKeys, 1)
	require.Equal(t, "k3", snap.HotKeys[0].Key)
	require.Equal(t, int32(7), snap.HotKeys[0].Count)
}

func TestParseExpvarSnapshotHotKeysMapFloat(t *testing.T) {
	snap := parseExpvarSnapshot(map[string]any{
		"NoKV.Stats.HotKeys": map[string]any{
			"k4": float64(3),
		},
	})
	require.Len(t, snap.HotKeys, 1)
	require.Equal(t, "k4", snap.HotKeys[0].Key)
	require.Equal(t, int32(3), snap.HotKeys[0].Count)
}

func TestFormatHelpers(t *testing.T) {
	require.Equal(t, "new", formatRegionState(manifest.RegionStateNew))
	require.Equal(t, "running", formatRegionState(manifest.RegionStateRunning))
	require.Equal(t, "removing", formatRegionState(manifest.RegionStateRemoving))
	require.Equal(t, "tombstone", formatRegionState(manifest.RegionStateTombstone))
	require.Equal(t, "unknown(99)", formatRegionState(99))

	peers := []manifest.PeerMeta{{StoreID: 1, PeerID: 2}}
	require.Equal(t, "[{store:1 peer:2}]", formatPeers(peers))
	require.Equal(t, "[]", formatPeers(nil))

	files := []manifest.FileMeta{
		{FileID: 1, Size: 10, ValueSize: 5},
		{FileID: 2, Size: 20, ValueSize: 7},
	}
	require.Equal(t, []uint64{1, 2}, fileIDs(files))
	require.Equal(t, uint64(30), totalSize(files))
	require.Equal(t, uint64(12), totalValue(files))
}

func TestRunSchedulerCmdNoStore(t *testing.T) {
	stores := storepkg.Stores()
	for _, st := range stores {
		storepkg.UnregisterStore(st)
	}
	var buf bytes.Buffer
	err := runSchedulerCmd(&buf, []string{"-json"})
	require.Error(t, err)
}

func TestPrintUsage(t *testing.T) {
	var buf bytes.Buffer
	printUsage(&buf)
	out := buf.String()
	if !strings.Contains(out, "Usage: nokv") {
		t.Fatalf("expected usage header, got %q", out)
	}
	if !strings.Contains(out, "serve") {
		t.Fatalf("expected serve command in usage, got %q", out)
	}
}

func TestEnsureManifestExists(t *testing.T) {
	dir := t.TempDir()
	if err := ensureManifestExists(dir); err == nil {
		t.Fatalf("expected missing manifest error")
	}

	path := filepath.Join(dir, "CURRENT")
	if err := os.WriteFile(path, []byte("MANIFEST-000001"), 0o644); err != nil {
		t.Fatalf("write CURRENT: %v", err)
	}
	if err := ensureManifestExists(dir); err != nil {
		t.Fatalf("expected manifest to exist: %v", err)
	}
}

func TestRunSchedulerCmdWithStore(t *testing.T) {
	withStoreRegistry(t, func() {
		storepkg.RegisterStore(&storepkg.Store{})
		var buf bytes.Buffer
		require.NoError(t, runSchedulerCmd(&buf, nil))
		require.Contains(t, buf.String(), "Stores (0)")
		require.Contains(t, buf.String(), "Regions (0)")

		buf.Reset()
		require.NoError(t, runSchedulerCmd(&buf, []string{"-json"}))
		var payload map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	})
}

func TestFirstRegionMetricsNone(t *testing.T) {
	withStoreRegistry(t, func() {
		if got := firstRegionMetrics(); got != nil {
			t.Fatalf("expected nil region metrics")
		}
	})
}

func TestMainHelp(t *testing.T) {
	oldArgs := os.Args
	os.Args = []string{"nokv", "help"}
	defer func() { os.Args = oldArgs }()
	main()
}

func TestMainMissingArgs(t *testing.T) {
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv"}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 1, code)
}

func TestMainUnknownCommand(t *testing.T) {
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "nope"}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 1, code)
}

func TestMainStatsError(t *testing.T) {
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "stats"}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 1, code)
}

func TestMainManifestCommand(t *testing.T) {
	dir := prepareDBWorkdir(t)
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "manifest", "-workdir", dir}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 0, code)
}

func TestMainVlogCommand(t *testing.T) {
	dir := prepareDBWorkdir(t)
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "vlog", "-workdir", dir}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 0, code)
}

func TestMainRegionsCommand(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	require.NoError(t, err)
	require.NoError(t, mgr.LogRegionUpdate(manifest.RegionMeta{
		ID:       1,
		State:    manifest.RegionStateRunning,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []manifest.PeerMeta{{StoreID: 1, PeerID: 10}},
	}))
	require.NoError(t, mgr.Close())
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "regions", "-workdir", dir}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 0, code)
}

func TestMainSchedulerCommand(t *testing.T) {
	withStoreRegistry(t, func() {
		storepkg.RegisterStore(&storepkg.Store{})
		code := captureExitCode(t, func() {
			oldArgs := os.Args
			os.Args = []string{"nokv", "scheduler", "-json"}
			defer func() { os.Args = oldArgs }()
			main()
		})
		require.Equal(t, 0, code)
	})
}

func TestMainServeCommand(t *testing.T) {
	origNotify := notifyContext
	notifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { notifyContext = origNotify })

	dir := t.TempDir()
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "serve", "-workdir", dir, "-store-id", "1", "-addr", "127.0.0.1:0"}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 0, code)
}

func TestRunStatsCmdMissingFlags(t *testing.T) {
	var buf bytes.Buffer
	err := runStatsCmd(&buf, nil)
	require.Error(t, err)
}

func TestRunStatsCmdParseError(t *testing.T) {
	var buf bytes.Buffer
	err := runStatsCmd(&buf, []string{"-bad-flag"})
	require.Error(t, err)
}

func TestRunStatsCmdNoRegionMetrics(t *testing.T) {
	dir := prepareDBWorkdir(t)
	var buf bytes.Buffer
	err := runStatsCmd(&buf, []string{"-workdir", dir, "-no-region-metrics", "-json"})
	require.NoError(t, err)
}

func TestRunStatsCmdExpvarPlain(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		payload := map[string]any{
			"NoKV.Stats.Entries": float64(9),
		}
		_ = json.NewEncoder(w).Encode(payload)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	var buf bytes.Buffer
	err := runStatsCmd(&buf, []string{"-expvar", server.URL})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "Entries")
}

func TestFetchExpvarSnapshotBadStatus(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	_, err := fetchExpvarSnapshot(server.URL)
	require.Error(t, err)
}

func TestFetchExpvarSnapshotBadJSON(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("{bad-json"))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	_, err := fetchExpvarSnapshot(server.URL)
	require.Error(t, err)
}

func TestFetchExpvarSnapshotTrailingSlash(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"NoKV.Stats.Entries": float64(1)})
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	url := strings.TrimPrefix(server.URL, "http://") + "/"
	snap, err := fetchExpvarSnapshot(url)
	require.NoError(t, err)
	require.Equal(t, int64(1), snap.Entries)
}

func TestParseExpvarSnapshotFull(t *testing.T) {
	data := map[string]any{
		"NoKV.Stats.Entries":                 float64(11),
		"NoKV.Stats.Flush.Pending":           map[string]any{"value": float64(2)},
		"NoKV.Stats.Compaction.MaxScore":     map[string]any{"value": float64(1.5)},
		"NoKV.Stats.Write.HotKeyLimited":     float64(-4),
		"NoKV.Stats.ValueLog.Segments":       float64(3),
		"NoKV.Stats.ValueLog.PendingDeletes": map[string]any{"value": float64(1)},
		"NoKV.Stats.ValueLog.DiscardQueue":   map[string]any{"value": float64(2)},
		"NoKV.Stats.Raft.Groups":             float64(2),
		"NoKV.Stats.Raft.LaggingGroups":      float64(1),
		"NoKV.Stats.Raft.MaxLagSegments":     float64(5),
		"NoKV.Stats.Raft.MinSegment":         float64(1),
		"NoKV.Stats.Raft.MaxSegment":         float64(9),
		"NoKV.Stats.LSM.ValueBytes":          float64(10),
		"NoKV.Stats.Compaction.ValueWeight":  map[string]any{"value": float64(2.0)},
		"NoKV.Stats.LSM.ValueDensityMax":     map[string]any{"value": float64(3.5)},
		"NoKV.Stats.LSM.ValueDensityAlert":   float64(1),
		"NoKV.Stats.Region.Total":            float64(4),
		"NoKV.Stats.Region.New":              float64(1),
		"NoKV.Stats.Region.Running":          float64(1),
		"NoKV.Stats.Region.Removing":         float64(1),
		"NoKV.Stats.Region.Tombstone":        float64(1),
		"NoKV.Stats.Region.Other":            float64(0),
		"NoKV.Txns.Active":                   float64(2),
		"NoKV.Txns.Started":                  float64(3),
		"NoKV.Txns.Committed":                float64(4),
		"NoKV.Txns.Conflicts":                float64(5),
		"NoKV.Stats.HotKeys": []any{
			map[string]any{"key": "hot", "count": map[string]any{"value": float64(9)}},
		},
		"NoKV.Stats.LSM.Levels": []any{
			map[string]any{
				"level":              float64(0),
				"tables":             float64(1),
				"size_bytes":         float64(10),
				"value_bytes":        float64(5),
				"stale_bytes":        float64(2),
				"ingest_tables":      float64(1),
				"ingest_size_bytes":  float64(3),
				"ingest_value_bytes": float64(4),
			},
		},
	}
	snap := parseExpvarSnapshot(data)
	require.Equal(t, int64(11), snap.Entries)
	require.Equal(t, uint64(0), snap.HotWriteLimited)
	require.True(t, snap.LSMValueDensityAlert)
	require.Len(t, snap.HotKeys, 1)
	require.Len(t, snap.LSMLevels, 1)
}

func TestRenderStatsFull(t *testing.T) {
	var buf bytes.Buffer
	snap := NoKV.StatsSnapshot{
		Entries:                        1,
		FlushPending:                   2,
		CompactionBacklog:              3,
		CompactionMaxScore:             4.5,
		FlushLastWaitMs:                1,
		FlushMaxWaitMs:                 2,
		FlushLastBuildMs:               3,
		FlushMaxBuildMs:                4,
		FlushLastReleaseMs:             5,
		FlushMaxReleaseMs:              6,
		CompactionLastDurationMs:       1.2,
		CompactionMaxDurationMs:        2.3,
		CompactionRuns:                 1,
		ValueLogSegments:               1,
		ValueLogPendingDel:             1,
		ValueLogDiscardQueue:           1,
		ValueLogHeads:                  map[uint32]kv.ValuePtr{0: {Bucket: 0, Fid: 1, Offset: 2, Len: 3}},
		HotWriteLimited:                2,
		CompactionValueWeight:          1.0,
		CompactionValueWeightSuggested: 2.0,
		LSMValueDensityMax:             1.5,
		LSMValueDensityAlert:           true,
		WALActiveSegment:               1,
		WALSegmentCount:                2,
		WALActiveSize:                  4096,
		WALSegmentsRemoved:             1,
		WALRecordCounts:                wal.RecordMetrics{Entries: 1},
		WALSegmentsWithRaftRecords:     1,
		WALRemovableRaftSegments:       1,
		WALTypedRecordRatio:            0.5,
		WALTypedRecordWarning:          true,
		WALTypedRecordReason:           "ratio low",
		WALAutoGCRuns:                  1,
		WALAutoGCRemoved:               2,
		WALAutoGCLastUnix:              time.Now().Unix(),
		RaftGroupCount:                 1,
		RaftLaggingGroups:              1,
		RaftMaxLagSegments:             2,
		RaftMinLogSegment:              1,
		RaftMaxLogSegment:              2,
		RaftLagWarnThreshold:           1,
		RaftLagWarning:                 true,
		TxnsActive:                     1,
		TxnsStarted:                    2,
		TxnsCommitted:                  3,
		TxnsConflicts:                  4,
		RegionTotal:                    5,
		RegionNew:                      1,
		RegionRunning:                  1,
		RegionRemoving:                 1,
		RegionTombstone:                1,
		RegionOther:                    1,
		LSMValueBytesTotal:             10,
		LSMLevels: []NoKV.LSMLevelStats{{
			Level:            0,
			TableCount:       1,
			SizeBytes:        2,
			ValueBytes:       3,
			StaleBytes:       4,
			IngestTables:     1,
			IngestSizeBytes:  2,
			IngestValueBytes: 3,
		}},
		ColumnFamilies: map[string]NoKV.ColumnFamilySnapshot{
			"default": {Reads: 1, Writes: 2},
		},
		HotKeys: []NoKV.HotKeyStat{{Key: "k", Count: 1}},
	}
	require.NoError(t, renderStats(&buf, snap, false))
	out := buf.String()
	require.Contains(t, out, "ValueLog.Head")
	require.Contains(t, out, "LSM.Levels:")
	require.Contains(t, out, "ColumnFamilies:")
	require.Contains(t, out, "HotKeys:")
}

func TestLocalStatsSnapshotMissingWorkdir(t *testing.T) {
	_, err := localStatsSnapshot("", false)
	require.Error(t, err)
}

func TestRunVlogCmdMissingDir(t *testing.T) {
	var buf bytes.Buffer
	err := runVlogCmd(&buf, []string{"-workdir", t.TempDir()})
	require.Error(t, err)
}

func TestRunVlogCmdMissingWorkdir(t *testing.T) {
	var buf bytes.Buffer
	require.Error(t, runVlogCmd(&buf, nil))
}

func TestRunRegionsCmdPlainNoRegions(t *testing.T) {
	var buf bytes.Buffer
	err := runRegionsCmd(&buf, []string{"-workdir", t.TempDir()})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "Regions: (none)")
}

func TestRunRegionsCmdMissingWorkdir(t *testing.T) {
	var buf bytes.Buffer
	require.Error(t, runRegionsCmd(&buf, nil))
}

func TestRunRegionsCmdPlainWithRegion(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir)
	require.NoError(t, err)
	meta := manifest.RegionMeta{
		ID:       10,
		State:    manifest.RegionStateTombstone,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []manifest.PeerMeta{{StoreID: 1, PeerID: 10}},
	}
	require.NoError(t, mgr.LogRegionUpdate(meta))
	require.NoError(t, mgr.Close())

	var buf bytes.Buffer
	err = runRegionsCmd(&buf, []string{"-workdir", dir})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "tombstone")
}

func TestRunManifestCmdMissingWorkdir(t *testing.T) {
	var buf bytes.Buffer
	err := runManifestCmd(&buf, nil)
	require.Error(t, err)
}

func TestRunManifestCmdMissingManifest(t *testing.T) {
	var buf bytes.Buffer
	err := runManifestCmd(&buf, []string{"-workdir", t.TempDir()})
	require.Error(t, err)
}

func TestRunSchedulerCmdSnapshot(t *testing.T) {
	withStoreRegistry(t, func() {
		coord := scheduler.NewCoordinator()
		store := storepkg.NewStoreWithConfig(storepkg.Config{
			StoreID:   1,
			Scheduler: coord,
		})
		defer store.Close()

		coord.SubmitStoreHeartbeat(scheduler.StoreStats{
			StoreID:   1,
			RegionNum: 2,
			LeaderNum: 1,
			Capacity:  1024,
			Available: 512,
		})
		coord.SubmitRegionHeartbeat(manifest.RegionMeta{
			ID:       21,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
			Epoch:    manifest.RegionEpoch{Version: 1, ConfVersion: 1},
			Peers:    []manifest.PeerMeta{{StoreID: 1, PeerID: 11}},
		})

		var buf bytes.Buffer
		require.NoError(t, runSchedulerCmd(&buf, nil))
		out := buf.String()
		require.Contains(t, out, "Stores (1)")
		require.Contains(t, out, "region=21")
		require.Contains(t, out, "last_heartbeat=")
	})
}

func TestFirstRegionMetricsFound(t *testing.T) {
	withStoreRegistry(t, func() {
		store := storepkg.NewStoreWithConfig(storepkg.Config{})
		defer store.Close()
		require.NotNil(t, firstRegionMetrics())
	})
}

func TestLocalStatsSnapshotWithMetrics(t *testing.T) {
	withStoreRegistry(t, func() {
		store := storepkg.NewStoreWithConfig(storepkg.Config{})
		defer store.Close()
		dir := prepareDBWorkdir(t)
		_, err := localStatsSnapshot(dir, true)
		require.NoError(t, err)
	})
}

func TestEnsureManifestExistsStatError(t *testing.T) {
	origStat := stat
	stat = func(string) (os.FileInfo, error) {
		return nil, errors.New("boom")
	}
	t.Cleanup(func() { stat = origStat })
	require.Error(t, ensureManifestExists(t.TempDir()))
}

func captureExitCode(t *testing.T, fn func()) (code int) {
	t.Helper()
	origExit := exit
	defer func() { exit = origExit }()
	exit = func(code int) {
		panic(code)
	}
	defer func() {
		if r := recover(); r != nil {
			if c, ok := r.(int); ok {
				code = c
				return
			}
			panic(r)
		}
	}()
	fn()
	return code
}

func withStoreRegistry(t *testing.T, fn func()) {
	t.Helper()
	original := storepkg.Stores()
	for _, st := range original {
		storepkg.UnregisterStore(st)
	}
	defer func() {
		for _, st := range storepkg.Stores() {
			storepkg.UnregisterStore(st)
		}
		for _, st := range original {
			storepkg.RegisterStore(st)
		}
	}()
	fn()
}

func prepareDBWorkdir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ValueThreshold = 0
	db := NoKV.Open(opt)
	require.NoError(t, db.Set([]byte("seed"), []byte("value")))
	require.NoError(t, db.Close())
	return dir
}
