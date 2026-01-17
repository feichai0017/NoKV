package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/manifest"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
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
	if _, ok := payload["segments"]; !ok {
		t.Fatalf("expected segments array in vlog output")
	}
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

func TestFormatHelpers(t *testing.T) {
	require.Equal(t, "running", formatRegionState(manifest.RegionStateRunning))
	require.Equal(t, "unknown(99)", formatRegionState(99))

	peers := []manifest.PeerMeta{{StoreID: 1, PeerID: 2}}
	require.Equal(t, "[{store:1 peer:2}]", formatPeers(peers))

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
