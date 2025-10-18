package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/utils"
)

func TestRunManifestCmd(t *testing.T) {
	dir := t.TempDir()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ValueThreshold = 0
	db := NoKV.Open(opt)
	e := utils.NewEntry([]byte("cli-manifest"), []byte("value"))
	if err := db.Set(e); err != nil {
		t.Fatalf("set: %v", err)
	}
	e.DecrRef()
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
}

func TestRunStatsCmd(t *testing.T) {
	dir := t.TempDir()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ValueThreshold = 0
	db := NoKV.Open(opt)
	e := utils.NewEntry([]byte("cli-stats"), []byte("value"))
	if err := db.Set(e); err != nil {
		t.Fatalf("set: %v", err)
	}
	e.DecrRef()
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
}

func TestRunVlogCmd(t *testing.T) {
	dir := t.TempDir()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ValueThreshold = 0
	db := NoKV.Open(opt)
	e := utils.NewEntry([]byte("cli-vlog"), []byte("value"))
	if err := db.Set(e); err != nil {
		t.Fatalf("set: %v", err)
	}
	e.DecrRef()
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
}
func TestRunRegionsCmd(t *testing.T) {
	dir := t.TempDir()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ValueThreshold = 0
	db := NoKV.Open(opt)
	e := utils.NewEntry([]byte("cli-region"), []byte("value"))
	if err := db.Set(e); err != nil {
		t.Fatalf("set: %v", err)
	}
	e.DecrRef()
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
