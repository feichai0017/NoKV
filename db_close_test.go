package NoKV

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/utils"
)

func TestDBCloseFlushesDiscardStats(t *testing.T) {
	workDir := t.TempDir()
	opt := testOptions(workDir)

	db := Open(opt)

	stats := map[uint32]int64{1: 11, 2: 7}
	select {
	case db.vlog.lfDiscardStats.flushChan <- stats:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out enqueueing discard stats")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	reopenOpt := testOptions(workDir)
	reopened := Open(reopenOpt)
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("reopen Close returned error: %v", err)
		}
	}()

	entry, err := reopened.GetCF(utils.CFDefault, lfDiscardStatsKey)
	if err != nil {
		t.Fatalf("GetCF returned error: %v", err)
	}
	if len(entry.Value) == 0 {
		t.Fatal("discard stats value is empty")
	}

	persisted := make(map[string]int64)
	if err := json.Unmarshal(entry.Value, &persisted); err != nil {
		t.Fatalf("failed to unmarshal discard stats: %v", err)
	}
	if got := persisted["1"]; got != stats[1] {
		t.Fatalf("unexpected discard stats for fid 1: got %d want %d", got, stats[1])
	}
	if got := persisted["2"]; got != stats[2] {
		t.Fatalf("unexpected discard stats for fid 2: got %d want %d", got, stats[2])
	}
}

func TestDBCloseHandlesStoppedCommitWorkers(t *testing.T) {
	workDir := t.TempDir()
	opt := testOptions(workDir)

	db := Open(opt)

	db.stopCommitWorkers()

	stats := map[uint32]int64{3: 5}
	select {
	case db.vlog.lfDiscardStats.flushChan <- stats:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out enqueueing discard stats")
	}

	time.Sleep(10 * time.Millisecond)

	closeErr := make(chan error, 1)
	go func() {
		closeErr <- db.Close()
	}()

	select {
	case err := <-closeErr:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close timed out waiting for discard stats goroutine to exit")
	}
}

func testOptions(workDir string) *Options {
	opt := NewDefaultOptions()
	opt.WorkDir = workDir
	opt.HotRingEnabled = false
	opt.EnableWALWatchdog = false
	opt.DiscardStatsFlushThreshold = 1
	opt.MaxBatchCount = 1024
	opt.MaxBatchSize = 1 << 20
	return opt
}
