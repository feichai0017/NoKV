package flush_test

import (
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/lsm/flush"
)

func TestManagerSubmitAndNext(t *testing.T) {
	mgr := flush.NewManager()
	defer mgr.Close()

	task := &flush.Task{SegmentID: 1}
	if _, err := mgr.Submit(task); err != nil {
		t.Fatalf("submit: %v", err)
	}

	next, ok := mgr.Next()
	if !ok {
		t.Fatalf("expected task")
	}
	if next.ID == 0 || next.Stage != flush.StageBuild {
		t.Fatalf("bad task: %+v", next)
	}

	if err := mgr.Update(next.ID, flush.StageInstall, "temp", nil); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := mgr.Update(next.ID, flush.StageRelease, nil, nil); err != nil {
		t.Fatalf("final update: %v", err)
	}
	stats := mgr.Stats()
	if stats.Pending != 0 {
		t.Fatalf("expected pending=0 got %d", stats.Pending)
	}
}

func TestManagerNextBlocksUntilSubmit(t *testing.T) {
	mgr := flush.NewManager()
	defer mgr.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		task, ok := mgr.Next()
		if !ok {
			t.Errorf("expected task")
			return
		}
		_ = mgr.Update(task.ID, flush.StageRelease, nil, nil)
	}()

	time.Sleep(10 * time.Millisecond)
	if _, err := mgr.Submit(&flush.Task{SegmentID: 2}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	wg.Wait()
}

func TestManagerUpdateOnMissingTask(t *testing.T) {
	mgr := flush.NewManager()
	defer mgr.Close()

	if err := mgr.Update(123, flush.StageInstall, nil, nil); err == nil {
		t.Fatalf("expected error")
	}
}
