package utils

import (
	"context"
	"testing"
	"time"
)

func TestWaterMarkAdvanceAndWait(t *testing.T) {
	w := &WaterMark{Name: "test.advance"}
	w.Init(nil)

	w.Begin(1)
	w.Done(1)

	if got := w.DoneUntil(); got != 1 {
		t.Fatalf("expected doneUntil=1, got %d", got)
	}

	if err := w.WaitForMark(context.Background(), 1); err != nil {
		t.Fatalf("wait should succeed: %v", err)
	}
}

func TestWaterMarkWaiterNotified(t *testing.T) {
	w := &WaterMark{Name: "test.waiter"}
	w.Init(nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		w.Begin(5)
		w.Done(5)
		close(done)
	}()

	if err := w.WaitForMark(ctx, 5); err != nil {
		t.Fatalf("wait should succeed: %v", err)
	}
	<-done
}

func TestWaterMarkWaitCancel(t *testing.T) {
	w := &WaterMark{Name: "test.cancel"}
	w.Init(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	if err := w.WaitForMark(ctx, 10); err == nil {
		t.Fatalf("expected context timeout")
	}

	// Later completion should advance doneUntil.
	w.Begin(10)
	w.Done(10)
	if got := w.DoneUntil(); got != 10 {
		t.Fatalf("expected doneUntil=10 after completion, got %d", got)
	}
}

func TestWaterMarkSetDoneUntil(t *testing.T) {
	w := &WaterMark{Name: "test.set"}
	w.Init(nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() {
		time.Sleep(5 * time.Millisecond)
		w.SetDoneUntil(7)
	}()

	if err := w.WaitForMark(ctx, 7); err != nil {
		t.Fatalf("wait should succeed after SetDoneUntil: %v", err)
	}
}

func TestWaterMarkBeginDoneMany(t *testing.T) {
	w := &WaterMark{Name: "test.many"}
	w.Init(nil)

	w.BeginMany([]uint64{3, 4, 5})
	if got := w.LastIndex(); got != 5 {
		t.Fatalf("expected lastIndex=5, got %d", got)
	}

	w.DoneMany([]uint64{3, 4, 5})
	if got := w.DoneUntil(); got != 5 {
		t.Fatalf("expected doneUntil=5, got %d", got)
	}
}

func TestWaterMarkSetLastIndexMonotonic(t *testing.T) {
	w := &WaterMark{Name: "test.lastindex"}
	w.Init(nil)

	w.SetLastIndex(10)
	w.SetLastIndex(5)
	if got := w.LastIndex(); got != 10 {
		t.Fatalf("expected lastIndex to remain 10, got %d", got)
	}
}

func TestWaterMarkWindowRebuild(t *testing.T) {
	w := &WaterMark{Name: "test.window"}
	w.Init(nil)

	// Force a window rebuild by jumping far ahead.
	w.Begin(1 << 18)
	w.Done(1 << 18)
	if got := w.DoneUntil(); got == 0 {
		t.Fatalf("expected doneUntil to advance, got %d", got)
	}
}
