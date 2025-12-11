package hotring

import (
	"sync"
	"testing"
	"time"
)

func TestHotRingTouchAndTopN(t *testing.T) {
	r := NewHotRing(4, nil)

	if count := r.Touch("alpha"); count != 1 {
		t.Fatalf("expected initial count 1, got %d", count)
	}
	if count := r.Touch("beta"); count != 1 {
		t.Fatalf("expected initial count 1, got %d", count)
	}
	if count := r.Touch("alpha"); count != 2 {
		t.Fatalf("expected second touch to reach 2, got %d", count)
	}
	r.Touch("gamma")

	top := r.TopN(2)
	if len(top) != 2 {
		t.Fatalf("expected top 2 items, got %d", len(top))
	}
	if top[0].Key != "alpha" || top[0].Count != 2 {
		t.Fatalf("expected alpha with count 2 at top, got %+v", top[0])
	}

	r.Remove("alpha")
	top = r.TopN(2)
	for _, item := range top {
		if item.Key == "alpha" {
			t.Fatalf("expected alpha to be removed, found in top list")
		}
	}
}

func TestHotRingConcurrentTouch(t *testing.T) {
	r := NewHotRing(4, nil)
	const goroutines = 8
	const perG = 500

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				r.Touch("shared")
			}
		}()
	}
	wg.Wait()

	if got := r.Frequency("shared"); got != int32(goroutines*perG) {
		t.Fatalf("expected %d touches, got %d", goroutines*perG, got)
	}
}

func TestHotRingFrequencyAndClamp(t *testing.T) {
	r := NewHotRing(4, nil)
	if freq := r.Frequency("missing"); freq != 0 {
		t.Fatalf("expected zero frequency for missing key, got %d", freq)
	}
	if count, limited := r.TouchAndClamp("hot", 3); count != 1 || limited {
		t.Fatalf("expected count=1 limited=false, got count=%d limited=%v", count, limited)
	}
	if count, limited := r.TouchAndClamp("hot", 3); count != 2 || limited {
		t.Fatalf("expected count=2 limited=false, got count=%d limited=%v", count, limited)
	}
	if count, limited := r.TouchAndClamp("hot", 3); !limited || count != 3 {
		t.Fatalf("expected limit reached at 3, got count=%d limited=%v", count, limited)
	}
	if freq := r.Frequency("hot"); freq != 3 {
		t.Fatalf("expected frequency 3, got %d", freq)
	}
	r.Touch("warm")
	r.Touch("warm")
	r.Touch("cool")
	above := r.KeysAbove(2)
	if len(above) == 0 {
		t.Fatalf("expected keys above threshold")
	}
	foundHot := false
	for _, item := range above {
		if item.Key == "hot" {
			foundHot = true
			if item.Count < 3 {
				t.Fatalf("expected hot count >=3, got %d", item.Count)
			}
		}
		if item.Count < 2 {
			t.Fatalf("expected all returned items to be >=2, got %+v", item)
		}
	}
	if !foundHot {
		t.Fatalf("expected hot key to be reported above threshold")
	}
}

func TestHotRingSlidingWindow(t *testing.T) {
	r := NewHotRing(4, nil)
	r.EnableSlidingWindow(4, 10*time.Millisecond)
	defer r.Close()

	for i := 0; i < 3; i++ {
		r.Touch("pulse")
	}
	if freq := r.Frequency("pulse"); freq != 3 {
		t.Fatalf("expected initial window count 3, got %d", freq)
	}
	time.Sleep(60 * time.Millisecond)
	if freq := r.Frequency("pulse"); freq != 0 {
		t.Fatalf("expected sliding window to decay to 0, got %d", freq)
	}
}

func TestHotRingDecayLoop(t *testing.T) {
	r := NewHotRing(4, nil)
	r.EnableDecay(10*time.Millisecond, 1)
	defer r.Close()

	for i := 0; i < 8; i++ {
		r.Touch("decay-key")
	}
	time.Sleep(35 * time.Millisecond)
	if freq := r.Frequency("decay-key"); freq >= 8 {
		t.Fatalf("expected decay to reduce count, still %d", freq)
	}
}
