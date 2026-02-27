package hotring

import (
	"testing"
	"time"
)

func TestRotatingHotRingRotate(t *testing.T) {
	r := NewRotatingHotRing(4, nil)
	r.EnableNodeSampling(1, 0)
	defer r.Close()

	if count := r.Touch("alpha"); count != 1 {
		t.Fatalf("expected initial count 1, got %d", count)
	}
	if count := r.Touch("alpha"); count != 2 {
		t.Fatalf("expected second touch to reach 2, got %d", count)
	}
	if freq := r.Frequency("alpha"); freq != 2 {
		t.Fatalf("expected frequency 2, got %d", freq)
	}

	r.Rotate()

	if freq := r.Frequency("alpha"); freq != 2 {
		t.Fatalf("expected frequency preserved by warm ring, got %d", freq)
	}

	if count := r.Touch("alpha"); count != 2 {
		t.Fatalf("expected touch to return max count 2, got %d", count)
	}
	if top := r.TopN(1); len(top) != 1 || top[0].Count != 3 {
		t.Fatalf("expected top count 3 after merge, got %+v", top)
	}

	stats := r.Stats()
	if stats.NodeCap != 1 {
		t.Fatalf("expected node cap 1 after rotation, got %d", stats.NodeCap)
	}

	r.Rotate()
	if freq := r.Frequency("alpha"); freq != 1 {
		t.Fatalf("expected frequency preserved from last active ring, got %d", freq)
	}
}

func TestRotatingHotRingEnableRotation(t *testing.T) {
	r := NewRotatingHotRing(4, nil)
	defer r.Close()

	r.EnableRotation(10 * time.Millisecond)
	time.Sleep(35 * time.Millisecond)

	stats := r.RotationStats()
	if stats.Rotations == 0 {
		t.Fatalf("expected at least one rotation, got %d", stats.Rotations)
	}
}

func TestRotatingHotRingClampMax(t *testing.T) {
	r := NewRotatingHotRing(4, nil)
	defer r.Close()

	r.Touch("hot")
	r.Touch("hot")
	r.Rotate()

	if count, limited := r.TouchAndClamp("hot", 2); count != 2 || !limited {
		t.Fatalf("expected clamp based on warm ring, got count=%d limited=%v", count, limited)
	}
	if count, limited := r.TouchAndClamp("hot", 3); count != 2 || limited {
		t.Fatalf("expected combined count 2 limited=false, got count=%d limited=%v", count, limited)
	}
	if freq := r.Frequency("hot"); freq != 2 {
		t.Fatalf("expected frequency max 2, got %d", freq)
	}
	if top := r.TopN(1); len(top) != 1 || top[0].Count != 4 {
		t.Fatalf("expected top count 4 after active refresh, got %+v", top)
	}
}
