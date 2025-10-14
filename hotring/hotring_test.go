package hotring

import "testing"

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
