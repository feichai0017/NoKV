package kv

import "testing"

func TestKeyHelpers(t *testing.T) {
	key := []byte("alpha")
	k1 := KeyWithTs(key, 1)
	k2 := KeyWithTs(key, 2)

	if !SameKey(k1, k2) {
		t.Fatalf("expected SameKey for different versions")
	}
	if SameKey(k1, KeyWithTs([]byte("beta"), 1)) {
		t.Fatalf("unexpected SameKey for different user keys")
	}

	if ParseTs(k1) != 1 {
		t.Fatalf("expected ts=1, got %d", ParseTs(k1))
	}

	h1 := MemHash(k1)
	h2 := MemHash(k1)
	if h1 != h2 {
		t.Fatalf("expected stable MemHash for same data")
	}

	hs1 := MemHashString("alpha")
	hs2 := MemHashString("alpha")
	if hs1 != hs2 {
		t.Fatalf("expected stable MemHashString for same input")
	}
}
