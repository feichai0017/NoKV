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

func TestBytesToStringAndSafeCopy(t *testing.T) {
	if got := BytesToString(nil); got != "" {
		t.Fatalf("expected empty string for nil, got %q", got)
	}
	input := []byte("hello")
	if got := BytesToString(input); got != "hello" {
		t.Fatalf("expected hello, got %q", got)
	}
	orig := []byte("copy")
	out := SafeCopy(nil, orig)
	if string(out) != "copy" {
		t.Fatalf("expected copy, got %q", out)
	}
	orig[0] = 'C'
	if string(out) != "copy" {
		t.Fatalf("expected SafeCopy to detach from source")
	}
}
