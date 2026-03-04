package kv

import "testing"

func TestKeyHelpers(t *testing.T) {
	key := []byte("alpha")
	k1 := InternalKey(CFDefault, key, 1)
	k2 := InternalKey(CFDefault, key, 2)

	if !SameKey(k1, k2) {
		t.Fatalf("expected SameKey for different versions")
	}
	if SameKey(k1, InternalKey(CFDefault, []byte("beta"), 1)) {
		t.Fatalf("unexpected SameKey for different user keys")
	}

	if Timestamp(k1) != 1 {
		t.Fatalf("expected ts=1, got %d", Timestamp(k1))
	}

	h1 := MemHash(k1)
	h2 := MemHash(k1)
	if h1 != h2 {
		t.Fatalf("expected stable MemHash for same data")
	}
}

func TestSplitInternalKeyStrict(t *testing.T) {
	internal := InternalKey(CFWrite, []byte("alpha"), 42)
	cf, userKey, ts, ok := SplitInternalKey(internal)
	if !ok {
		t.Fatalf("expected strict decode success")
	}
	if cf != CFWrite {
		t.Fatalf("expected CFWrite, got %v", cf)
	}
	if string(userKey) != "alpha" {
		t.Fatalf("expected user key alpha, got %q", userKey)
	}
	if ts != 42 {
		t.Fatalf("expected ts=42, got %d", ts)
	}

	if _, _, _, ok := SplitInternalKey([]byte("raw-key")); ok {
		t.Fatalf("expected strict decode failure for raw key")
	}

	nonCanonical := append([]byte("plain"), make([]byte, 8)...)
	if _, _, _, ok := SplitInternalKey(nonCanonical); ok {
		t.Fatalf("expected strict decode failure without CF marker")
	}
}

func TestStripTimestampSplitInternalKeyAndSafeCopy(t *testing.T) {
	if got := StripTimestamp([]byte("raw")); string(got) != "raw" {
		t.Fatalf("expected raw key unchanged, got %q", got)
	}
	internal := InternalKey(CFLock, []byte("hello"), 5)
	_, userKey, _, ok := SplitInternalKey(internal)
	if !ok || string(userKey) != "hello" {
		t.Fatalf("expected split user key hello, got ok=%v key=%q", ok, userKey)
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
