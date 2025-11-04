package kv

import (
	"testing"

)

func TestEncodeDecodeColumnFamilyKey(t *testing.T) {
	key := []byte("alpha")
	encoded := EncodeKeyWithCF(CFWrite, key)
	cf, userKey, ok := DecodeKeyCF(encoded)
	if !ok {
		t.Fatalf("expected encoded key to carry marker")
	}
	if cf != CFWrite {
		t.Fatalf("expected CFWrite, got %v", cf)
	}
	if string(userKey) != string(key) {
		t.Fatalf("expected user key %q, got %q", key, userKey)
	}

	cf2, userKey2, ts := SplitInternalKey(InternalKey(CFLock, key, 42))
	if cf2 != CFLock {
		t.Fatalf("expected CFLock, got %v", cf2)
	}
	if string(userKey2) != string(key) {
		t.Fatalf("unexpected user key %q", userKey2)
	}
	if ts != 42 {
		t.Fatalf("expected ts 42, got %d", ts)
	}
}
