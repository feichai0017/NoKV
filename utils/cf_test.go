package utils

import (
	"testing"

	"github.com/feichai0017/NoKV/kv"
)

func TestEncodeDecodeColumnFamilyKey(t *testing.T) {
	key := []byte("alpha")
	encoded := kv.EncodeKeyWithCF(kv.CFWrite, key)
	cf, userKey, ok := kv.DecodeKeyCF(encoded)
	if !ok {
		t.Fatalf("expected encoded key to carry marker")
	}
	if cf != kv.CFWrite {
		t.Fatalf("expected CFWrite, got %v", cf)
	}
	if string(userKey) != string(key) {
		t.Fatalf("expected user key %q, got %q", key, userKey)
	}

	cf2, userKey2, ts := kv.SplitInternalKey(kv.InternalKey(kv.CFLock, key, 42))
	if cf2 != kv.CFLock {
		t.Fatalf("expected CFLock, got %v", cf2)
	}
	if string(userKey2) != string(key) {
		t.Fatalf("unexpected user key %q", userKey2)
	}
	if ts != 42 {
		t.Fatalf("expected ts 42, got %d", ts)
	}
}
