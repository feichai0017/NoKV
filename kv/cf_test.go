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

func TestColumnFamilyStringAndParse(t *testing.T) {
	if CFDefault.String() != "default" {
		t.Fatalf("expected default, got %s", CFDefault.String())
	}
	if CFLock.String() != "lock" {
		t.Fatalf("expected lock, got %s", CFLock.String())
	}
	if CFWrite.String() != "write" {
		t.Fatalf("expected write, got %s", CFWrite.String())
	}
	if ColumnFamily(99).String() == "" {
		t.Fatalf("expected unknown CF string to be non-empty")
	}

	cf, err := ParseColumnFamily("DEFAULT")
	if err != nil || cf != CFDefault {
		t.Fatalf("expected parse default, got %v err=%v", cf, err)
	}
	_, err = ParseColumnFamily("unknown")
	if err == nil {
		t.Fatalf("expected error for unknown CF")
	}
}
