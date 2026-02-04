package kv

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"unsafe"
)

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// ParseKey parses the actual key from the key bytes.
func ParseKey(key []byte) []byte {
	if len(key) < 8 {
		return key
	}
	return key[:len(key)-8]
}

// ParseTs parses the timestamp from the key bytes.
func ParseTs(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	return math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
}

// SameKey checks for key equality ignoring the version timestamp suffix.
func SameKey(src, dst []byte) bool {
	if len(src) != len(dst) {
		return false
	}
	return bytes.Equal(ParseKey(src), ParseKey(dst))
}

// KeyWithTs generates a new key by appending ts to key.
func KeyWithTs(key []byte, ts uint64) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], math.MaxUint64-ts)
	return out
}

// InternalKey encodes (column family, user key, timestamp) into the canonical
// on-disk layout used by the LSM:
//
//	+------------+----------+----------------------+
//	| CF marker  | User key | Timestamp (uint64 BE)|
//	+------------+----------+----------------------+
//	| 4 bytes    | raw      | 8 bytes (descending) |
//	+------------+----------+----------------------+
//
// CF marker uses 3 fixed bytes (0xFF,'C','F') plus the CF byte. Timestamp is
// bitwise inverted (^ts) so that newer versions sort before older ones.
func InternalKey(cf ColumnFamily, key []byte, ts uint64) []byte {
	if !cf.Valid() {
		cf = CFDefault
	}
	out := make([]byte, cfHeaderSize+len(key)+8)
	out[0] = cfMarker0
	out[1] = cfMarker1
	out[2] = cfMarker2
	out[3] = byte(cf)
	copy(out[cfHeaderSize:], key)
	binary.BigEndian.PutUint64(out[len(out)-8:], math.MaxUint64-ts)
	return out
}

// SplitInternalKey decodes (column family, user key, timestamp) from an internal
// key created by InternalKey.
func SplitInternalKey(internal []byte) (ColumnFamily, []byte, uint64) {
	ts := ParseTs(internal)
	base := ParseKey(internal)
	cf, userKey, _ := DecodeKeyCF(base)
	return cf, userKey, ts
}

// MemHash is the hash function used by go map, it utilizes available hardware instructions.
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

// MemHashString is the hash function used by go map, it utilizes available hardware instructions.
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

// BytesToString converts a byte slice to string without extra allocation.
// The caller must ensure the slice won't be mutated while the string is in use.
func BytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// SafeCopy does append(a[:0], src...).
func SafeCopy(a, src []byte) []byte {
	return append(a[:0], src...)
}

// ValueLogBucket returns the hash bucket for a key when using a bucketed value log.
// The hash is computed on the key without the MVCC timestamp suffix, so all
// versions of a key land in the same bucket.
func ValueLogBucket(key []byte, buckets uint32) uint32 {
	if buckets <= 1 || len(key) == 0 {
		return 0
	}
	base := ParseKey(key)
	hash := crc32.Checksum(base, CastagnoliCrcTable)
	return hash % buckets
}
