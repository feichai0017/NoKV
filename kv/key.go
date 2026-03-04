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

// MaxVersion is the canonical upper-bound MVCC version sentinel.
const MaxVersion uint64 = math.MaxUint64

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// StripTimestamp removes the trailing 8-byte timestamp suffix from an internal key.
// For non-internal keys (len<8) it returns the input unchanged.
func StripTimestamp(key []byte) []byte {
	if len(key) < 8 {
		return key
	}
	return key[:len(key)-8]
}

// Timestamp decodes the MVCC timestamp from the trailing 8-byte suffix.
// It returns 0 when the key is too short to carry a timestamp.
func Timestamp(key []byte) uint64 {
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
	return bytes.Equal(StripTimestamp(src), StripTimestamp(dst))
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

// SplitInternalKey decodes an internal key into (cf, userKey, ts).
// It returns ok=false when the key does not carry a valid CF marker + timestamp layout.
func SplitInternalKey(internal []byte) (ColumnFamily, []byte, uint64, bool) {
	if len(internal) <= 8 {
		return CFDefault, nil, 0, false
	}
	base := StripTimestamp(internal)
	ts := Timestamp(internal)
	cf, userKey, ok := DecodeKeyCF(base)
	if !ok {
		return CFDefault, nil, 0, false
	}
	return cf, userKey, ts, true
}

// MemHash is the hash function used by go map, it utilizes available hardware instructions.
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

// SafeCopy does append(a[:0], src...).
func SafeCopy(a, src []byte) []byte {
	return append(a[:0], src...)
}

// ValueLogBucket returns the hash bucket for a key when using a bucketed value log.
// The hash is computed on the key without the MVCC timestamp suffix, so all
// versions of a key land in the same bucket.
func ValueLogBucket(key []byte, buckets uint32) uint32 {
	if buckets <= 1 {
		return 0
	}
	return ValueLogBucketFromHash(ValueLogHash(key), buckets)
}

// ValueLogHash returns the stable hash used for value-log bucket routing.
// The hash is computed on the key without the MVCC timestamp suffix.
func ValueLogHash(key []byte) uint32 {
	if len(key) == 0 {
		return 0
	}
	base := StripTimestamp(key)
	if len(base) == 0 {
		return 0
	}
	return crc32.Checksum(base, CastagnoliCrcTable)
}

// ValueLogBucketFromHash maps a precomputed hash to a bucket index.
func ValueLogBucketFromHash(hash uint32, buckets uint32) uint32 {
	if buckets <= 1 {
		return 0
	}
	return hash % buckets
}
