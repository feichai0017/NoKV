package kv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var EntryPool = sync.Pool{
	New: func() any {
		return &Entry{}
	},
}

// Entry represents the top-level mutation record stored in WAL/vlog segments.
//
// Binary layout when encoded (via EncodeEntryTo):
//
//	+---------+------+--------+--------+
//	| Header  | Key  | Value  | CRC32  |
//	+---------+------+--------+--------+
//	| varints | raw  | raw    | 4 bytes|
//	+---------+------+--------+--------+
//
// Header encodes KeyLen, ValueLen, Meta, ExpiresAt using Uvarint. Key and Value
// are already in their internal encodings before hitting EncodeEntryTo:
//   - Key must be an InternalKey (see key.go) with CF marker + user key + timestamp.
//   - Value must be a ValueStruct encoding (meta + ExpiresAt + inline payload or ValuePtr).
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	CF ColumnFamily

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int // Length of the header.
	ValThreshold int64

	ref int32
}

// IncrRef increments the entry reference count.
func (e *Entry) IncrRef() {
	if e == nil {
		return
	}
	atomic.AddInt32(&e.ref, 1)
}

// DecrRef decrements the entry reference count and releases it to the pool when it reaches zero.
// It panics on refcount underflow (decrement past zero), which indicates a
// lifecycle bug in the caller.
func (e *Entry) DecrRef() {
	if e == nil {
		return
	}
	for {
		current := atomic.LoadInt32(&e.ref)
		if current <= 0 {
			panic(fmt.Sprintf("kv.Entry.DecrRef: refcount underflow (current_ref=%d)", current))
		}
		if !atomic.CompareAndSwapInt32(&e.ref, current, current-1) {
			continue
		}
		if current == 1 {
			e.reset()
			EntryPool.Put(e)
		}
		return
	}
}

func (e *Entry) reset() {
	e.Key = nil
	e.Value = nil
	e.ExpiresAt = 0
	e.CF = CFDefault
	e.Meta = 0
	e.Version = 0
	e.Offset = 0
	e.Hlen = 0
	e.ValThreshold = 0
	e.ref = 0
}

// NewEntry creates a lightweight entry from the provided key/value.
//
// It does not parse or validate key layout. CF is initialized to CFDefault and
// Version remains unset (0) until filled by a caller that knows version context.
func NewEntry(key, value []byte) *Entry {
	e := EntryPool.Get().(*Entry)
	e.Key = key
	e.Value = value
	e.CF = CFDefault
	e.IncrRef()
	return e
}

// NewInternalEntry creates an Entry whose key is encoded as an internal key.
//
// Ownership note: userKey is encoded into a newly allocated internal-key buffer,
// while value is referenced directly (no deep copy). Callers must keep value
// immutable until the entry is no longer used.
//
// This helper also sets CF and Version to the supplied MVCC context.
func NewInternalEntry(cf ColumnFamily, userKey []byte, version uint64, value []byte, meta byte, expiresAt uint64) *Entry {
	if !cf.Valid() {
		cf = CFDefault
	}
	e := EntryPool.Get().(*Entry)
	e.Key = InternalKey(cf, userKey, version)
	e.Value = value
	e.CF = cf
	e.Version = version
	e.Meta = meta
	e.ExpiresAt = expiresAt
	e.IncrRef()
	return e
}

// Entry returns itself to satisfy iterator item interfaces.
func (e *Entry) Entry() *Entry {
	return e
}

// PopulateInternalMeta parses e.Key as an internal key and fills CF/Version.
// It returns false when the key is not in canonical internal-key format.
func (e *Entry) PopulateInternalMeta() bool {
	if e == nil {
		return false
	}
	cf, _, ts, ok := SplitInternalKey(e.Key)
	if !ok {
		e.CF = CFDefault
		e.Version = 0
		return false
	}
	e.CF = cf
	e.Version = ts
	return true
}

// IsDeletedOrExpired reports whether the entry is a tombstone or has passed its expiry.
func (e *Entry) IsDeletedOrExpired() bool {
	if e == nil || e.Value == nil {
		return true
	}
	if e.Meta&BitDelete > 0 {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}
	return e.ExpiresAt <= uint64(time.Now().Unix())
}

// WithTTL sets the TTL for the entry.
func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

// EncodedSize is the size of the Entry value when encoded.
func (e *Entry) EncodedSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarint(uint64(e.Meta))
	enc += sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}

// EstimateSize estimates the size of the entry when stored inline versus via value log pointer.
func (e *Entry) EstimateSize(threshold int) int {
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 1 // Meta
	}
	return len(e.Key) + 12 + 1 // 12 for ValuePointer, 1 for meta.
}
