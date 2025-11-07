package kv

import (
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
	atomic.AddInt32(&e.ref, 1)
}

// DecrRef decrements the entry reference count and releases it to the pool when it reaches zero.
func (e *Entry) DecrRef() {
	nRef := atomic.AddInt32(&e.ref, -1)
	if nRef > 0 {
		return
	}
	e.reset()
	EntryPool.Put(e)
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
}

// NewEntry creates a new entry in the default column family.
func NewEntry(key, value []byte) *Entry {
	return NewEntryWithCF(CFDefault, key, value)
}

// NewEntryWithCF creates an Entry for the specified column family.
func NewEntryWithCF(cf ColumnFamily, key, value []byte) *Entry {
	e := EntryPool.Get().(*Entry)
	e.Key = key
	e.Value = value
	if !cf.Valid() {
		cf = CFDefault
	}
	e.CF = cf
	e.IncrRef()
	return e
}

// Entry returns itself. It is kept for compatibility with iterator interfaces.
func (e *Entry) Entry() *Entry {
	return e
}

// IsDeletedOrExpired reports whether the entry is a tombstone or has passed its expiry.
func (e *Entry) IsDeletedOrExpired() bool {
	if e.Value == nil {
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

// WithColumnFamily sets the column family for the entry.
func (e *Entry) WithColumnFamily(cf ColumnFamily) *Entry {
	if e == nil {
		return e
	}
	if !cf.Valid() {
		cf = CFDefault
	}
	e.CF = cf
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
