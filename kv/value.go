package kv

import (
	"encoding/binary"
	"time"
	"unsafe"
)

const (
	// ValueLogHeaderSize is the size of the fixed vlog header.
	// +----------------+------------------+
	// | keyID(8 bytes) |  baseIV(12 bytes)|
	// +----------------+------------------+
	ValueLogHeaderSize    = 20
	valuePtrEncodedSize   = 12
)

// ValueStruct is the serialized form of a value (inline or pointer) used inside
// Entry.Value, SST blocks, WAL/value log payloads, etc.
//
// Binary layout (EncodeValue):
//
//	+------+-------------+-------------+
//	| Meta | ExpiresAt   | Value bytes |
//	+------+-------------+-------------+
//	| 1B   | varint(u64) | raw payload |
//	+------+-------------+-------------+
//
// Meta bits include deletion/value-pointer flags. When Meta has BitValuePointer
// set, Value bytes contains ValuePtr.Encode() output rather than user data.
type ValueStruct struct {
	Meta      byte
	Value     []byte
	ExpiresAt uint64

	Version uint64 // This field is not serialized. Only for internal usage.
}

// EncodedSize returns the size of the encoded value structure.
func (vs *ValueStruct) EncodedSize() uint32 {
	sz := len(vs.Value) + 1 // meta
	enc := sizeVarint(vs.ExpiresAt)
	return uint32(sz + enc)
}

// DecodeValue decodes the provided buffer into the value structure.
func (vs *ValueStruct) DecodeValue(buf []byte) {
	vs.Meta = buf[0]
	var sz int
	vs.ExpiresAt, sz = binary.Uvarint(buf[1:])
	vs.Value = buf[1+sz:]
}

// EncodeValue encodes the value structure into the provided buffer and returns the bytes written.
func (vs *ValueStruct) EncodeValue(b []byte) uint32 {
	b[0] = vs.Meta
	sz := binary.PutUvarint(b[1:], vs.ExpiresAt)
	n := copy(b[1+sz:], vs.Value)
	return uint32(1 + sz + n)
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

// ValuePtr points to a value stored in the value log and replaces the inline
// Value when entries exceed ValueThreshold. Its binary encoding is a fixed-size
// struct copied by value (see Encode/Decode):
//
//	+------+--------+-----+
//	| Len  | Offset | Fid |
//	+------+--------+-----+
//	| 4B   | 4B     | 4B  |
//	+------+--------+-----+
type ValuePtr struct {
	Len    uint32
	Offset uint32
	Fid    uint32
}

func (p ValuePtr) Less(o *ValuePtr) bool {
	if o == nil {
		return false
	}
	if p.Fid != o.Fid {
		return p.Fid < o.Fid
	}
	if p.Offset != o.Offset {
		return p.Offset < o.Offset
	}
	return p.Len < o.Len
}

func (p ValuePtr) IsZero() bool {
	return p.Fid == 0 && p.Offset == 0 && p.Len == 0
}

// Encode encodes the pointer using fixed big-endian fields to remain portable across architectures.
func (p ValuePtr) Encode() []byte {
	b := make([]byte, valuePtrEncodedSize)
	binary.BigEndian.PutUint32(b[0:4], p.Len)
	binary.BigEndian.PutUint32(b[4:8], p.Offset)
	binary.BigEndian.PutUint32(b[8:12], p.Fid)
	return b
}

// Decode decodes the pointer from a fixed big-endian byte slice.
func (p *ValuePtr) Decode(b []byte) {
	if len(b) < valuePtrEncodedSize {
		*p = ValuePtr{}
		return
	}
	p.Len = binary.BigEndian.Uint32(b[0:4])
	p.Offset = binary.BigEndian.Uint32(b[4:8])
	p.Fid = binary.BigEndian.Uint32(b[8:12])
}

func IsValuePtr(e *Entry) bool {
	return e.Meta&BitValuePointer > 0
}

// BytesToU32 converts the given byte slice to uint32
func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// BytesToU64 converts the given byte slice to uint64
func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// U32SliceToBytes converts the given Uint32 slice to byte slice
func U32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&u32s[0])), len(u32s)*4)
}

// U32ToBytes converts the given Uint32 to bytes
func U32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.BigEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

// U64ToBytes converts the given Uint64 to bytes
func U64ToBytes(v uint64) []byte {
	var uBuf [8]byte
	binary.BigEndian.PutUint64(uBuf[:], v)
	return uBuf[:]
}

// BytesToU32Slice converts the given byte slice to uint32 slice
func BytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*uint32)(unsafe.Pointer(&b[0])), len(b)/4)
}

// RunCallback runs callback if not nil.
func RunCallback(cb func()) {
	if cb != nil {
		cb()
	}
}

func IsDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if meta&BitDelete > 0 {
		return true
	}
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}

func DiscardEntry(e, vs *Entry) bool {
	if IsDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return true
	}
	if (vs.Meta & BitValuePointer) == 0 {
		return true
	}
	return false
}
