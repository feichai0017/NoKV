package proof

import (
	"crypto/sha256"
	"encoding/binary"
)

type digestBuilder struct {
	stack [512]byte
	off   int
	heap  []byte
}

func newDigestBuilder() digestBuilder {
	return digestBuilder{}
}

func (b *digestBuilder) writeString(value string) {
	b.writeUint64(uint64(len(value)))
	b.writeRawString(value)
}

func (b *digestBuilder) writeBytes(value []byte) {
	b.writeUint64(uint64(len(value)))
	b.writeRaw(value)
}

func (b *digestBuilder) writeRaw(value []byte) {
	if len(value) == 0 {
		return
	}
	if b.heap != nil {
		b.heap = append(b.heap, value...)
		return
	}
	if len(value) <= len(b.stack)-b.off {
		copy(b.stack[b.off:], value)
		b.off += len(value)
		return
	}
	b.spill(len(value))
	b.heap = append(b.heap, value...)
}

func (b *digestBuilder) writeRawString(value string) {
	if len(value) == 0 {
		return
	}
	if b.heap != nil {
		b.heap = append(b.heap, value...)
		return
	}
	if len(value) <= len(b.stack)-b.off {
		b.off += copy(b.stack[b.off:], value)
		return
	}
	b.spill(len(value))
	b.heap = append(b.heap, value...)
}

func (b *digestBuilder) writeUint64(value uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	b.writeRaw(buf[:])
}

func (b *digestBuilder) writeBool(value bool) {
	if value {
		b.writeUint64(1)
		return
	}
	b.writeUint64(0)
}

func (b *digestBuilder) writeByte(value byte) {
	if b.heap != nil {
		b.heap = append(b.heap, value)
		return
	}
	if b.off < len(b.stack) {
		b.stack[b.off] = value
		b.off++
		return
	}
	b.spill(1)
	b.heap = append(b.heap, value)
}

func (b *digestBuilder) sum() [32]byte {
	if b.heap != nil {
		return sha256.Sum256(b.heap)
	}
	return sha256.Sum256(b.stack[:b.off])
}

func (b *digestBuilder) spill(extra int) {
	needed := b.off + extra
	capacity := len(b.stack) * 2
	if needed > capacity {
		capacity = needed
	}
	b.heap = make([]byte, b.off, capacity)
	copy(b.heap, b.stack[:b.off])
}
