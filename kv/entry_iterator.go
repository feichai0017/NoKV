package kv

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math"
)

// EntryIterator sequentially decodes entries stored using the unified
// value-log/WAL layout:
//   | header(varint fields) | key bytes | value bytes | crc32 (4B) |
// The header encodes key length, value length, meta, and expiresAt via
// uvarint encoding (see EntryHeader).
type EntryIterator struct {
	reader    *bufio.Reader
	current   *Entry
	recordLen uint32
	err       error
}

// NewEntryIterator constructs an iterator over the provided reader. The reader
// must contain records encoded via EncodeEntryTo in sequential order. Callers
// must Close the iterator to return pooled entries.
func NewEntryIterator(r io.Reader) *EntryIterator {
	if r == nil {
		return &EntryIterator{err: io.EOF}
	}
	if br, ok := r.(*bufio.Reader); ok {
		return &EntryIterator{reader: br}
	}
	return &EntryIterator{reader: bufio.NewReader(r)}
}

// Next advances to the next entry, returning true when an entry was decoded
// successfully. The returned entry maintains a reference count that must be
// released via Close or by advancing to the next record.
func (it *EntryIterator) Next() bool {
	if it == nil || it.err != nil {
		return false
	}
	it.releaseCurrent()

	hashReader := NewHashReader(it.reader)

	readVarint := func(allowEOF bool) (uint64, error) {
		val, err := binary.ReadUvarint(hashReader)
		if err == nil {
			return val, nil
		}
		switch {
		case errors.Is(err, io.EOF):
			if allowEOF {
				return 0, io.EOF
			}
			return 0, ErrPartialEntry
		case errors.Is(err, io.ErrUnexpectedEOF):
			return 0, ErrPartialEntry
		default:
			return 0, err
		}
	}

	klen, err := readVarint(true)
	if err != nil {
		if errors.Is(err, io.EOF) {
			it.err = io.EOF
			return false
		}
		it.err = err
		return false
	}
	vlen, err := readVarint(false)
	if err != nil {
		it.err = err
		return false
	}
	meta, err := readVarint(false)
	if err != nil {
		it.err = err
		return false
	}
	expiresAt, err := readVarint(false)
	if err != nil {
		it.err = err
		return false
	}

	if klen > math.MaxUint32 || vlen > math.MaxUint32 || meta > math.MaxUint8 {
		it.err = ErrPartialEntry
		return false
	}

	keyLen := int(klen)
	valLen := int(vlen)

	entry := EntryPool.Get().(*Entry)
	entry.IncrRef()
	entry.Version = 0
	entry.ValThreshold = 0
	entry.Hlen = hashReader.BytesRead
	entry.Offset = 0
	entry.Meta = byte(meta)
	entry.ExpiresAt = expiresAt

	if cap(entry.Key) < keyLen {
		entry.Key = make([]byte, keyLen)
	} else {
		entry.Key = entry.Key[:keyLen]
	}
	if _, err := io.ReadFull(hashReader, entry.Key); err != nil {
		entry.DecrRef()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			it.err = ErrPartialEntry
		} else {
			it.err = err
		}
		return false
	}

	if cap(entry.Value) < valLen {
		entry.Value = make([]byte, valLen)
	} else {
		entry.Value = entry.Value[:valLen]
	}
	if _, err := io.ReadFull(hashReader, entry.Value); err != nil {
		entry.DecrRef()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			it.err = ErrPartialEntry
		} else {
			it.err = err
		}
		return false
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(it.reader, crcBuf[:]); err != nil {
		entry.DecrRef()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			it.err = ErrPartialEntry
		} else {
			it.err = err
		}
		return false
	}
	if BytesToU32(crcBuf[:]) != hashReader.Sum32() {
		entry.DecrRef()
		it.err = ErrBadChecksum
		return false
	}

	it.current = entry
	it.recordLen = uint32(entry.Hlen) + uint32(len(entry.Key)) + uint32(len(entry.Value)) + 4
	return true
}

// Entry returns the current entry. It remains valid until the next call to Next
// or Close.
func (it *EntryIterator) Entry() *Entry {
	if it == nil {
		return nil
	}
	return it.current
}

// RecordLen reports the encoded length of the current record.
func (it *EntryIterator) RecordLen() uint32 {
	if it == nil {
		return 0
	}
	return it.recordLen
}

// Err returns the terminal error that stopped iteration. A nil or io.EOF error
// indicates successful exhaustion.
func (it *EntryIterator) Err() error {
	if it == nil {
		return nil
	}
	return it.err
}

// Close releases the current entry to the pool.
func (it *EntryIterator) Close() error {
	if it == nil {
		return nil
	}
	it.releaseCurrent()
	return nil
}

func (it *EntryIterator) releaseCurrent() {
	if it == nil || it.current == nil {
		return
	}
	it.current.DecrRef()
	it.current = nil
}
