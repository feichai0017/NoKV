package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"sync"
)

var (
	// ErrBadChecksum indicates a mismatch between the stored CRC32 and the computed checksum.
	ErrBadChecksum = errors.New("bad check sum")
)

var crc32Pool = sync.Pool{
	New: func() any {
		return crc32.New(CastagnoliCrcTable)
	},
}

// CRC32 returns a Castagnoli hash from the shared pool.
func CRC32() hash.Hash32 {
	h := crc32Pool.Get().(hash.Hash32)
	h.Reset()
	return h
}

// PutCRC32 returns a hash to the pool.
func PutCRC32(h hash.Hash32) {
	if h == nil {
		return
	}
	h.Reset()
	crc32Pool.Put(h)
}

// LogEntry represents the callback signature used during WAL/vlog replay.
type LogEntry func(e *Entry, vp *ValuePtr) error

// EntryHeader is the unified entry header used by WAL and value log encodings.
type EntryHeader struct {
	KeyLen    uint32
	ValueLen  uint32
	Meta      byte
	ExpiresAt uint64
}

// MaxEntryHeaderSize defines the maximum number of bytes required to encode an EntryHeader.
const MaxEntryHeaderSize int = 4 * binary.MaxVarintLen64

// Encode serializes the header using uvarint encoding for each field.
func (h EntryHeader) Encode(out []byte) int {
	idx := 0
	idx = binary.PutUvarint(out[idx:], uint64(h.KeyLen))
	idx += binary.PutUvarint(out[idx:], uint64(h.ValueLen))
	idx += binary.PutUvarint(out[idx:], uint64(h.Meta))
	idx += binary.PutUvarint(out[idx:], h.ExpiresAt)
	return idx
}

// DecodeFrom consumes the header from a HashReader.
func (h *EntryHeader) DecodeFrom(reader *HashReader) (int, error) {
	readVarint := func() (uint64, error) {
		return binary.ReadUvarint(reader)
	}

	klen, err := readVarint()
	if err != nil {
		return 0, err
	}
	h.KeyLen = uint32(klen)

	vlen, err := readVarint()
	if err != nil {
		return 0, err
	}
	h.ValueLen = uint32(vlen)

	meta, err := readVarint()
	if err != nil {
		return 0, err
	}
	if meta > math.MaxUint8 {
		return 0, fmt.Errorf("entry header meta overflow: %d", meta)
	}
	h.Meta = byte(meta)

	expiresAt, err := readVarint()
	if err != nil {
		return 0, err
	}
	h.ExpiresAt = expiresAt
	return reader.BytesRead, nil
}

// Decode parses the header directly from the provided byte slice.
func (h *EntryHeader) Decode(buf []byte) (int, error) {
	idx := 0
	readVarint := func() (uint64, error) {
		if idx >= len(buf) {
			return 0, io.ErrUnexpectedEOF
		}
		val, n := binary.Uvarint(buf[idx:])
		if n <= 0 {
			return 0, io.ErrUnexpectedEOF
		}
		idx += n
		return val, nil
	}

	klen, err := readVarint()
	if err != nil {
		return 0, err
	}
	h.KeyLen = uint32(klen)

	vlen, err := readVarint()
	if err != nil {
		return 0, err
	}
	h.ValueLen = uint32(vlen)

	meta, err := readVarint()
	if err != nil {
		return 0, err
	}
	if meta > math.MaxUint8 {
		return 0, fmt.Errorf("entry header meta overflow: %d", meta)
	}
	h.Meta = byte(meta)

	expiresAt, err := readVarint()
	if err != nil {
		return 0, err
	}
	h.ExpiresAt = expiresAt
	return idx, nil
}

// EncodeEntry writes the WAL/value log entry encoding into the provided buffer and
// returns the encoded payload. The returned slice aliases the supplied buffer
// when possible.
// Layout: | header | key | value | crc32 |
func EncodeEntry(buf *bytes.Buffer, e *Entry) ([]byte, error) {
	if buf == nil {
		buf = &bytes.Buffer{}
	}
	buf.Reset()
	n, err := EncodeEntryTo(buf, e)
	if err != nil {
		return nil, err
	}
	out := buf.Bytes()
	if len(out) >= n {
		return out[:n], nil
	}
	dup := make([]byte, n)
	copy(dup, out)
	return dup, nil
}

// EncodeEntryTo streams the entry encoding directly into the provided writer.
// Layout: | header | key | value | crc32 |
func EncodeEntryTo(w io.Writer, e *Entry) (int, error) {
	header := EntryHeader{
		KeyLen:    uint32(len(e.Key)),
		ValueLen:  uint32(len(e.Value)),
		Meta:      e.Meta,
		ExpiresAt: e.ExpiresAt,
	}

	var headerBuf [MaxEntryHeaderSize]byte
	sz := header.Encode(headerBuf[:])
	if sz > len(headerBuf) {
		return 0, fmt.Errorf("entry header overflow: sz=%d cap=%d key=%d val=%d meta=%d expires=%d", sz, len(headerBuf), len(e.Key), len(e.Value), header.Meta, header.ExpiresAt)
	}

	crc := CRC32()
	defer PutCRC32(crc)
	if _, err := crc.Write(headerBuf[:sz]); err != nil {
		return 0, err
	}

	total := 0
	write := func(b []byte) error {
		if len(b) == 0 {
			return nil
		}
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n != len(b) {
			return io.ErrShortWrite
		}
		total += n
		return nil
	}
	writeSection := func(b []byte) error {
		if err := write(b); err != nil {
			return err
		}
		if _, err := crc.Write(b); err != nil {
			return err
		}
		return nil
	}

	if err := write(headerBuf[:sz]); err != nil {
		return 0, err
	}
	if err := writeSection(e.Key); err != nil {
		return 0, err
	}
	if err := writeSection(e.Value); err != nil {
		return 0, err
	}

	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc.Sum32())
	if err := write(crcBuf[:]); err != nil {
		return 0, err
	}

	return total, nil
}

// EstimateEncodeSize estimates the encoded size of an entry in the WAL/value log.
func EstimateEncodeSize(e *Entry) int {
	return len(e.Key) + len(e.Value) + 8 /* ExpiresAt uint64 */ +
		crc32.Size + MaxEntryHeaderSize
}

// DecodeEntry parses a WAL/value log payload into an Entry instance.
func DecodeEntry(data []byte) (*Entry, error) {
	reader := bytes.NewReader(data)
	hashReader := NewHashReader(reader)

	var header EntryHeader
	if _, err := header.DecodeFrom(hashReader); err != nil {
		return nil, err
	}

	if header.KeyLen > uint32(len(data)) || header.ValueLen > uint32(len(data)) {
		return nil, io.ErrUnexpectedEOF
	}

	buf := make([]byte, header.KeyLen+header.ValueLen)
	if _, err := io.ReadFull(hashReader, buf); err != nil {
		return nil, err
	}

	e := NewEntry(buf[:header.KeyLen], buf[header.KeyLen:])
	e.ExpiresAt = header.ExpiresAt
	e.Meta = header.Meta

	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		return nil, err
	}
	expected := binary.BigEndian.Uint32(crcBuf[:])
	if expected != hashReader.Sum32() {
		return nil, ErrBadChecksum
	}
	return e, nil
}

// DecodeValueSlice parses a value log payload and returns a slice referencing the encoded value.
// The returned slice aliases the provided data.
func DecodeValueSlice(data []byte) ([]byte, EntryHeader, error) {
	var header EntryHeader
	idx, err := header.Decode(data)
	if err != nil {
		return nil, EntryHeader{}, err
	}

	totalLen := int(header.KeyLen) + int(header.ValueLen)
	if totalLen < 0 {
		return nil, EntryHeader{}, io.ErrUnexpectedEOF
	}
	payloadEnd := idx + totalLen
	checksumEnd := payloadEnd + crc32.Size
	if payloadEnd < idx || checksumEnd > len(data) {
		return nil, EntryHeader{}, io.ErrUnexpectedEOF
	}

	expected := binary.BigEndian.Uint32(data[payloadEnd:checksumEnd])
	actual := crc32.Checksum(data[:payloadEnd], CastagnoliCrcTable)
	if expected != actual {
		return nil, EntryHeader{}, ErrBadChecksum
	}

	valueStart := idx + int(header.KeyLen)
	return data[valueStart:payloadEnd], header, nil
}