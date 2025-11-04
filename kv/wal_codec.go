package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"sync"
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

type WalHeader struct {
	KeyLen    uint32
	ValueLen  uint32
	Meta      byte
	ExpiresAt uint64
}

const maxHeaderSize int = 4 * binary.MaxVarintLen64

func (h WalHeader) Encode(out []byte) int {
	index := 0
	index = binary.PutUvarint(out[index:], uint64(h.KeyLen))
	index += binary.PutUvarint(out[index:], uint64(h.ValueLen))
	index += binary.PutUvarint(out[index:], uint64(h.Meta))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

func (h *WalHeader) Decode(reader *HashReader) (int, error) {
	var err error

	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KeyLen = uint32(klen)

	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.ValueLen = uint32(vlen)

	meta, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.Meta = byte(meta)
	h.ExpiresAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.BytesRead, nil
}

// WalCodec writes the WAL entry encoding into the provided buffer.
// Layout: | header | key | value | crc32 |
func WalCodec(buf *bytes.Buffer, e *Entry) int {
	buf.Reset()
	n, err := EncodeEntryTo(buf, e)
	if err != nil {
		panic(err)
	}
	return n
}

// EncodeEntryTo streams the WAL entry encoding directly into the provided writer.
// Layout: | header | key | value | crc32 |
func EncodeEntryTo(w io.Writer, e *Entry) (int, error) {
	h := WalHeader{
		KeyLen:    uint32(len(e.Key)),
		ValueLen:  uint32(len(e.Value)),
		Meta:      e.Meta,
		ExpiresAt: e.ExpiresAt,
	}

	var headerEnc [maxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	if sz > len(headerEnc) {
		return 0, fmt.Errorf("wal header overflow: sz=%d cap=%d key=%d val=%d meta=%d expires=%d", sz, len(headerEnc), len(e.Key), len(e.Value), h.Meta, h.ExpiresAt)
	}

	crc := CRC32()
	defer PutCRC32(crc)
	if _, err := crc.Write(headerEnc[:sz]); err != nil {
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

	if err := write(headerEnc[:sz]); err != nil {
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

// EstimateWalCodecSize estimates the encoded size of an entry in the WAL.
func EstimateWalCodecSize(e *Entry) int {
	return len(e.Key) + len(e.Value) + 8 /* ExpiresAt uint64 */ +
		crc32.Size + maxHeaderSize
}
