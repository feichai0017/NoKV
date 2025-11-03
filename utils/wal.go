package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"sync"
)

var (
	crc32Pool = sync.Pool{
		New: func() any {
			return crc32.New(CastagnoliCrcTable)
		},
	}
)

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

// LogEntry
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

// WalCodec 写入wal文件的编码
// | header | key | value | crc32 |
func WalCodec(buf *bytes.Buffer, e *Entry) int {
	buf.Reset()
	n, err := EncodeEntryTo(buf, e)
	if err != nil {
		panic(err)
	}
	return n
}

// EncodeEntryTo streams the WAL entry encoding directly into the provided writer.
// | header | key | value | crc32 |
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

	if err := write(headerEnc[:sz]); err != nil {
		return 0, err
	}
	if err := write(e.Key); err != nil {
		return 0, err
	}
	if _, err := crc.Write(e.Key); err != nil {
		return 0, err
	}
	if err := write(e.Value); err != nil {
		return 0, err
	}
	if _, err := crc.Write(e.Value); err != nil {
		return 0, err
	}

	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc.Sum32())
	if err := write(crcBuf[:]); err != nil {
		return 0, err
	}

	return total, nil
}

// EstimateWalCodecSize 预估当前kv 写入wal文件占用的空间大小
func EstimateWalCodecSize(e *Entry) int {
	return len(e.Key) + len(e.Value) + 8 /* ExpiresAt uint64 */ +
		crc32.Size + maxHeaderSize
}

type HashReader struct {
	R         io.Reader
	H         hash.Hash32
	BytesRead int // Number of bytes read.
}

func NewHashReader(r io.Reader) *HashReader {
	hash := crc32.New(CastagnoliCrcTable)
	return &HashReader{
		R: r,
		H: hash,
	}
}

// Read reads len(p) bytes from the reader. Returns the number of bytes read, error on failure.
func (t *HashReader) Read(p []byte) (int, error) {
	n, err := t.R.Read(p)
	if err != nil {
		return n, err
	}
	t.BytesRead += n
	return t.H.Write(p[:n])
}

// ReadByte reads exactly one byte from the reader. Returns error on failure.
func (t *HashReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := t.Read(b)
	return b[0], err
}

// Sum32 returns the sum32 of the underlying hash.
func (t *HashReader) Sum32() uint32 {
	return t.H.Sum32()
}

// IsZero _
func (e *Entry) IsZero() bool {
	return len(e.Key) == 0
}

// LogHeaderLen _
func (e *Entry) LogHeaderLen() int {
	return e.Hlen
}

// LogOffset _
func (e *Entry) LogOffset() uint32 {
	return e.Offset
}
