package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

// EncodeEntry encodes an entry using kv.WalCodec and returns the encoded bytes.
// Callers should append the returned slice before reusing the provided buffer.
//
// Binary Format:
// +----------------+----------------+------+-------+----------+
// | Key Length (v) | Val Length (v) | Meta | ExpAt | Key      |
// +----------------+----------------+------+-------+----------+
// | Value          | Checksum (4B)  |
// +----------------+----------------+
// (v) denotes Uvarint encoding.
func EncodeEntry(buf *bytes.Buffer, e *kv.Entry) []byte {
	if buf == nil {
		buf = &bytes.Buffer{}
	}
	sz := kv.WalCodec(buf, e)
	data := buf.Bytes()
	if len(data) >= sz {
		return data[:sz]
	}
	// Should not happen, but fall back to a copy to avoid panics.
	out := make([]byte, sz)
	copy(out, data)
	return out
}

// DecodeEntry parses a WAL payload into an Entry.
//
// Binary Format:
// +----------------+----------------+------+-------+----------+
// | Key Length (v) | Val Length (v) | Meta | ExpAt | Key      |
// +----------------+----------------+------+-------+----------+
// | Value          | Checksum (4B)  |
// +----------------+----------------+
// (v) denotes Uvarint encoding.
func DecodeEntry(data []byte) (*kv.Entry, error) {
	reader := bytes.NewReader(data)
	hashReader := kv.NewHashReader(reader)

	var h kv.WalHeader
	if _, err := h.Decode(hashReader); err != nil {
		return nil, err
	}
	if h.KeyLen > uint32(len(data)) || h.ValueLen > uint32(len(data)) {
		return nil, io.ErrUnexpectedEOF
	}

	buf := make([]byte, h.KeyLen+h.ValueLen)
	if _, err := io.ReadFull(hashReader, buf); err != nil {
		return nil, err
	}

	e := kv.NewEntry(buf[:h.KeyLen], buf[h.KeyLen:])
	e.ExpiresAt = h.ExpiresAt
	e.Meta = h.Meta

	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		return nil, err
	}
	expected := binary.BigEndian.Uint32(crcBuf[:])
	if expected != hashReader.Sum32() {
		return nil, utils.ErrBadChecksum
	}
	return e, nil
}

// DecodeValueSlice parses a value log payload and returns a slice referencing the encoded value.
// The returned slice aliases the provided data. Callers must not use it after invoking the callback
// returned by vlog.Manager.Read.
//
// Binary Format:
// +----------------+----------------+------+-------+----------+
// | Key Length (v) | Val Length (v) | Meta | ExpAt | Key      |
// // +----------------+----------------+------+-------+----------+
// | Value          | Checksum (4B)  |
// +----------------+----------------+
// (v) denotes Uvarint encoding.
func DecodeValueSlice(data []byte) ([]byte, kv.WalHeader, error) {
	var h kv.WalHeader
	var idx int

	readVarint := func() (uint64, error) {
		val, n := binary.Uvarint(data[idx:])
		if n > 0 {
			idx += n
			return val, nil
		}
		return 0, io.ErrUnexpectedEOF
	}

	keyLen, err := readVarint()
	if err != nil {
		return nil, kv.WalHeader{}, err
	}
	h.KeyLen = uint32(keyLen)

	valueLen, err := readVarint()
	if err != nil {
		return nil, kv.WalHeader{}, err
	}
	h.ValueLen = uint32(valueLen)

	meta, err := readVarint()
	if err != nil {
		return nil, kv.WalHeader{}, err
	}
	if meta > 255 {
		return nil, kv.WalHeader{}, io.ErrUnexpectedEOF
	}
	h.Meta = byte(meta)

	expiresAt, err := readVarint()
	if err != nil {
		return nil, kv.WalHeader{}, err
	}
	h.ExpiresAt = expiresAt

	totalLen := int(h.KeyLen) + int(h.ValueLen)
	if totalLen < 0 {
		return nil, kv.WalHeader{}, io.ErrUnexpectedEOF
	}
	payloadEnd := idx + totalLen
	checksumEnd := payloadEnd + crc32.Size
	if payloadEnd < idx || checksumEnd > len(data) {
		return nil, kv.WalHeader{}, io.ErrUnexpectedEOF
	}

	expected := binary.BigEndian.Uint32(data[payloadEnd:checksumEnd])
	actual := crc32.Checksum(data[:payloadEnd], kv.CastagnoliCrcTable)
	if expected != actual {
		return nil, kv.WalHeader{}, utils.ErrBadChecksum
	}

	valueStart := idx + int(h.KeyLen)
	return data[valueStart:payloadEnd], h, nil
}
