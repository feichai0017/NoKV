package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/feichai0017/NoKV/utils"
)

// EncodeEntry encodes an entry using utils.WalCodec and returns a copy of the bytes.
func EncodeEntry(buf *bytes.Buffer, e *utils.Entry) []byte {
	if buf == nil {
		buf = &bytes.Buffer{}
	}
	buf.Reset()
	sz := utils.WalCodec(buf, e)
	out := make([]byte, sz)
	copy(out, buf.Bytes()[:sz])
	return out
}

// DecodeEntry parses a WAL payload into an Entry.
func DecodeEntry(data []byte) (*utils.Entry, error) {
	reader := bytes.NewReader(data)
	hashReader := utils.NewHashReader(reader)

	var h utils.WalHeader
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

	e := &utils.Entry{}
	e.Key = buf[:h.KeyLen]
	e.Value = buf[h.KeyLen:]
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
