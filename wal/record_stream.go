package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"

	"github.com/feichai0017/NoKV/kv"
)

var (
	// ErrPartialRecord indicates that a record could not be fully read due to an
	// unexpected EOF. Callers typically truncate the tail when this occurs.
	ErrPartialRecord = errors.New("wal: partial record")
	// ErrEmptyRecord indicates that a record header advertised zero length.
	ErrEmptyRecord = errors.New("wal: empty record")
)

// RecordStream provides iterator-style access over WAL records stored in the
// | length (4B) | type (1B) | payload | crc32 (4B) | layout.
type RecordStream struct {
	reader  *bufio.Reader
	buffer  []byte
	recType RecordType
	length  uint32
	err     error
}

// NewRecordStream constructs a RecordStream using the provided reader.
func NewRecordStream(r io.Reader, bufSize int) *RecordStream {
	if r == nil {
		return &RecordStream{err: io.EOF}
	}
	br, ok := r.(*bufio.Reader)
	if !ok {
		if bufSize <= 0 {
			bufSize = defaultBufferSize
		}
		br = bufio.NewReaderSize(r, bufSize)
	}
	return &RecordStream{reader: br}
}

// Next advances the stream to the next record.
func (rs *RecordStream) Next() bool {
	if rs == nil || rs.err != nil {
		return false
	}
	rs.length = 0

	var header [4]byte
	if _, err := io.ReadFull(rs.reader, header[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			rs.err = io.EOF
		} else {
			rs.err = err
		}
		return false
	}

	length := binary.BigEndian.Uint32(header[:])
	if length == 0 {
		rs.err = ErrEmptyRecord
		return false
	}

	if cap(rs.buffer) < int(length) {
		rs.buffer = make([]byte, length)
	}
	buf := rs.buffer[:length]

	if _, err := io.ReadFull(rs.reader, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			rs.err = ErrPartialRecord
		} else {
			rs.err = err
		}
		return false
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(rs.reader, crcBuf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			rs.err = ErrPartialRecord
		} else {
			rs.err = err
		}
		return false
	}

	expected := binary.BigEndian.Uint32(crcBuf[:])
	hasher := kv.CRC32()
	if _, err := hasher.Write(buf[:1]); err != nil {
		kv.PutCRC32(hasher)
		rs.err = err
		return false
	}
	if _, err := hasher.Write(buf[1:]); err != nil {
		kv.PutCRC32(hasher)
		rs.err = err
		return false
	}
	sum := hasher.Sum32()
	kv.PutCRC32(hasher)
	if expected != sum {
		rs.err = kv.ErrBadChecksum
		return false
	}

	rs.recType = RecordType(buf[0])
	rs.length = length
	return true
}

// Payload returns the record payload excluding the record type byte.
func (rs *RecordStream) Payload() []byte {
	if rs == nil {
		return nil
	}
	if rs.length <= 1 {
		return nil
	}
	return append([]byte(nil), rs.buffer[1:rs.length]...)
}

// Type returns the record type of the current record.
func (rs *RecordStream) Type() RecordType {
	if rs == nil {
		return RecordTypeEntry
	}
	return rs.recType
}

// Length returns the encoded payload length (type byte + payload bytes).
func (rs *RecordStream) Length() uint32 {
	if rs == nil {
		return 0
	}
	return rs.length
}

// Err returns the terminal error for the stream.
func (rs *RecordStream) Err() error {
	if rs == nil {
		return nil
	}
	return rs.err
}

// Close releases resources held by the stream.
func (rs *RecordStream) Close() error {
	if rs == nil {
		return nil
	}
	rs.buffer = nil
	return nil
}
