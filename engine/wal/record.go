package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/feichai0017/NoKV/engine/kv"
)

// RecordType identifies the kind of payload stored in the WAL.
type RecordType uint8

const (
	// RecordTypeEntry represents an LSM mutation (default behaviour).
	RecordTypeEntry RecordType = iota
	// RecordTypeRaftEntry encodes a batch of raft log entries.
	RecordTypeRaftEntry
	// RecordTypeRaftState encodes a raft HardState update.
	RecordTypeRaftState
	// RecordTypeRaftSnapshot encodes a raft snapshot payload.
	RecordTypeRaftSnapshot
	// RecordTypeEntryBatch encodes a batch of LSM mutations in one WAL record.
	RecordTypeEntryBatch
)

// Record describes a typed WAL payload.
//
// The WAL record is stored on disk in the following format:
//
//	+--------+-----------+-----------+---------+
//	| Length | Type      | Payload   | CRC32   |
//	| [4]byte| [1]byte   | [N]byte   | [4]byte |
//	+--------+-----------+-----------+---------+
//
// - Length: The length of the Type and Payload fields.
// - Type: The type of the record, as defined by RecordType.
// - Payload: The record's data.
// - CRC32: A CRC32 checksum of the Type and Payload.
type Record struct {
	Type    RecordType
	Payload []byte
}

func DecodeRecord(r io.Reader) (RecordType, []byte, uint32, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, 0, io.EOF
		}
		return 0, nil, 0, err
	}

	length := binary.BigEndian.Uint32(header[:])
	if length == 0 {
		return 0, nil, 0, ErrEmptyRecord
	}

	// Allocate buffer for type byte + payload.
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, 0, ErrPartialRecord
		}
		return 0, nil, 0, err
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, 0, ErrPartialRecord
		}
		return 0, nil, 0, err
	}

	expected := binary.BigEndian.Uint32(crcBuf[:])
	hasher := kv.CRC32()
	// Calculate CRC over type byte + payload.
	if _, err := hasher.Write(buf); err != nil {
		kv.PutCRC32(hasher)
		return 0, nil, 0, err
	}
	sum := hasher.Sum32()
	kv.PutCRC32(hasher)
	if expected != sum {
		return 0, nil, 0, kv.ErrBadChecksum
	}

	recType := RecordType(buf[0])
	payload := buf[1:] // Payload is the rest after the type byte.

	return recType, payload, length, nil
}

func EncodeRecord(w io.Writer, recType RecordType, payload []byte) (int, error) {
	total := len(payload) + 1 // Type byte + payload length.
	length := uint32(total)

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], length)
	if _, err := w.Write(hdr[:]); err != nil {
		return 0, err
	}

	typeByte := byte(recType)
	if _, err := w.Write([]byte{typeByte}); err != nil {
		return 0, err
	}

	if _, err := w.Write(payload); err != nil {
		return 0, err
	}

	hasher := kv.CRC32()
	typeBuf := [1]byte{typeByte}
	if _, err := hasher.Write(typeBuf[:]); err != nil {
		kv.PutCRC32(hasher)
		return 0, err
	}
	if _, err := hasher.Write(payload); err != nil {
		kv.PutCRC32(hasher)
		return 0, err
	}
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], hasher.Sum32())
	kv.PutCRC32(hasher)
	if _, err := w.Write(crcBuf[:]); err != nil {
		return 0, err
	}

	return int(length) + 8, nil // length + 4 bytes for header + 4 bytes for CRC.
}

// EncodeEntryBatch encodes entries into a single payload suitable for RecordTypeEntryBatch.
//
// Payload layout:
//
//	+--------------+------------------------------+
//	| entry_count  | repeated(entry_len + entry) |
//	| uint32 (BE)  | uint32 (BE) + raw bytes     |
//	+--------------+------------------------------+
func EncodeEntryBatch(entries []*kv.Entry) ([]byte, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("wal: empty entry batch")
	}
	var out bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(entries)))
	if _, err := out.Write(header[:]); err != nil {
		return nil, err
	}
	var entryBuf bytes.Buffer
	for _, e := range entries {
		if e == nil || len(e.Key) == 0 {
			return nil, fmt.Errorf("wal: invalid entry in batch")
		}
		payload, err := kv.EncodeEntry(&entryBuf, e)
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
		if _, err := out.Write(header[:]); err != nil {
			return nil, err
		}
		if _, err := out.Write(payload); err != nil {
			return nil, err
		}
	}
	return out.Bytes(), nil
}

// DecodeEntryBatch decodes a RecordTypeEntryBatch payload.
// Returned entries are pooled objects with refcount 1; caller must DecrRef().
func DecodeEntryBatch(payload []byte) ([]*kv.Entry, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("wal: malformed entry batch payload")
	}
	count := binary.BigEndian.Uint32(payload[:4])
	rest := payload[4:]
	if count == 0 {
		return nil, fmt.Errorf("wal: malformed entry batch payload")
	}
	// Each batch element must at least contain a 4-byte length field and
	// a non-empty encoded entry payload.
	if uint64(count) > uint64(len(rest))/5 {
		return nil, fmt.Errorf("wal: malformed entry batch payload")
	}
	maxInt := int(^uint(0) >> 1)
	if uint64(count) > uint64(maxInt) {
		return nil, fmt.Errorf("wal: malformed entry batch payload")
	}
	prealloc := min(int(count), 1024)
	entries := make([]*kv.Entry, 0, prealloc)
	defer func() {
		if rest != nil {
			for _, e := range entries {
				e.DecrRef()
			}
		}
	}()
	for range count {
		if len(rest) < 4 {
			return nil, fmt.Errorf("wal: malformed entry batch payload")
		}
		ln := binary.BigEndian.Uint32(rest[:4])
		rest = rest[4:]
		if ln == 0 || uint32(len(rest)) < ln {
			return nil, fmt.Errorf("wal: malformed entry batch payload")
		}
		entry, err := kv.DecodeEntry(rest[:ln])
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
		rest = rest[ln:]
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("wal: malformed entry batch payload")
	}
	decoded := entries
	rest = nil
	return decoded, nil
}
