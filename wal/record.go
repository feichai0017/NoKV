package wal

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
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

// DecodeRecord is part of the exported package API.
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
		return 0, nil, 0, utils.ErrEmptyRecord
	}

	// Allocate buffer for type byte + payload.
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, 0, utils.ErrPartialRecord
		}
		return 0, nil, 0, err
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, 0, utils.ErrPartialRecord
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

// EncodeRecord is part of the exported package API.
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
