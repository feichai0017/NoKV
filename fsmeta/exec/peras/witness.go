// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

type WitnessReplica interface {
	ID() string
	AppendSegment(context.Context, compile.AuthorityScope, SegmentWitnessRecord) error
	Probe(context.Context, uint64) (WitnessSnapshot, error)
}

type WitnessRecordKind uint8

const WitnessRecordSegment WitnessRecordKind = 1

var witnessRecordMagic = [4]byte{'N', 'P', 'W', 2}

// SegmentWitnessRecord is the only durable Peras witness evidence. Individual
// metadata operations enter the holder overlay; witnesses persist the sealed
// authority-local segment, not individual operation records.
type SegmentWitnessRecord struct {
	EpochID              uint64
	SegmentRoot          [32]byte
	SegmentPayloadDigest [32]byte
	PredecessorDigest    [32]byte
	SegmentPayloadSize   uint64
	SegmentPointer       string
	SegmentPayload       []byte
	OperationCount       uint64
	EntryCount           uint64
	TimestampUnixNano    int64
	HolderID             string
}

type WitnessFrame struct {
	Kind    WitnessRecordKind
	Segment SegmentWitnessRecord
}

type WitnessSnapshot struct {
	Segments []SegmentWitnessRecord
}

func EncodeSegmentWitnessRecord(record SegmentWitnessRecord) ([]byte, error) {
	if err := validateSegmentWitnessRecord(record); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.Grow(segmentWitnessRecordEncodedSize(record))
	writeWitnessHeader(&out, WitnessRecordSegment)
	writeUint64(&out, record.EpochID)
	out.Write(record.SegmentRoot[:])
	out.Write(record.SegmentPayloadDigest[:])
	out.Write(record.PredecessorDigest[:])
	writeUint64(&out, record.SegmentPayloadSize)
	writeString(&out, record.SegmentPointer)
	writeBytes(&out, record.SegmentPayload)
	writeUint64(&out, record.OperationCount)
	writeUint64(&out, record.EntryCount)
	writeInt64(&out, record.TimestampUnixNano)
	writeString(&out, record.HolderID)
	return out.Bytes(), nil
}

func VerifySegmentWitnessRecord(record SegmentWitnessRecord) error {
	return validateSegmentWitnessRecord(record)
}

func DecodeWitnessFrame(payload []byte) (WitnessFrame, error) {
	r := witnessReader{buf: payload}
	kind, err := r.readHeader()
	if err != nil {
		return WitnessFrame{}, err
	}
	if kind != WitnessRecordSegment {
		return WitnessFrame{}, ErrInvalidWitnessRecord
	}
	record, err := r.readSegment()
	if err != nil {
		return WitnessFrame{}, err
	}
	if !r.done() {
		return WitnessFrame{}, ErrInvalidWitnessRecord
	}
	return WitnessFrame{Kind: kind, Segment: record}, nil
}

func validateSegmentWitnessRecord(record SegmentWitnessRecord) error {
	if record.EpochID == 0 || record.HolderID == "" || record.OperationCount == 0 || record.EntryCount == 0 || record.SegmentPayloadSize == 0 {
		return ErrInvalidWitnessRecord
	}
	if record.SegmentRoot == ([32]byte{}) || record.SegmentPayloadDigest == ([32]byte{}) {
		return ErrInvalidWitnessRecord
	}
	if len(record.SegmentPayload) == 0 && record.SegmentPointer == "" {
		return ErrInvalidWitnessRecord
	}
	if len(record.SegmentPayload) > 0 {
		if uint64(len(record.SegmentPayload)) != record.SegmentPayloadSize {
			return ErrInvalidWitnessRecord
		}
		digest, err := PerasSegmentPayloadDigest(record.SegmentPayload)
		if err != nil || digest != record.SegmentPayloadDigest {
			return ErrInvalidWitnessRecord
		}
	}
	return nil
}

func segmentWitnessRecordEncodedSize(record SegmentWitnessRecord) int {
	return len(witnessRecordMagic) + 1 + 8 + 32 + 32 + 32 + 8 + stringEncodedSize(record.SegmentPointer) + 4 + len(record.SegmentPayload) + 8 + 8 + 8 + stringEncodedSize(record.HolderID)
}

func stringEncodedSize(value string) int {
	return 4 + len(value)
}

func writeWitnessHeader(out io.Writer, kind WitnessRecordKind) {
	_, _ = out.Write(witnessRecordMagic[:])
	_, _ = out.Write([]byte{byte(kind)})
}

func writeFixed(out io.Writer, value []byte) {
	_, _ = out.Write(value)
}

func writeBytes(out io.Writer, value []byte) {
	writeUint32(out, uint32(len(value)))
	_, _ = out.Write(value)
}

func writeBool(out io.Writer, value bool) {
	if value {
		_, _ = out.Write([]byte{1})
		return
	}
	_, _ = out.Write([]byte{0})
}

func writeOperationID(out io.Writer, id OperationID) {
	writeString(out, id.ClientID)
	writeUint64(out, id.Seq)
}

func writeString(out io.Writer, value string) {
	writeUint32(out, uint32(len(value)))
	_, _ = io.WriteString(out, value)
}

func writeUint64(out io.Writer, value uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	_, _ = out.Write(buf[:])
}

func writeInt64(out io.Writer, value int64) {
	writeUint64(out, uint64(value))
}

func writeUint32(out io.Writer, value uint32) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], value)
	_, _ = out.Write(buf[:])
}

type witnessReader struct {
	buf []byte
	off int
}

func (r *witnessReader) readHeader() (WitnessRecordKind, error) {
	if len(r.buf) < len(witnessRecordMagic)+1 {
		return 0, ErrInvalidWitnessRecord
	}
	if !bytes.Equal(r.buf[:len(witnessRecordMagic)], witnessRecordMagic[:]) {
		return 0, ErrInvalidWitnessRecord
	}
	r.off = len(witnessRecordMagic)
	kind := WitnessRecordKind(r.buf[r.off])
	r.off++
	return kind, nil
}

func (r *witnessReader) readMagic(magic [4]byte) error {
	if len(r.buf)-r.off < len(magic) {
		return ErrInvalidWitnessRecord
	}
	if !bytes.Equal(r.buf[r.off:r.off+len(magic)], magic[:]) {
		return ErrInvalidWitnessRecord
	}
	r.off += len(magic)
	return nil
}

func (r *witnessReader) readSegment() (SegmentWitnessRecord, error) {
	var record SegmentWitnessRecord
	var err error
	if record.EpochID, err = r.readUint64(); err != nil {
		return SegmentWitnessRecord{}, err
	}
	if err := r.readFixed(record.SegmentRoot[:]); err != nil {
		return SegmentWitnessRecord{}, err
	}
	if err := r.readFixed(record.SegmentPayloadDigest[:]); err != nil {
		return SegmentWitnessRecord{}, err
	}
	if err := r.readFixed(record.PredecessorDigest[:]); err != nil {
		return SegmentWitnessRecord{}, err
	}
	if record.SegmentPayloadSize, err = r.readUint64(); err != nil {
		return SegmentWitnessRecord{}, err
	}
	if record.SegmentPointer, err = r.readString(); err != nil {
		return SegmentWitnessRecord{}, err
	}
	if record.SegmentPayload, err = r.readBytes(); err != nil {
		return SegmentWitnessRecord{}, err
	}
	if record.OperationCount, err = r.readUint64(); err != nil {
		return SegmentWitnessRecord{}, err
	}
	if record.EntryCount, err = r.readUint64(); err != nil {
		return SegmentWitnessRecord{}, err
	}
	var ts uint64
	if ts, err = r.readUint64(); err != nil {
		return SegmentWitnessRecord{}, err
	}
	record.TimestampUnixNano = int64(ts)
	if record.HolderID, err = r.readString(); err != nil {
		return SegmentWitnessRecord{}, err
	}
	if err := validateSegmentWitnessRecord(record); err != nil {
		return SegmentWitnessRecord{}, err
	}
	return record, nil
}

func (r *witnessReader) readOperationID() (OperationID, error) {
	clientID, err := r.readString()
	if err != nil {
		return OperationID{}, err
	}
	seq, err := r.readUint64()
	if err != nil {
		return OperationID{}, err
	}
	id := OperationID{ClientID: clientID, Seq: seq}
	if !id.Valid() {
		return OperationID{}, ErrInvalidWitnessRecord
	}
	return id, nil
}

func (r *witnessReader) readString() (string, error) {
	length, err := r.readUint32()
	if err != nil {
		return "", err
	}
	if length > uint32(len(r.buf)-r.off) {
		return "", ErrInvalidWitnessRecord
	}
	value := string(r.buf[r.off : r.off+int(length)])
	r.off += int(length)
	return value, nil
}

func (r *witnessReader) readBytes() ([]byte, error) {
	length, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	if length > uint32(len(r.buf)-r.off) {
		return nil, ErrInvalidWitnessRecord
	}
	value := cloneBytes(r.buf[r.off : r.off+int(length)])
	r.off += int(length)
	return value, nil
}

func (r *witnessReader) readBool() (bool, error) {
	if len(r.buf)-r.off < 1 {
		return false, ErrInvalidWitnessRecord
	}
	value := r.buf[r.off]
	r.off++
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, ErrInvalidWitnessRecord
	}
}

func (r *witnessReader) readUint64() (uint64, error) {
	if len(r.buf)-r.off < 8 {
		return 0, ErrInvalidWitnessRecord
	}
	value := binary.BigEndian.Uint64(r.buf[r.off : r.off+8])
	r.off += 8
	return value, nil
}

func (r *witnessReader) readUint32() (uint32, error) {
	if len(r.buf)-r.off < 4 {
		return 0, ErrInvalidWitnessRecord
	}
	value := binary.BigEndian.Uint32(r.buf[r.off : r.off+4])
	r.off += 4
	return value, nil
}

func (r *witnessReader) readFixed(out []byte) error {
	if len(r.buf)-r.off < len(out) {
		return ErrInvalidWitnessRecord
	}
	copy(out, r.buf[r.off:r.off+len(out)])
	r.off += len(out)
	return nil
}

func (r witnessReader) done() bool {
	return r.off == len(r.buf)
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
