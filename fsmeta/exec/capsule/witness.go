package capsule

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"slices"

	"github.com/feichai0017/NoKV/engine/wal"
)

var (
	ErrInvalidWitnessRecord = errors.New("fsmeta capsule: invalid witness record")
	ErrWitnessLogRequired   = errors.New("fsmeta capsule: witness log required")
)

type WitnessRecordKind uint8

const (
	WitnessRecordPrepare WitnessRecordKind = iota + 1
	WitnessRecordCommitCertificate
)

var witnessRecordMagic = [4]byte{'N', 'C', 'W', 1}

type PrepareRecord struct {
	EpochID              uint64
	OpID                 OperationID
	DeltaPayload         []byte
	DeltaDigest          [32]byte
	PredicateDigest      [32]byte
	AuthorityProofDigest [32]byte
	DependencyFrontier   []OperationID
	TimestampUnixNano    int64
	HolderID             string
	HolderSignature      [64]byte
}

type CommitCertificateRecord struct {
	EpochID           uint64
	OpID              OperationID
	PrepareDigest     [32]byte
	QuorumAckSet      []string
	TimestampUnixNano int64
	HolderID          string
	HolderSignature   [64]byte
}

type WitnessFrame struct {
	Kind    WitnessRecordKind
	Prepare PrepareRecord
	Commit  CommitCertificateRecord
}

type WitnessSnapshot struct {
	Prepares []PrepareRecord
	Commits  []CommitCertificateRecord
}

type WALWitnessLog struct {
	wal        *wal.Manager
	durability wal.DurabilityPolicy
}

func NewWALWitnessLog(manager *wal.Manager, durability wal.DurabilityPolicy) (*WALWitnessLog, error) {
	if manager == nil {
		return nil, ErrWitnessLogRequired
	}
	return &WALWitnessLog{wal: manager, durability: durability}, nil
}

func (l *WALWitnessLog) AppendPrepare(ctx context.Context, record PrepareRecord) (wal.EntryInfo, error) {
	if err := ctxErr(ctx); err != nil {
		return wal.EntryInfo{}, err
	}
	payload, err := EncodePrepareRecord(record)
	if err != nil {
		return wal.EntryInfo{}, err
	}
	return l.appendPayload(payload)
}

func (l *WALWitnessLog) AppendCommitCertificate(ctx context.Context, record CommitCertificateRecord) (wal.EntryInfo, error) {
	if err := ctxErr(ctx); err != nil {
		return wal.EntryInfo{}, err
	}
	payload, err := EncodeCommitCertificateRecord(record)
	if err != nil {
		return wal.EntryInfo{}, err
	}
	return l.appendPayload(payload)
}

func (l *WALWitnessLog) Probe(ctx context.Context, epochID uint64) (WitnessSnapshot, error) {
	if l == nil || l.wal == nil {
		return WitnessSnapshot{}, ErrWitnessLogRequired
	}
	var out WitnessSnapshot
	err := l.wal.ReplayFiltered(
		func(info wal.EntryInfo) bool {
			return info.Type == wal.RecordTypeCapsuleWitness
		},
		func(_ wal.EntryInfo, payload []byte) error {
			if err := ctxErr(ctx); err != nil {
				return err
			}
			frame, err := DecodeWitnessFrame(payload)
			if err != nil {
				return err
			}
			switch frame.Kind {
			case WitnessRecordPrepare:
				if frame.Prepare.EpochID == epochID {
					out.Prepares = append(out.Prepares, clonePrepareRecord(frame.Prepare))
				}
			case WitnessRecordCommitCertificate:
				if frame.Commit.EpochID == epochID {
					out.Commits = append(out.Commits, cloneCommitCertificateRecord(frame.Commit))
				}
			default:
				return ErrInvalidWitnessRecord
			}
			return nil
		},
	)
	return out, err
}

func (l *WALWitnessLog) appendPayload(payload []byte) (wal.EntryInfo, error) {
	if l == nil || l.wal == nil {
		return wal.EntryInfo{}, ErrWitnessLogRequired
	}
	infos, err := l.wal.AppendRecords(l.durability, wal.Record{
		Type:    wal.RecordTypeCapsuleWitness,
		Payload: payload,
	})
	if err != nil {
		return wal.EntryInfo{}, err
	}
	if len(infos) != 1 {
		return wal.EntryInfo{}, ErrInvalidWitnessRecord
	}
	return infos[0], nil
}

func EncodePrepareRecord(record PrepareRecord) ([]byte, error) {
	if err := validatePrepareRecord(record); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.Grow(prepareRecordEncodedSize(record))
	writeWitnessHeader(&out, WitnessRecordPrepare)
	writeUint64(&out, record.EpochID)
	writeOperationID(&out, record.OpID)
	writeBytes(&out, record.DeltaPayload)
	out.Write(record.DeltaDigest[:])
	out.Write(record.PredicateDigest[:])
	out.Write(record.AuthorityProofDigest[:])
	writeOperationIDs(&out, record.DependencyFrontier)
	writeInt64(&out, record.TimestampUnixNano)
	writeString(&out, record.HolderID)
	out.Write(record.HolderSignature[:])
	return out.Bytes(), nil
}

func EncodeCommitCertificateRecord(record CommitCertificateRecord) ([]byte, error) {
	if err := validateCommitCertificateRecord(record); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.Grow(commitCertificateRecordEncodedSize(record))
	writeWitnessHeader(&out, WitnessRecordCommitCertificate)
	writeUint64(&out, record.EpochID)
	writeOperationID(&out, record.OpID)
	out.Write(record.PrepareDigest[:])
	writeStrings(&out, record.QuorumAckSet)
	writeInt64(&out, record.TimestampUnixNano)
	writeString(&out, record.HolderID)
	out.Write(record.HolderSignature[:])
	return out.Bytes(), nil
}

func DecodeWitnessFrame(payload []byte) (WitnessFrame, error) {
	r := witnessReader{buf: payload}
	kind, err := r.readHeader()
	if err != nil {
		return WitnessFrame{}, err
	}
	switch kind {
	case WitnessRecordPrepare:
		record, err := r.readPrepare()
		if err != nil {
			return WitnessFrame{}, err
		}
		if !r.done() {
			return WitnessFrame{}, ErrInvalidWitnessRecord
		}
		return WitnessFrame{Kind: kind, Prepare: record}, nil
	case WitnessRecordCommitCertificate:
		record, err := r.readCommitCertificate()
		if err != nil {
			return WitnessFrame{}, err
		}
		if !r.done() {
			return WitnessFrame{}, ErrInvalidWitnessRecord
		}
		return WitnessFrame{Kind: kind, Commit: record}, nil
	default:
		return WitnessFrame{}, ErrInvalidWitnessRecord
	}
}

func PrepareDigest(record PrepareRecord) ([32]byte, error) {
	payload, err := EncodePrepareRecord(record)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(payload), nil
}

func validatePrepareRecord(record PrepareRecord) error {
	if record.EpochID == 0 || !record.OpID.Valid() || record.HolderID == "" {
		return ErrInvalidWitnessRecord
	}
	digest, err := SemanticDeltaPayloadDigest(record.DeltaPayload)
	if err != nil || digest != record.DeltaDigest {
		return ErrInvalidWitnessRecord
	}
	for _, id := range record.DependencyFrontier {
		if !id.Valid() {
			return ErrInvalidWitnessRecord
		}
	}
	return nil
}

func validateCommitCertificateRecord(record CommitCertificateRecord) error {
	if record.EpochID == 0 || !record.OpID.Valid() || record.HolderID == "" || len(record.QuorumAckSet) == 0 {
		return ErrInvalidWitnessRecord
	}
	if slices.Contains(record.QuorumAckSet, "") {
		return ErrInvalidWitnessRecord
	}
	return nil
}

func prepareRecordEncodedSize(record PrepareRecord) int {
	size := len(witnessRecordMagic) + 1 + 8 + operationIDEncodedSize(record.OpID) + 4 + len(record.DeltaPayload) + 32 + 32 + 32 + 4 + 8 + stringEncodedSize(record.HolderID) + 64
	for _, id := range record.DependencyFrontier {
		size += operationIDEncodedSize(id)
	}
	return size
}

func commitCertificateRecordEncodedSize(record CommitCertificateRecord) int {
	size := len(witnessRecordMagic) + 1 + 8 + operationIDEncodedSize(record.OpID) + 32 + 4 + 8 + stringEncodedSize(record.HolderID) + 64
	for _, peer := range record.QuorumAckSet {
		size += stringEncodedSize(peer)
	}
	return size
}

func operationIDEncodedSize(id OperationID) int {
	return stringEncodedSize(id.ClientID) + 8
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

func writeOperationIDs(out io.Writer, ids []OperationID) {
	writeUint32(out, uint32(len(ids)))
	for _, id := range ids {
		writeOperationID(out, id)
	}
}

func writeOperationID(out io.Writer, id OperationID) {
	writeString(out, id.ClientID)
	writeUint64(out, id.Seq)
}

func writeStrings(out io.Writer, values []string) {
	writeUint32(out, uint32(len(values)))
	for _, value := range values {
		writeString(out, value)
	}
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

func (r *witnessReader) readPrepare() (PrepareRecord, error) {
	var record PrepareRecord
	var err error
	if record.EpochID, err = r.readUint64(); err != nil {
		return PrepareRecord{}, err
	}
	if record.OpID, err = r.readOperationID(); err != nil {
		return PrepareRecord{}, err
	}
	if record.DeltaPayload, err = r.readBytes(); err != nil {
		return PrepareRecord{}, err
	}
	if err := r.readFixed(record.DeltaDigest[:]); err != nil {
		return PrepareRecord{}, err
	}
	if err := r.readFixed(record.PredicateDigest[:]); err != nil {
		return PrepareRecord{}, err
	}
	if err := r.readFixed(record.AuthorityProofDigest[:]); err != nil {
		return PrepareRecord{}, err
	}
	if record.DependencyFrontier, err = r.readOperationIDs(); err != nil {
		return PrepareRecord{}, err
	}
	var ts uint64
	if ts, err = r.readUint64(); err != nil {
		return PrepareRecord{}, err
	}
	record.TimestampUnixNano = int64(ts)
	if record.HolderID, err = r.readString(); err != nil {
		return PrepareRecord{}, err
	}
	if err := r.readFixed(record.HolderSignature[:]); err != nil {
		return PrepareRecord{}, err
	}
	if err := validatePrepareRecord(record); err != nil {
		return PrepareRecord{}, err
	}
	return record, nil
}

func (r *witnessReader) readCommitCertificate() (CommitCertificateRecord, error) {
	var record CommitCertificateRecord
	var err error
	if record.EpochID, err = r.readUint64(); err != nil {
		return CommitCertificateRecord{}, err
	}
	if record.OpID, err = r.readOperationID(); err != nil {
		return CommitCertificateRecord{}, err
	}
	if err := r.readFixed(record.PrepareDigest[:]); err != nil {
		return CommitCertificateRecord{}, err
	}
	if record.QuorumAckSet, err = r.readStrings(); err != nil {
		return CommitCertificateRecord{}, err
	}
	var ts uint64
	if ts, err = r.readUint64(); err != nil {
		return CommitCertificateRecord{}, err
	}
	record.TimestampUnixNano = int64(ts)
	if record.HolderID, err = r.readString(); err != nil {
		return CommitCertificateRecord{}, err
	}
	if err := r.readFixed(record.HolderSignature[:]); err != nil {
		return CommitCertificateRecord{}, err
	}
	if err := validateCommitCertificateRecord(record); err != nil {
		return CommitCertificateRecord{}, err
	}
	return record, nil
}

func (r *witnessReader) readOperationIDs() ([]OperationID, error) {
	count, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	out := make([]OperationID, 0, count)
	for range count {
		id, err := r.readOperationID()
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
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

func (r *witnessReader) readStrings() ([]string, error) {
	count, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, count)
	for range count {
		value, err := r.readString()
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
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

func clonePrepareRecord(record PrepareRecord) PrepareRecord {
	record.DeltaPayload = cloneBytes(record.DeltaPayload)
	record.DependencyFrontier = slices.Clone(record.DependencyFrontier)
	return record
}

func cloneCommitCertificateRecord(record CommitCertificateRecord) CommitCertificateRecord {
	record.QuorumAckSet = slices.Clone(record.QuorumAckSet)
	return record
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
