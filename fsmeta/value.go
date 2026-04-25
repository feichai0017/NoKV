package fsmeta

import (
	"encoding/binary"
	"fmt"
)

var valueMagic = []byte{'f', 's', 'v', 0}

const valueSchemaVersion byte = 1

// ValueKind classifies one fsmeta value family.
type ValueKind byte

const (
	ValueKindInode   ValueKind = 'i'
	ValueKindDentry  ValueKind = 'd'
	ValueKindSession ValueKind = 's'
	ValueKindUsage   ValueKind = 'u'
)

// InodeType describes the user-visible inode kind tracked by fsmeta.
type InodeType string

const (
	InodeTypeFile      InodeType = "file"
	InodeTypeDirectory InodeType = "directory"
)

// InodeRecord is the value stored under an inode key.
type InodeRecord struct {
	Inode         InodeID   `json:"inode"`
	Type          InodeType `json:"type,omitempty"`
	Size          uint64    `json:"size,omitempty"`
	Mode          uint32    `json:"mode,omitempty"`
	LinkCount     uint32    `json:"link_count,omitempty"`
	CreatedUnixNs int64     `json:"created_unix_ns,omitempty"`
	UpdatedUnixNs int64     `json:"updated_unix_ns,omitempty"`
}

// DentryRecord is the value stored under a parent/name dentry key.
type DentryRecord struct {
	Parent InodeID   `json:"parent"`
	Name   string    `json:"name"`
	Inode  InodeID   `json:"inode"`
	Type   InodeType `json:"type,omitempty"`
}

// SessionRecord is the value stored under a writer/session key.
type SessionRecord struct {
	Session       SessionID `json:"session"`
	Inode         InodeID   `json:"inode"`
	ExpiresUnixNs int64     `json:"expires_unix_ns,omitempty"`
}

// UsageRecord is the value stored under quota usage counter keys.
type UsageRecord struct {
	Bytes  uint64 `json:"bytes,omitempty"`
	Inodes uint64 `json:"inodes,omitempty"`
}

func (k ValueKind) String() string {
	switch k {
	case ValueKindInode:
		return "inode"
	case ValueKindDentry:
		return "dentry"
	case ValueKindSession:
		return "session"
	case ValueKindUsage:
		return "usage"
	default:
		return fmt.Sprintf("unknown(%d)", byte(k))
	}
}

// EncodeInodeValue returns the canonical value encoding for an inode record.
func EncodeInodeValue(record InodeRecord) ([]byte, error) {
	if err := validateInodeID(record.Inode); err != nil {
		return nil, err
	}
	typ, err := encodeInodeType(record.Type)
	if err != nil {
		return nil, err
	}
	body := make([]byte, 0, 41)
	body = binary.BigEndian.AppendUint64(body, uint64(record.Inode))
	body = append(body, typ)
	body = binary.BigEndian.AppendUint64(body, record.Size)
	body = binary.BigEndian.AppendUint32(body, record.Mode)
	body = binary.BigEndian.AppendUint32(body, record.LinkCount)
	body = binary.BigEndian.AppendUint64(body, uint64(record.CreatedUnixNs))
	body = binary.BigEndian.AppendUint64(body, uint64(record.UpdatedUnixNs))
	return encodeValue(ValueKindInode, body), nil
}

// DecodeInodeValue decodes an inode record.
func DecodeInodeValue(value []byte) (InodeRecord, error) {
	var record InodeRecord
	if err := decodeValue(value, ValueKindInode, &record); err != nil {
		return InodeRecord{}, err
	}
	return record, nil
}

// EncodeDentryValue returns the canonical value encoding for a dentry record.
func EncodeDentryValue(record DentryRecord) ([]byte, error) {
	if err := validateInodeID(record.Parent); err != nil {
		return nil, err
	}
	if err := validateName(record.Name); err != nil {
		return nil, err
	}
	if err := validateInodeID(record.Inode); err != nil {
		return nil, err
	}
	typ, err := encodeInodeType(record.Type)
	if err != nil {
		return nil, err
	}
	body := make([]byte, 0, 8+binary.MaxVarintLen64+len(record.Name)+8+1)
	body = binary.BigEndian.AppendUint64(body, uint64(record.Parent))
	body = binary.AppendUvarint(body, uint64(len(record.Name)))
	body = append(body, record.Name...)
	body = binary.BigEndian.AppendUint64(body, uint64(record.Inode))
	body = append(body, typ)
	return encodeValue(ValueKindDentry, body), nil
}

// DecodeDentryValue decodes a dentry record.
func DecodeDentryValue(value []byte) (DentryRecord, error) {
	var record DentryRecord
	if err := decodeValue(value, ValueKindDentry, &record); err != nil {
		return DentryRecord{}, err
	}
	return record, nil
}

// EncodeSessionValue returns the canonical value encoding for a session record.
func EncodeSessionValue(record SessionRecord) ([]byte, error) {
	if err := validateSessionID(record.Session); err != nil {
		return nil, err
	}
	if err := validateInodeID(record.Inode); err != nil {
		return nil, err
	}
	body := make([]byte, 0, binary.MaxVarintLen64+len(record.Session)+16)
	body = binary.AppendUvarint(body, uint64(len(record.Session)))
	body = append(body, string(record.Session)...)
	body = binary.BigEndian.AppendUint64(body, uint64(record.Inode))
	body = binary.BigEndian.AppendUint64(body, uint64(record.ExpiresUnixNs))
	return encodeValue(ValueKindSession, body), nil
}

// DecodeSessionValue decodes a session record.
func DecodeSessionValue(value []byte) (SessionRecord, error) {
	var record SessionRecord
	if err := decodeValue(value, ValueKindSession, &record); err != nil {
		return SessionRecord{}, err
	}
	return record, nil
}

// EncodeUsageValue returns the canonical value encoding for a usage counter.
func EncodeUsageValue(record UsageRecord) ([]byte, error) {
	body := make([]byte, 0, 16)
	body = binary.BigEndian.AppendUint64(body, record.Bytes)
	body = binary.BigEndian.AppendUint64(body, record.Inodes)
	return encodeValue(ValueKindUsage, body), nil
}

// DecodeUsageValue decodes a usage counter value.
func DecodeUsageValue(value []byte) (UsageRecord, error) {
	var record UsageRecord
	if err := decodeValue(value, ValueKindUsage, &record); err != nil {
		return UsageRecord{}, err
	}
	return record, nil
}

// ValueKindOf returns the kind byte encoded in a fsmeta value.
func ValueKindOf(value []byte) (ValueKind, error) {
	pos, err := decodeValueHeader(value)
	if err != nil {
		return 0, err
	}
	if pos >= len(value) {
		return 0, ErrInvalidValue
	}
	kind := ValueKind(value[pos])
	switch kind {
	case ValueKindInode, ValueKindDentry, ValueKindSession, ValueKindUsage:
		return kind, nil
	default:
		return 0, ErrInvalidValueKind
	}
}

func encodeValue(kind ValueKind, body []byte) []byte {
	out := make([]byte, 0, len(valueMagic)+2+len(body))
	out = append(out, valueMagic...)
	out = append(out, valueSchemaVersion)
	out = append(out, byte(kind))
	out = append(out, body...)
	return out
}

func decodeValue(value []byte, expected ValueKind, out any) error {
	pos, err := decodeValueHeader(value)
	if err != nil {
		return err
	}
	if pos >= len(value) {
		return ErrInvalidValue
	}
	kind := ValueKind(value[pos])
	if kind != expected {
		return ErrInvalidValueKind
	}
	body := value[pos+1:]
	switch expected {
	case ValueKindInode:
		record, ok := out.(*InodeRecord)
		if !ok {
			return ErrInvalidValue
		}
		decoded, err := decodeInodeBody(body)
		if err != nil {
			return err
		}
		*record = decoded
	case ValueKindDentry:
		record, ok := out.(*DentryRecord)
		if !ok {
			return ErrInvalidValue
		}
		decoded, err := decodeDentryBody(body)
		if err != nil {
			return err
		}
		*record = decoded
	case ValueKindSession:
		record, ok := out.(*SessionRecord)
		if !ok {
			return ErrInvalidValue
		}
		decoded, err := decodeSessionBody(body)
		if err != nil {
			return err
		}
		*record = decoded
	case ValueKindUsage:
		record, ok := out.(*UsageRecord)
		if !ok {
			return ErrInvalidValue
		}
		decoded, err := decodeUsageBody(body)
		if err != nil {
			return err
		}
		*record = decoded
	default:
		return ErrInvalidValueKind
	}
	return nil
}

func decodeInodeBody(body []byte) (InodeRecord, error) {
	const size = 8 + 1 + 8 + 4 + 4 + 8 + 8
	if len(body) != size {
		return InodeRecord{}, ErrInvalidValue
	}
	record := InodeRecord{
		Inode:         InodeID(binary.BigEndian.Uint64(body[:8])),
		Size:          binary.BigEndian.Uint64(body[9:17]),
		Mode:          binary.BigEndian.Uint32(body[17:21]),
		LinkCount:     binary.BigEndian.Uint32(body[21:25]),
		CreatedUnixNs: int64(binary.BigEndian.Uint64(body[25:33])),
		UpdatedUnixNs: int64(binary.BigEndian.Uint64(body[33:41])),
	}
	typ, err := decodeInodeType(body[8])
	if err != nil {
		return InodeRecord{}, err
	}
	record.Type = typ
	if err := validateInodeID(record.Inode); err != nil {
		return InodeRecord{}, err
	}
	return record, nil
}

func decodeDentryBody(body []byte) (DentryRecord, error) {
	if len(body) < 8+1+8+1 {
		return DentryRecord{}, ErrInvalidValue
	}
	parent := InodeID(binary.BigEndian.Uint64(body[:8]))
	pos := 8
	nameLen, n := binary.Uvarint(body[pos:])
	if n <= 0 {
		return DentryRecord{}, ErrInvalidValue
	}
	pos += n
	remaining := uint64(len(body) - pos)
	// Subtract first to avoid uint64 overflow when nameLen is near MaxUint64.
	// Need: nameLen >= 1, remaining >= nameLen, and remaining-nameLen >= 8 (inode) + 1 (type) = 9.
	if nameLen == 0 || nameLen > remaining || remaining-nameLen < 9 {
		return DentryRecord{}, ErrInvalidValue
	}
	name := string(body[pos : pos+int(nameLen)])
	pos += int(nameLen)
	inode := InodeID(binary.BigEndian.Uint64(body[pos : pos+8]))
	pos += 8
	if pos != len(body)-1 {
		return DentryRecord{}, ErrInvalidValue
	}
	typ, err := decodeInodeType(body[pos])
	if err != nil {
		return DentryRecord{}, err
	}
	record := DentryRecord{Parent: parent, Name: name, Inode: inode, Type: typ}
	if err := validateInodeID(record.Parent); err != nil {
		return DentryRecord{}, err
	}
	if err := validateName(record.Name); err != nil {
		return DentryRecord{}, err
	}
	if err := validateInodeID(record.Inode); err != nil {
		return DentryRecord{}, err
	}
	return record, nil
}

func decodeSessionBody(body []byte) (SessionRecord, error) {
	sessionLen, n := binary.Uvarint(body)
	if n <= 0 {
		return SessionRecord{}, ErrInvalidValue
	}
	pos := n
	remaining := uint64(len(body) - pos)
	// Subtract first to avoid uint64 overflow when sessionLen is near MaxUint64.
	// Body shape after the varint: sessionLen bytes + 8 (inode) + 8 (expiry) = sessionLen + 16.
	if sessionLen == 0 || sessionLen > remaining || remaining-sessionLen != 16 {
		return SessionRecord{}, ErrInvalidValue
	}
	session := SessionID(string(body[pos : pos+int(sessionLen)]))
	pos += int(sessionLen)
	record := SessionRecord{
		Session:       session,
		Inode:         InodeID(binary.BigEndian.Uint64(body[pos : pos+8])),
		ExpiresUnixNs: int64(binary.BigEndian.Uint64(body[pos+8 : pos+16])),
	}
	if err := validateSessionID(record.Session); err != nil {
		return SessionRecord{}, err
	}
	if err := validateInodeID(record.Inode); err != nil {
		return SessionRecord{}, err
	}
	return record, nil
}

func decodeUsageBody(body []byte) (UsageRecord, error) {
	if len(body) != 16 {
		return UsageRecord{}, ErrInvalidValue
	}
	return UsageRecord{
		Bytes:  binary.BigEndian.Uint64(body[:8]),
		Inodes: binary.BigEndian.Uint64(body[8:16]),
	}, nil
}

func decodeValueHeader(value []byte) (int, error) {
	if len(value) < len(valueMagic)+2 {
		return 0, ErrInvalidValue
	}
	for i := range valueMagic {
		if value[i] != valueMagic[i] {
			return 0, ErrInvalidValue
		}
	}
	pos := len(valueMagic)
	if value[pos] != valueSchemaVersion {
		return 0, ErrInvalidValue
	}
	pos++
	return pos, nil
}

func encodeInodeType(typ InodeType) (byte, error) {
	switch typ {
	case "":
		return 0, nil
	case InodeTypeFile:
		return 1, nil
	case InodeTypeDirectory:
		return 2, nil
	default:
		return 0, ErrInvalidValue
	}
}

func decodeInodeType(encoded byte) (InodeType, error) {
	switch encoded {
	case 0:
		return "", nil
	case 1:
		return InodeTypeFile, nil
	case 2:
		return InodeTypeDirectory, nil
	default:
		return "", ErrInvalidValue
	}
}
