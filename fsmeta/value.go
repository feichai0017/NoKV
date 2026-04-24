package fsmeta

import (
	"encoding/json"
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

func (k ValueKind) String() string {
	switch k {
	case ValueKindInode:
		return "inode"
	case ValueKindDentry:
		return "dentry"
	case ValueKindSession:
		return "session"
	default:
		return fmt.Sprintf("unknown(%d)", byte(k))
	}
}

// EncodeInodeValue returns the canonical value encoding for an inode record.
func EncodeInodeValue(record InodeRecord) ([]byte, error) {
	if err := validateInodeID(record.Inode); err != nil {
		return nil, err
	}
	return encodeValue(ValueKindInode, record)
}

// DecodeInodeValue decodes an inode record.
func DecodeInodeValue(value []byte) (InodeRecord, error) {
	var record InodeRecord
	if err := decodeValue(value, ValueKindInode, &record); err != nil {
		return InodeRecord{}, err
	}
	if err := validateInodeID(record.Inode); err != nil {
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
	return encodeValue(ValueKindDentry, record)
}

// DecodeDentryValue decodes a dentry record.
func DecodeDentryValue(value []byte) (DentryRecord, error) {
	var record DentryRecord
	if err := decodeValue(value, ValueKindDentry, &record); err != nil {
		return DentryRecord{}, err
	}
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

// EncodeSessionValue returns the canonical value encoding for a session record.
func EncodeSessionValue(record SessionRecord) ([]byte, error) {
	if err := validateSessionID(record.Session); err != nil {
		return nil, err
	}
	if err := validateInodeID(record.Inode); err != nil {
		return nil, err
	}
	return encodeValue(ValueKindSession, record)
}

// DecodeSessionValue decodes a session record.
func DecodeSessionValue(value []byte) (SessionRecord, error) {
	var record SessionRecord
	if err := decodeValue(value, ValueKindSession, &record); err != nil {
		return SessionRecord{}, err
	}
	if err := validateSessionID(record.Session); err != nil {
		return SessionRecord{}, err
	}
	if err := validateInodeID(record.Inode); err != nil {
		return SessionRecord{}, err
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
	case ValueKindInode, ValueKindDentry, ValueKindSession:
		return kind, nil
	default:
		return 0, ErrInvalidValueKind
	}
}

func encodeValue(kind ValueKind, record any) ([]byte, error) {
	body, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(valueMagic)+2+len(body))
	out = append(out, valueMagic...)
	out = append(out, valueSchemaVersion)
	out = append(out, byte(kind))
	out = append(out, body...)
	return out, nil
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
	if err := json.Unmarshal(value[pos+1:], out); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidValue, err)
	}
	return nil
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
