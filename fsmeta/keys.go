package fsmeta

import (
	"encoding/binary"
	"fmt"
)

var keyMagic = []byte{'f', 's', 'm', 0}

const keySchemaVersion byte = 1

// KeyKind classifies one fsmeta key family.
type KeyKind byte

const (
	KeyKindMount   KeyKind = 'm'
	KeyKindInode   KeyKind = 'i'
	KeyKindDentry  KeyKind = 'd'
	KeyKindChunk   KeyKind = 'c'
	KeyKindSession KeyKind = 's'
	KeyKindUsage   KeyKind = 'u'
)

func (k KeyKind) String() string {
	switch k {
	case KeyKindMount:
		return "mount"
	case KeyKindInode:
		return "inode"
	case KeyKindDentry:
		return "dentry"
	case KeyKindChunk:
		return "chunk"
	case KeyKindSession:
		return "session"
	case KeyKindUsage:
		return "usage"
	default:
		return fmt.Sprintf("unknown(%d)", byte(k))
	}
}

// EncodeMountKey returns the mount-level metadata record key.
func EncodeMountKey(mount MountID) ([]byte, error) {
	return encodeKey(mount, KeyKindMount, nil)
}

// EncodeInodeKey returns the inode attribute record key.
func EncodeInodeKey(mount MountID, inode InodeID) ([]byte, error) {
	if err := validateInodeID(inode); err != nil {
		return nil, err
	}
	var body [8]byte
	binary.BigEndian.PutUint64(body[:], uint64(inode))
	return encodeKey(mount, KeyKindInode, body[:])
}

// EncodeDentryPrefix returns the scan prefix for entries directly under parent.
func EncodeDentryPrefix(mount MountID, parent InodeID) ([]byte, error) {
	if err := validateInodeID(parent); err != nil {
		return nil, err
	}
	var body [8]byte
	binary.BigEndian.PutUint64(body[:], uint64(parent))
	return encodeKey(mount, KeyKindDentry, body[:])
}

// EncodeDentryKey returns the dentry record key for parent/name.
func EncodeDentryKey(mount MountID, parent InodeID, name string) ([]byte, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	prefix, err := EncodeDentryPrefix(mount, parent)
	if err != nil {
		return nil, err
	}
	return append(prefix, name...), nil
}

// EncodeChunkKey returns the chunk mapping record key for inode/chunk.
func EncodeChunkKey(mount MountID, inode InodeID, chunk ChunkIndex) ([]byte, error) {
	if err := validateInodeID(inode); err != nil {
		return nil, err
	}
	var body [16]byte
	binary.BigEndian.PutUint64(body[:8], uint64(inode))
	binary.BigEndian.PutUint64(body[8:], uint64(chunk))
	return encodeKey(mount, KeyKindChunk, body[:])
}

// EncodeSessionKey returns the client/session state key.
func EncodeSessionKey(mount MountID, session SessionID) ([]byte, error) {
	if err := validateSessionID(session); err != nil {
		return nil, err
	}
	return encodeKey(mount, KeyKindSession, []byte(session))
}

// EncodeUsageKey returns the per-directory usage/counter key.
func EncodeUsageKey(mount MountID, dir InodeID) ([]byte, error) {
	if err := validateInodeID(dir); err != nil {
		return nil, err
	}
	var body [8]byte
	binary.BigEndian.PutUint64(body[:], uint64(dir))
	return encodeKey(mount, KeyKindUsage, body[:])
}

// KeyKindOf returns the kind byte encoded in a fsmeta key.
func KeyKindOf(key []byte) (KeyKind, error) {
	pos, err := decodeHeader(key)
	if err != nil {
		return 0, err
	}
	if pos >= len(key) {
		return 0, ErrInvalidKey
	}
	kind := KeyKind(key[pos])
	switch kind {
	case KeyKindMount, KeyKindInode, KeyKindDentry, KeyKindChunk, KeyKindSession, KeyKindUsage:
		return kind, nil
	default:
		return 0, ErrInvalidKeyKind
	}
}

func encodeKey(mount MountID, kind KeyKind, body []byte) ([]byte, error) {
	if err := validateMountID(mount); err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(keyMagic)+2+len(mount)+1+len(body))
	out = append(out, keyMagic...)
	out = append(out, keySchemaVersion)
	out = binary.AppendUvarint(out, uint64(len(mount)))
	out = append(out, string(mount)...)
	out = append(out, byte(kind))
	out = append(out, body...)
	return out, nil
}

func decodeHeader(key []byte) (int, error) {
	if len(key) < len(keyMagic)+2 {
		return 0, ErrInvalidKey
	}
	for i := range keyMagic {
		if key[i] != keyMagic[i] {
			return 0, ErrInvalidKey
		}
	}
	pos := len(keyMagic)
	if key[pos] != keySchemaVersion {
		return 0, ErrInvalidKey
	}
	pos++
	mountLen, n := binary.Uvarint(key[pos:])
	if n <= 0 {
		return 0, ErrInvalidKey
	}
	pos += n
	if mountLen == 0 || uint64(len(key)-pos) < mountLen+1 {
		return 0, ErrInvalidKey
	}
	pos += int(mountLen)
	return pos, nil
}
