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

// EncodeMountPrefix returns the common key prefix for one mount. All fsmeta
// records under the mount share this prefix regardless of key kind.
func EncodeMountPrefix(mount MountID) ([]byte, error) {
	return encodeMountPrefix(mount)
}

// EncodeMountKeyRange returns the half-open key range covering all fsmeta
// records under one mount.
func EncodeMountKeyRange(mount MountID) (start, end []byte, err error) {
	start, err = EncodeMountPrefix(mount)
	if err != nil {
		return nil, nil, err
	}
	return start, prefixUpperBound(start), nil
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

// EncodeInodeSessionKey returns the exclusive writer-owner key for one inode.
// Session IDs cannot contain NUL, so the 0x00 marker cannot collide with
// EncodeSessionKey.
func EncodeInodeSessionKey(mount MountID, inode InodeID) ([]byte, error) {
	if err := validateInodeID(inode); err != nil {
		return nil, err
	}
	var body [9]byte
	binary.BigEndian.PutUint64(body[1:], uint64(inode))
	return encodeKey(mount, KeyKindSession, body[:])
}

// EncodeSessionPrefix returns the scan prefix covering both session IDs and
// inode owner records for one mount.
func EncodeSessionPrefix(mount MountID) ([]byte, error) {
	return encodeKey(mount, KeyKindSession, nil)
}

// EncodeUsageKey returns a quota usage/counter key. Scope 0 is mount-wide;
// non-zero scopes are direct quota accounting roots.
func EncodeUsageKey(mount MountID, scope InodeID) ([]byte, error) {
	var body [8]byte
	binary.BigEndian.PutUint64(body[:], uint64(scope))
	return encodeKey(mount, KeyKindUsage, body[:])
}

// KeyKindOf returns the kind byte encoded in a fsmeta key.
func KeyKindOf(key []byte) (KeyKind, error) {
	_, pos, err := decodeHeaderParts(key)
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

// MountIDOfKey returns the mount encoded in a full fsmeta key.
func MountIDOfKey(key []byte) (MountID, bool) {
	mount, _, err := decodeHeaderParts(key)
	if err != nil {
		return "", false
	}
	return mount, true
}

// StringMountResolver adapts fsmeta keys to raftstore MVCC GC mount-scoped
// retention policy.
func StringMountResolver(key []byte) (string, bool) {
	mount, ok := MountIDOfKey(key)
	return string(mount), ok
}

func encodeKey(mount MountID, kind KeyKind, body []byte) ([]byte, error) {
	out, err := encodeMountPrefix(mount)
	if err != nil {
		return nil, err
	}
	out = append(out, byte(kind))
	out = append(out, body...)
	return out, nil
}

func encodeMountPrefix(mount MountID) ([]byte, error) {
	if err := validateMountID(mount); err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(keyMagic)+2+len(mount))
	out = append(out, keyMagic...)
	out = append(out, keySchemaVersion)
	out = binary.AppendUvarint(out, uint64(len(mount)))
	out = append(out, string(mount)...)
	return out, nil
}

func prefixUpperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

func decodeHeaderParts(key []byte) (MountID, int, error) {
	if len(key) < len(keyMagic)+2 {
		return "", 0, ErrInvalidKey
	}
	for i := range keyMagic {
		if key[i] != keyMagic[i] {
			return "", 0, ErrInvalidKey
		}
	}
	pos := len(keyMagic)
	if key[pos] != keySchemaVersion {
		return "", 0, ErrInvalidKey
	}
	pos++
	mountLen, n := binary.Uvarint(key[pos:])
	if n <= 0 {
		return "", 0, ErrInvalidKey
	}
	pos += n
	if mountLen == 0 || uint64(len(key)-pos) < mountLen+1 {
		return "", 0, ErrInvalidKey
	}
	mount := MountID(string(key[pos : pos+int(mountLen)]))
	pos += int(mountLen)
	return mount, pos, nil
}
