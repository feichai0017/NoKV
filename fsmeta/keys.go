package fsmeta

import (
	"encoding/binary"
	"fmt"

	"github.com/feichai0017/NoKV/utils"
)

// fsmeta key layout:
//
//	common prefix:
//	  magic[4] = "fsm\0"
//	  version  = 0x01
//	  mount_len uvarint
//	  mount bytes
//	  kind byte
//
//	kind bodies:
//	  mount   'm' : empty
//	  inode   'i' : inode be64
//	  dentry  'd' : parent inode be64 | name bytes
//	  chunk   'c' : inode be64 | chunk index be64
//	  session 's' : inode be64 | 0x01 | session bytes, or inode be64 | 0x00 for writer ownership
//	  usage   'u' : quota scope inode be64; scope 0 is mount-wide usage
//
// Big-endian integer fields preserve numeric order inside each key family.
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
func EncodeSessionKey(mount MountID, inode InodeID, session SessionID) ([]byte, error) {
	if err := validateInodeID(inode); err != nil {
		return nil, err
	}
	if err := validateSessionID(session); err != nil {
		return nil, err
	}
	body := make([]byte, 9, 9+len(session))
	binary.BigEndian.PutUint64(body[:8], uint64(inode))
	body[8] = 0x01
	body = append(body, string(session)...)
	return encodeKey(mount, KeyKindSession, body)
}

// EncodeInodeSessionKey returns the exclusive writer-owner key for one inode.
func EncodeInodeSessionKey(mount MountID, inode InodeID) ([]byte, error) {
	if err := validateInodeID(inode); err != nil {
		return nil, err
	}
	var body [9]byte
	binary.BigEndian.PutUint64(body[:8], uint64(inode))
	body[8] = 0x00
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

// ShardForUserKey is the fsmeta physical placement policy used by local DB
// runtimes. It keeps semantically-related metadata keys on one local shard
// without teaching the local engine fsmeta's key families.
func ShardForUserKey(key []byte, shardCount int) int {
	shardCount = utils.NormalizeShardCount(shardCount)
	if shardCount <= 1 {
		return 0
	}
	_, pos, err := decodeHeaderParts(key)
	if err != nil || pos >= len(key) {
		return utils.ShardForUserKey(key, shardCount)
	}
	kind := KeyKind(key[pos])
	body := key[pos+1:]
	switch kind {
	case KeyKindMount:
		mount, _, err := decodeHeaderParts(key)
		if err != nil {
			return utils.ShardForUserKey(key, shardCount)
		}
		return utils.ShardForUserKey([]byte(mount), shardCount)
	case KeyKindInode:
		if len(body) < 8 {
			return utils.ShardForUserKey(key, shardCount)
		}
		return shardForInodeAffinity(InodeID(binary.BigEndian.Uint64(body[:8])), shardCount)
	case KeyKindDentry:
		if len(body) < 8 {
			return utils.ShardForUserKey(key, shardCount)
		}
		parent := InodeID(binary.BigEndian.Uint64(body[:8]))
		if parent == RootInode && len(body) > 8 {
			return utils.ShardForUserKey(body[8:], shardCount)
		}
		return shardForInodeAffinity(parent, shardCount)
	case KeyKindChunk:
		if len(body) < 8 {
			return utils.ShardForUserKey(key, shardCount)
		}
		return shardForInodeAffinity(InodeID(binary.BigEndian.Uint64(body[:8])), shardCount)
	case KeyKindSession:
		if len(body) < 9 {
			return utils.ShardForUserKey(key, shardCount)
		}
		return shardForInodeAffinity(InodeID(binary.BigEndian.Uint64(body[:8])), shardCount)
	case KeyKindUsage:
		if len(body) < 8 {
			return utils.ShardForUserKey(key, shardCount)
		}
		return shardForInodeAffinity(InodeID(binary.BigEndian.Uint64(body[:8])), shardCount)
	default:
		return utils.ShardForUserKey(key, shardCount)
	}
}

func shardForInodeAffinity(inode InodeID, shardCount int) int {
	var body [8]byte
	binary.BigEndian.PutUint64(body[:], uint64(inode))
	return utils.ShardForUserKey(body[:], shardCount)
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
