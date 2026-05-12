package fsmeta

import (
	"encoding/binary"
	"fmt"

	localdb "github.com/feichai0017/NoKV/local"
	"github.com/feichai0017/NoKV/utils"
)

// fsmeta key layout:
//
//	common prefix:
//	  magic[4] = "fsm\0"
//	  version  = 0x01
//	  mount_key_id be64
//	  affinity_bucket be16
//	  kind byte
//
//	kind bodies:
//	  mount   'm' : empty
//	  inode   'i' : inode be64
//	  dentry  'd' : parent inode be64 | name bytes
//	  chunk   'c' : inode be64 | chunk index be64
//	  session 's' : inode be64 | 0x01 | session bytes, or inode be64 | 0x00 for writer ownership
//	  usage   'u' : quota scope inode be64; scope 0 is mount-wide usage
//	  peras   'p' : record-kind byte | sealed segment root sha256
//
// Big-endian integer fields preserve numeric order inside each key family.
var keyMagic = []byte{'f', 's', 'm', 0}

const keySchemaVersion byte = 1

const (
	DefaultAffinityBucketCount = 16
	RootAffinityBucket         = AffinityBucket(0)
	encodedMountKeyBytes       = 8
	encodedBucketBytes         = 2
)

type AffinityBucket uint16

// KeyKind classifies one fsmeta key family.
type KeyKind byte

const (
	KeyKindMount   KeyKind = 'm'
	KeyKindInode   KeyKind = 'i'
	KeyKindDentry  KeyKind = 'd'
	KeyKindChunk   KeyKind = 'c'
	KeyKindSession KeyKind = 's'
	KeyKindUsage   KeyKind = 'u'
	KeyKindPeras   KeyKind = 'p'
)

const (
	PerasSegmentRecordObject byte = 'o'
	PerasSegmentRecordIndex  byte = 'i'
)

type KeyParts struct {
	MountKeyID  MountKeyID
	Bucket      AffinityBucket
	Kind        KeyKind
	Parent      InodeID
	Inode       InodeID
	UsageScope  InodeID
	PerasRecord byte
	PerasRoot   [32]byte
}

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
	case KeyKindPeras:
		return "peras"
	default:
		return fmt.Sprintf("unknown(%d)", byte(k))
	}
}

// EncodeMountKey returns the mount-level metadata record key.
func EncodeMountKey(mount MountIdentity) ([]byte, error) {
	return encodeKey(mount, RootAffinityBucket, KeyKindMount, nil)
}

// EncodeMountPrefix returns the common key prefix for one mount. All fsmeta
// records under the mount share this prefix regardless of key kind.
func EncodeMountPrefix(mount MountIdentity) ([]byte, error) {
	return encodeMountPrefix(mount, 0)
}

// EncodeMountKeyRange returns the half-open key range covering all fsmeta
// records under one mount.
func EncodeMountKeyRange(mount MountIdentity) (start, end []byte, err error) {
	start, err = EncodeMountPrefix(mount)
	if err != nil {
		return nil, nil, err
	}
	return start, prefixUpperBound(start), nil
}

// EncodeBucketPrefix returns the key prefix for one mount-local affinity
// bucket. Coordinator placement can split these byte ranges without learning
// fsmeta's record families.
func EncodeBucketPrefix(mount MountIdentity, bucket AffinityBucket) ([]byte, error) {
	prefix, err := encodeMountPrefix(mount, encodedBucketBytes)
	if err != nil {
		return nil, err
	}
	return appendBucket(prefix, bucket), nil
}

func EncodeBucketRange(mount MountIdentity, bucket AffinityBucket) (start, end []byte, err error) {
	start, err = EncodeBucketPrefix(mount, bucket)
	if err != nil {
		return nil, nil, err
	}
	return start, prefixUpperBound(start), nil
}

// EncodeInodeKey returns the inode attribute record key.
func EncodeInodeKey(mount MountIdentity, inode InodeID) ([]byte, error) {
	if err := validateInodeID(inode); err != nil {
		return nil, err
	}
	var body [8]byte
	binary.BigEndian.PutUint64(body[:], uint64(inode))
	return encodeKey(mount, BucketForInodeID(inode), KeyKindInode, body[:])
}

// EncodeDentryPrefix returns the scan prefix for entries directly under parent.
func EncodeDentryPrefix(mount MountIdentity, parent InodeID) ([]byte, error) {
	if err := validateInodeID(parent); err != nil {
		return nil, err
	}
	var body [8]byte
	binary.BigEndian.PutUint64(body[:], uint64(parent))
	return encodeKey(mount, BucketForInodeID(parent), KeyKindDentry, body[:])
}

// EncodeDentryKey returns the dentry record key for parent/name.
func EncodeDentryKey(mount MountIdentity, parent InodeID, name string) ([]byte, error) {
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
func EncodeChunkKey(mount MountIdentity, inode InodeID, chunk ChunkIndex) ([]byte, error) {
	if err := validateInodeID(inode); err != nil {
		return nil, err
	}
	var body [16]byte
	binary.BigEndian.PutUint64(body[:8], uint64(inode))
	binary.BigEndian.PutUint64(body[8:], uint64(chunk))
	return encodeKey(mount, BucketForInodeID(inode), KeyKindChunk, body[:])
}

// EncodeSessionKey returns the client/session state key.
func EncodeSessionKey(mount MountIdentity, inode InodeID, session SessionID) ([]byte, error) {
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
	return encodeKey(mount, BucketForInodeID(inode), KeyKindSession, body)
}

// EncodeSessionBucketPrefix returns the prefix for all session records in one
// mount-local affinity bucket. Maintenance scanners use this bucket-local
// prefix instead of the mount prefix so routed scans stay inside real fsmeta
// data ranges.
func EncodeSessionBucketPrefix(mount MountIdentity, bucket AffinityBucket) ([]byte, error) {
	return encodeKey(mount, bucket, KeyKindSession, nil)
}

// EncodeInodeSessionKey returns the exclusive writer-owner key for one inode.
func EncodeInodeSessionKey(mount MountIdentity, inode InodeID) ([]byte, error) {
	if err := validateInodeID(inode); err != nil {
		return nil, err
	}
	var body [9]byte
	binary.BigEndian.PutUint64(body[:8], uint64(inode))
	body[8] = 0x00
	return encodeKey(mount, BucketForInodeID(inode), KeyKindSession, body[:])
}

// EncodeUsageKey returns a quota usage/counter key. Scope 0 is mount-wide;
// non-zero scopes are direct quota accounting roots.
func EncodeUsageKey(mount MountIdentity, scope InodeID) ([]byte, error) {
	var body [8]byte
	binary.BigEndian.PutUint64(body[:], uint64(scope))
	return encodeKey(mount, BucketForInodeID(scope), KeyKindUsage, body[:])
}

// EncodePerasSegmentCatalogIndexKey returns the hidden per-bucket catalog key
// that points reads at one durable Peras segment object. It deliberately takes
// the rooted mount key id rather than a human mount name because recovery
// reconstructs it from already-encoded fsmeta keys.
func EncodePerasSegmentCatalogIndexKey(mountKeyID MountKeyID, bucket AffinityBucket, root [32]byte) ([]byte, error) {
	return encodePerasSegmentKey(mountKeyID, bucket, PerasSegmentRecordIndex, root)
}

// EncodePerasSegmentObjectKey returns the hidden segment-object key that stores
// one copy of a sealed segment payload. Per-bucket catalog keys point at this
// object so multi-bucket segments do not duplicate payload bytes.
func EncodePerasSegmentObjectKey(mountKeyID MountKeyID, bucket AffinityBucket, root [32]byte) ([]byte, error) {
	return encodePerasSegmentKey(mountKeyID, bucket, PerasSegmentRecordObject, root)
}

func encodePerasSegmentKey(mountKeyID MountKeyID, bucket AffinityBucket, record byte, root [32]byte) ([]byte, error) {
	if err := validateMountKeyID(mountKeyID); err != nil {
		return nil, err
	}
	if root == ([32]byte{}) {
		return nil, ErrInvalidKey
	}
	switch record {
	case PerasSegmentRecordObject, PerasSegmentRecordIndex:
	default:
		return nil, ErrInvalidKey
	}
	body := make([]byte, 1+len(root))
	body[0] = record
	copy(body[1:], root[:])
	return encodeKeyForMountKeyID(mountKeyID, bucket, KeyKindPeras, body), nil
}

// EncodePerasSegmentCatalogIndexPrefix returns the hidden catalog scan prefix
// for durable Peras segment indexes in one mount-local affinity bucket.
func EncodePerasSegmentCatalogIndexPrefix(mountKeyID MountKeyID, bucket AffinityBucket) ([]byte, error) {
	if err := validateMountKeyID(mountKeyID); err != nil {
		return nil, err
	}
	return encodeKeyForMountKeyID(mountKeyID, bucket, KeyKindPeras, []byte{PerasSegmentRecordIndex}), nil
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
	case KeyKindMount, KeyKindInode, KeyKindDentry, KeyKindChunk, KeyKindSession, KeyKindUsage, KeyKindPeras:
		return kind, nil
	default:
		return 0, ErrInvalidKeyKind
	}
}

// DentryNameOfKey extracts the final name component from a concrete dentry
// storage key. It is intended for watch diagnostics that receive committed
// storage keys but must not know the mount_key_id that produced them.
func DentryNameOfKey(key []byte) (string, bool) {
	_, kindPos, err := decodeHeaderParts(key)
	if err != nil || kindPos >= len(key) || KeyKind(key[kindPos]) != KeyKindDentry {
		return "", false
	}
	body := key[kindPos+1:]
	if len(body) <= 8 {
		return "", false
	}
	return string(body[8:]), true
}

// MountKeyIDOfKey returns the rooted storage mount identity encoded in a full
// fsmeta key.
func MountKeyIDOfKey(key []byte) (MountKeyID, bool) {
	mount, _, err := decodeHeaderParts(key)
	if err != nil {
		return 0, false
	}
	return mount, true
}

func BucketOfKey(key []byte) (AffinityBucket, bool) {
	_, pos, err := decodeMountPrefix(key)
	if err != nil || len(key)-pos < encodedBucketBytes+1 {
		return 0, false
	}
	return AffinityBucket(binary.BigEndian.Uint16(key[pos : pos+encodedBucketBytes])), true
}

func InspectKey(key []byte) (KeyParts, bool) {
	mount, bucketPos, err := decodeMountPrefix(key)
	if err != nil || len(key)-bucketPos < encodedBucketBytes+1 {
		return KeyParts{}, false
	}
	kindPos := bucketPos + encodedBucketBytes
	kind := KeyKind(key[kindPos])
	parts := KeyParts{
		MountKeyID: mount,
		Bucket:     AffinityBucket(binary.BigEndian.Uint16(key[bucketPos:kindPos])),
		Kind:       kind,
	}
	body := key[kindPos+1:]
	switch kind {
	case KeyKindMount:
		return parts, len(body) == 0
	case KeyKindDentry:
		if len(body) <= 8 {
			return KeyParts{}, false
		}
		parts.Parent = InodeID(binary.BigEndian.Uint64(body[:8]))
		return parts, true
	case KeyKindInode:
		if len(body) != 8 {
			return KeyParts{}, false
		}
		parts.Inode = InodeID(binary.BigEndian.Uint64(body[:8]))
		return parts, true
	case KeyKindChunk:
		if len(body) != 16 {
			return KeyParts{}, false
		}
		parts.Inode = InodeID(binary.BigEndian.Uint64(body[:8]))
		return parts, true
	case KeyKindSession:
		if len(body) < 9 {
			return KeyParts{}, false
		}
		parts.Inode = InodeID(binary.BigEndian.Uint64(body[:8]))
		return parts, true
	case KeyKindUsage:
		if len(body) != 8 {
			return KeyParts{}, false
		}
		parts.UsageScope = InodeID(binary.BigEndian.Uint64(body[:8]))
		return parts, true
	case KeyKindPeras:
		if len(body) != 1+len(parts.PerasRoot) {
			return KeyParts{}, false
		}
		switch body[0] {
		case PerasSegmentRecordObject, PerasSegmentRecordIndex:
			parts.PerasRecord = body[0]
		default:
			return KeyParts{}, false
		}
		copy(parts.PerasRoot[:], body[1:])
		return parts, true
	default:
		return KeyParts{}, false
	}
}

// MountKeyResolver adapts fsmeta keys to raftstore MVCC GC mount-scoped
// retention policy.
func MountKeyResolver(key []byte) (uint64, bool) {
	mount, ok := MountKeyIDOfKey(key)
	return uint64(mount), ok
}

// UserKeyShape exposes fsmeta key shape to the local engine without making the
// engine import fsmeta. The shape keeps all keys in one mount-local affinity
// bucket on the same local shard and gives prefix bloom filters record-family
// prefixes that match the current key layout.
func UserKeyShape(key []byte) localdb.UserKeyShape {
	_, bucketPos, err := decodeMountPrefix(key)
	if err != nil || len(key)-bucketPos < encodedBucketBytes+1 {
		return localdb.UserKeyShape{}
	}
	kindPos := bucketPos + encodedBucketBytes
	kind := KeyKind(key[kindPos])
	bodyPos := kindPos + 1
	shape := localdb.UserKeyShape{
		LocalityPrefix: key[:kindPos],
		ShardKey:       key[:kindPos],
		Family:         byte(kind),
	}
	switch kind {
	case KeyKindMount:
		shape.BloomPrefix = key[:bodyPos]
	case KeyKindInode, KeyKindDentry, KeyKindChunk, KeyKindSession, KeyKindUsage:
		if len(key) < bodyPos+8 {
			return localdb.UserKeyShape{}
		}
		shape.BloomPrefix = key[:bodyPos+8]
	case KeyKindPeras:
		if len(key) < bodyPos+33 {
			return localdb.UserKeyShape{}
		}
		shape.BloomPrefix = key[:bodyPos+33]
	default:
		return localdb.UserKeyShape{}
	}
	return shape
}

// ShardForUserKey is the fsmeta physical placement policy used by local DB
// runtimes. It keeps semantically-related metadata keys on one local shard
// without teaching the local engine fsmeta's key families.
func ShardForUserKey(key []byte, shardCount int) int {
	shardCount = utils.NormalizeShardCount(shardCount)
	if shardCount <= 1 {
		return 0
	}
	shape := UserKeyShape(key)
	if len(shape.ShardKey) > 0 {
		return utils.ShardForUserKey(shape.ShardKey, shardCount)
	}
	if len(shape.LocalityPrefix) > 0 {
		return utils.ShardForUserKey(shape.LocalityPrefix, shardCount)
	}
	return utils.ShardForUserKey(key, shardCount)
}

func BucketForInodeID(inode InodeID) AffinityBucket {
	if inode == RootInode {
		return RootAffinityBucket
	}
	var body [8]byte
	binary.BigEndian.PutUint64(body[:], uint64(inode))
	return AffinityBucket(utils.ShardForUserKey(body[:], DefaultAffinityBucketCount))
}

func ChooseWorkspaceBucket(mount MountIdentity, name string) AffinityBucket {
	var mountBuf [8]byte
	binary.BigEndian.PutUint64(mountBuf[:], uint64(mount.MountKeyID))
	key := make([]byte, 0, len(mountBuf)+1+len(name))
	key = append(key, mountBuf[:]...)
	key = append(key, 0)
	key = append(key, name...)
	return AffinityBucket(utils.ShardForUserKey(key, DefaultAffinityBucketCount))
}

func encodeKey(mount MountIdentity, bucket AffinityBucket, kind KeyKind, body []byte) ([]byte, error) {
	if err := validateMountIdentity(mount); err != nil {
		return nil, err
	}
	return encodeKeyForMountKeyID(mount.MountKeyID, bucket, kind, body), nil
}

func encodeKeyForMountKeyID(mountKeyID MountKeyID, bucket AffinityBucket, kind KeyKind, body []byte) []byte {
	out := make([]byte, 0, len(keyMagic)+1+encodedMountKeyBytes+encodedBucketBytes+1+len(body))
	out = append(out, keyMagic...)
	out = append(out, keySchemaVersion)
	var mountBuf [encodedMountKeyBytes]byte
	binary.BigEndian.PutUint64(mountBuf[:], uint64(mountKeyID))
	out = append(out, mountBuf[:]...)
	out = appendBucket(out, bucket)
	out = append(out, byte(kind))
	out = append(out, body...)
	return out
}

func encodeMountPrefix(mount MountIdentity, suffixLen int) ([]byte, error) {
	if err := validateMountIdentity(mount); err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(keyMagic)+1+encodedMountKeyBytes+suffixLen)
	out = append(out, keyMagic...)
	out = append(out, keySchemaVersion)
	var buf [encodedMountKeyBytes]byte
	binary.BigEndian.PutUint64(buf[:], uint64(mount.MountKeyID))
	out = append(out, buf[:]...)
	return out, nil
}

func appendBucket(out []byte, bucket AffinityBucket) []byte {
	var buf [encodedBucketBytes]byte
	binary.BigEndian.PutUint16(buf[:], uint16(bucket))
	return append(out, buf[:]...)
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

func decodeHeaderParts(key []byte) (MountKeyID, int, error) {
	mount, pos, err := decodeMountPrefix(key)
	if err != nil {
		return 0, 0, err
	}
	if len(key)-pos < encodedBucketBytes+1 {
		return 0, 0, ErrInvalidKey
	}
	return mount, pos + encodedBucketBytes, nil
}

func decodeMountPrefix(key []byte) (MountKeyID, int, error) {
	if len(key) < len(keyMagic)+1+encodedMountKeyBytes {
		return 0, 0, ErrInvalidKey
	}
	for i := range keyMagic {
		if key[i] != keyMagic[i] {
			return 0, 0, ErrInvalidKey
		}
	}
	pos := len(keyMagic)
	if key[pos] != keySchemaVersion {
		return 0, 0, ErrInvalidKey
	}
	pos++
	if len(key)-pos < encodedMountKeyBytes {
		return 0, 0, ErrInvalidKey
	}
	mount := MountKeyID(binary.BigEndian.Uint64(key[pos : pos+encodedMountKeyBytes]))
	if mount == 0 {
		return 0, 0, ErrInvalidKey
	}
	pos += encodedMountKeyBytes
	return mount, pos, nil
}
