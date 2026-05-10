package mvcc

import (
	"encoding/binary"
	"fmt"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

const (
	lockCodecVersion  byte = 1
	writeCodecVersion byte = 1
)

// Lock encoding layout:
//
//	version byte = 0x01
//	primary_len uvarint
//	primary key bytes
//	start_ts uvarint
//	start_time_unix_ms uvarint
//	ttl_ms uvarint
//	mutation_kind byte
//	min_commit_ts uvarint
//	has_short_value byte
//	if has_short_value == 1:
//	  short_value_len uvarint
//	  short_value bytes
//	if short value or expires_at is present:
//	  expires_at uvarint
//
// TTL is measured from start_time_unix_ms. start_ts remains the MVCC logical
// timestamp and must not be used as a physical clock.

// Lock captures the metadata recorded in the lock column family during
// prewrite.
type Lock struct {
	Primary []byte
	// Ts is the logical transaction start timestamp from the TSO.
	Ts uint64
	// StartTime is the physical Unix millisecond time when the lock was
	// created. TTL is measured from StartTime, never from Ts.
	StartTime   uint64
	TTL         uint64
	Kind        kvrpcpb.Mutation_Op
	MinCommitTs uint64
	ShortValue  []byte
	ExpiresAt   uint64
}

// EncodeLock serialises a lock entry.
func EncodeLock(lock Lock) []byte {
	primaryLen := len(lock.Primary)
	buf := make([]byte, 0, 1+binary.MaxVarintLen64*7+primaryLen+len(lock.ShortValue))
	buf = append(buf, lockCodecVersion)
	buf = binary.AppendUvarint(buf, uint64(primaryLen))
	buf = append(buf, lock.Primary...)
	buf = binary.AppendUvarint(buf, lock.Ts)
	buf = binary.AppendUvarint(buf, lock.StartTime)
	buf = binary.AppendUvarint(buf, lock.TTL)
	buf = append(buf, byte(lock.Kind))
	buf = binary.AppendUvarint(buf, lock.MinCommitTs)
	if len(lock.ShortValue) > 0 {
		buf = append(buf, 1)
		buf = binary.AppendUvarint(buf, uint64(len(lock.ShortValue)))
		buf = append(buf, lock.ShortValue...)
	} else {
		buf = append(buf, 0)
	}
	if len(lock.ShortValue) > 0 || lock.ExpiresAt > 0 {
		buf = binary.AppendUvarint(buf, lock.ExpiresAt)
	}
	return buf
}

// DecodeLock deserialises a lock entry.
func DecodeLock(data []byte) (Lock, error) {
	if len(data) == 0 {
		return Lock{}, fmt.Errorf("mvcc: empty lock payload")
	}
	if data[0] != lockCodecVersion {
		return Lock{}, fmt.Errorf("mvcc: unsupported lock version %d", data[0])
	}
	pos := 1
	readUvarint := func() (uint64, error) {
		val, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return 0, fmt.Errorf("mvcc: lock payload truncated")
		}
		pos += n
		return val, nil
	}
	primaryLen, err := readUvarint()
	if err != nil {
		return Lock{}, err
	}
	if primaryLen > uint64(len(data)-pos) {
		return Lock{}, fmt.Errorf("mvcc: lock primary truncated")
	}
	lock := Lock{
		Primary: append([]byte(nil), data[pos:pos+int(primaryLen)]...),
	}
	pos += int(primaryLen)
	if lock.Ts, err = readUvarint(); err != nil {
		return Lock{}, err
	}
	if lock.StartTime, err = readUvarint(); err != nil {
		return Lock{}, err
	}
	if lock.TTL, err = readUvarint(); err != nil {
		return Lock{}, err
	}
	if lock.TTL > 0 && lock.StartTime == 0 {
		return Lock{}, fmt.Errorf("mvcc: lock start time missing")
	}
	if pos >= len(data) {
		return Lock{}, fmt.Errorf("mvcc: lock kind missing")
	}
	lock.Kind = kvrpcpb.Mutation_Op(data[pos])
	pos++
	if pos < len(data) {
		if lock.MinCommitTs, err = readUvarint(); err != nil {
			return Lock{}, err
		}
	}
	if pos < len(data) {
		hasShort := data[pos]
		pos++
		switch hasShort {
		case 0:
		case 1:
			sz, err := readUvarint()
			if err != nil {
				return Lock{}, err
			}
			if sz > uint64(len(data)-pos) {
				return Lock{}, fmt.Errorf("mvcc: lock short value truncated")
			}
			lock.ShortValue = append([]byte(nil), data[pos:pos+int(sz)]...)
			pos += int(sz)
		default:
			return Lock{}, fmt.Errorf("mvcc: invalid lock short value marker %d", hasShort)
		}
	}
	if pos < len(data) {
		if lock.ExpiresAt, err = readUvarint(); err != nil {
			return Lock{}, err
		}
	}
	return lock, nil
}

// Write captures the payload stored in the write column family once a
// transaction commits.
type Write struct {
	Kind       kvrpcpb.Mutation_Op
	StartTs    uint64
	ShortValue []byte
	ExpiresAt  uint64
}

const DefaultShortValueMaxBytes = 128

// CanInlineShortValue reports whether a Put value can be carried by the MVCC
// lock/write records instead of the default CF. Empty values stay on the
// default CF because ShortValue's nil/empty representation is reserved for
// "not inlined".
func CanInlineShortValue(kind kvrpcpb.Mutation_Op, value []byte) bool {
	return kind == kvrpcpb.Mutation_Put && len(value) > 0 && len(value) <= DefaultShortValueMaxBytes
}

// Write encoding layout:
//
//	version byte = 0x01
//	mutation_kind byte
//	start_ts uvarint
//	has_short_value byte
//	if has_short_value == 1:
//	  short_value_len uvarint
//	  short_value bytes
//	if short value or expires_at is present:
//	  expires_at_unix_ms uvarint
//
// Missing expires_at means zero. That keeps ordinary non-TTL writes compact
// while preserving TTL metadata for short-value writes.
//
// EncodeWrite serialises a write entry.
func EncodeWrite(w Write) []byte {
	buf := make([]byte, 0, 1+1+binary.MaxVarintLen64*3+len(w.ShortValue))
	buf = append(buf, writeCodecVersion)
	buf = append(buf, byte(w.Kind))
	buf = binary.AppendUvarint(buf, w.StartTs)
	if len(w.ShortValue) > 0 {
		buf = append(buf, 1)
		buf = binary.AppendUvarint(buf, uint64(len(w.ShortValue)))
		buf = append(buf, w.ShortValue...)
	} else {
		buf = append(buf, 0)
	}
	if len(w.ShortValue) > 0 || w.ExpiresAt > 0 {
		buf = binary.AppendUvarint(buf, w.ExpiresAt)
	}
	return buf
}

// DecodeWrite deserialises a write entry.
func DecodeWrite(data []byte) (Write, error) {
	if len(data) < 3 {
		return Write{}, fmt.Errorf("mvcc: write payload too short")
	}
	if data[0] != writeCodecVersion {
		return Write{}, fmt.Errorf("mvcc: unsupported write version %d", data[0])
	}
	pos := 1
	write := Write{Kind: kvrpcpb.Mutation_Op(data[pos])}
	pos++
	startTs, n := binary.Uvarint(data[pos:])
	if n <= 0 {
		return Write{}, fmt.Errorf("mvcc: write payload truncated")
	}
	write.StartTs = startTs
	pos += n
	if pos >= len(data) {
		return write, nil
	}
	hasShort := data[pos]
	pos++
	if hasShort == 1 {
		sz, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return Write{}, fmt.Errorf("mvcc: write short value truncated")
		}
		pos += n
		if pos+int(sz) > len(data) {
			return Write{}, fmt.Errorf("mvcc: write short value overflow")
		}
		write.ShortValue = append([]byte(nil), data[pos:pos+int(sz)]...)
		pos += int(sz)
	}
	if pos < len(data) {
		expiresAt, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return Write{}, fmt.Errorf("mvcc: write expires_at truncated")
		}
		write.ExpiresAt = expiresAt
	}
	return write, nil
}
