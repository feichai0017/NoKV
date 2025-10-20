package mvcc

import (
	"encoding/binary"
	"fmt"

	"github.com/feichai0017/NoKV/pb"
)

const (
	lockCodecVersion  byte = 1
	writeCodecVersion byte = 1
)

// Lock captures the metadata recorded in the lock column family during
// prewrite.
type Lock struct {
	Primary     []byte
	Ts          uint64
	TTL         uint64
	Kind        pb.Mutation_Op
	MinCommitTs uint64
}

// EncodeLock serialises a lock entry.
func EncodeLock(lock Lock) []byte {
	primaryLen := len(lock.Primary)
	buf := make([]byte, 0, 1+binary.MaxVarintLen64*4+primaryLen)
	buf = append(buf, lockCodecVersion)
	buf = binary.AppendUvarint(buf, uint64(primaryLen))
	buf = append(buf, lock.Primary...)
	buf = binary.AppendUvarint(buf, lock.Ts)
	buf = binary.AppendUvarint(buf, lock.TTL)
	buf = append(buf, byte(lock.Kind))
	buf = binary.AppendUvarint(buf, lock.MinCommitTs)
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
	if pos+int(primaryLen) > len(data) {
		return Lock{}, fmt.Errorf("mvcc: lock primary truncated")
	}
	lock := Lock{
		Primary: append([]byte(nil), data[pos:pos+int(primaryLen)]...),
	}
	pos += int(primaryLen)
	if lock.Ts, err = readUvarint(); err != nil {
		return Lock{}, err
	}
	if lock.TTL, err = readUvarint(); err != nil {
		return Lock{}, err
	}
	if pos >= len(data) {
		return Lock{}, fmt.Errorf("mvcc: lock kind missing")
	}
	lock.Kind = pb.Mutation_Op(data[pos])
	pos++
	if pos < len(data) {
		if lock.MinCommitTs, err = readUvarint(); err != nil {
			return Lock{}, err
		}
	}
	return lock, nil
}

// Write captures the payload stored in the write column family once a
// transaction commits.
type Write struct {
	Kind       pb.Mutation_Op
	StartTs    uint64
	ShortValue []byte
}

// EncodeWrite serialises a write entry.
func EncodeWrite(w Write) []byte {
	buf := make([]byte, 0, 1+1+binary.MaxVarintLen64*2+len(w.ShortValue))
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
	write := Write{Kind: pb.Mutation_Op(data[pos])}
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
	if data[pos] == 1 {
		pos++
		sz, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return Write{}, fmt.Errorf("mvcc: write short value truncated")
		}
		pos += n
		if pos+int(sz) > len(data) {
			return Write{}, fmt.Errorf("mvcc: write short value overflow")
		}
		write.ShortValue = append([]byte(nil), data[pos:pos+int(sz)]...)
	}
	return write, nil
}
