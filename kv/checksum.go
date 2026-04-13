package kv

import (
	"hash/crc32"

	"github.com/pkg/errors"
)

// ErrChecksumMismatch is returned at checksum mismatch.
var ErrChecksumMismatch = errors.New("checksum mismatch")

// VerifyChecksum crc32
func VerifyChecksum(data []byte, expected []byte) error {
	if len(expected) < 8 {
		return errors.Wrapf(ErrChecksumMismatch, "expected checksum length %d < 8", len(expected))
	}
	actual := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	expectedU64 := BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(ErrChecksumMismatch, "actual: %d, expected: %d", actual, expectedU64)
	}

	return nil
}

// CalculateChecksum _
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}
