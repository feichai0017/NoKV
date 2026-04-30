package kv

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	// BitDelete marks an entry as a deletion tombstone.
	BitDelete byte = 1 << 0
	// BitValuePointer marks a legacy value-log pointer. New writes must not set it.
	BitValuePointer byte = 1 << 1
	// BitRangeDelete marks an entry as a range tombstone.
	BitRangeDelete byte = 1 << 2

	// MaxEntryHeaderSize defines the maximum number of bytes required to encode an EntryHeader.
	MaxEntryHeaderSize int = 4 * binary.MaxVarintLen64
)

var (
	// CastagnoliCrcTable is the shared CRC32 polynomial used across WAL and SST codecs.
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)
