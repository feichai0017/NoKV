package kv

import "errors"

var (
	// ErrChecksumMismatch indicates a CRC mismatch between expected and actual payload bytes.
	ErrChecksumMismatch = errors.New("checksum mismatch")
	// ErrBadChecksum indicates a mismatch between the stored CRC32 and the computed checksum.
	ErrBadChecksum = errors.New("bad check sum")
	// ErrPartialEntry indicates that an entry ended before a full payload could be decoded.
	ErrPartialEntry = errors.New("kv: partial entry")
)
