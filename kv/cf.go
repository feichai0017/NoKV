package kv

import "fmt"

// ColumnFamily identifies a logical column family.
type ColumnFamily uint8

const (
	// CFDefault stores user values.
	CFDefault ColumnFamily = iota
	// CFLock stores transaction lock records.
	CFLock
	// CFWrite stores transaction write records.
	CFWrite

	maxColumnFamily = CFWrite

	cfMarker0    byte = 0xFF
	cfMarker1    byte = 'C'
	cfMarker2    byte = 'F'
	cfHeaderSize      = 4 // marker + family id
)

var cfNames = map[ColumnFamily]string{
	CFDefault: "default",
	CFLock:    "lock",
	CFWrite:   "write",
}

// String implements fmt.Stringer.
func (cf ColumnFamily) String() string {
	if name, ok := cfNames[cf]; ok {
		return name
	}
	return fmt.Sprintf("cf(%d)", cf)
}

// Valid reports whether the column family is defined.
func (cf ColumnFamily) Valid() bool {
	return cf <= maxColumnFamily
}

// DecodeKeyCF returns the column family and user key for an encoded key without timestamp.
func DecodeKeyCF(key []byte) (ColumnFamily, []byte, bool) {
	if len(key) >= cfHeaderSize &&
		key[0] == cfMarker0 &&
		key[1] == cfMarker1 &&
		key[2] == cfMarker2 {
		cf := ColumnFamily(key[3])
		if cf.Valid() {
			return cf, key[cfHeaderSize:], true
		}
	}
	return CFDefault, key, false
}
