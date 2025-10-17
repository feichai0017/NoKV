package utils

import (
	"fmt"
	"strings"
)

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

var cfByName = func() map[string]ColumnFamily {
	out := make(map[string]ColumnFamily, len(cfNames))
	for cf, name := range cfNames {
		out[name] = cf
	}
	return out
}()

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

// ParseColumnFamily returns the column family for the provided name.
func ParseColumnFamily(name string) (ColumnFamily, error) {
	if cf, ok := cfByName[strings.ToLower(name)]; ok {
		return cf, nil
	}
	return ColumnFamily(0), fmt.Errorf("unknown column family %q", name)
}

// EncodeKeyWithCF prefixes userKey with the column family marker.
func EncodeKeyWithCF(cf ColumnFamily, userKey []byte) []byte {
	if !cf.Valid() {
		cf = CFDefault
	}
	out := make([]byte, len(userKey)+cfHeaderSize)
	out[0] = cfMarker0
	out[1] = cfMarker1
	out[2] = cfMarker2
	out[3] = byte(cf)
	copy(out[cfHeaderSize:], userKey)
	return out
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

// InternalKey generates an LSM key carrying both CF prefix and timestamp.
func InternalKey(cf ColumnFamily, key []byte, ts uint64) []byte {
	return KeyWithTs(EncodeKeyWithCF(cf, key), ts)
}

// SplitInternalKey decodes column family, user key, and timestamp from an internal key.
func SplitInternalKey(internal []byte) (ColumnFamily, []byte, uint64) {
	ts := ParseTs(internal)
	base := ParseKey(internal)
	cf, userKey, _ := DecodeKeyCF(base)
	return cf, userKey, ts
}
