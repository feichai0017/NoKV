package utils

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/vfs"
	"github.com/pkg/errors"
)

// FID parses the file ID from an sstable filename.
func FID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, ".sst") {
		return 0
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, ".sst")
	id, err := strconv.Atoi(name)
	if err != nil {
		return 0
	}
	// Ensure the parsed ID fits into 32 bits so downstream uint32 casts are safe.
	if id < 0 || uint64(id) > math.MaxUint32 {
		return 0
	}
	return uint64(id)
}

func VlogFilePath(dirPath string, fid uint32) string {
	return fmt.Sprintf("%s%s%05d.vlog", dirPath, string(os.PathSeparator), fid)
}

// FileNameSSTable returns the SSTable filename for the given ID.
func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}

// LoadIDMap Get the id of all sst files in the current folder
func LoadIDMap(fs vfs.FS, dir string) map[uint64]struct{} {
	fs = vfs.Ensure(fs)
	idMap := make(map[uint64]struct{})
	entries, err := fs.ReadDir(dir)
	if err != nil {
		return idMap
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fileID := FID(entry.Name())
		if fileID != 0 {
			idMap[fileID] = struct{}{}
		}
	}
	return idMap
}

// CompareKeys checks the key without timestamp and checks the timestamp if keyNoTs
// is same.
// a<timestamp> would be sorted higher than aa<timestamp> if we use bytes.compare
// All keys should have timestamp.
func CompareKeys(key1, key2 []byte) int {
	if len(key1) <= 8 || len(key2) <= 8 {
		CondPanicFunc(true, func() error {
			return fmt.Errorf("%s,%s < 8", string(key1), string(key2))
		})
	}
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

// CompareUserKeys compares user-key portions of two internal keys.
// Both inputs must use the InternalKey layout.
func CompareUserKeys(key1, key2 []byte) int {
	if len(key1) == 0 || len(key2) == 0 {
		return bytes.Compare(key1, key2)
	}
	_, uk1, _, ok1 := kv.SplitInternalKey(key1)
	_, uk2, _, ok2 := kv.SplitInternalKey(key2)
	if !ok1 || !ok2 {
		CondPanicFunc(true, func() error {
			return fmt.Errorf("CompareUserKeys requires internal keys (ok1=%t ok2=%t)", ok1, ok2)
		})
	}
	return bytes.Compare(uk1, uk2)
}

// VerifyChecksum crc32
func VerifyChecksum(data []byte, expected []byte) error {
	if len(expected) < 8 {
		return errors.Wrapf(ErrChecksumMismatch, "expected checksum length %d < 8", len(expected))
	}
	actual := uint64(crc32.Checksum(data, kv.CastagnoliCrcTable))
	expectedU64 := kv.BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(ErrChecksumMismatch, "actual: %d, expected: %d", actual, expectedU64)
	}

	return nil
}

// CalculateChecksum _
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, kv.CastagnoliCrcTable))
}
