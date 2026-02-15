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
		_ = Err(err)
		return 0
	}
	// Ensure the parsed ID fits into 32 bits so downstream uint32 casts are safe.
	if id < 0 || uint64(id) > math.MaxUint32 {
		_ = Err(fmt.Errorf("FID out of range: %d", id))
		return 0
	}
	return uint64(id)
}

func VlogFilePath(dirPath string, fid uint32) string {
	return fmt.Sprintf("%s%s%05d.vlog", dirPath, string(os.PathSeparator), fid)
}

// CreateSyncedFile creates a new file (using O_EXCL), errors if it already existed.
func CreateSyncedFile(fs vfs.FS, filename string, sync bool) (vfs.File, error) {
	fs = vfs.Ensure(fs)
	flags := os.O_RDWR | os.O_CREATE | os.O_EXCL
	if sync {
		flags |= datasyncFileFlag
	}
	return fs.OpenFileHandle(filename, flags, 0600)
}

// FileNameSSTable returns the SSTable filename for the given ID.
func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}

// SyncDir When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes). (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func SyncDir(fs vfs.FS, dir string) error {
	fs = vfs.Ensure(fs)
	f, err := fs.OpenHandle(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s.", dir)
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "While syncing directory: %s.", dir)
	}
	return errors.Wrapf(closeErr, "While closing directory: %s.", dir)
}

// LoadIDMap Get the id of all sst files in the current folder
func LoadIDMap(fs vfs.FS, dir string) map[uint64]struct{} {
	fs = vfs.Ensure(fs)
	idMap := make(map[uint64]struct{})
	entries, err := fs.ReadDir(dir)
	if err != nil {
		_ = Err(err)
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
		CondPanic(true, fmt.Errorf("%s,%s < 8", string(key1), string(key2)))
	}
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

// CompareUserKeys compares keys ignoring any internal timestamp suffix.
// It accepts either internal keys or raw user keys.
func CompareUserKeys(key1, key2 []byte) int {
	return bytes.Compare(kv.ParseKey(key1), kv.ParseKey(key2))
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

// RemoveDir _
func RemoveDir(fs vfs.FS, dir string) {
	fs = vfs.Ensure(fs)
	if err := fs.RemoveAll(dir); err != nil {
		panic(err)
	}
}
