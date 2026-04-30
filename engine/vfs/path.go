package vfs

import (
	"fmt"
	"math"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

// FID parses the file ID from an sstable filename.
func FID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, ".sst") {
		return 0
	}
	name = strings.TrimSuffix(name, ".sst")
	id, err := strconv.Atoi(name)
	if err != nil {
		return 0
	}
	if id < 0 || uint64(id) > math.MaxUint32 {
		return 0
	}
	return uint64(id)
}

// FileNameSSTable returns the SSTable filename for the given ID.
func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}

// LoadIDMap returns the set of SSTable IDs present in dir.
func LoadIDMap(fs FS, dir string) map[uint64]struct{} {
	fs = Ensure(fs)
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
