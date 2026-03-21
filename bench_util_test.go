package NoKV

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func openTestDB(t testing.TB, opt *Options) *DB {
	t.Helper()
	db, err := Open(opt)
	require.NoError(t, err)
	return db
}

func newBenchDB(b *testing.B, optFn func(*Options)) *DB {
	b.Helper()
	opt := NewDefaultOptions()
	opt.WorkDir = b.TempDir()
	opt.EnableWALWatchdog = false
	opt.ValueLogGCInterval = 0
	opt.SyncWrites = false
	opt.ManifestSync = false
	opt.WriteBatchWait = 0
	if optFn != nil {
		optFn(opt)
	}
	db := openTestDB(b, opt)
	b.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func benchKeyBuffer() []byte {
	key := make([]byte, 16)
	copy(key, "benchkey")
	return key
}

func setBenchKey(buf []byte, i uint64) {
	binary.LittleEndian.PutUint64(buf[len(buf)-8:], i)
}

func makeBenchKey(i int) []byte {
	key := benchKeyBuffer()
	setBenchKey(key, uint64(i))
	return key
}

func loadBenchKeys(b *testing.B, db *DB, n int, value []byte) [][]byte {
	b.Helper()
	keys := make([][]byte, n)
	for i := range n {
		key := makeBenchKey(i)
		if err := db.Set(key, value); err != nil {
			b.Fatalf("preload key %d: %v", i, err)
		}
		keys[i] = key
	}
	// Give background workers a brief moment to settle before timing.
	time.Sleep(10 * time.Millisecond)
	return keys
}
