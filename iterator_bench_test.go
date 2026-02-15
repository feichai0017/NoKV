package NoKV

import (
	"testing"

	"github.com/feichai0017/NoKV/utils"
)

func BenchmarkDBIteratorScan(b *testing.B) {
	db := newBenchDB(b, nil)
	value := make([]byte, 128)
	_ = loadBenchKeys(b, db, 20_000, value)
	it := db.NewIterator(&utils.Options{IsAsc: true})
	defer func() { _ = it.Close() }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it.Rewind()
		for it.Valid() {
			it.Next()
		}
	}
}

func BenchmarkDBIteratorSeek(b *testing.B) {
	db := newBenchDB(b, nil)
	value := make([]byte, 128)
	keys := loadBenchKeys(b, db, 20_000, value)
	it := db.NewIterator(&utils.Options{IsAsc: true})
	defer func() { _ = it.Close() }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		it.Seek(key)
		if it.Valid() {
			_ = it.Item()
		}
	}
}
