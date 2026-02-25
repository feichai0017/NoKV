package NoKV

import "testing"

func BenchmarkDBSetSmall(b *testing.B) {
	db := newBenchDB(b, nil)
	value := make([]byte, 32)
	key := benchKeyBuffer()
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))

	for i := 0; b.Loop(); i++ {
		setBenchKey(key, uint64(i))
		if err := db.Set(key, value); err != nil {
			b.Fatalf("set: %v", err)
		}
	}
}

func BenchmarkDBSetLarge(b *testing.B) {
	db := newBenchDB(b, func(opt *Options) {
		opt.ValueThreshold = 64
	})
	value := make([]byte, 4<<10)
	key := benchKeyBuffer()
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))

	for i := 0; b.Loop(); i++ {
		setBenchKey(key, uint64(i))
		if err := db.Set(key, value); err != nil {
			b.Fatalf("set: %v", err)
		}
	}
}

func BenchmarkDBGetSmall(b *testing.B) {
	db := newBenchDB(b, nil)
	value := make([]byte, 64)
	keys := loadBenchKeys(b, db, 10_000, value)
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))

	for i := 0; b.Loop(); i++ {
		if _, err := db.Get(keys[i%len(keys)]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkDBGetLarge(b *testing.B) {
	db := newBenchDB(b, func(opt *Options) {
		opt.ValueThreshold = 64
	})
	value := make([]byte, 4<<10)
	keys := loadBenchKeys(b, db, 10_000, value)
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))

	for i := 0; b.Loop(); i++ {
		if _, err := db.Get(keys[i%len(keys)]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}
