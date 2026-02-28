package hotring

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkTouch(b *testing.B) {
	ring := NewHotRing(12, nil)
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.Touch(keys[i%len(keys)])
	}
}

func BenchmarkTouchParallel(b *testing.B) {
	ring := NewHotRing(12, nil)
	b.SetParallelism(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			ring.Touch(fmt.Sprintf("key-%d", idx%1024))
			idx++
		}
	})
}

func BenchmarkTouchAndClamp(b *testing.B) {
	ring := NewHotRing(12, nil)
	limit := int32(64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.TouchAndClamp("hot", limit)
	}
}

func BenchmarkFrequency(b *testing.B) {
	ring := NewHotRing(12, nil)
	for i := range 10000 {
		ring.Touch(fmt.Sprintf("key-%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.Frequency("key-7777")
	}
}

func BenchmarkTopN(b *testing.B) {
	ring := NewHotRing(12, nil)
	for i := range 50000 {
		ring.Touch(fmt.Sprintf("key-%d", i))
		if i%7 == 0 {
			ring.Touch("hot")
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ring.TopN(16)
	}
}

func BenchmarkSlidingWindow(b *testing.B) {
	ring := NewHotRing(12, nil)
	ring.EnableSlidingWindow(8, 10*time.Millisecond)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.Touch("burst")
	}
}

func BenchmarkDecay(b *testing.B) {
	ring := NewHotRing(12, nil)
	for i := range 10000 {
		ring.Touch(fmt.Sprintf("key-%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.applyDecay(1)
	}
}
