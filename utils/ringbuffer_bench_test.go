package utils

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkRingPushPop(b *testing.B) {
	for _, producers := range []int{1, 4, 8, 16} {
		b.Run("producers="+itoa(producers), func(b *testing.B) {
			r := NewRing[int](1024)
			var next atomic.Int64
			var wg sync.WaitGroup
			wg.Add(producers)
			for range producers {
				go func() {
					defer wg.Done()
					for {
						n := int(next.Add(1))
						if n > b.N {
							return
						}
						for !r.Push(n) {
							if r.Closed() {
								return
							}
							runtime.Gosched()
						}
					}
				}()
			}
			b.ResetTimer()
			count := 0
			for count < b.N {
				if _, ok := r.Pop(); ok {
					count++
					continue
				}
				runtime.Gosched()
			}
			b.StopTimer()
			r.Close()
			wg.Wait()
		})
	}
}

func BenchmarkRingPopBurst(b *testing.B) {
	r := NewRing[int](1024)
	for i := 0; i < r.Cap()/2; i++ {
		if !r.Push(i) {
			b.Fatalf("prefill push failed")
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := r.Pop(); !ok {
			b.Fatalf("pop failed")
		}
		for !r.Push(i) {
			runtime.Gosched()
		}
	}
}
