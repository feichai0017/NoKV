package utils

import (
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkMPSCQueuePushPop(b *testing.B) {
	for _, producers := range []int{1, 4, 8, 16} {
		b.Run("producers="+itoa(producers), func(b *testing.B) {
			q := NewMPSCQueue[int](1024)
			var next atomic.Int64
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(producers)
			for i := 0; i < producers; i++ {
				go func() {
					defer wg.Done()
					for {
						n := int(next.Add(1))
						if n > b.N {
							return
						}
						if !q.Push(n) {
							return
						}
					}
				}()
			}
			b.ResetTimer()
			count := 0
			for count < b.N {
				if _, ok := q.Pop(); !ok {
					break
				}
				count++
			}
			b.StopTimer()
			close(stop)
			_ = stop
			q.Close()
			wg.Wait()
		})
	}
}

func BenchmarkMPSCQueueTryPopBurst(b *testing.B) {
	q := NewMPSCQueue[int](1024)
	for i := 0; i < q.Cap()/2; i++ {
		if !q.Push(i) {
			b.Fatalf("prefill push failed")
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := q.TryPop(); !ok {
			b.Fatalf("try pop failed")
		}
		if !q.Push(i) {
			b.Fatalf("push failed")
		}
	}
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
