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
			q.Close()
			wg.Wait()
		})
	}
}

func BenchmarkMPSCQueueConsumerSessionPushPop(b *testing.B) {
	for _, producers := range []int{1, 4, 8, 16} {
		b.Run("producers="+itoa(producers), func(b *testing.B) {
			q := NewMPSCQueue[int](1024)
			c := q.AcquireConsumer()
			defer c.Close()

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
						if !q.Push(n) {
							return
						}
					}
				}()
			}
			b.ResetTimer()
			count := 0
			for count < b.N {
				if _, ok := c.Pop(); !ok {
					break
				}
				count++
			}
			b.StopTimer()
			q.Close()
			wg.Wait()
		})
	}
}

func BenchmarkMPSCQueuePushOnlyContention(b *testing.B) {
	for _, producers := range []int{1, 4, 8, 16} {
		b.Run("producers="+itoa(producers), func(b *testing.B) {
			q := NewMPSCQueue[int](1024)
			done := make(chan struct{})
			consumerDone := make(chan struct{})
			go func() {
				defer close(consumerDone)
				for {
					if _, ok := q.Pop(); !ok {
						return
					}
					select {
					case <-done:
						return
					default:
					}
				}
			}()

			var next atomic.Int64
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(producers)
			for range producers {
				go func() {
					defer wg.Done()
					<-start
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
			close(start)
			wg.Wait()
			b.StopTimer()
			close(done)
			q.Close()
			<-consumerDone
		})
	}
}

func BenchmarkMPSCQueuePopOnlyReady(b *testing.B) {
	q := NewMPSCQueue[int](1024)
	for i := 0; i < q.Cap(); i++ {
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

func BenchmarkMPSCQueueFullQueueWake(b *testing.B) {
	q := NewMPSCQueue[int](1)
	if !q.Push(1) {
		b.Fatalf("initial push failed")
	}
	start := make(chan struct{})
	ready := make(chan struct{}, 1)
	done := make(chan struct{}, 1)
	go func() {
		ready <- struct{}{}
		<-start
		for range b.N {
			if !q.Push(1) {
				done <- struct{}{}
				return
			}
		}
		done <- struct{}{}
	}()
	<-ready
	b.ResetTimer()
	close(start)
	for range b.N {
		if _, ok := q.Pop(); !ok {
			b.Fatalf("pop failed")
		}
	}
	<-done
	b.StopTimer()
	q.Close()
	for {
		if _, ok := q.TryPop(); !ok {
			break
		}
	}
}

func BenchmarkMPSCQueueCloseDrain(b *testing.B) {
	for _, producers := range []int{4, 16} {
		b.Run("producers="+itoa(producers), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				q := NewMPSCQueue[int](64)
				for j := 0; j < q.Cap(); j++ {
					if !q.Push(j) {
						b.Fatalf("prefill push failed")
					}
				}
				var wg sync.WaitGroup
				wg.Add(producers)
				for range producers {
					go func() {
						defer wg.Done()
						_ = q.Push(1)
					}()
				}
				q.Close()
				for {
					if _, ok := q.TryPop(); !ok {
						break
					}
				}
				wg.Wait()
			}
		})
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
