package store

import (
	"time"

	"github.com/feichai0017/NoKV/manifest"
)

type heartbeatLoop struct {
	interval time.Duration
	sink     SchedulerClient
	storeID  uint64
	regions  func() []manifest.RegionMeta
	stats    func() StoreStats
	enqueue  func(Operation)
	stop     chan struct{}
	done     chan struct{}
}

// newHeartbeatLoop creates the periodic scheduler bridge for a store instance.
// It publishes region/store heartbeats and immediately enqueues returned
// operations.
func newHeartbeatLoop(interval time.Duration, sink SchedulerClient, storeID uint64,
	regions func() []manifest.RegionMeta, stats func() StoreStats,
	enqueue func(Operation)) *heartbeatLoop {
	if sink == nil || interval <= 0 {
		return nil
	}
	return &heartbeatLoop{
		interval: interval,
		sink:     sink,
		storeID:  storeID,
		regions:  regions,
		stats:    stats,
		enqueue:  enqueue,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (hl *heartbeatLoop) start() {
	if hl == nil {
		return
	}
	hl.sendHeartbeats()
	go hl.run()
}

func (hl *heartbeatLoop) stopLoop() {
	if hl == nil {
		return
	}
	close(hl.stop)
	<-hl.done
}

func (hl *heartbeatLoop) run() {
	defer close(hl.done)
	ticker := time.NewTicker(hl.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			hl.sendHeartbeats()
		case <-hl.stop:
			return
		}
	}
}

func (hl *heartbeatLoop) sendHeartbeats() {
	if hl == nil || hl.sink == nil {
		return
	}
	for _, meta := range hl.regions() {
		hl.sink.PublishRegion(meta)
	}
	if hl.storeID == 0 {
		return
	}
	for _, op := range hl.sink.StoreHeartbeat(hl.stats()) {
		hl.enqueue(op)
	}
}
