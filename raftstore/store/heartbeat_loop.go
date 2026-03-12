package store

import (
	"time"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/raftstore/scheduler"
)

type heartbeatLoop struct {
	interval time.Duration
	sink     scheduler.RegionSink
	storeID  uint64
	regions  func() []manifest.RegionMeta
	stats    func() scheduler.StoreStats
	enqueue  func(scheduler.Operation)
	stop     chan struct{}
	done     chan struct{}
}

// newHeartbeatLoop creates the periodic scheduler bridge for a store instance.
// It publishes region/store heartbeats and drains scheduler operations.
func newHeartbeatLoop(interval time.Duration, sink scheduler.RegionSink, storeID uint64,
	regions func() []manifest.RegionMeta, stats func() scheduler.StoreStats,
	enqueue func(scheduler.Operation)) *heartbeatLoop {
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

// sendHeartbeats pushes current region/store state to the scheduler sink, then
// drains pending operations from the same sink.
func (hl *heartbeatLoop) sendHeartbeats() {
	if hl == nil || hl.sink == nil {
		return
	}
	for _, meta := range hl.regions() {
		hl.sink.SubmitRegionHeartbeat(meta)
	}
	if hl.storeID != 0 {
		hl.sink.SubmitStoreHeartbeat(hl.stats())
	}
	for _, op := range hl.sink.DrainOperations() {
		hl.enqueue(op)
	}
}
