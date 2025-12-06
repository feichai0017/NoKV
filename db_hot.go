package NoKV

import (
	"maps"
	"math"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

type prefetchRequest struct {
	key string
	hot bool
}

type prefetchState struct {
	pend       map[string]struct{}
	prefetched map[string]time.Time
}

func (s *prefetchState) clone() *prefetchState {
	if s == nil {
		return &prefetchState{
			pend:       make(map[string]struct{}),
			prefetched: make(map[string]time.Time),
		}
	}
	ns := &prefetchState{
		pend:       make(map[string]struct{}, len(s.pend)),
		prefetched: make(map[string]time.Time, len(s.prefetched)),
	}
	maps.Copy(ns.pend, s.pend)
	maps.Copy(ns.prefetched, s.prefetched)
	return ns
}

func (db *DB) recordRead(key []byte) {
	if db == nil || db.hot == nil || len(key) == 0 {
		return
	}
	skey := string(key)
	if db.prefetchRing == nil {
		db.hot.Touch(skey)
		return
	}
	clamp := db.prefetchClamp
	if clamp <= 0 {
		clamp = db.prefetchHot
		if clamp <= 0 {
			clamp = db.prefetchWarm
		}
		if clamp <= 0 {
			clamp = 1
		}
	}
	count, _ := db.hot.TouchAndClamp(skey, clamp)
	if db.prefetchHot > 0 && count >= db.prefetchHot {
		db.enqueuePrefetch(skey, true)
		return
	}
	if db.prefetchWarm > 0 && count >= db.prefetchWarm {
		db.enqueuePrefetch(skey, false)
	}
}

func (db *DB) maybeThrottleWrite(cf kv.ColumnFamily, key []byte) error {
	if db == nil || db.hot == nil || len(key) == 0 {
		return nil
	}
	limit := db.opt.WriteHotKeyLimit
	if limit <= 0 {
		return nil
	}
	skey := cfHotKey(cf, key)
	_, limited := db.hot.TouchAndClamp(skey, limit)
	if !limited {
		return nil
	}
	atomic.AddUint64(&db.hotWriteLimited, 1)
	return utils.ErrHotKeyWriteThrottle
}

func cfHotKey(cf kv.ColumnFamily, key []byte) string {
	if !cf.Valid() || cf == kv.CFDefault {
		return string(key)
	}
	buf := make([]byte, len(key)+1)
	buf[0] = byte(cf)
	copy(buf[1:], key)
	return string(buf)
}

func (db *DB) enqueuePrefetch(key string, hot bool) {
	if db == nil || db.prefetchRing == nil || key == "" {
		return
	}
	now := time.Now()
	for {
		state := db.prefetchState.Load()
		if state == nil {
			return
		}
		if expiry, ok := state.prefetched[key]; ok && expiry.After(now) {
			return
		}
		if _, pending := state.pend[key]; pending {
			return
		}
		next := state.clone()
		delete(next.prefetched, key)
		next.pend[key] = struct{}{}
		if db.prefetchState.CompareAndSwap(state, next) {
			break
		}
	}

	req := prefetchRequest{key: key, hot: hot}
	if ok := db.prefetchRing.Push(req); !ok {
		for {
			state := db.prefetchState.Load()
			if state == nil {
				return
			}
			next := state.clone()
			delete(next.pend, key)
			if db.prefetchState.CompareAndSwap(state, next) {
				return
			}
		}
	}
}

func (db *DB) prefetchLoop() {
	defer db.prefetchWG.Done()
	for {
		req, ok := db.prefetchRing.Pop()
		if !ok {
			if db.prefetchRing.Closed() {
				return
			}
			runtime.Gosched()
			continue
		}
		db.executePrefetch(req)
	}
}

func (db *DB) executePrefetch(req prefetchRequest) {
	if db == nil {
		return
	}
	key := req.key
	if db.lsm != nil {
		internal := kv.InternalKey(kv.CFDefault, []byte(key), math.MaxUint32)
		db.lsm.Prefetch(internal, req.hot)
	}
	for {
		state := db.prefetchState.Load()
		if state == nil {
			return
		}
		next := state.clone()
		delete(next.pend, key)
		if db.prefetchCooldown > 0 {
			next.prefetched[key] = time.Now().Add(db.prefetchCooldown)
		} else {
			delete(next.prefetched, key)
		}
		if db.prefetchState.CompareAndSwap(state, next) {
			return
		}
	}
}
