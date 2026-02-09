package NoKV

import (
	"maps"
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
	if db == nil || db.hotRead == nil || len(key) == 0 {
		return
	}
	skey := string(key)
	count := db.hotRead.Touch(skey)
	if db.prefetchRing == nil {
		return
	}
	clamp := db.prefetchHot
	if clamp <= 0 {
		clamp = max(db.prefetchWarm, 1)
	}
	if clamp > 0 && count >= clamp {
		db.enqueuePrefetch(skey, true)
	}
}

func (db *DB) maybeThrottleWrite(cf kv.ColumnFamily, key []byte) error {
	if db == nil || db.hotWrite == nil || len(key) == 0 {
		return nil
	}
	limit := db.opt.WriteHotKeyLimit
	skey := cfHotKey(cf, key)
	if skey == "" {
		return nil
	}
	if limit > 0 {
		_, limited := db.hotWrite.TouchAndClamp(skey, limit)
		if !limited {
			return nil
		}
		atomic.AddUint64(&db.hotWriteLimited, 1)
		return utils.ErrHotKeyWriteThrottle
	}
	if db.opt.HotWriteBurstThreshold > 0 {
		db.hotWrite.Touch(skey)
	}
	return nil
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

// isHotWrite tags a small set as hot when repeated writes hit HotRing.
func (db *DB) isHotWrite(entries []*kv.Entry) bool {
	if db == nil || db.hotWrite == nil {
		return false
	}
	if len(entries) != 1 {
		return false
	}
	thr := db.opt.HotWriteBurstThreshold
	if thr <= 0 {
		return false
	}
	key := hotWriteKeyForEntry(entries[0])
	if key == "" {
		return false
	}
	return db.hotWrite.Frequency(key) >= thr
}

func hotWriteKeyForEntry(e *kv.Entry) string {
	if e == nil || len(e.Key) == 0 {
		return ""
	}
	base := kv.ParseKey(e.Key)
	if cf, userKey, ok := kv.DecodeKeyCF(base); ok {
		return cfHotKey(cf, userKey)
	}
	return cfHotKey(e.CF, e.Key)
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
	} else if db.prefetchItems != nil {
		select {
		case db.prefetchItems <- struct{}{}:
		default:
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
			if db.prefetchItems != nil {
				<-db.prefetchItems
				continue
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
		internal := kv.InternalKey(kv.CFDefault, []byte(key), nonTxnMaxVersion)
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
