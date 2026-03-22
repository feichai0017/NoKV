package NoKV

import (
	"time"

	"github.com/feichai0017/NoKV/hotring"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

func newHotWriteRing(opt *Options) *hotring.RotatingHotRing {
	if opt == nil || !opt.HotRingEnabled || opt.WriteHotKeyLimit <= 0 {
		return nil
	}
	ring := hotring.NewRotatingHotRing(opt.HotRingBits, nil)
	if opt.HotRingWindowSlots > 0 && opt.HotRingWindowSlotDuration > 0 {
		ring.EnableSlidingWindow(opt.HotRingWindowSlots, opt.HotRingWindowSlotDuration)
	}
	if opt.HotRingDecayInterval > 0 && opt.HotRingDecayShift > 0 {
		ring.EnableDecay(opt.HotRingDecayInterval, opt.HotRingDecayShift)
	}
	if opt.HotRingNodeCap > 0 {
		ring.EnableNodeSampling(opt.HotRingNodeCap, opt.HotRingNodeSampleBits)
	}
	if opt.HotRingRotationInterval > 0 {
		ring.EnableRotation(time.Duration(opt.HotRingRotationInterval))
	}
	return ring
}

func (db *DB) maybeThrottleWrite(cf kv.ColumnFamily, key []byte) error {
	if db == nil || db.hotWrite == nil || len(key) == 0 {
		return nil
	}
	limit := db.opt.WriteHotKeyLimit
	if limit <= 0 {
		return nil
	}
	skey := cfHotKey(cf, key)
	if skey == "" {
		return nil
	}
	_, limited := db.hotWrite.TouchAndClamp(skey, limit)
	if !limited {
		return nil
	}
	db.hotWriteLimited.Add(1)
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
