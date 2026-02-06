package NoKV

import (
	"time"

	"github.com/feichai0017/hotring"
)

type hotTracker interface {
	Touch(key string) int32
	Frequency(key string) int32
	TouchAndClamp(key string, limit int32) (int32, bool)
	TopN(n int) []hotring.Item
	KeysAbove(threshold int32) []hotring.Item
	Stats() hotring.Stats
	Close()
}

type hotRingConfigurer interface {
	EnableSlidingWindow(slots int, slotDuration time.Duration)
	EnableDecay(interval time.Duration, shift uint32)
	EnableNodeSampling(cap uint64, sampleBits uint8)
}

func newHotTracker(opt *Options) hotTracker {
	if opt == nil || !opt.HotRingEnabled {
		return nil
	}
	if opt.HotRingRotationInterval > 0 {
		ring := hotring.NewRotatingHotRing(opt.HotRingBits, nil)
		applyHotRingConfig(ring, opt)
		ring.EnableRotation(opt.HotRingRotationInterval)
		return ring
	}
	ring := hotring.NewHotRing(opt.HotRingBits, nil)
	applyHotRingConfig(ring, opt)
	return ring
}

func applyHotRingConfig(ring hotRingConfigurer, opt *Options) {
	if ring == nil || opt == nil {
		return
	}
	if opt.HotRingWindowSlots > 0 && opt.HotRingWindowSlotDuration > 0 {
		ring.EnableSlidingWindow(opt.HotRingWindowSlots, opt.HotRingWindowSlotDuration)
	}
	if opt.HotRingDecayInterval > 0 && opt.HotRingDecayShift > 0 {
		ring.EnableDecay(opt.HotRingDecayInterval, opt.HotRingDecayShift)
	}
	if opt.HotRingNodeCap > 0 {
		ring.EnableNodeSampling(opt.HotRingNodeCap, opt.HotRingNodeSampleBits)
	}
}
