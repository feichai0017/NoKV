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

type hotRingConfig struct {
	bits             uint8
	rotationInterval time.Duration
	windowSlots      int
	windowSlotDur    time.Duration
	decayInterval    time.Duration
	decayShift       uint32
	nodeCap          uint64
	nodeSampleBits   uint8
}

func newHotTracker(opt *Options) hotTracker {
	if opt == nil || !opt.HotRingEnabled {
		return nil
	}
	return newHotTrackerFromConfig(hotRingConfigFromOptions(opt))
}

func newHotTrackerForVLog(opt *Options) hotTracker {
	if opt == nil || !opt.HotRingEnabled {
		return nil
	}
	return newHotTrackerFromConfig(hotRingConfigForVLog(opt))
}

func newHotTrackerFromConfig(cfg hotRingConfig) hotTracker {
	if cfg.rotationInterval > 0 {
		ring := hotring.NewRotatingHotRing(cfg.bits, nil)
		applyHotRingConfig(ring, cfg)
		ring.EnableRotation(cfg.rotationInterval)
		return ring
	}
	ring := hotring.NewHotRing(cfg.bits, nil)
	applyHotRingConfig(ring, cfg)
	return ring
}

func applyHotRingConfig(ring hotRingConfigurer, cfg hotRingConfig) {
	if ring == nil {
		return
	}
	if cfg.windowSlots > 0 && cfg.windowSlotDur > 0 {
		ring.EnableSlidingWindow(cfg.windowSlots, cfg.windowSlotDur)
	}
	if cfg.decayInterval > 0 && cfg.decayShift > 0 {
		ring.EnableDecay(cfg.decayInterval, cfg.decayShift)
	}
	if cfg.nodeCap > 0 {
		ring.EnableNodeSampling(cfg.nodeCap, cfg.nodeSampleBits)
	}
}

func hotRingConfigFromOptions(opt *Options) hotRingConfig {
	if opt == nil {
		return hotRingConfig{}
	}
	return hotRingConfig{
		bits:             opt.HotRingBits,
		rotationInterval: opt.HotRingRotationInterval,
		windowSlots:      opt.HotRingWindowSlots,
		windowSlotDur:    opt.HotRingWindowSlotDuration,
		decayInterval:    opt.HotRingDecayInterval,
		decayShift:       opt.HotRingDecayShift,
		nodeCap:          opt.HotRingNodeCap,
		nodeSampleBits:   opt.HotRingNodeSampleBits,
	}
}

func hotRingConfigForVLog(opt *Options) hotRingConfig {
	base := hotRingConfigFromOptions(opt)
	if opt == nil || !opt.ValueLogHotRingOverride {
		return base
	}
	return hotRingConfig{
		bits:             opt.ValueLogHotRingBits,
		rotationInterval: opt.ValueLogHotRingRotationInterval,
		windowSlots:      opt.ValueLogHotRingWindowSlots,
		windowSlotDur:    opt.ValueLogHotRingWindowSlotDuration,
		decayInterval:    opt.ValueLogHotRingDecayInterval,
		decayShift:       opt.ValueLogHotRingDecayShift,
		nodeCap:          opt.ValueLogHotRingNodeCap,
		nodeSampleBits:   opt.ValueLogHotRingNodeSampleBits,
	}
}
