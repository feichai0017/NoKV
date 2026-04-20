package thermos

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type ringConfig struct {
	bits           uint8
	hashFn         HashFn
	windowSlots    int
	windowSlotDur  time.Duration
	decayInterval  time.Duration
	decayShift     uint32
	nodeCap        uint64
	nodeSampleBits uint8
}

// RotationStats reports rotation activity for a RotatingThermos.
type RotationStats struct {
	Interval       time.Duration `json:"interval"`
	Rotations      uint64        `json:"rotations"`
	LastRotateUnix int64         `json:"last_rotate_unix"`
}

// RotatingThermos wraps Thermos with dual-ring time-based rotation.
// Rotation swaps in a fresh active ring and keeps the previous generation warm.
type RotatingThermos struct {
	active atomic.Pointer[Thermos]
	warm   atomic.Pointer[Thermos]

	cfgMu sync.Mutex
	cfg   ringConfig

	observer atomic.Pointer[observerHolder]

	rotateInterval atomic.Int64
	rotations      atomic.Uint64
	lastRotateUnix atomic.Int64

	rotateMu   sync.Mutex
	rotateStop chan struct{}
	rotateWG   sync.WaitGroup
}

// NewRotatingThermos builds a rotating ring with 2^bits buckets.
func NewRotatingThermos(bits uint8, fn HashFn) *RotatingThermos {
	r := &RotatingThermos{}
	r.cfg = ringConfig{bits: bits, hashFn: fn}
	ring := NewThermos(bits, fn)
	r.active.Store(ring)
	return r
}

// EnableRotation starts or stops time-based rotation. interval <= 0 disables rotation.
func (r *RotatingThermos) EnableRotation(interval time.Duration) {
	if r == nil {
		return
	}
	r.stopRotation()
	if interval <= 0 {
		r.rotateInterval.Store(0)
		return
	}
	r.rotateInterval.Store(interval.Nanoseconds())
	r.rotateMu.Lock()
	stop := make(chan struct{})
	r.rotateStop = stop
	r.rotateWG.Add(1)
	r.rotateMu.Unlock()

	go r.rotateLoop(stop, interval)
}

// Rotate swaps in a fresh ring and releases background resources of the old ring.
func (r *RotatingThermos) Rotate() {
	if r == nil {
		return
	}
	r.cfgMu.Lock()
	newRing := r.newRingLocked()
	r.cfgMu.Unlock()

	oldActive := r.active.Swap(newRing)
	oldWarm := r.warm.Swap(oldActive)
	if oldWarm != nil {
		oldWarm.Close()
	}
	r.rotations.Add(1)
	r.lastRotateUnix.Store(time.Now().Unix())
}

// Close releases background resources attached to the rotating ring.
func (r *RotatingThermos) Close() {
	if r == nil {
		return
	}
	r.stopRotation()
	if ring := r.active.Load(); ring != nil {
		ring.Close()
	}
	if ring := r.warm.Load(); ring != nil {
		ring.Close()
	}
}

// RotationStats returns rotation counters and configuration.
func (r *RotatingThermos) RotationStats() RotationStats {
	if r == nil {
		return RotationStats{}
	}
	return RotationStats{
		Interval:       time.Duration(r.rotateInterval.Load()),
		Rotations:      r.rotations.Load(),
		LastRotateUnix: r.lastRotateUnix.Load(),
	}
}

// Touch records a key access and returns the updated counter.
func (r *RotatingThermos) Touch(key string) int32 {
	active := r.active.Load()
	if active == nil {
		return 0
	}
	count := active.Touch(key)
	if warm := r.warm.Load(); warm != nil {
		warmCount := warm.Frequency(key)
		return maxCount(count, warmCount)
	}
	return count
}

// Frequency returns the current access counter for key without mutating state.
func (r *RotatingThermos) Frequency(key string) int32 {
	active := r.active.Load()
	if active == nil {
		return 0
	}
	count := active.Frequency(key)
	if warm := r.warm.Load(); warm != nil {
		warmCount := warm.Frequency(key)
		return maxCount(count, warmCount)
	}
	return count
}

// TouchAndClamp increments the counter if below the provided limit.
func (r *RotatingThermos) TouchAndClamp(key string, limit int32) (int32, bool) {
	if limit <= 0 {
		return r.Touch(key), false
	}
	active := r.active.Load()
	if active == nil {
		return 0, false
	}
	warmCount := int32(0)
	if warm := r.warm.Load(); warm != nil {
		warmCount = warm.Frequency(key)
	}
	activeCount, limited := active.TouchAndClamp(key, limit)
	combined := maxCount(activeCount, warmCount)
	if combined >= limit {
		limited = true
	}
	return combined, limited
}

// Remove deletes a key from the active ring.
func (r *RotatingThermos) Remove(key string) {
	if ring := r.active.Load(); ring != nil {
		ring.Remove(key)
	}
	if ring := r.warm.Load(); ring != nil {
		ring.Remove(key)
	}
}

// TopN returns at most n hot keys ordered by access count (descending).
func (r *RotatingThermos) TopN(n int) []Item {
	if n <= 0 {
		return nil
	}
	items := r.mergeItems(sumCounts)
	if len(items) == 0 {
		return nil
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Key < items[j].Key
		}
		return items[i].Count > items[j].Count
	})
	if len(items) > n {
		items = append([]Item(nil), items[:n]...)
	} else {
		items = append([]Item(nil), items...)
	}
	return items
}

// KeysAbove returns all keys whose counters are at least threshold.
func (r *RotatingThermos) KeysAbove(threshold int32) []Item {
	if threshold <= 0 {
		return nil
	}
	items := r.mergeItems(sumCounts)
	if len(items) == 0 {
		return nil
	}
	filtered := make([]Item, 0, len(items))
	for _, item := range items {
		if item.Count >= threshold {
			filtered = append(filtered, item)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Count == filtered[j].Count {
			return filtered[i].Key < filtered[j].Key
		}
		return filtered[i].Count > filtered[j].Count
	})
	return filtered
}

// Stats returns a lightweight view of ring configuration and counters.
func (r *RotatingThermos) Stats() Stats {
	if ring := r.active.Load(); ring != nil {
		return ring.Stats()
	}
	return Stats{}
}

// WarmStats returns stats for the warm ring.
func (r *RotatingThermos) WarmStats() Stats {
	if ring := r.warm.Load(); ring != nil {
		return ring.Stats()
	}
	return Stats{}
}

// ActiveStats returns stats for the active ring.
func (r *RotatingThermos) ActiveStats() Stats {
	if ring := r.active.Load(); ring != nil {
		return ring.Stats()
	}
	return Stats{}
}

// SnapshotTopN captures a Top-N snapshot with a timestamp.
func (r *RotatingThermos) SnapshotTopN(n int) Snapshot {
	return Snapshot{TakenAt: time.Now(), Items: r.TopN(n)}
}

// SnapshotKeysAbove captures a threshold snapshot with a timestamp.
func (r *RotatingThermos) SnapshotKeysAbove(threshold int32) Snapshot {
	return Snapshot{TakenAt: time.Now(), Items: r.KeysAbove(threshold)}
}

// SnapshotTopNMax captures a Top-N snapshot using max merge semantics.
func (r *RotatingThermos) SnapshotTopNMax(n int) Snapshot {
	return Snapshot{TakenAt: time.Now(), Items: r.TopNMax(n)}
}

// SnapshotKeysAboveMax captures a threshold snapshot using max merge semantics.
func (r *RotatingThermos) SnapshotKeysAboveMax(threshold int32) Snapshot {
	return Snapshot{TakenAt: time.Now(), Items: r.KeysAboveMax(threshold)}
}

// TopNMax returns at most n hot keys ordered by access count (descending) using max merge.
func (r *RotatingThermos) TopNMax(n int) []Item {
	if n <= 0 {
		return nil
	}
	items := r.mergeItems(maxCount)
	if len(items) == 0 {
		return nil
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Key < items[j].Key
		}
		return items[i].Count > items[j].Count
	})
	if len(items) > n {
		items = append([]Item(nil), items[:n]...)
	} else {
		items = append([]Item(nil), items...)
	}
	return items
}

// KeysAboveMax returns all keys whose counters are at least threshold using max merge.
func (r *RotatingThermos) KeysAboveMax(threshold int32) []Item {
	if threshold <= 0 {
		return nil
	}
	items := r.mergeItems(maxCount)
	if len(items) == 0 {
		return nil
	}
	filtered := make([]Item, 0, len(items))
	for _, item := range items {
		if item.Count >= threshold {
			filtered = append(filtered, item)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Count == filtered[j].Count {
			return filtered[i].Key < filtered[j].Key
		}
		return filtered[i].Count > filtered[j].Count
	})
	return filtered
}

// EnableSlidingWindow configures the ring to maintain a time-based sliding window.
func (r *RotatingThermos) EnableSlidingWindow(slots int, slotDuration time.Duration) {
	if r == nil {
		return
	}
	r.cfgMu.Lock()
	r.cfg.windowSlots = slots
	r.cfg.windowSlotDur = slotDuration
	r.cfgMu.Unlock()

	if ring := r.active.Load(); ring != nil {
		ring.EnableSlidingWindow(slots, slotDuration)
	}
	if ring := r.warm.Load(); ring != nil {
		ring.EnableSlidingWindow(slots, slotDuration)
	}
}

// EnableDecay applies periodic right-shift decay to the raw counters.
func (r *RotatingThermos) EnableDecay(interval time.Duration, shift uint32) {
	if r == nil {
		return
	}
	r.cfgMu.Lock()
	r.cfg.decayInterval = interval
	r.cfg.decayShift = shift
	r.cfgMu.Unlock()

	if ring := r.active.Load(); ring != nil {
		ring.EnableDecay(interval, shift)
	}
	if ring := r.warm.Load(); ring != nil {
		ring.EnableDecay(interval, shift)
	}
}

// EnableNodeSampling caps node growth and applies stable sampling once the cap is reached.
func (r *RotatingThermos) EnableNodeSampling(cap uint64, sampleBits uint8) {
	if r == nil {
		return
	}
	r.cfgMu.Lock()
	r.cfg.nodeCap = cap
	r.cfg.nodeSampleBits = sampleBits
	r.cfgMu.Unlock()

	if ring := r.active.Load(); ring != nil {
		ring.EnableNodeSampling(cap, sampleBits)
	}
	if ring := r.warm.Load(); ring != nil {
		ring.EnableNodeSampling(cap, sampleBits)
	}
}

// SetObserver registers an optional observer hook.
func (r *RotatingThermos) SetObserver(obs Observer) {
	if r == nil {
		return
	}
	if obs == nil {
		r.observer.Store(nil)
		if ring := r.active.Load(); ring != nil {
			ring.SetObserver(nil)
		}
		if ring := r.warm.Load(); ring != nil {
			ring.SetObserver(nil)
		}
		return
	}
	r.observer.Store(&observerHolder{obs: obs})
	if ring := r.active.Load(); ring != nil {
		ring.SetObserver(obs)
	}
	if ring := r.warm.Load(); ring != nil {
		ring.SetObserver(obs)
	}
}

func (r *RotatingThermos) getObserver() Observer {
	if r == nil {
		return nil
	}
	holder := r.observer.Load()
	if holder == nil {
		return nil
	}
	return holder.obs
}

func (r *RotatingThermos) newRingLocked() *Thermos {
	cfg := r.cfg
	ring := NewThermos(cfg.bits, cfg.hashFn)
	if cfg.windowSlots > 0 && cfg.windowSlotDur > 0 {
		ring.EnableSlidingWindow(cfg.windowSlots, cfg.windowSlotDur)
	}
	if cfg.decayInterval > 0 && cfg.decayShift > 0 {
		ring.EnableDecay(cfg.decayInterval, cfg.decayShift)
	}
	if cfg.nodeCap > 0 {
		ring.EnableNodeSampling(cfg.nodeCap, cfg.nodeSampleBits)
	}
	if obs := r.getObserver(); obs != nil {
		ring.SetObserver(obs)
	}
	return ring
}

func (r *RotatingThermos) mergeItems(merge func(int32, int32) int32) []Item {
	counts := make(map[string]int32)
	r.addRingCounts(r.active.Load(), counts, merge)
	r.addRingCounts(r.warm.Load(), counts, merge)
	if len(counts) == 0 {
		return nil
	}
	items := make([]Item, 0, len(counts))
	for key, count := range counts {
		items = append(items, Item{Key: key, Count: count})
	}
	return items
}

func (r *RotatingThermos) addRingCounts(ring *Thermos, counts map[string]int32, merge func(int32, int32) int32) {
	if ring == nil {
		return
	}
	slot, slots := ring.slotState()
	for i := range ring.buckets {
		for node := ring.buckets[i].Load(); node != nil; node = node.Next() {
			cnt := ring.nodeCount(node, slots, slot)
			if cnt <= 0 {
				continue
			}
			if existing, ok := counts[node.key]; ok {
				counts[node.key] = merge(existing, cnt)
			} else {
				counts[node.key] = cnt
			}
		}
	}
}

func maxCount(a, b int32) int32 {
	if a >= b {
		return a
	}
	return b
}

func sumCounts(a, b int32) int32 {
	sum := int64(a) + int64(b)
	if sum > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(sum)
}

func (r *RotatingThermos) rotateLoop(stop <-chan struct{}, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		r.rotateWG.Done()
	}()
	for {
		select {
		case <-ticker.C:
			r.Rotate()
		case <-stop:
			return
		}
	}
}

func (r *RotatingThermos) stopRotation() {
	r.rotateMu.Lock()
	stop := r.rotateStop
	if stop != nil {
		close(stop)
		r.rotateStop = nil
	}
	r.rotateMu.Unlock()
	if stop != nil {
		r.rotateWG.Wait()
	}
}
