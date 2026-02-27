package hotring

import "time"

// Stats exposes lightweight observability data for a HotRing instance.
type Stats struct {
	Buckets            int           `json:"buckets"`
	Nodes              uint64        `json:"nodes"`
	LoadFactor         float64       `json:"load_factor"`
	WindowSlots        int           `json:"window_slots"`
	WindowSlotDuration time.Duration `json:"window_slot_duration"`
	DecayInterval      time.Duration `json:"decay_interval"`
	DecayShift         uint32        `json:"decay_shift"`
	NodeCap            uint64        `json:"node_cap"`
	SampleMask         uint32        `json:"sample_mask"`
	Touches            uint64        `json:"touches"`
	Inserts            uint64        `json:"inserts"`
	Removes            uint64        `json:"removes"`
	Clamps             uint64        `json:"clamps"`
	SampleDrops        uint64        `json:"sample_drops"`
	DecayRuns          uint64        `json:"decay_runs"`
	LastDecayUnix      int64         `json:"last_decay_unix"`
}

// Snapshot captures a point-in-time view of hot keys.
type Snapshot struct {
	TakenAt time.Time `json:"taken_at"`
	Items   []Item    `json:"items"`
}

// Observer receives optional notifications for observability hooks.
type Observer interface {
	OnTouch(key string, count int32)
	OnClamp(key string, limit int32, count int32)
	OnDecay(shift uint32)
}

type observerHolder struct {
	obs Observer
}

// Stats returns a lightweight view of ring configuration and counters.
func (h *HotRing) Stats() Stats {
	if h == nil {
		return Stats{}
	}
	stats := Stats{
		Buckets:            len(h.buckets),
		Nodes:              h.nodes.Load(),
		WindowSlots:        int(h.windowSlots.Load()),
		WindowSlotDuration: time.Duration(h.windowSlotDur.Load()),
		DecayInterval:      time.Duration(h.decayInterval.Load()),
		DecayShift:         h.decayShift.Load(),
		NodeCap:            h.nodeCap.Load(),
		SampleMask:         h.sampleMask.Load(),
		Touches:            h.touches.Load(),
		Inserts:            h.inserts.Load(),
		Removes:            h.removes.Load(),
		Clamps:             h.clamps.Load(),
		SampleDrops:        h.sampleDrops.Load(),
		DecayRuns:          h.decayRuns.Load(),
		LastDecayUnix:      h.lastDecayUnix.Load(),
	}
	if stats.Buckets > 0 {
		stats.LoadFactor = float64(stats.Nodes) / float64(stats.Buckets)
	}
	return stats
}

// SnapshotTopN captures a Top-N snapshot with a timestamp.
func (h *HotRing) SnapshotTopN(n int) Snapshot {
	return Snapshot{TakenAt: time.Now(), Items: h.TopN(n)}
}

// SnapshotKeysAbove captures a threshold snapshot with a timestamp.
func (h *HotRing) SnapshotKeysAbove(threshold int32) Snapshot {
	return Snapshot{TakenAt: time.Now(), Items: h.KeysAbove(threshold)}
}

// SetObserver registers an optional observer hook.
func (h *HotRing) SetObserver(obs Observer) {
	if h == nil {
		return
	}
	if obs == nil {
		h.observer.Store(nil)
		return
	}
	h.observer.Store(&observerHolder{obs: obs})
}

func (h *HotRing) getObserver() Observer {
	if h == nil {
		return nil
	}
	holder := h.observer.Load()
	if holder == nil {
		return nil
	}
	return holder.obs
}
