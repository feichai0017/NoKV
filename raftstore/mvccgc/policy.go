package mvccgc

import (
	"github.com/feichai0017/NoKV/engine/mvcc"
	"github.com/feichai0017/NoKV/fsmeta"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// SafePointPolicy resolves the safe point used for one user key. It keeps
// fsmeta snapshot scoping out of transaction apply code and keeps deletion
// policy out of rootstate.
type SafePointPolicy struct {
	RequestedSafePoint uint64
	SnapshotRetention  rootstate.SnapshotRetentionIndex
	TxnFloor           uint64
}

// EffectiveForKey returns the key-local safe point after applying active
// snapshot and transaction floors. Fsmeta keys use mount-scoped snapshot floors.
// Unknown key layouts fall back to the global floor conservatively.
func (p SafePointPolicy) EffectiveForKey(userKey []byte) uint64 {
	if p.RequestedSafePoint == 0 {
		return 0
	}
	effective := p.RequestedSafePoint
	if p.TxnFloor != 0 && p.TxnFloor < effective {
		effective = p.TxnFloor
	}
	if mount, ok := fsmeta.MountIDOfKey(userKey); ok {
		if floor, ok := p.SnapshotRetention.FloorForMount(string(mount)); ok {
			if floor != 0 && floor < effective {
				effective = floor
			}
		}
	} else if p.SnapshotRetention.Active() {
		if floor := p.SnapshotRetention.GlobalFloor; floor != 0 && floor < effective {
			effective = floor
		}
	}
	return effective
}

// PlanWritesForKey returns the MVCC GC plan for one key's CFWrite versions.
func (p SafePointPolicy) PlanWritesForKey(userKey []byte, versions []mvcc.GCWriteVersion) []mvcc.GCWriteDecision {
	_, decisions := p.AppendPlanWritesForKey(nil, userKey, versions)
	return decisions
}

// AppendPlanWritesForKey appends a key-local GC plan to dst and returns the
// effective safe point used for that key. The append form keeps long scans off
// the allocator hot path.
func (p SafePointPolicy) AppendPlanWritesForKey(dst []mvcc.GCWriteDecision, userKey []byte, versions []mvcc.GCWriteVersion) (uint64, []mvcc.GCWriteDecision) {
	safePoint := p.EffectiveForKey(userKey)
	return safePoint, mvcc.AppendWriteGCDecisions(dst, versions, safePoint)
}
