package kv

import (
	"github.com/feichai0017/NoKV/fsmeta"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/percolator"
)

// MVCCGCSafePointPolicy resolves the safe point used for one user key. It
// keeps fsmeta snapshot scoping out of Percolator and keeps deletion policy out
// of rootstate.
type MVCCGCSafePointPolicy struct {
	RequestedSafePoint uint64
	SnapshotRetention  rootstate.SnapshotRetentionIndex
	TxnFloor           uint64
}

// EffectiveForKey returns the key-local safe point after applying active
// snapshot and transaction floors. Fsmeta keys use mount-scoped snapshot floors.
// Unknown key layouts fall back to the global floor conservatively.
func (p MVCCGCSafePointPolicy) EffectiveForKey(userKey []byte) uint64 {
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
func (p MVCCGCSafePointPolicy) PlanWritesForKey(userKey []byte, versions []percolator.GCWriteVersion) []percolator.GCWriteDecision {
	_, decisions := p.AppendPlanWritesForKey(nil, userKey, versions)
	return decisions
}

// AppendPlanWritesForKey appends a key-local GC plan to dst and returns the
// effective safe point used for that key. The append form keeps long scans off
// the allocator hot path.
func (p MVCCGCSafePointPolicy) AppendPlanWritesForKey(dst []percolator.GCWriteDecision, userKey []byte, versions []percolator.GCWriteVersion) (uint64, []percolator.GCWriteDecision) {
	safePoint := p.EffectiveForKey(userKey)
	return safePoint, percolator.AppendWriteGCDecisions(dst, versions, safePoint)
}
