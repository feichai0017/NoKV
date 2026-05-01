package mvccgc

import (
	"github.com/feichai0017/NoKV/engine/mvcc"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

// MountResolver extracts a namespace mount identifier from a user key. GC
// policy treats nil or unresolved keys as unknown layouts and falls back to the
// global snapshot floor.
type MountResolver func(userKey []byte) (mount string, ok bool)

// SafePointPolicy resolves the safe point used for one user key. It keeps
// namespace snapshot scoping out of transaction apply code and keeps deletion
// policy out of rootstate.
type SafePointPolicy struct {
	RequestedSafePoint uint64
	SnapshotRetention  rootstate.SnapshotRetentionIndex
	TxnFloor           uint64
	Mount              MountResolver
}

// EffectiveForKey returns the key-local safe point after applying active
// snapshot and transaction floors. Resolved mount keys use mount-scoped
// snapshot floors. Unknown key layouts fall back to the global floor
// conservatively.
func (p SafePointPolicy) EffectiveForKey(userKey []byte) uint64 {
	if p.RequestedSafePoint == 0 {
		return 0
	}
	effective := p.RequestedSafePoint
	if p.TxnFloor != 0 && p.TxnFloor < effective {
		effective = p.TxnFloor
	}
	if p.Mount != nil {
		if mount, ok := p.Mount(userKey); ok {
			floor, ok := p.SnapshotRetention.FloorForMount(mount)
			if !ok {
				return effective
			}
			if floor != 0 && floor < effective {
				effective = floor
			}
			return effective
		}
	}
	if p.SnapshotRetention.Active() {
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
