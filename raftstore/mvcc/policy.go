package mvcc

import (
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	txnmvcc "github.com/feichai0017/NoKV/txn/mvcc"
)

const defaultMaxBufferedVersionsPerKey = 65536

// MountResolver extracts a numeric namespace mount key from a user key. GC
// policy treats nil or unresolved keys as unknown layouts and falls back to the
// global snapshot floor.
type MountResolver func(userKey []byte) (mountKeyID uint64, ok bool)

// SafePointPolicy resolves the safe point used for one user key. It keeps
// namespace snapshot scoping out of transaction apply code and keeps deletion
// policy out of rootstate.
type SafePointPolicy struct {
	RequestedSafePoint uint64
	SnapshotRetention  rootstate.SnapshotRetentionIndex
	TxnFloor           uint64
	Mount              MountResolver
	// MaxVersionsPerKey bounds transient memory while planning one hot key.
	// Zero uses a conservative default. The scanner returns an error instead of
	// trying to hold an unbounded version list in memory.
	MaxVersionsPerKey uint64
}

func (p SafePointPolicy) maxVersionsPerKey() uint64 {
	if p.MaxVersionsPerKey != 0 {
		return p.MaxVersionsPerKey
	}
	return defaultMaxBufferedVersionsPerKey
}

// EffectiveForKey returns the key-local safe point after applying active
// snapshot and transaction floors. Resolved mount keys use mount-scoped
// snapshot floors. Unknown key layouts fall back to the aggregate global floor
// conservatively. The global floor is the minimum of all active snapshot
// epochs, not a separate all-mount snapshot; applying it to resolved mount keys
// would let one mount pin GC for unrelated mounts.
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
func (p SafePointPolicy) PlanWritesForKey(userKey []byte, versions []txnmvcc.GCWriteVersion) []txnmvcc.GCWriteDecision {
	_, decisions := p.AppendPlanWritesForKey(nil, userKey, versions)
	return decisions
}

// AppendPlanWritesForKey appends a key-local GC plan to dst and returns the
// effective safe point used for that key. The append form keeps long scans off
// the allocator hot path.
func (p SafePointPolicy) AppendPlanWritesForKey(dst []txnmvcc.GCWriteDecision, userKey []byte, versions []txnmvcc.GCWriteVersion) (uint64, []txnmvcc.GCWriteDecision) {
	safePoint := p.EffectiveForKey(userKey)
	return safePoint, txnmvcc.AppendWriteGCDecisions(dst, versions, safePoint)
}
