package lsm

// WALGCPolicy decides whether a WAL segment is safe to remove.
// Implementations may consult external replication state (for example raft
// apply/truncate pointers) to keep lagging followers safe.
type WALGCPolicy interface {
	CanRemoveSegment(segmentID uint32) bool
}

// AllowAllWALGCPolicy allows removing every WAL segment.
// This is the default when no policy is provided.
type AllowAllWALGCPolicy struct{}

// CanRemoveSegment always returns true.
func (AllowAllWALGCPolicy) CanRemoveSegment(uint32) bool { return true }

func normalizeWALGCPolicy(policy WALGCPolicy) WALGCPolicy {
	if policy == nil {
		return AllowAllWALGCPolicy{}
	}
	return policy
}
