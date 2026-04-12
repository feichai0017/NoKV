package main

import rootremote "github.com/feichai0017/NoKV/meta/root/remote"

type metaRootLeaderStatus interface {
	IsLeader() bool
	LeaderID() uint64
}

func metaRootDiagnosticsSnapshot(backend rootremote.Backend) map[string]any {
	if backend == nil {
		return map[string]any{}
	}
	snapshot, err := backend.Snapshot()
	if err != nil {
		return map[string]any{
			"snapshot_error": err.Error(),
		}
	}
	isLeader := true
	leaderID := uint64(0)
	if leader, ok := backend.(metaRootLeaderStatus); ok {
		isLeader = leader.IsLeader()
		leaderID = leader.LeaderID()
	}
	return map[string]any{
		"is_leader":        isLeader,
		"leader_id":        leaderID,
		"cluster_epoch":    snapshot.State.ClusterEpoch,
		"membership_epoch": snapshot.State.MembershipEpoch,
		"last_committed": map[string]any{
			"term":  snapshot.State.LastCommitted.Term,
			"index": snapshot.State.LastCommitted.Index,
		},
		"descriptors":             len(snapshot.Descriptors),
		"pending_peer_changes":    len(snapshot.PendingPeerChanges),
		"pending_range_changes":   len(snapshot.PendingRangeChanges),
		"id_fence":                snapshot.State.IDFence,
		"tso_fence":               snapshot.State.TSOFence,
		"lease_holder_id":         snapshot.State.CoordinatorLease.HolderID,
		"lease_expires_unix_nano": snapshot.State.CoordinatorLease.ExpiresUnixNano,
	}
}
