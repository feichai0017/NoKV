package main

import (
	"expvar"
	"net"
	"sync"
	"sync/atomic"

	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	rootserver "github.com/feichai0017/NoKV/meta/root/server"
	metricspkg "github.com/feichai0017/NoKV/metrics"
)

type debugSnapshotFunc func() any

var (
	coordinatorExpvarOnce     sync.Once
	coordinatorExpvarProvider atomic.Value
)

func installCoordinatorExpvar(svc *coordserver.Service) {
	if svc == nil {
		return
	}
	coordinatorExpvarProvider.Store(debugSnapshotFunc(func() any {
		return map[string]any{
			"state": svc.DiagnosticsSnapshot(),
		}
	}))
	coordinatorExpvarOnce.Do(func() {
		expvar.Publish("nokv_coordinator", expvar.Func(func() any {
			provider, _ := coordinatorExpvarProvider.Load().(debugSnapshotFunc)
			if provider == nil {
				return map[string]any{}
			}
			return provider()
		}))
	})
}

var (
	metaRootExpvarOnce     sync.Once
	metaRootExpvarProvider atomic.Value
)

type metaRootExpvarContext struct {
	addr    string
	nodeID  uint64
	backend rootserver.Backend
}

// installMetaRootExpvar publishes `nokv_meta_root` under /debug/vars with the
// listen address, node id, and leader/committed snapshot view. It is safe to
// call even when the expvar server is not started (the var just sits unused).
func installMetaRootExpvar(ctx metaRootExpvarContext) {
	metaRootExpvarProvider.Store(debugSnapshotFunc(func() any {
		return metaRootSnapshot(ctx)
	}))
	metaRootExpvarOnce.Do(func() {
		expvar.Publish("nokv_meta_root", expvar.Func(func() any {
			provider, _ := metaRootExpvarProvider.Load().(debugSnapshotFunc)
			if provider == nil {
				return map[string]any{}
			}
			return provider()
		}))
	})
}

func metaRootSnapshot(ctx metaRootExpvarContext) map[string]any {
	out := map[string]any{
		"addr":    ctx.addr,
		"node_id": ctx.nodeID,
	}
	if ctx.backend == nil {
		return out
	}
	if snap, err := ctx.backend.Snapshot(); err == nil {
		state := snap.State
		out["cluster_epoch"] = state.ClusterEpoch
		out["membership_epoch"] = state.MembershipEpoch
		out["id_fence"] = state.IDFence
		out["tso_fence"] = state.TSOFence
		out["last_committed"] = map[string]any{
			"term":  state.LastCommitted.Term,
			"index": state.LastCommitted.Index,
		}
		out["descriptor_count"] = len(snap.Descriptors)
		out["pending_peer_changes"] = len(snap.PendingPeerChanges)
		out["pending_range_changes"] = len(snap.PendingRangeChanges)
	}
	if leader, ok := ctx.backend.(interface {
		IsLeader() bool
		LeaderID() uint64
	}); ok {
		out["is_leader"] = leader.IsLeader()
		out["leader_id"] = leader.LeaderID()
	}
	return out
}

// startExpvarServer starts an optional HTTP endpoint exposing /debug/vars.
// An empty address disables the server and returns nil.
func startExpvarServer(addr string) (net.Listener, error) {
	return metricspkg.StartExpvarServer(addr)
}
