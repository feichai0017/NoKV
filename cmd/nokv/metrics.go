package main

import (
	"expvar"
	"net"
	"sync"
	"sync/atomic"

	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	metricspkg "github.com/feichai0017/NoKV/metrics"
)

type debugSnapshotFunc func() any

var (
	coordinatorExpvarOnce     sync.Once
	coordinatorExpvarProvider atomic.Value
)

func installCoordinatorExpvar(mode string, svc *coordserver.Service) {
	if svc == nil {
		return
	}
	coordinatorExpvarProvider.Store(debugSnapshotFunc(func() any {
		return map[string]any{
			"root_mode": mode,
			"state":     svc.DiagnosticsSnapshot(),
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

// startExpvarServer starts an optional HTTP endpoint exposing /debug/vars.
// An empty address disables the server and returns nil.
func startExpvarServer(addr string) (net.Listener, error) {
	return metricspkg.StartExpvarServer(addr)
}
