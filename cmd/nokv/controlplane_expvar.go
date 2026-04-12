package main

import (
	"expvar"
	"sync"
	"sync/atomic"

	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	rootremote "github.com/feichai0017/NoKV/meta/root/remote"
)

type debugSnapshotFunc func() any

var (
	coordinatorExpvarOnce     sync.Once
	coordinatorExpvarProvider atomic.Value
	metaRootExpvarOnce        sync.Once
	metaRootExpvarProvider    atomic.Value
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

func installMetaRootExpvar(mode string, backend rootremote.Backend) {
	if backend == nil {
		return
	}
	metaRootExpvarProvider.Store(debugSnapshotFunc(func() any {
		return map[string]any{
			"mode":  mode,
			"state": metaRootDiagnosticsSnapshot(backend),
		}
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
