package server

import (
	txnstore "github.com/feichai0017/NoKV/percolator/storage"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	"github.com/feichai0017/NoKV/raftstore/store"
)

func newMVCCMaintenanceWorker(cfg MVCCMaintenanceConfig, mvccStore txnstore.Store, raftStore *store.Store) (*storemvcc.MaintenanceWorker, bool) {
	if raftStore == nil {
		return nil, false
	}
	return storemvcc.NewMaintenanceWorker(storemvcc.MaintenanceWorkerConfig{
		MVCCStore:           mvccStore,
		MaintenanceProposer: raftStore,
		LockResolver:        cfg.LockResolver,
		Interval:            cfg.Interval,
		Timeout:             cfg.Timeout,
		SafePoint:           cfg.SafePoint,
		CurrentTs:           cfg.CurrentTs,
		Retention:           cfg.Retention,
		Mount:               cfg.Mount,
		Apply:               cfg.Apply,
		ResolveLocks:        cfg.ResolveLocks,
		RunOrphanDefaults:   cfg.RunOrphanDefaults,
		OrphanDefaults:      cfg.OrphanDefaults,
	})
}
