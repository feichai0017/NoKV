package kv

import (
	"context"

	"github.com/feichai0017/NoKV/engine/mvcc"
	"github.com/feichai0017/NoKV/raftstore/mvccgc"
)

type MVCCGCPlanStats = mvccgc.PlanStats
type MVCCGCApplyStats = mvccgc.ApplyStats
type MVCCGCApplyOptions = mvccgc.ApplyOptions

func PlanMVCCGC(ctx context.Context, db mvcc.Store, policy MVCCGCSafePointPolicy) (MVCCGCPlanStats, error) {
	return mvccgc.Plan(ctx, db, policy)
}

func ApplyMVCCGC(ctx context.Context, db mvcc.Store, policy MVCCGCSafePointPolicy, opt MVCCGCApplyOptions) (MVCCGCApplyStats, error) {
	return mvccgc.Apply(ctx, db, policy, opt)
}
