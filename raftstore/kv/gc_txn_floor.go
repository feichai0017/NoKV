package kv

import (
	"context"

	"github.com/feichai0017/NoKV/engine/mvcc"
	"github.com/feichai0017/NoKV/raftstore/mvccgc"
)

type MVCCGCTxnFloor = mvccgc.TxnFloor

func PlanMVCCGCTxnFloor(ctx context.Context, db mvcc.Store) (MVCCGCTxnFloor, error) {
	return mvccgc.PlanTxnFloor(ctx, db)
}
