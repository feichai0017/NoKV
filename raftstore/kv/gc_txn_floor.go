package kv

import (
	"context"
	"fmt"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/engine/index"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/percolator"
)

// MVCCGCTxnFloor summarizes active Percolator locks. The oldest lock start
// timestamp is the transaction floor that MVCC GC must not cross.
type MVCCGCTxnFloor struct {
	ActiveLocks   uint64
	OldestStartTs uint64
	MaxStartTs    uint64
}

// Active reports whether any live lock was found.
func (f MVCCGCTxnFloor) Active() bool {
	return f.ActiveLocks > 0 && f.OldestStartTs != 0
}

// PlanMVCCGCTxnFloor scans CFLock and returns the oldest active transaction
// start timestamp. It is read-only and ignores lock tombstones.
func PlanMVCCGCTxnFloor(ctx context.Context, db NoKV.MVCCStore) (MVCCGCTxnFloor, error) {
	var floor MVCCGCTxnFloor
	if db == nil {
		return floor, fmt.Errorf("kv: nil MVCC store")
	}
	iter := db.NewInternalIterator(&index.Options{IsAsc: true})
	if iter == nil {
		return floor, nil
	}
	defer func() { _ = iter.Close() }()

	iter.Seek(entrykv.InternalKey(entrykv.CFLock, nil, entrykv.MaxVersion))
	for iter.Valid() {
		if err := ctx.Err(); err != nil {
			return floor, err
		}
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
		if !ok {
			return floor, fmt.Errorf("kv: MVCC GC txn floor expects internal key, got %x", entry.Key)
		}
		if cf != entrykv.CFLock {
			break
		}
		if entry.Meta&entrykv.BitDelete > 0 {
			iter.Next()
			continue
		}
		lock, err := percolator.DecodeLock(entry.Value)
		if err != nil {
			return floor, fmt.Errorf("kv: decode CFLock %x: %w", userKey, err)
		}
		if lock.Ts == 0 {
			iter.Next()
			continue
		}
		floor.ActiveLocks++
		if floor.OldestStartTs == 0 || lock.Ts < floor.OldestStartTs {
			floor.OldestStartTs = lock.Ts
		}
		if lock.Ts > floor.MaxStartTs {
			floor.MaxStartTs = lock.Ts
		}
		iter.Next()
	}
	return floor, nil
}
