package mvcc

import (
	"context"
	"fmt"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
)

// MaintenanceProposer submits MVCC maintenance entries through the replicated
// raft command path. Implementations must treat each entry key as a user-key
// scoped mutation after decoding its internal key.
//
// A call may span multiple regions. Implementations are allowed to commit a
// subset of region-local batches before returning an error; MVCC maintenance is
// intentionally at-least-once and idempotent, not cross-region atomic.
type MaintenanceProposer interface {
	ProposeMVCCMaintenance(context.Context, []*entrykv.Entry) (entries, writeDeletes, defaultDeletes uint64, err error)
}

// LockResolverProposer submits semantic ResolveLock commands through raft.
// Implementations must preserve the normal Percolator apply path instead of
// translating locks into raw tombstones.
type LockResolverProposer interface {
	ProposeResolveLocks(ctx context.Context, startVersion, commitVersion uint64, keys [][]byte) (uint64, error)
}

type maintenanceSubmitResult struct {
	entries        uint64
	writeDeletes   uint64
	defaultDeletes uint64
}

func proposeMaintenanceEntries(ctx context.Context, proposer MaintenanceProposer, entries []*entrykv.Entry) (maintenanceSubmitResult, error) {
	if len(entries) == 0 {
		return maintenanceSubmitResult{}, nil
	}
	if proposer == nil {
		return maintenanceSubmitResult{}, fmt.Errorf("raftstore/mvcc: nil maintenance proposer")
	}
	applied, writeDeletes, defaultDeletes, err := proposer.ProposeMVCCMaintenance(ctx, entries)
	result := maintenanceSubmitResult{
		entries:        applied,
		writeDeletes:   writeDeletes,
		defaultDeletes: defaultDeletes,
	}
	if err != nil {
		return result, err
	}
	if writeDeletes+defaultDeletes != applied {
		return result, fmt.Errorf("raftstore/mvcc: maintenance proposer applied %d entries but reported %d write and %d default deletes", applied, writeDeletes, defaultDeletes)
	}
	if applied != uint64(len(entries)) {
		return result, fmt.Errorf("raftstore/mvcc: maintenance proposer applied %d entries, expected %d", applied, len(entries))
	}
	return result, nil
}

func proposeResolveLocks(ctx context.Context, proposer LockResolverProposer, startVersion, commitVersion uint64, keys [][]byte) (uint64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	if proposer == nil {
		return 0, fmt.Errorf("raftstore/mvcc: nil lock resolver proposer")
	}
	return proposer.ProposeResolveLocks(ctx, startVersion, commitVersion, keys)
}
