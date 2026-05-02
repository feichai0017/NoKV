package store

import (
	"context"
	"fmt"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
)

// ResolveLocks submits a semantic Percolator ResolveLock command through raft.
// It is used by cluster-mode MVCC maintenance; direct lock tombstones would
// bypass apply observers and WatchSubtree notifications.
//
// The operation is atomic per region batch, not across regions. If one region
// succeeds and a later region fails, the next maintenance pass must rescan and
// converge through idempotent lock resolution.
func (s *Store) ResolveLocks(ctx context.Context, startVersion, commitVersion uint64, keys [][]byte) (uint64, error) {
	if startVersion == 0 {
		return 0, fmt.Errorf("raftstore: resolve locks start version is required")
	}
	if len(keys) == 0 {
		return 0, nil
	}
	batches := make([]resolveLockRegionBatch, 0, 1)
	regionIndex := make(map[uint64]int)
	for _, key := range keys {
		if len(key) == 0 {
			return 0, fmt.Errorf("raftstore: empty resolve-lock key")
		}
		meta, ok := s.RegionMetaByKey(key)
		if !ok {
			return 0, fmt.Errorf("raftstore: no region for resolve-lock key %x", key)
		}
		idx, ok := regionIndex[meta.ID]
		if !ok {
			idx = len(batches)
			regionIndex[meta.ID] = idx
			batches = append(batches, resolveLockRegionBatch{meta: meta})
		}
		batches[idx].keys = append(batches[idx].keys, append([]byte(nil), key...))
	}

	var resolved uint64
	for _, batch := range batches {
		count, err := s.proposeResolveLockBatch(ctx, startVersion, commitVersion, batch)
		if err != nil {
			return resolved, err
		}
		resolved += count
	}
	return resolved, nil
}

// CheckTxnStatus routes the primary-key status check through the primary
// region's normal raft apply path.
func (s *Store) CheckTxnStatus(ctx context.Context, primary []byte, lockTs, currentTs, currentTime uint64) (*kvrpcpb.CheckTxnStatusResponse, error) {
	if len(primary) == 0 {
		return nil, fmt.Errorf("raftstore: primary key is required for check txn status")
	}
	meta, ok := s.RegionMetaByKey(primary)
	if !ok {
		return nil, fmt.Errorf("raftstore: no region for check txn status primary %x", primary)
	}
	resp, err := s.ProposeCommand(ctx, &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId: meta.ID,
			RegionEpoch: &metapb.RegionEpoch{
				Version:     meta.Epoch.Version,
				ConfVersion: meta.Epoch.ConfVersion,
			},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS,
			Cmd: &raftcmdpb.Request_CheckTxnStatus{CheckTxnStatus: &kvrpcpb.CheckTxnStatusRequest{
				PrimaryKey:         append([]byte(nil), primary...),
				LockTs:             lockTs,
				CurrentTs:          currentTs,
				CallerStartTs:      currentTs,
				RollbackIfNotExist: true,
				CurrentTime:        currentTime,
			}},
		}},
	})
	if err != nil {
		return nil, err
	}
	if regionErr := resp.GetRegionError(); regionErr != nil {
		return nil, fmt.Errorf("raftstore: check txn status region %d failed: %v", meta.ID, regionErr)
	}
	responses := resp.GetResponses()
	if len(responses) != 1 || responses[0].GetCheckTxnStatus() == nil {
		return nil, fmt.Errorf("raftstore: check txn status region %d returned invalid response", meta.ID)
	}
	return responses[0].GetCheckTxnStatus(), nil
}

type resolveLockRegionBatch struct {
	meta localmeta.RegionMeta
	keys [][]byte
}

func (s *Store) proposeResolveLockBatch(ctx context.Context, startVersion, commitVersion uint64, batch resolveLockRegionBatch) (uint64, error) {
	resp, err := s.ProposeCommand(ctx, &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId: batch.meta.ID,
			RegionEpoch: &metapb.RegionEpoch{
				Version:     batch.meta.Epoch.Version,
				ConfVersion: batch.meta.Epoch.ConfVersion,
			},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_RESOLVE_LOCK,
			Cmd: &raftcmdpb.Request_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockRequest{
				StartVersion:  startVersion,
				CommitVersion: commitVersion,
				Keys:          batch.keys,
			}},
		}},
	})
	if err != nil {
		return 0, err
	}
	if regionErr := resp.GetRegionError(); regionErr != nil {
		return 0, fmt.Errorf("raftstore: resolve locks region %d failed: %v", batch.meta.ID, regionErr)
	}
	responses := resp.GetResponses()
	if len(responses) != 1 || responses[0].GetResolveLock() == nil {
		return 0, fmt.Errorf("raftstore: resolve locks region %d returned invalid response", batch.meta.ID)
	}
	out := responses[0].GetResolveLock()
	if keyErr := out.GetError(); keyErr != nil {
		return 0, fmt.Errorf("raftstore: resolve locks region %d key error: %v", batch.meta.ID, keyErr)
	}
	return out.GetResolvedLocks(), nil
}
