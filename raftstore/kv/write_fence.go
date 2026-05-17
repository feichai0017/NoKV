// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package kv

import (
	"time"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"github.com/feichai0017/NoKV/txn/latch"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
)

func applyBatchWithFence(
	db txnstore.Store,
	latches *latch.Manager,
	cfg applyConfig,
	reqs []*raftcmdpb.RaftCmdRequest,
) ([]*raftcmdpb.RaftCmdResponse, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	if cfg.writeFence == nil {
		return ApplyBatch(db, latches, reqs)
	}
	resps := make([]*raftcmdpb.RaftCmdResponse, len(reqs))
	for i := 0; i < len(reqs); {
		if resp, fenced := rejectWriteFencedRequest(cfg, reqs[i]); fenced {
			resps[i] = resp
			i++
			continue
		}
		end := i + 1
		for end < len(reqs) {
			if _, fenced := rejectWriteFencedRequest(cfg, reqs[end]); fenced {
				break
			}
			end++
		}
		run, err := ApplyBatch(db, latches, reqs[i:end])
		if err != nil {
			return nil, err
		}
		copy(resps[i:end], run)
		i = end
	}
	return resps, nil
}

func rejectWriteFencedRequest(cfg applyConfig, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, bool) {
	if cfg.writeFence == nil || req == nil {
		return nil, false
	}
	var keyErr *kvrpcpb.KeyError
	for _, r := range req.GetRequests() {
		if err := writeFenceErrorForCommand(cfg, r); err != nil {
			keyErr = err
			break
		}
	}
	if keyErr == nil {
		return nil, false
	}
	resp := &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}
	for _, r := range req.GetRequests() {
		resp.Responses = append(resp.Responses, writeFenceResponseForCommand(r, keyErr))
	}
	return resp, true
}

func writeFenceErrorForCommand(cfg applyConfig, r *raftcmdpb.Request) *kvrpcpb.KeyError {
	if r == nil {
		return nil
	}
	check := func(key []byte) *kvrpcpb.KeyError {
		return writeFenceErrorForKey(cfg, key)
	}
	switch r.GetCmdType() {
	case raftcmdpb.CmdType_CMD_GET,
		raftcmdpb.CmdType_CMD_SCAN,
		raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC:
		return nil
	case raftcmdpb.CmdType_CMD_PREWRITE:
		req := r.GetPrewrite()
		if req == nil {
			return nil
		}
		if err := check(req.GetPrimaryLock()); err != nil {
			return err
		}
		for _, mutation := range req.GetMutations() {
			if mutation == nil {
				continue
			}
			if err := check(mutation.GetKey()); err != nil {
				return err
			}
		}
	case raftcmdpb.CmdType_CMD_COMMIT:
		return firstWriteFenceError(cfg, r.GetCommit().GetKeys())
	case raftcmdpb.CmdType_CMD_BATCH_ROLLBACK:
		return firstWriteFenceError(cfg, r.GetBatchRollback().GetKeys())
	case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
		return firstWriteFenceError(cfg, r.GetResolveLock().GetKeys())
	case raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS:
		return check(r.GetCheckTxnStatus().GetPrimaryKey())
	case raftcmdpb.CmdType_CMD_TXN_HEART_BEAT:
		return check(r.GetTxnHeartBeat().GetPrimaryKey())
	case raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE:
		req := r.GetTryAtomicMutate()
		if req == nil {
			return nil
		}
		for _, predicate := range req.GetPredicates() {
			if predicate == nil {
				continue
			}
			if err := check(predicate.GetKey()); err != nil {
				return err
			}
		}
		for _, mutation := range req.GetMutations() {
			if mutation == nil {
				continue
			}
			if err := check(mutation.GetKey()); err != nil {
				return err
			}
		}
	case raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE:
		req := r.GetMvccMaintenance()
		if req == nil {
			return nil
		}
		for _, tombstone := range req.GetTombstones() {
			if tombstone == nil {
				continue
			}
			if err := check(tombstone.GetKey()); err != nil {
				return err
			}
		}
	default:
		return nil
	}
	return nil
}

func firstWriteFenceError(cfg applyConfig, keys [][]byte) *kvrpcpb.KeyError {
	for _, key := range keys {
		if err := writeFenceErrorForKey(cfg, key); err != nil {
			return err
		}
	}
	return nil
}

func writeFenceErrorForKey(cfg applyConfig, key []byte) *kvrpcpb.KeyError {
	if len(key) == 0 || cfg.writeFence == nil {
		return nil
	}
	now := time.Now
	if cfg.now != nil {
		now = cfg.now
	}
	decision, err := cfg.writeFence.FenceKey(key, now())
	if err != nil {
		return &kvrpcpb.KeyError{Retryable: "write fence: " + err.Error()}
	}
	if !decision.Fenced {
		return nil
	}
	if decision.Reason == "" {
		return &kvrpcpb.KeyError{Retryable: "write fence"}
	}
	return &kvrpcpb.KeyError{Retryable: "write fence: " + decision.Reason}
}

func writeFenceResponseForCommand(r *raftcmdpb.Request, keyErr *kvrpcpb.KeyError) *raftcmdpb.Response {
	if r == nil {
		return &raftcmdpb.Response{}
	}
	switch r.GetCmdType() {
	case raftcmdpb.CmdType_CMD_GET:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Get{Get: &kvrpcpb.GetResponse{Error: keyErr}}}
	case raftcmdpb.CmdType_CMD_PREWRITE:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Prewrite{Prewrite: &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{keyErr}}}}
	case raftcmdpb.CmdType_CMD_COMMIT:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Commit{Commit: &kvrpcpb.CommitResponse{Error: keyErr}}}
	case raftcmdpb.CmdType_CMD_BATCH_ROLLBACK:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_BatchRollback{BatchRollback: &kvrpcpb.BatchRollbackResponse{Error: keyErr}}}
	case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockResponse{Error: keyErr}}}
	case raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_CheckTxnStatus{CheckTxnStatus: &kvrpcpb.CheckTxnStatusResponse{Error: keyErr}}}
	case raftcmdpb.CmdType_CMD_TXN_HEART_BEAT:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_TxnHeartBeat{TxnHeartBeat: &kvrpcpb.TxnHeartBeatResponse{Error: keyErr}}}
	case raftcmdpb.CmdType_CMD_SCAN:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Scan{Scan: &kvrpcpb.ScanResponse{Error: keyErr}}}
	case raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateResponse{Error: keyErr}}}
	case raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_MvccMaintenance{MvccMaintenance: &kvrpcpb.MVCCMaintenanceResponse{Error: keyErr}}}
	case raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_InstallPreparedMvcc{InstallPreparedMvcc: &kvrpcpb.InstallPreparedMVCCEntriesResponse{Error: keyErr}}}
	default:
		return &raftcmdpb.Response{}
	}
}
