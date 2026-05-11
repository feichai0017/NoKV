package store

import (
	"fmt"

	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
)

type commandApplyPlan struct {
	entry       myraft.Entry
	req         *raftcmdpb.RaftCmdRequest
	proposalKey commandProposalKey
	keys        []string
	barrier     bool
}

func commandApplyPlans(entries []myraft.Entry) ([]commandApplyPlan, error) {
	plans := make([]commandApplyPlan, 0, len(entries))
	for _, entry := range entries {
		if entry.Type != myraft.EntryNormal || len(entry.Data) == 0 {
			continue
		}
		req, isCmd, err := command.Decode(entry.Data)
		if err != nil {
			return nil, err
		}
		if !isCmd {
			return nil, fmt.Errorf("commandPipeline: unsupported unframed raft payload")
		}
		keys, barrier := commandApplyKeys(req)
		plans = append(plans, commandApplyPlan{
			entry:       entry,
			req:         req,
			proposalKey: proposalKeyFromHeader(req.GetHeader()),
			keys:        keys,
			barrier:     barrier,
		})
	}
	return plans, nil
}

func commandApplyKeys(req *raftcmdpb.RaftCmdRequest) ([]string, bool) {
	if req == nil || len(req.GetRequests()) == 0 {
		return nil, true
	}
	keys := make([]string, 0, len(req.GetRequests()))
	for _, r := range req.GetRequests() {
		if r == nil {
			return nil, true
		}
		switch r.GetCmdType() {
		case raftcmdpb.CmdType_CMD_PREWRITE:
			prewrite := r.GetPrewrite()
			if prewrite == nil || len(prewrite.GetMutations()) == 0 {
				return nil, true
			}
			for _, mut := range prewrite.GetMutations() {
				if mut != nil {
					keys = appendCommandApplyKey(keys, mut.GetKey())
				}
			}
		case raftcmdpb.CmdType_CMD_COMMIT:
			commit := r.GetCommit()
			if commit == nil {
				return nil, true
			}
			keys = appendCommandApplyKeys(keys, commit.GetKeys())
		case raftcmdpb.CmdType_CMD_BATCH_ROLLBACK:
			rollback := r.GetBatchRollback()
			if rollback == nil {
				return nil, true
			}
			keys = appendCommandApplyKeys(keys, rollback.GetKeys())
		case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
			resolve := r.GetResolveLock()
			if resolve == nil {
				return nil, true
			}
			keys = appendCommandApplyKeys(keys, resolve.GetKeys())
		case raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS:
			check := r.GetCheckTxnStatus()
			if check == nil || len(check.GetPrimaryKey()) == 0 {
				return nil, true
			}
			keys = appendCommandApplyKey(keys, check.GetPrimaryKey())
		case raftcmdpb.CmdType_CMD_TXN_HEART_BEAT:
			heartbeat := r.GetTxnHeartBeat()
			if heartbeat == nil || len(heartbeat.GetPrimaryKey()) == 0 {
				return nil, true
			}
			keys = appendCommandApplyKey(keys, heartbeat.GetPrimaryKey())
		case raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE:
			atomic := r.GetTryAtomicMutate()
			if atomic == nil || len(atomic.GetMutations()) == 0 {
				return nil, true
			}
			for _, pred := range atomic.GetPredicates() {
				if pred != nil {
					keys = appendCommandApplyKey(keys, pred.GetKey())
				}
			}
			for _, mut := range atomic.GetMutations() {
				if mut != nil {
					keys = appendCommandApplyKey(keys, mut.GetKey())
				}
			}
		case raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE:
			maintenance := r.GetMvccMaintenance()
			if maintenance == nil || len(maintenance.GetTombstones()) == 0 {
				return nil, true
			}
			for _, tombstone := range maintenance.GetTombstones() {
				if tombstone != nil {
					keys = appendCommandApplyKey(keys, tombstone.GetKey())
				}
			}
		case raftcmdpb.CmdType_CMD_GET, raftcmdpb.CmdType_CMD_SCAN:
			return nil, true
		default:
			return nil, true
		}
	}
	if len(keys) == 0 {
		return nil, true
	}
	return keys, false
}

func appendCommandApplyKeys(dst []string, keys [][]byte) []string {
	for _, key := range keys {
		dst = appendCommandApplyKey(dst, key)
	}
	return dst
}

func appendCommandApplyKey(dst []string, key []byte) []string {
	if len(key) == 0 {
		return dst
	}
	return append(dst, string(key))
}

func commandApplyPlanConflicts(waveKeys map[string]struct{}, plan commandApplyPlan) bool {
	for _, key := range plan.keys {
		if _, ok := waveKeys[key]; ok {
			return true
		}
	}
	return false
}

func commandApplyPlanAddKeys(waveKeys map[string]struct{}, plan commandApplyPlan) {
	for _, key := range plan.keys {
		waveKeys[key] = struct{}{}
	}
}
