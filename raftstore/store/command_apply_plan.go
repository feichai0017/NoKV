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
	deps        []commandApplyDependency
	barrier     bool
}

type commandApplyDependencyMode uint8

const (
	commandApplyDependencyRead commandApplyDependencyMode = iota
	commandApplyDependencyWrite
)

type commandApplyDependencyClass string

const (
	commandApplyDependencyUserKey    commandApplyDependencyClass = "user-key"
	commandApplyDependencyTxnPrimary commandApplyDependencyClass = "txn-primary"
	commandApplyDependencyTxnIntent  commandApplyDependencyClass = "txn-intent"
)

type commandApplyDependencyKey struct {
	class   commandApplyDependencyClass
	key     string
	version uint64
}

type commandApplyDependency struct {
	key  commandApplyDependencyKey
	mode commandApplyDependencyMode
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
		deps, barrier := commandApplyDependencies(req)
		plans = append(plans, commandApplyPlan{
			entry:       entry,
			req:         req,
			proposalKey: proposalKeyFromHeader(req.GetHeader()),
			deps:        deps,
			barrier:     barrier,
		})
	}
	return plans, nil
}

func commandApplyDependencies(req *raftcmdpb.RaftCmdRequest) ([]commandApplyDependency, bool) {
	if req == nil || len(req.GetRequests()) == 0 {
		return nil, true
	}
	deps := make([]commandApplyDependency, 0, len(req.GetRequests()))
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
					deps = appendCommandApplyUserWrite(deps, mut.GetKey())
					deps = appendCommandApplyVersionedWrite(deps, commandApplyDependencyTxnIntent, mut.GetKey(), prewrite.GetStartVersion())
				}
			}
		case raftcmdpb.CmdType_CMD_COMMIT:
			commit := r.GetCommit()
			if commit == nil {
				return nil, true
			}
			deps = appendCommandApplyUserWrites(deps, commit.GetKeys())
			deps = appendCommandApplyVersionedWrites(deps, commandApplyDependencyTxnIntent, commit.GetKeys(), commit.GetStartVersion())
		case raftcmdpb.CmdType_CMD_BATCH_ROLLBACK:
			rollback := r.GetBatchRollback()
			if rollback == nil {
				return nil, true
			}
			deps = appendCommandApplyUserWrites(deps, rollback.GetKeys())
			deps = appendCommandApplyVersionedWrites(deps, commandApplyDependencyTxnIntent, rollback.GetKeys(), rollback.GetStartVersion())
		case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
			resolve := r.GetResolveLock()
			if resolve == nil {
				return nil, true
			}
			deps = appendCommandApplyUserWrites(deps, resolve.GetKeys())
			deps = appendCommandApplyVersionedWrites(deps, commandApplyDependencyTxnIntent, resolve.GetKeys(), resolve.GetStartVersion())
		case raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS:
			check := r.GetCheckTxnStatus()
			if check == nil || len(check.GetPrimaryKey()) == 0 {
				return nil, true
			}
			deps = appendCommandApplyUserWrite(deps, check.GetPrimaryKey())
			deps = appendCommandApplyVersionedWrite(deps, commandApplyDependencyTxnPrimary, check.GetPrimaryKey(), check.GetLockTs())
		case raftcmdpb.CmdType_CMD_TXN_HEART_BEAT:
			heartbeat := r.GetTxnHeartBeat()
			if heartbeat == nil || len(heartbeat.GetPrimaryKey()) == 0 {
				return nil, true
			}
			deps = appendCommandApplyUserWrite(deps, heartbeat.GetPrimaryKey())
			deps = appendCommandApplyVersionedWrite(deps, commandApplyDependencyTxnPrimary, heartbeat.GetPrimaryKey(), heartbeat.GetStartVersion())
		case raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE:
			atomic := r.GetTryAtomicMutate()
			if atomic == nil || len(atomic.GetMutations()) == 0 {
				return nil, true
			}
			for _, pred := range atomic.GetPredicates() {
				if pred != nil {
					deps = appendCommandApplyUserRead(deps, pred.GetKey())
				}
			}
			for _, mut := range atomic.GetMutations() {
				if mut != nil {
					deps = appendCommandApplyUserWrite(deps, mut.GetKey())
				}
			}
		case raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE:
			maintenance := r.GetMvccMaintenance()
			if maintenance == nil || len(maintenance.GetTombstones()) == 0 {
				return nil, true
			}
			return nil, true
		case raftcmdpb.CmdType_CMD_GET, raftcmdpb.CmdType_CMD_SCAN:
			return nil, true
		default:
			return nil, true
		}
	}
	if len(deps) == 0 {
		return nil, true
	}
	return deps, false
}

func appendCommandApplyUserWrites(dst []commandApplyDependency, keys [][]byte) []commandApplyDependency {
	for _, key := range keys {
		dst = appendCommandApplyUserWrite(dst, key)
	}
	return dst
}

func appendCommandApplyVersionedWrites(dst []commandApplyDependency, class commandApplyDependencyClass, keys [][]byte, version uint64) []commandApplyDependency {
	for _, key := range keys {
		dst = appendCommandApplyVersionedWrite(dst, class, key, version)
	}
	return dst
}

func appendCommandApplyUserRead(dst []commandApplyDependency, key []byte) []commandApplyDependency {
	return appendCommandApplyDependency(dst, commandApplyDependencyUserKey, key, 0, commandApplyDependencyRead)
}

func appendCommandApplyUserWrite(dst []commandApplyDependency, key []byte) []commandApplyDependency {
	return appendCommandApplyDependency(dst, commandApplyDependencyUserKey, key, 0, commandApplyDependencyWrite)
}

func appendCommandApplyVersionedWrite(dst []commandApplyDependency, class commandApplyDependencyClass, key []byte, version uint64) []commandApplyDependency {
	return appendCommandApplyDependency(dst, class, key, version, commandApplyDependencyWrite)
}

func appendCommandApplyDependency(dst []commandApplyDependency, class commandApplyDependencyClass, key []byte, version uint64, mode commandApplyDependencyMode) []commandApplyDependency {
	if len(key) == 0 {
		return dst
	}
	return append(dst, commandApplyDependency{
		key: commandApplyDependencyKey{
			class:   class,
			key:     string(key),
			version: version,
		},
		mode: mode,
	})
}

func commandApplyPlanConflicts(waveDeps map[commandApplyDependencyKey]commandApplyDependencyMode, plan commandApplyPlan) bool {
	for _, dep := range plan.deps {
		existing, ok := waveDeps[dep.key]
		if !ok {
			continue
		}
		if existing == commandApplyDependencyWrite || dep.mode == commandApplyDependencyWrite {
			return true
		}
	}
	return false
}

func commandApplyPlanAddDependencies(waveDeps map[commandApplyDependencyKey]commandApplyDependencyMode, plan commandApplyPlan) {
	for _, dep := range plan.deps {
		if existing, ok := waveDeps[dep.key]; ok && existing == commandApplyDependencyWrite {
			continue
		}
		waveDeps[dep.key] = dep.mode
	}
}
