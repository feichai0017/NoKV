package kv

import (
	"fmt"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/mvcc"
	"github.com/feichai0017/NoKV/mvcc/latch"
	"github.com/feichai0017/NoKV/pb"
)

var defaultLatches = latch.NewManager(512)

// Apply executes a RaftCmdRequest against the provided DB. The returned
// response mirrors the request ordering. Only MVCC prewrite/commit operations
// are supported at the moment.
func Apply(db *NoKV.DB, req *pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("kv: nil raft command")
	}
	resp := &pb.RaftCmdResponse{Header: req.Header}
	for _, r := range req.Requests {
		if r == nil {
			continue
		}
		switch r.GetCmdType() {
		case pb.CmdType_CMD_PREWRITE:
			result := &pb.PrewriteResponse{Errors: mvcc.Prewrite(db, defaultLatches, r.GetPrewrite())}
			resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_Prewrite{Prewrite: result}})
		case pb.CmdType_CMD_COMMIT:
			err := mvcc.Commit(db, defaultLatches, r.GetCommit())
			resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_Commit{Commit: &pb.CommitResponse{Error: err}}})
		case pb.CmdType_CMD_BATCH_ROLLBACK:
			err := mvcc.BatchRollback(db, defaultLatches, r.GetBatchRollback())
			resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_BatchRollback{BatchRollback: &pb.BatchRollbackResponse{Error: err}}})
		case pb.CmdType_CMD_RESOLVE_LOCK:
			count, err := mvcc.ResolveLock(db, defaultLatches, r.GetResolveLock())
			resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_ResolveLock{ResolveLock: &pb.ResolveLockResponse{ResolvedLocks: count, Error: err}}})
		case pb.CmdType_CMD_CHECK_TXN_STATUS:
			result := mvcc.CheckTxnStatus(db, defaultLatches, r.GetCheckTxnStatus())
			resp.Responses = append(resp.Responses, &pb.Response{Cmd: &pb.Response_CheckTxnStatus{CheckTxnStatus: result}})
		default:
			return nil, fmt.Errorf("kv: unsupported command %v", r.GetCmdType())
		}
	}
	return resp, nil
}

// NewApplier wraps Apply into a reusable function suitable for store command
// execution wiring.
func NewApplier(db *NoKV.DB) func(*pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
	return func(req *pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
		return Apply(db, req)
	}
}
