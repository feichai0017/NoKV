package kv

import (
	"context"
	"errors"
	"fmt"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"

	"github.com/feichai0017/NoKV/raftstore/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service exposes NoKV gRPC handlers backed by a raftstore Store.
type Service struct {
	kvrpcpb.UnimplementedNoKVServer
	store *store.Store
}

// NewService constructs a NoKV service bound to the provided store.
func NewService(st *store.Store) *Service {
	return &Service{store: st}
}

func (s *Service) KvGet(ctx context.Context, req *kvrpcpb.KvGetRequest) (*kvrpcpb.KvGetResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	readReq := req.GetRequest()
	if readReq == nil {
		return nil, status.Error(codes.InvalidArgument, "get request missing payload")
	}
	cmd := &raftcmdpb.RaftCmdRequest{
		Header: header,
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: readReq},
		}},
	}
	result, err := s.read(ctx, cmd)
	if err != nil {
		return nil, rpcStatus(err)
	}
	resp := &kvrpcpb.KvGetResponse{RegionError: result.GetRegionError()}
	if len(result.GetResponses()) > 0 && result.GetResponses()[0].GetGet() != nil {
		resp.Response = result.GetResponses()[0].GetGet()
	}
	return resp, nil
}

func (s *Service) KvBatchGet(ctx context.Context, req *kvrpcpb.KvBatchGetRequest) (*kvrpcpb.KvBatchGetResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	batch := req.GetRequest()
	if batch == nil {
		return nil, status.Error(codes.InvalidArgument, "batch get request missing payload")
	}
	if len(batch.GetRequests()) == 0 {
		return &kvrpcpb.KvBatchGetResponse{
			Response: &kvrpcpb.BatchGetResponse{},
		}, nil
	}
	requests := make([]*raftcmdpb.Request, 0, len(batch.GetRequests()))
	for _, getReq := range batch.GetRequests() {
		if getReq == nil {
			continue
		}
		requests = append(requests, &raftcmdpb.Request{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: getReq},
		})
	}
	cmd := &raftcmdpb.RaftCmdRequest{
		Header:   header,
		Requests: requests,
	}
	result, err := s.read(ctx, cmd)
	if err != nil {
		return nil, rpcStatus(err)
	}
	resp := &kvrpcpb.KvBatchGetResponse{RegionError: result.GetRegionError()}
	if result.GetRegionError() == nil {
		responses := make([]*kvrpcpb.GetResponse, 0, len(requests))
		for _, r := range result.GetResponses() {
			if r == nil {
				responses = append(responses, &kvrpcpb.GetResponse{NotFound: true})
				continue
			}
			if get := r.GetGet(); get != nil {
				responses = append(responses, get)
				continue
			}
			responses = append(responses, &kvrpcpb.GetResponse{NotFound: true})
		}
		// Ensure the response count matches the request count.
		for len(responses) < len(requests) {
			responses = append(responses, &kvrpcpb.GetResponse{NotFound: true})
		}
		resp.Response = &kvrpcpb.BatchGetResponse{Responses: responses}
	}
	return resp, nil
}

func (s *Service) KvScan(ctx context.Context, req *kvrpcpb.KvScanRequest) (*kvrpcpb.KvScanResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	scanReq := req.GetRequest()
	if scanReq == nil {
		return nil, status.Error(codes.InvalidArgument, "scan request missing payload")
	}
	if scanReq.GetReverse() {
		return nil, status.Error(codes.Unimplemented, "KvScan reverse scans are not supported yet")
	}
	cmd := &raftcmdpb.RaftCmdRequest{
		Header: header,
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_SCAN,
			Cmd:     &raftcmdpb.Request_Scan{Scan: scanReq},
		}},
	}
	result, err := s.read(ctx, cmd)
	if err != nil {
		return nil, rpcStatus(err)
	}
	resp := &kvrpcpb.KvScanResponse{RegionError: result.GetRegionError()}
	if len(result.GetResponses()) > 0 && result.GetResponses()[0].GetScan() != nil {
		resp.Response = result.GetResponses()[0].GetScan()
	}
	return resp, nil
}

func (s *Service) KvPrewrite(ctx context.Context, req *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetRequest() == nil {
		return nil, status.Error(codes.InvalidArgument, "prewrite request missing payload")
	}
	resp, err := s.propose(ctx, &raftcmdpb.RaftCmdRequest{
		Header: header,
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
			Cmd:     &raftcmdpb.Request_Prewrite{Prewrite: req.GetRequest()},
		}},
	})
	if err != nil {
		return nil, rpcStatus(err)
	}
	out := &kvrpcpb.KvPrewriteResponse{RegionError: resp.GetRegionError()}
	if len(resp.GetResponses()) > 0 && resp.GetResponses()[0].GetPrewrite() != nil {
		out.Response = resp.GetResponses()[0].GetPrewrite()
	}
	return out, nil
}

func (s *Service) KvCommit(ctx context.Context, req *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetRequest() == nil {
		return nil, status.Error(codes.InvalidArgument, "commit request missing payload")
	}
	resp, err := s.propose(ctx, &raftcmdpb.RaftCmdRequest{
		Header: header,
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_COMMIT,
			Cmd:     &raftcmdpb.Request_Commit{Commit: req.GetRequest()},
		}},
	})
	if err != nil {
		return nil, rpcStatus(err)
	}
	out := &kvrpcpb.KvCommitResponse{RegionError: resp.GetRegionError()}
	if len(resp.GetResponses()) > 0 && resp.GetResponses()[0].GetCommit() != nil {
		out.Response = resp.GetResponses()[0].GetCommit()
	}
	return out, nil
}

func (s *Service) KvBatchRollback(ctx context.Context, req *kvrpcpb.KvBatchRollbackRequest) (*kvrpcpb.KvBatchRollbackResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetRequest() == nil {
		return nil, status.Error(codes.InvalidArgument, "rollback request missing payload")
	}
	resp, err := s.propose(ctx, &raftcmdpb.RaftCmdRequest{
		Header: header,
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_BATCH_ROLLBACK,
			Cmd:     &raftcmdpb.Request_BatchRollback{BatchRollback: req.GetRequest()},
		}},
	})
	if err != nil {
		return nil, rpcStatus(err)
	}
	out := &kvrpcpb.KvBatchRollbackResponse{RegionError: resp.GetRegionError()}
	if len(resp.GetResponses()) > 0 && resp.GetResponses()[0].GetBatchRollback() != nil {
		out.Response = resp.GetResponses()[0].GetBatchRollback()
	}
	return out, nil
}

func (s *Service) KvResolveLock(ctx context.Context, req *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetRequest() == nil {
		return nil, status.Error(codes.InvalidArgument, "resolve lock request missing payload")
	}
	resp, err := s.propose(ctx, &raftcmdpb.RaftCmdRequest{
		Header: header,
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_RESOLVE_LOCK,
			Cmd:     &raftcmdpb.Request_ResolveLock{ResolveLock: req.GetRequest()},
		}},
	})
	if err != nil {
		return nil, rpcStatus(err)
	}
	out := &kvrpcpb.KvResolveLockResponse{RegionError: resp.GetRegionError()}
	if len(resp.GetResponses()) > 0 && resp.GetResponses()[0].GetResolveLock() != nil {
		out.Response = resp.GetResponses()[0].GetResolveLock()
	}
	return out, nil
}

func (s *Service) KvCheckTxnStatus(ctx context.Context, req *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetRequest() == nil {
		return nil, status.Error(codes.InvalidArgument, "check txn status request missing payload")
	}
	resp, err := s.propose(ctx, &raftcmdpb.RaftCmdRequest{
		Header: header,
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS,
			Cmd:     &raftcmdpb.Request_CheckTxnStatus{CheckTxnStatus: req.GetRequest()},
		}},
	})
	if err != nil {
		return nil, rpcStatus(err)
	}
	out := &kvrpcpb.KvCheckTxnStatusResponse{RegionError: resp.GetRegionError()}
	if len(resp.GetResponses()) > 0 && resp.GetResponses()[0].GetCheckTxnStatus() != nil {
		out.Response = resp.GetResponses()[0].GetCheckTxnStatus()
	}
	return out, nil
}

func (s *Service) read(ctx context.Context, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
	if s.store == nil {
		return nil, fmt.Errorf("raftstore: store not initialized")
	}
	return s.store.ReadCommand(ctx, req)
}

func (s *Service) propose(ctx context.Context, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
	if s.store == nil {
		return nil, fmt.Errorf("raftstore: store not initialized")
	}
	return s.store.ProposeCommand(ctx, req)
}

func buildHeader(ctx *kvrpcpb.Context) (*raftcmdpb.CmdHeader, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	if ctx.GetRegionId() == 0 {
		return nil, fmt.Errorf("region id is required")
	}
	header := &raftcmdpb.CmdHeader{RegionId: ctx.GetRegionId(), RegionEpoch: ctx.GetRegionEpoch()}
	if peer := ctx.GetPeer(); peer != nil {
		header.PeerId = peer.GetPeerId()
		header.StoreId = peer.GetStoreId()
	}
	return header, nil
}

func rpcStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
