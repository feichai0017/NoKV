package kv

import (
	"context"
	"fmt"

	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/raftstore/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service exposes TinyKV gRPC handlers backed by a raftstore Store.
type Service struct {
	pb.UnimplementedTinyKvServer
	store *store.Store
}

// NewService constructs a TinyKV service bound to the provided store.
func NewService(st *store.Store) *Service {
	return &Service{store: st}
}

func (s *Service) KvGet(ctx context.Context, req *pb.KvGetRequest) (*pb.KvGetResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	readReq := req.GetRequest()
	if readReq == nil {
		return nil, status.Error(codes.InvalidArgument, "get request missing payload")
	}
	cmd := &pb.RaftCmdRequest{
		Header: header,
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_GET,
			Cmd:     &pb.Request_Get{Get: readReq},
		}},
	}
	result, err := s.read(cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	resp := &pb.KvGetResponse{RegionError: result.GetRegionError()}
	if len(result.GetResponses()) > 0 && result.GetResponses()[0].GetGet() != nil {
		resp.Response = result.GetResponses()[0].GetGet()
	}
	return resp, nil
}

func (s *Service) KvBatchGet(ctx context.Context, req *pb.KvBatchGetRequest) (*pb.KvBatchGetResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	batch := req.GetRequest()
	if batch == nil {
		return nil, status.Error(codes.InvalidArgument, "batch get request missing payload")
	}
	if len(batch.GetRequests()) == 0 {
		return &pb.KvBatchGetResponse{
			Response: &pb.BatchGetResponse{},
		}, nil
	}
	requests := make([]*pb.Request, 0, len(batch.GetRequests()))
	for _, getReq := range batch.GetRequests() {
		if getReq == nil {
			continue
		}
		requests = append(requests, &pb.Request{
			CmdType: pb.CmdType_CMD_GET,
			Cmd:     &pb.Request_Get{Get: getReq},
		})
	}
	cmd := &pb.RaftCmdRequest{
		Header:   header,
		Requests: requests,
	}
	result, err := s.read(cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	resp := &pb.KvBatchGetResponse{RegionError: result.GetRegionError()}
	if result.GetRegionError() == nil {
		responses := make([]*pb.GetResponse, 0, len(requests))
		for _, r := range result.GetResponses() {
			if r == nil {
				responses = append(responses, &pb.GetResponse{NotFound: true})
				continue
			}
			if get := r.GetGet(); get != nil {
				responses = append(responses, get)
				continue
			}
			responses = append(responses, &pb.GetResponse{NotFound: true})
		}
		// Ensure the response count matches the request count.
		for len(responses) < len(requests) {
			responses = append(responses, &pb.GetResponse{NotFound: true})
		}
		resp.Response = &pb.BatchGetResponse{Responses: responses}
	}
	return resp, nil
}

func (s *Service) KvScan(ctx context.Context, req *pb.KvScanRequest) (*pb.KvScanResponse, error) {
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
	cmd := &pb.RaftCmdRequest{
		Header: header,
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_SCAN,
			Cmd:     &pb.Request_Scan{Scan: scanReq},
		}},
	}
	result, err := s.read(cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	resp := &pb.KvScanResponse{RegionError: result.GetRegionError()}
	if len(result.GetResponses()) > 0 && result.GetResponses()[0].GetScan() != nil {
		resp.Response = result.GetResponses()[0].GetScan()
	}
	return resp, nil
}

func (s *Service) KvPrewrite(ctx context.Context, req *pb.KvPrewriteRequest) (*pb.KvPrewriteResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetRequest() == nil {
		return nil, status.Error(codes.InvalidArgument, "prewrite request missing payload")
	}
	resp, err := s.propose(&pb.RaftCmdRequest{
		Header: header,
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_PREWRITE,
			Cmd:     &pb.Request_Prewrite{Prewrite: req.GetRequest()},
		}},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	out := &pb.KvPrewriteResponse{RegionError: resp.GetRegionError()}
	if len(resp.GetResponses()) > 0 && resp.GetResponses()[0].GetPrewrite() != nil {
		out.Response = resp.GetResponses()[0].GetPrewrite()
	}
	return out, nil
}

func (s *Service) KvCommit(ctx context.Context, req *pb.KvCommitRequest) (*pb.KvCommitResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetRequest() == nil {
		return nil, status.Error(codes.InvalidArgument, "commit request missing payload")
	}
	resp, err := s.propose(&pb.RaftCmdRequest{
		Header: header,
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_COMMIT,
			Cmd:     &pb.Request_Commit{Commit: req.GetRequest()},
		}},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	out := &pb.KvCommitResponse{RegionError: resp.GetRegionError()}
	if len(resp.GetResponses()) > 0 && resp.GetResponses()[0].GetCommit() != nil {
		out.Response = resp.GetResponses()[0].GetCommit()
	}
	return out, nil
}

func (s *Service) KvBatchRollback(ctx context.Context, req *pb.KvBatchRollbackRequest) (*pb.KvBatchRollbackResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetRequest() == nil {
		return nil, status.Error(codes.InvalidArgument, "rollback request missing payload")
	}
	resp, err := s.propose(&pb.RaftCmdRequest{
		Header: header,
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_BATCH_ROLLBACK,
			Cmd:     &pb.Request_BatchRollback{BatchRollback: req.GetRequest()},
		}},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	out := &pb.KvBatchRollbackResponse{RegionError: resp.GetRegionError()}
	if len(resp.GetResponses()) > 0 && resp.GetResponses()[0].GetBatchRollback() != nil {
		out.Response = resp.GetResponses()[0].GetBatchRollback()
	}
	return out, nil
}

func (s *Service) KvResolveLock(ctx context.Context, req *pb.KvResolveLockRequest) (*pb.KvResolveLockResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetRequest() == nil {
		return nil, status.Error(codes.InvalidArgument, "resolve lock request missing payload")
	}
	resp, err := s.propose(&pb.RaftCmdRequest{
		Header: header,
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_RESOLVE_LOCK,
			Cmd:     &pb.Request_ResolveLock{ResolveLock: req.GetRequest()},
		}},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	out := &pb.KvResolveLockResponse{RegionError: resp.GetRegionError()}
	if len(resp.GetResponses()) > 0 && resp.GetResponses()[0].GetResolveLock() != nil {
		out.Response = resp.GetResponses()[0].GetResolveLock()
	}
	return out, nil
}

func (s *Service) KvCheckTxnStatus(ctx context.Context, req *pb.KvCheckTxnStatusRequest) (*pb.KvCheckTxnStatusResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetRequest() == nil {
		return nil, status.Error(codes.InvalidArgument, "check txn status request missing payload")
	}
	resp, err := s.propose(&pb.RaftCmdRequest{
		Header: header,
		Requests: []*pb.Request{{
			CmdType: pb.CmdType_CMD_CHECK_TXN_STATUS,
			Cmd:     &pb.Request_CheckTxnStatus{CheckTxnStatus: req.GetRequest()},
		}},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	out := &pb.KvCheckTxnStatusResponse{RegionError: resp.GetRegionError()}
	if len(resp.GetResponses()) > 0 && resp.GetResponses()[0].GetCheckTxnStatus() != nil {
		out.Response = resp.GetResponses()[0].GetCheckTxnStatus()
	}
	return out, nil
}

func (s *Service) read(req *pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
	if s.store == nil {
		return nil, fmt.Errorf("raftstore: store not initialized")
	}
	return s.store.ReadCommand(req)
}

func (s *Service) propose(req *pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
	if s.store == nil {
		return nil, fmt.Errorf("raftstore: store not initialized")
	}
	return s.store.ProposeCommand(req)
}

func buildHeader(ctx *pb.Context) (*pb.CmdHeader, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	if ctx.GetRegionId() == 0 {
		return nil, fmt.Errorf("region id is required")
	}
	header := &pb.CmdHeader{RegionId: ctx.GetRegionId(), RegionEpoch: ctx.GetRegionEpoch()}
	if peer := ctx.GetPeer(); peer != nil {
		header.PeerId = peer.GetPeerId()
	}
	return header, nil
}
