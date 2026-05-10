package kv

import (
	"context"
	"fmt"
	"time"

	errorpb "github.com/feichai0017/NoKV/pb/error"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"

	"github.com/feichai0017/NoKV/raftstore/store"
)

// Service exposes StoreKV gRPC handlers backed by a raftstore Store.
type Service struct {
	kvrpcpb.UnimplementedStoreKVServer
	store        *store.Store
	writeBatcher *writeCommandBatcher
}

// NewService constructs a StoreKV service bound to the provided store.
func NewService(st *store.Store) *Service {
	s := &Service{store: st}
	s.writeBatcher = newWriteCommandBatcher(s.propose, defaultWriteCommandBatchMaxSize, defaultWriteCommandBatchMaxWait)
	return s
}

// Stats exposes low-cardinality StoreKV service counters for expvar and
// benchmark artifacts.
func (s *Service) Stats() map[string]any {
	if s == nil || s.writeBatcher == nil {
		var empty *writeCommandBatcher
		return empty.Stats()
	}
	return s.writeBatcher.Stats()
}

func servicePhysicalTimeMillis() uint64 {
	return uint64(time.Now().UnixMilli())
}

func checkTxnStatusRequestWithServiceTime(req *kvrpcpb.CheckTxnStatusRequest) *kvrpcpb.CheckTxnStatusRequest {
	currentTime := req.GetCurrentTime()
	if currentTime == 0 {
		currentTime = servicePhysicalTimeMillis()
	}
	return &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         append([]byte(nil), req.GetPrimaryKey()...),
		LockTs:             req.GetLockTs(),
		CurrentTs:          req.GetCurrentTs(),
		RollbackIfNotExist: req.GetRollbackIfNotExist(),
		CallerStartTs:      req.GetCallerStartTs(),
		CurrentTime:        currentTime,
	}
}

func txnHeartBeatRequestWithServiceTime(req *kvrpcpb.TxnHeartBeatRequest) *kvrpcpb.TxnHeartBeatRequest {
	currentTime := req.GetCurrentTime()
	if currentTime == 0 {
		currentTime = servicePhysicalTimeMillis()
	}
	return &kvrpcpb.TxnHeartBeatRequest{
		PrimaryKey:   append([]byte(nil), req.GetPrimaryKey()...),
		StartVersion: req.GetStartVersion(),
		TtlExtension: req.GetTtlExtension(),
		CurrentTime:  currentTime,
	}
}

func (s *Service) Get(ctx context.Context, req *kvrpcpb.KvGetRequest) (*kvrpcpb.KvGetResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	readReq := req.GetRequest()
	if readReq == nil {
		return nil, rpcInvalidArgument("get request missing payload")
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
	if resp.GetRegionError() != nil {
		return resp, nil
	}
	first, err := singleRaftResponse("get", result)
	if err != nil {
		return nil, err
	}
	if first.GetGet() == nil {
		return nil, raftPayloadError("get", "missing get payload")
	}
	resp.Response = first.GetGet()
	return resp, nil
}

func (s *Service) BatchGet(ctx context.Context, req *kvrpcpb.KvBatchGetRequest) (*kvrpcpb.KvBatchGetResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	batch := req.GetRequest()
	if batch == nil {
		return nil, rpcInvalidArgument("batch get request missing payload")
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
		if len(result.GetResponses()) != len(requests) {
			return nil, raftPayloadError("batch get", fmt.Sprintf("expected %d raft responses, got %d", len(requests), len(result.GetResponses())))
		}
		responses := make([]*kvrpcpb.GetResponse, 0, len(requests))
		for i, r := range result.GetResponses() {
			if r == nil || r.GetGet() == nil {
				return nil, raftPayloadError("batch get", fmt.Sprintf("response %d missing get payload", i))
			}
			responses = append(responses, r.GetGet())
		}
		resp.Response = &kvrpcpb.BatchGetResponse{Responses: responses}
	}
	return resp, nil
}

func (s *Service) Scan(ctx context.Context, req *kvrpcpb.KvScanRequest) (*kvrpcpb.KvScanResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	scanReq := req.GetRequest()
	if scanReq == nil {
		return nil, rpcInvalidArgument("scan request missing payload")
	}
	if scanReq.GetReverse() {
		return nil, rpcUnsupported("StoreKV Scan reverse scans are not supported yet")
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
	if resp.GetRegionError() != nil {
		return resp, nil
	}
	first, err := singleRaftResponse("scan", result)
	if err != nil {
		return nil, err
	}
	if first.GetScan() == nil {
		return nil, raftPayloadError("scan", "missing scan payload")
	}
	resp.Response = first.GetScan()
	return resp, nil
}

func (s *Service) Prewrite(ctx context.Context, req *kvrpcpb.KvPrewriteRequest) (*kvrpcpb.KvPrewriteResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if req.GetRequest() == nil {
		return nil, rpcInvalidArgument("prewrite request missing payload")
	}
	first, regionErr, err := s.submitWriteCommand(ctx, header, &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
		Cmd:     &raftcmdpb.Request_Prewrite{Prewrite: req.GetRequest()},
	})
	if err != nil {
		return nil, err
	}
	out := &kvrpcpb.KvPrewriteResponse{RegionError: regionErr}
	if out.GetRegionError() != nil {
		return out, nil
	}
	if first.GetPrewrite() == nil {
		return nil, raftPayloadError("prewrite", "missing prewrite payload")
	}
	out.Response = first.GetPrewrite()
	return out, nil
}

func (s *Service) Commit(ctx context.Context, req *kvrpcpb.KvCommitRequest) (*kvrpcpb.KvCommitResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if req.GetRequest() == nil {
		return nil, rpcInvalidArgument("commit request missing payload")
	}
	first, regionErr, err := s.submitWriteCommand(ctx, header, &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_COMMIT,
		Cmd:     &raftcmdpb.Request_Commit{Commit: req.GetRequest()},
	})
	if err != nil {
		return nil, err
	}
	out := &kvrpcpb.KvCommitResponse{RegionError: regionErr}
	if out.GetRegionError() != nil {
		return out, nil
	}
	if first.GetCommit() == nil {
		return nil, raftPayloadError("commit", "missing commit payload")
	}
	out.Response = first.GetCommit()
	return out, nil
}

func (s *Service) BatchRollback(ctx context.Context, req *kvrpcpb.KvBatchRollbackRequest) (*kvrpcpb.KvBatchRollbackResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if req.GetRequest() == nil {
		return nil, rpcInvalidArgument("rollback request missing payload")
	}
	first, regionErr, err := s.submitWriteCommand(ctx, header, &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_BATCH_ROLLBACK,
		Cmd:     &raftcmdpb.Request_BatchRollback{BatchRollback: req.GetRequest()},
	})
	if err != nil {
		return nil, err
	}
	out := &kvrpcpb.KvBatchRollbackResponse{RegionError: regionErr}
	if out.GetRegionError() != nil {
		return out, nil
	}
	if first.GetBatchRollback() == nil {
		return nil, raftPayloadError("batch rollback", "missing batch rollback payload")
	}
	out.Response = first.GetBatchRollback()
	return out, nil
}

func (s *Service) ResolveLock(ctx context.Context, req *kvrpcpb.KvResolveLockRequest) (*kvrpcpb.KvResolveLockResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if req.GetRequest() == nil {
		return nil, rpcInvalidArgument("resolve lock request missing payload")
	}
	first, regionErr, err := s.submitWriteCommand(ctx, header, &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_RESOLVE_LOCK,
		Cmd:     &raftcmdpb.Request_ResolveLock{ResolveLock: req.GetRequest()},
	})
	if err != nil {
		return nil, err
	}
	out := &kvrpcpb.KvResolveLockResponse{RegionError: regionErr}
	if out.GetRegionError() != nil {
		return out, nil
	}
	if first.GetResolveLock() == nil {
		return nil, raftPayloadError("resolve lock", "missing resolve lock payload")
	}
	out.Response = first.GetResolveLock()
	return out, nil
}

func (s *Service) CheckTxnStatus(ctx context.Context, req *kvrpcpb.KvCheckTxnStatusRequest) (*kvrpcpb.KvCheckTxnStatusResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if req.GetRequest() == nil {
		return nil, rpcInvalidArgument("check txn status request missing payload")
	}
	checkReq := checkTxnStatusRequestWithServiceTime(req.GetRequest())
	resp, err := s.propose(ctx, &raftcmdpb.RaftCmdRequest{
		Header: header,
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS,
			Cmd:     &raftcmdpb.Request_CheckTxnStatus{CheckTxnStatus: checkReq},
		}},
	})
	if err != nil {
		return nil, rpcStatus(err)
	}
	out := &kvrpcpb.KvCheckTxnStatusResponse{RegionError: resp.GetRegionError()}
	if out.GetRegionError() != nil {
		return out, nil
	}
	first, err := singleRaftResponse("check txn status", resp)
	if err != nil {
		return nil, err
	}
	if first.GetCheckTxnStatus() == nil {
		return nil, raftPayloadError("check txn status", "missing check txn status payload")
	}
	out.Response = first.GetCheckTxnStatus()
	return out, nil
}

func (s *Service) TxnHeartBeat(ctx context.Context, req *kvrpcpb.KvTxnHeartBeatRequest) (*kvrpcpb.KvTxnHeartBeatResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if req.GetRequest() == nil {
		return nil, rpcInvalidArgument("txn heartbeat request missing payload")
	}
	heartBeatReq := txnHeartBeatRequestWithServiceTime(req.GetRequest())
	resp, err := s.propose(ctx, &raftcmdpb.RaftCmdRequest{
		Header: header,
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_TXN_HEART_BEAT,
			Cmd:     &raftcmdpb.Request_TxnHeartBeat{TxnHeartBeat: heartBeatReq},
		}},
	})
	if err != nil {
		return nil, rpcStatus(err)
	}
	out := &kvrpcpb.KvTxnHeartBeatResponse{RegionError: resp.GetRegionError()}
	if out.GetRegionError() != nil {
		return out, nil
	}
	first, err := singleRaftResponse("txn heartbeat", resp)
	if err != nil {
		return nil, err
	}
	if first.GetTxnHeartBeat() == nil {
		return nil, raftPayloadError("txn heartbeat", "missing txn heartbeat payload")
	}
	out.Response = first.GetTxnHeartBeat()
	return out, nil
}

func (s *Service) TryAtomicMutate(ctx context.Context, req *kvrpcpb.KvTryAtomicMutateRequest) (*kvrpcpb.KvTryAtomicMutateResponse, error) {
	header, err := buildHeader(req.GetContext())
	if err != nil {
		return nil, rpcInvalidArgument(err.Error())
	}
	if req.GetRequest() == nil {
		return nil, rpcInvalidArgument("atomic mutate request missing payload")
	}
	first, regionErr, err := s.submitWriteCommand(ctx, header, &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
		Cmd:     &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: req.GetRequest()},
	})
	if err != nil {
		return nil, err
	}
	out := &kvrpcpb.KvTryAtomicMutateResponse{RegionError: regionErr}
	if out.GetRegionError() != nil {
		return out, nil
	}
	if first.GetTryAtomicMutate() == nil {
		return nil, raftPayloadError("atomic mutate", "missing atomic mutate payload")
	}
	out.Response = first.GetTryAtomicMutate()
	return out, nil
}

func (s *Service) submitWriteCommand(ctx context.Context, header *raftcmdpb.CmdHeader, request *raftcmdpb.Request) (*raftcmdpb.Response, *errorpb.RegionError, error) {
	if s.writeBatcher != nil {
		return s.writeBatcher.submit(ctx, header, request)
	}
	resp, err := s.propose(ctx, &raftcmdpb.RaftCmdRequest{
		Header:   header,
		Requests: []*raftcmdpb.Request{request},
	})
	if err != nil {
		return nil, nil, rpcStatus(err)
	}
	if regionErr := resp.GetRegionError(); regionErr != nil {
		return nil, regionErr, nil
	}
	first, err := singleRaftResponse(writeCommandName(request.GetCmdType()), resp)
	if err != nil {
		return nil, nil, err
	}
	return first, nil, nil
}

func (s *Service) read(ctx context.Context, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
	if s.store == nil {
		return nil, errStoreNotInitialized
	}
	return s.store.ReadCommand(ctx, req)
}

func (s *Service) propose(ctx context.Context, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
	if s.store == nil {
		return nil, errStoreNotInitialized
	}
	return s.store.ProposeCommand(ctx, req)
}

func singleRaftResponse(op string, resp *raftcmdpb.RaftCmdResponse) (*raftcmdpb.Response, error) {
	if resp == nil {
		return nil, raftPayloadError(op, "missing raft response")
	}
	if resp.GetRegionError() != nil {
		return nil, nil
	}
	if len(resp.GetResponses()) != 1 || resp.GetResponses()[0] == nil {
		return nil, raftPayloadError(op, fmt.Sprintf("expected one raft response, got %d", len(resp.GetResponses())))
	}
	return resp.GetResponses()[0], nil
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
	header.ReadConsistency = ctx.GetReadConsistency()
	header.ReadPreference = ctx.GetReadPreference()
	header.MaxStaleReadIndex = ctx.GetMaxStaleReadIndex()
	header.MaxStaleReadMs = ctx.GetMaxStaleReadMs()
	return header, nil
}
