package kv

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/feichai0017/NoKV/fsmeta/runtime/perasauthority"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"github.com/feichai0017/NoKV/txn/latch"
	"github.com/feichai0017/NoKV/txn/mvcc"
	"github.com/feichai0017/NoKV/txn/percolator"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
)

const defaultLatchSlots = 512

type ApplyOption func(*applyConfig)

type applyConfig struct {
	perasAuthorityTable *perasauthority.ActiveAuthorities
	now                 func() time.Time
}

func WithPerasAuthorityFence(authorityTable *perasauthority.ActiveAuthorities) ApplyOption {
	return func(cfg *applyConfig) {
		cfg.perasAuthorityTable = authorityTable
	}
}

func newApplyConfig(opts []ApplyOption) applyConfig {
	cfg := applyConfig{now: time.Now}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// Apply executes a RaftCmdRequest against the provided DB. The returned
// response mirrors the request ordering. Mutation commands are expected to
// arrive through the replicated raft log.
func Apply(db txnstore.Store, latches *latch.Manager, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("kv: nil raft command")
	}
	if latches == nil {
		latches = latch.NewManager(defaultLatchSlots)
	}
	resp := &raftcmdpb.RaftCmdResponse{Header: req.Header}
	for i := 0; i < len(req.Requests); i++ {
		r := req.Requests[i]
		if r == nil {
			continue
		}
		switch r.GetCmdType() {
		case raftcmdpb.CmdType_CMD_GET:
			result, keyErr, err := handleGet(db, r.GetGet())
			if err != nil {
				return nil, err
			}
			if keyErr != nil {
				result.Error = keyErr
			}
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Get{Get: result}})
		case raftcmdpb.CmdType_CMD_PREWRITE:
			end := collectCommandRun(req.Requests, i, raftcmdpb.CmdType_CMD_PREWRITE)
			batch := []*kvrpcpb.PrewriteRequest{r.GetPrewrite()}
			for j := i + 1; j < end; j++ {
				batch = append(batch, req.Requests[j].GetPrewrite())
			}
			for _, errs := range percolator.PrewriteBatch(db, latches, batch) {
				result := &kvrpcpb.PrewriteResponse{Errors: errs}
				resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Prewrite{Prewrite: result}})
			}
			i = end - 1
		case raftcmdpb.CmdType_CMD_COMMIT:
			end := collectCommandRun(req.Requests, i, raftcmdpb.CmdType_CMD_COMMIT)
			batch := []*kvrpcpb.CommitRequest{r.GetCommit()}
			for j := i + 1; j < end; j++ {
				batch = append(batch, req.Requests[j].GetCommit())
			}
			for _, err := range percolator.CommitBatch(db, latches, batch) {
				resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Commit{Commit: &kvrpcpb.CommitResponse{Error: err}}})
			}
			i = end - 1
		case raftcmdpb.CmdType_CMD_BATCH_ROLLBACK:
			end := collectCommandRun(req.Requests, i, raftcmdpb.CmdType_CMD_BATCH_ROLLBACK)
			batch := []*kvrpcpb.BatchRollbackRequest{r.GetBatchRollback()}
			for j := i + 1; j < end; j++ {
				batch = append(batch, req.Requests[j].GetBatchRollback())
			}
			for _, err := range percolator.BatchRollbackBatch(db, latches, batch) {
				resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_BatchRollback{BatchRollback: &kvrpcpb.BatchRollbackResponse{Error: err}}})
			}
			i = end - 1
		case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
			end := collectCommandRun(req.Requests, i, raftcmdpb.CmdType_CMD_RESOLVE_LOCK)
			batch := []*kvrpcpb.ResolveLockRequest{r.GetResolveLock()}
			for j := i + 1; j < end; j++ {
				batch = append(batch, req.Requests[j].GetResolveLock())
			}
			for _, result := range percolator.ResolveLockBatch(db, latches, batch) {
				resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockResponse{
					ResolvedLocks: result.ResolvedLocks,
					Error:         result.Error,
				}}})
			}
			i = end - 1
		case raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS:
			result := percolator.CheckTxnStatus(db, latches, r.GetCheckTxnStatus())
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_CheckTxnStatus{CheckTxnStatus: result}})
		case raftcmdpb.CmdType_CMD_TXN_HEART_BEAT:
			result := percolator.TxnHeartBeat(db, latches, r.GetTxnHeartBeat())
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_TxnHeartBeat{TxnHeartBeat: result}})
		case raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE:
			end := collectCommandRun(req.Requests, i, raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE)
			batch := []*kvrpcpb.TryAtomicMutateRequest{r.GetTryAtomicMutate()}
			for j := i + 1; j < end; j++ {
				batch = append(batch, req.Requests[j].GetTryAtomicMutate())
			}
			for _, result := range percolator.ApplyAtomicMutateBatch(db, latches, batch) {
				resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateResponse{
					Error:                    result.Error,
					AppliedKeys:              result.AppliedKeys,
					FallbackToTwoPhaseCommit: result.Fallback,
				}}})
			}
			i = end - 1
		case raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE:
			result, err := applyMVCCMaintenance(db, r.GetMvccMaintenance())
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_MvccMaintenance{MvccMaintenance: result}})
		case raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT:
			result, err := applyPerasInstallSegment(db, r.GetPerasInstallSegment())
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_PerasInstallSegment{PerasInstallSegment: result}})
		case raftcmdpb.CmdType_CMD_SCAN:
			result, err := handleScan(db, r.GetScan())
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Scan{Scan: result}})
		default:
			return nil, fmt.Errorf("kv: unsupported command %v", r.GetCmdType())
		}
	}
	return resp, nil
}

// ApplyBatch executes a committed apply batch made of independent raft command
// requests. It preserves one response per input request while fusing the MVCC
// command bodies across entries when they share a batchable command type.
func ApplyBatch(db txnstore.Store, latches *latch.Manager, reqs []*raftcmdpb.RaftCmdRequest) ([]*raftcmdpb.RaftCmdResponse, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	if latches == nil {
		latches = latch.NewManager(defaultLatchSlots)
	}
	resps := make([]*raftcmdpb.RaftCmdResponse, len(reqs))
	for i := 0; i < len(reqs); {
		cmdType, ok := singleBatchableCommand(reqs[i])
		if !ok {
			resp, err := Apply(db, latches, reqs[i])
			if err != nil {
				return nil, err
			}
			resps[i] = resp
			i++
			continue
		}
		end := i + 1
		for end < len(reqs) {
			nextType, nextOK := singleBatchableCommand(reqs[end])
			if !nextOK || nextType != cmdType {
				break
			}
			end++
		}
		if err := applyBatchRun(db, latches, reqs[i:end], resps[i:end], cmdType); err != nil {
			return nil, err
		}
		i = end
	}
	return resps, nil
}

func singleBatchableCommand(req *raftcmdpb.RaftCmdRequest) (raftcmdpb.CmdType, bool) {
	if req == nil || len(req.GetRequests()) != 1 {
		return 0, false
	}
	r := req.GetRequests()[0]
	if r == nil {
		return 0, false
	}
	switch r.GetCmdType() {
	case raftcmdpb.CmdType_CMD_PREWRITE,
		raftcmdpb.CmdType_CMD_COMMIT,
		raftcmdpb.CmdType_CMD_BATCH_ROLLBACK,
		raftcmdpb.CmdType_CMD_RESOLVE_LOCK,
		raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
		raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT:
		return r.GetCmdType(), true
	default:
		return 0, false
	}
}

func applyBatchRun(
	db txnstore.Store,
	latches *latch.Manager,
	reqs []*raftcmdpb.RaftCmdRequest,
	resps []*raftcmdpb.RaftCmdResponse,
	cmdType raftcmdpb.CmdType,
) error {
	switch cmdType {
	case raftcmdpb.CmdType_CMD_PREWRITE:
		var batchBuf [64]*kvrpcpb.PrewriteRequest
		batch := batchBuf[:0]
		for _, req := range reqs {
			batch = append(batch, req.GetRequests()[0].GetPrewrite())
		}
		results := percolator.PrewriteBatch(db, latches, batch)
		for i, result := range results {
			resps[i] = &raftcmdpb.RaftCmdResponse{
				Header: reqs[i].GetHeader(),
				Responses: []*raftcmdpb.Response{{
					Cmd: &raftcmdpb.Response_Prewrite{Prewrite: &kvrpcpb.PrewriteResponse{Errors: result}},
				}},
			}
		}
	case raftcmdpb.CmdType_CMD_COMMIT:
		var batchBuf [64]*kvrpcpb.CommitRequest
		batch := batchBuf[:0]
		for _, req := range reqs {
			batch = append(batch, req.GetRequests()[0].GetCommit())
		}
		results := percolator.CommitBatch(db, latches, batch)
		for i, result := range results {
			resps[i] = &raftcmdpb.RaftCmdResponse{
				Header: reqs[i].GetHeader(),
				Responses: []*raftcmdpb.Response{{
					Cmd: &raftcmdpb.Response_Commit{Commit: &kvrpcpb.CommitResponse{Error: result}},
				}},
			}
		}
	case raftcmdpb.CmdType_CMD_BATCH_ROLLBACK:
		var batchBuf [64]*kvrpcpb.BatchRollbackRequest
		batch := batchBuf[:0]
		for _, req := range reqs {
			batch = append(batch, req.GetRequests()[0].GetBatchRollback())
		}
		results := percolator.BatchRollbackBatch(db, latches, batch)
		for i, result := range results {
			resps[i] = &raftcmdpb.RaftCmdResponse{
				Header: reqs[i].GetHeader(),
				Responses: []*raftcmdpb.Response{{
					Cmd: &raftcmdpb.Response_BatchRollback{BatchRollback: &kvrpcpb.BatchRollbackResponse{Error: result}},
				}},
			}
		}
	case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
		var batchBuf [64]*kvrpcpb.ResolveLockRequest
		batch := batchBuf[:0]
		for _, req := range reqs {
			batch = append(batch, req.GetRequests()[0].GetResolveLock())
		}
		results := percolator.ResolveLockBatch(db, latches, batch)
		for i, result := range results {
			resps[i] = &raftcmdpb.RaftCmdResponse{
				Header: reqs[i].GetHeader(),
				Responses: []*raftcmdpb.Response{{
					Cmd: &raftcmdpb.Response_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockResponse{
						ResolvedLocks: result.ResolvedLocks,
						Error:         result.Error,
					}},
				}},
			}
		}
	case raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE:
		var batchBuf [64]*kvrpcpb.TryAtomicMutateRequest
		batch := batchBuf[:0]
		for _, req := range reqs {
			batch = append(batch, req.GetRequests()[0].GetTryAtomicMutate())
		}
		results := percolator.ApplyAtomicMutateBatch(db, latches, batch)
		for i, result := range results {
			resps[i] = &raftcmdpb.RaftCmdResponse{
				Header: reqs[i].GetHeader(),
				Responses: []*raftcmdpb.Response{{
					Cmd: &raftcmdpb.Response_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateResponse{
						Error:                    result.Error,
						AppliedKeys:              result.AppliedKeys,
						FallbackToTwoPhaseCommit: result.Fallback,
					}},
				}},
			}
		}
	case raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT:
		for i, req := range reqs {
			result, err := applyPerasInstallSegment(db, req.GetRequests()[0].GetPerasInstallSegment())
			if err != nil {
				return err
			}
			resps[i] = &raftcmdpb.RaftCmdResponse{
				Header: req.GetHeader(),
				Responses: []*raftcmdpb.Response{{
					Cmd: &raftcmdpb.Response_PerasInstallSegment{PerasInstallSegment: result},
				}},
			}
		}
	default:
		return fmt.Errorf("kv: unsupported batch command %v", cmdType)
	}
	return nil
}

func collectCommandRun(reqs []*raftcmdpb.Request, start int, cmdType raftcmdpb.CmdType) int {
	end := start + 1
	for end < len(reqs) {
		next := reqs[end]
		if next == nil || next.GetCmdType() != cmdType {
			break
		}
		end++
	}
	return end
}

func applyMVCCMaintenance(db txnstore.Store, req *kvrpcpb.MVCCMaintenanceRequest) (*kvrpcpb.MVCCMaintenanceResponse, error) {
	entries, keyErr := buildMVCCMaintenanceEntries(req)
	if keyErr != nil {
		return &kvrpcpb.MVCCMaintenanceResponse{Error: keyErr}, nil
	}
	if len(entries) == 0 {
		return &kvrpcpb.MVCCMaintenanceResponse{}, nil
	}
	defer func() {
		for _, entry := range entries {
			if entry != nil {
				entry.DecrRef()
			}
		}
	}()
	// ApplyInternalEntries is the raft apply batch boundary for MVCC
	// maintenance. NoKV's DB implementation maps it to one atomic LSM batch;
	// if another Store implementation reports an error after partial
	// persistence, the caller retries the whole tombstone batch and relies on
	// tombstones being idempotent.
	if err := db.ApplyInternalEntries(entries); err != nil {
		return nil, err
	}
	return &kvrpcpb.MVCCMaintenanceResponse{AppliedEntries: uint64(len(entries))}, nil
}

func applyPerasInstallSegment(db txnstore.Store, req *kvrpcpb.PerasInstallSegmentRequest) (*kvrpcpb.PerasInstallSegmentResponse, error) {
	segment, digest, keyErr := decodePerasInstallSegmentRequest(req)
	if keyErr != nil {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: keyErr}, nil
	}
	materialize := req.GetMaterializeMvcc()
	if !materialize {
		if ok, err := LoadPerasSegmentCatalogInstallForObjectKey(db, segment, req.GetRoutingKey()); err != nil {
			return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}, nil
		} else if ok {
			stats := segment.Stats()
			return &kvrpcpb.PerasInstallSegmentResponse{
				SegmentRoot:    append([]byte(nil), segment.Root[:]...),
				OperationCount: stats.OperationCount,
				EntryCount:     stats.EntryCount,
				AppliedEntries: 1,
			}, nil
		}
	}
	var entries []*kv.Entry
	var err error
	if materialize {
		entries, err = buildMVCCSegmentInstallEntriesWithVerifiedPayload(segment, req.GetInstallVersion(), req.GetSegmentPayload(), digest)
	} else {
		entries, err = buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayloadForObjectKey(segment, req.GetInstallVersion(), req.GetSegmentPayload(), digest, req.GetRoutingKey())
	}
	if err != nil {
		return &kvrpcpb.PerasInstallSegmentResponse{Error: perasInstallAbort(err.Error())}, nil
	}
	defer releaseEntries(entries)
	if len(entries) > 0 {
		if err := db.ApplyInternalEntries(entries); err != nil {
			return nil, err
		}
	}
	stats := segment.Stats()
	return &kvrpcpb.PerasInstallSegmentResponse{
		SegmentRoot:    append([]byte(nil), segment.Root[:]...),
		OperationCount: stats.OperationCount,
		EntryCount:     stats.EntryCount,
		AppliedEntries: uint64(len(entries)),
	}, nil
}

func decodePerasInstallSegmentRequest(req *kvrpcpb.PerasInstallSegmentRequest) (fsperas.PerasSegment, [32]byte, *kvrpcpb.KeyError) {
	if req == nil {
		return fsperas.PerasSegment{}, [32]byte{}, perasInstallAbort("missing request")
	}
	if len(req.GetRoutingKey()) == 0 || len(req.GetSegmentPayload()) == 0 || req.GetInstallVersion() == 0 {
		return fsperas.PerasSegment{}, [32]byte{}, perasInstallAbort("missing routing key, segment payload, or install version")
	}
	var root [32]byte
	if len(req.GetSegmentRoot()) != len(root) {
		return fsperas.PerasSegment{}, [32]byte{}, perasInstallAbort("invalid segment root")
	}
	copy(root[:], req.GetSegmentRoot())
	var digest [32]byte
	if len(req.GetSegmentPayloadDigest()) != len(digest) {
		return fsperas.PerasSegment{}, [32]byte{}, perasInstallAbort("invalid segment payload digest")
	}
	copy(digest[:], req.GetSegmentPayloadDigest())
	segment, err := fsperas.VerifyPerasSegmentPayload(req.GetSegmentPayload(), root, digest)
	if err != nil {
		return fsperas.PerasSegment{}, [32]byte{}, perasInstallAbort(err.Error())
	}
	return segment, digest, nil
}

func buildMVCCMaintenanceEntries(req *kvrpcpb.MVCCMaintenanceRequest) ([]*kv.Entry, *kvrpcpb.KeyError) {
	if req == nil || len(req.GetTombstones()) == 0 {
		return nil, nil
	}
	entries := make([]*kv.Entry, 0, len(req.GetTombstones()))
	for i, tombstone := range req.GetTombstones() {
		if tombstone == nil {
			continue
		}
		cf, ok := maintenanceColumnFamily(tombstone.GetColumnFamily())
		if !ok {
			releaseEntries(entries)
			return nil, maintenanceAbort("invalid column family")
		}
		if len(tombstone.GetKey()) == 0 {
			releaseEntries(entries)
			return nil, maintenanceAbort("empty key")
		}
		entry := kv.NewInternalEntry(cf, tombstone.GetKey(), tombstone.GetVersion(), nil, kv.BitDelete, 0)
		if entry == nil {
			releaseEntries(entries)
			return nil, maintenanceAbort(fmt.Sprintf("entry %d build failed", i))
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func releaseEntries(entries []*kv.Entry) {
	for _, entry := range entries {
		if entry != nil {
			entry.DecrRef()
		}
	}
}

func maintenanceColumnFamily(cf kvrpcpb.InternalEntryTombstone_ColumnFamily) (kv.ColumnFamily, bool) {
	switch cf {
	case kvrpcpb.InternalEntryTombstone_DEFAULT:
		return kv.CFDefault, true
	case kvrpcpb.InternalEntryTombstone_WRITE:
		return kv.CFWrite, true
	default:
		return 0, false
	}
}

func maintenanceAbort(msg string) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{Abort: msg}
}

func perasInstallAbort(msg string) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{Abort: msg}
}

// NewApplier wraps Apply into a reusable function suitable for store command
// execution wiring.
func NewApplier(db txnstore.Store, latches *latch.Manager, opts ...ApplyOption) func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
	if latches == nil {
		latches = latch.NewManager(defaultLatchSlots)
	}
	cfg := newApplyConfig(opts)
	return func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		if resp, fenced := rejectPerasFencedRequest(cfg, req); fenced {
			return resp, nil
		}
		return Apply(db, latches, req)
	}
}

// NewBatchApplier wraps ApplyBatch for store command execution wiring.
func NewBatchApplier(db txnstore.Store, latches *latch.Manager, opts ...ApplyOption) func([]*raftcmdpb.RaftCmdRequest) ([]*raftcmdpb.RaftCmdResponse, error) {
	if latches == nil {
		latches = latch.NewManager(defaultLatchSlots)
	}
	cfg := newApplyConfig(opts)
	return func(reqs []*raftcmdpb.RaftCmdRequest) ([]*raftcmdpb.RaftCmdResponse, error) {
		return applyBatchWithFence(db, latches, cfg, reqs)
	}
}

func applyBatchWithFence(
	db txnstore.Store,
	latches *latch.Manager,
	cfg applyConfig,
	reqs []*raftcmdpb.RaftCmdRequest,
) ([]*raftcmdpb.RaftCmdResponse, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	if cfg.perasAuthorityTable == nil {
		return ApplyBatch(db, latches, reqs)
	}
	resps := make([]*raftcmdpb.RaftCmdResponse, len(reqs))
	for i := 0; i < len(reqs); {
		if resp, fenced := rejectPerasFencedRequest(cfg, reqs[i]); fenced {
			resps[i] = resp
			i++
			continue
		}
		end := i + 1
		for end < len(reqs) {
			if _, fenced := rejectPerasFencedRequest(cfg, reqs[end]); fenced {
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

func rejectPerasFencedRequest(cfg applyConfig, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, bool) {
	if cfg.perasAuthorityTable == nil || req == nil {
		return nil, false
	}
	var keyErr *kvrpcpb.KeyError
	for _, r := range req.GetRequests() {
		if err := perasFenceErrorForCommand(cfg, r); err != nil {
			keyErr = err
			break
		}
	}
	if keyErr == nil {
		return nil, false
	}
	resp := &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}
	for _, r := range req.GetRequests() {
		resp.Responses = append(resp.Responses, perasFenceResponseForCommand(r, keyErr))
	}
	return resp, true
}

func perasFenceErrorForCommand(cfg applyConfig, r *raftcmdpb.Request) *kvrpcpb.KeyError {
	if r == nil {
		return nil
	}
	check := func(key []byte) *kvrpcpb.KeyError {
		return perasFenceErrorForKey(cfg, key)
	}
	switch r.GetCmdType() {
	case raftcmdpb.CmdType_CMD_GET,
		raftcmdpb.CmdType_CMD_SCAN,
		raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT:
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
		return firstPerasFenceError(cfg, r.GetCommit().GetKeys())
	case raftcmdpb.CmdType_CMD_BATCH_ROLLBACK:
		return firstPerasFenceError(cfg, r.GetBatchRollback().GetKeys())
	case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
		return firstPerasFenceError(cfg, r.GetResolveLock().GetKeys())
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

func firstPerasFenceError(cfg applyConfig, keys [][]byte) *kvrpcpb.KeyError {
	for _, key := range keys {
		if err := perasFenceErrorForKey(cfg, key); err != nil {
			return err
		}
	}
	return nil
}

func perasFenceErrorForKey(cfg applyConfig, key []byte) *kvrpcpb.KeyError {
	if len(key) == 0 || cfg.perasAuthorityTable == nil {
		return nil
	}
	now := time.Now
	if cfg.now != nil {
		now = cfg.now
	}
	grant, ok, err := cfg.perasAuthorityTable.FencesKey(key, now())
	if err != nil {
		return &kvrpcpb.KeyError{Retryable: "peras authority fence: " + err.Error()}
	}
	if !ok {
		return nil
	}
	if grant.GrantID == "" {
		return &kvrpcpb.KeyError{Retryable: "peras authority fence"}
	}
	return &kvrpcpb.KeyError{Retryable: "peras authority fence: " + grant.GrantID}
}

func perasFenceResponseForCommand(r *raftcmdpb.Request, keyErr *kvrpcpb.KeyError) *raftcmdpb.Response {
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
	case raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_PerasInstallSegment{PerasInstallSegment: &kvrpcpb.PerasInstallSegmentResponse{Error: keyErr}}}
	default:
		return &raftcmdpb.Response{}
	}
}

func handleGet(db txnstore.Store, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, *kvrpcpb.KeyError, error) {
	if req == nil {
		return &kvrpcpb.GetResponse{NotFound: true}, nil, nil
	}
	reader := percolator.NewReader(db)
	lock, err := reader.GetLock(req.GetKey())
	if err != nil {
		return nil, nil, err
	}
	if lock != nil && req.GetVersion() >= lock.Ts {
		keyErr := &kvrpcpb.KeyError{Locked: &kvrpcpb.Locked{
			PrimaryLock: lock.Primary,
			Key:         kv.SafeCopy(nil, req.GetKey()),
			LockVersion: lock.Ts,
			LockTtl:     lock.TTL,
			LockType:    lock.Kind,
			MinCommitTs: lock.MinCommitTs,
		}}
		return &kvrpcpb.GetResponse{}, keyErr, nil
	}
	val, expiresAt, err := reader.GetValue(req.GetKey(), req.GetVersion())
	if err != nil {
		if errors.Is(err, utils.ErrKeyNotFound) {
			return &kvrpcpb.GetResponse{NotFound: true}, nil, nil
		}
		return nil, nil, err
	}
	return &kvrpcpb.GetResponse{Value: val, ExpiresAt: expiresAt}, nil, nil
}

func handleScan(db txnstore.Store, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	if req == nil {
		return &kvrpcpb.ScanResponse{}, nil
	}
	if req.GetReverse() {
		return nil, fmt.Errorf("kv: reverse scan not supported")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 1
	}
	readTs := req.GetVersion()
	if readTs == 0 {
		readTs = kv.MaxVersion
	}
	iter := db.NewInternalIterator(&index.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()

	startKey := append([]byte(nil), req.GetStartKey()...)
	includeStart := req.GetIncludeStart()
	started := len(startKey) == 0

	resp := &kvrpcpb.ScanResponse{}
	iter.Seek(kv.InternalKey(kv.CFWrite, startKey, kv.MaxVersion))
	reader := percolator.NewReader(db)
	for iter.Valid() && len(resp.Kvs) < limit {
		item := iter.Item()
		if item == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		if entry == nil {
			iter.Next()
			continue
		}
		cf, userKey, _, ok := kv.SplitInternalKey(entry.Key)
		if !ok {
			return nil, fmt.Errorf("kv: scan iterator expects internal key, got %x", entry.Key)
		}
		if cf != kv.CFWrite {
			// Since iterator is seeked into CFWrite range, encountering any non-write CF
			// means there are no more write records for subsequent keys.
			break
		}
		key := kv.SafeCopy(nil, userKey)
		if len(key) == 0 {
			iter.Next()
			continue
		}
		if !started {
			cmp := bytes.Compare(key, startKey)
			if cmp < 0 || (cmp == 0 && !includeStart) {
				advanceToNextUserKey(iter, key)
				continue
			}
			started = true
		}
		lock, err := reader.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && readTs >= lock.Ts {
			resp.Error = lockedError(key, lock)
			advanceToNextUserKey(iter, key)
			break
		}
		value, expiresAt, found, err := collectVisibleValue(db, iter, key, readTs)
		if err != nil {
			return nil, err
		}
		if found {
			resp.Kvs = append(resp.Kvs, &kvrpcpb.KV{
				Key:       key,
				Value:     value,
				Version:   readTs,
				ExpiresAt: expiresAt,
			})
		}
	}
	return resp, nil
}

func advanceToNextUserKey(iter index.Iterator, current []byte) {
	if iter == nil {
		return
	}
	for iter.Next(); iter.Valid(); iter.Next() {
		item := iter.Item()
		if item == nil {
			continue
		}
		entry := item.Entry()
		if entry == nil {
			continue
		}
		_, userKey, _, ok := kv.SplitInternalKey(entry.Key)
		if !ok {
			utils.CondPanicFunc(true, func() error {
				return fmt.Errorf("kv: advanceToNextUserKey expects internal key, got %x", entry.Key)
			})
			return
		}
		if !bytes.Equal(userKey, current) {
			return
		}
	}
}

func collectVisibleValue(db txnstore.Store, iter index.Iterator, key []byte, readTs uint64) ([]byte, uint64, bool, error) {
	for iter.Valid() {
		item := iter.Item()
		if item == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		if entry == nil {
			iter.Next()
			continue
		}
		cf, userKey, ts, ok := kv.SplitInternalKey(entry.Key)
		if !ok {
			return nil, 0, false, fmt.Errorf("kv: collectVisibleValue expects internal key, got %x", entry.Key)
		}
		if cf != kv.CFWrite || !bytes.Equal(userKey, key) {
			return nil, 0, false, nil
		}
		if ts > readTs {
			iter.Next()
			continue
		}
		write, err := mvcc.DecodeWrite(entry.Value)
		if err != nil {
			return nil, 0, false, err
		}
		switch write.Kind {
		case kvrpcpb.Mutation_Lock, kvrpcpb.Mutation_Rollback:
			iter.Next()
			continue
		case kvrpcpb.Mutation_Delete:
			advanceToNextUserKey(iter, key)
			return nil, 0, false, nil
		default:
			var value []byte
			var expiresAt uint64
			if len(write.ShortValue) > 0 {
				value = write.ShortValue
				expiresAt = write.ExpiresAt
				if expiresAt > 0 && expiresAt <= uint64(time.Now().Unix()) {
					advanceToNextUserKey(iter, key)
					return nil, 0, false, nil
				}
			} else {
				entryVal, err := db.GetInternalEntry(kv.CFDefault, key, write.StartTs)
				if err != nil {
					if errors.Is(err, utils.ErrKeyNotFound) {
						iter.Next()
						continue
					}
					return nil, 0, false, err
				}
				if entryVal.IsDeletedOrExpired() {
					entryVal.DecrRef()
					advanceToNextUserKey(iter, key)
					return nil, 0, false, nil
				}
				value = kv.SafeCopy(nil, entryVal.Value)
				expiresAt = entryVal.ExpiresAt
				entryVal.DecrRef()
			}
			advanceToNextUserKey(iter, key)
			return value, expiresAt, true, nil
		}
	}
	return nil, 0, false, nil
}

func lockedError(key []byte, lock *mvcc.Lock) *kvrpcpb.KeyError {
	if lock == nil {
		return nil
	}
	return &kvrpcpb.KeyError{
		Locked: &kvrpcpb.Locked{
			PrimaryLock: lock.Primary,
			Key:         kv.SafeCopy(nil, key),
			LockVersion: lock.Ts,
			LockTtl:     lock.TTL,
			LockType:    lock.Kind,
			MinCommitTs: lock.MinCommitTs,
		},
	}
}
