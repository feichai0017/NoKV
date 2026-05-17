// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package kv

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	errorpb "github.com/feichai0017/NoKV/pb/error"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"github.com/stretchr/testify/require"
)

type writeBatchTestResult struct {
	response    *raftcmdpb.Response
	regionError *errorpb.RegionError
	err         error
}

func TestWriteCommandBatcherCoalescesSameHeaderAndCommand(t *testing.T) {
	var (
		mu       sync.Mutex
		proposed []*raftcmdpb.RaftCmdRequest
	)
	batcher := newWriteCommandBatcher(func(_ context.Context, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		mu.Lock()
		proposed = append(proposed, req)
		mu.Unlock()
		responses := make([]*raftcmdpb.Response, 0, len(req.GetRequests()))
		for i := range req.GetRequests() {
			responses = append(responses, writeBatchResponse(req.GetRequests()[i].GetCmdType(), uint64(i+1)))
		}
		return &raftcmdpb.RaftCmdResponse{Responses: responses}, nil
	}, 2, time.Hour)

	first, second := submitTwoWriteCommands(t, batcher, writeBatchTestHeader(7), writeBatchTestHeader(7), writeBatchAtomicRequest("k1"), writeBatchAtomicRequest("k2"))
	require.ElementsMatch(t, []uint64{1, 2}, []uint64{first.GetTryAtomicMutate().GetAppliedKeys(), second.GetTryAtomicMutate().GetAppliedKeys()})

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, proposed, 1)
	require.Len(t, proposed[0].GetRequests(), 2)
	require.Equal(t, uint64(1), batcher.Stats()["write_command_batch_batches_total"])
	require.Equal(t, uint64(2), batcher.Stats()["write_command_batch_batched_requests_total"])
	require.Equal(t, uint64(1), batcher.Stats()["write_command_batch_batches_by_command"].(map[string]uint64)["atomic_mutate"])
	require.Equal(t, uint64(2), batcher.Stats()["write_command_batch_batched_requests_by_command"].(map[string]uint64)["atomic_mutate"])
}

func TestWriteCommandBatcherDoesNotMixCommands(t *testing.T) {
	var (
		mu       sync.Mutex
		proposed []*raftcmdpb.RaftCmdRequest
	)
	batcher := newWriteCommandBatcher(func(_ context.Context, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		mu.Lock()
		proposed = append(proposed, req)
		mu.Unlock()
		responses := make([]*raftcmdpb.Response, 0, len(req.GetRequests()))
		for _, request := range req.GetRequests() {
			responses = append(responses, writeBatchResponse(request.GetCmdType(), 1))
		}
		return &raftcmdpb.RaftCmdResponse{Responses: responses}, nil
	}, 8, time.Millisecond)

	first, second := submitTwoWriteCommands(t, batcher, writeBatchTestHeader(7), writeBatchTestHeader(7), writeBatchPrewriteRequest("k1"), writeBatchCommitRequest("k2"))
	require.NotNil(t, first.GetPrewrite())
	require.NotNil(t, second.GetCommit())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, proposed, 2)
	for _, req := range proposed {
		require.Len(t, req.GetRequests(), 1)
	}
}

func TestWriteCommandBatcherDoesNotMixHeaders(t *testing.T) {
	var (
		mu       sync.Mutex
		proposed []*raftcmdpb.RaftCmdRequest
	)
	batcher := newWriteCommandBatcher(func(_ context.Context, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		mu.Lock()
		proposed = append(proposed, req)
		mu.Unlock()
		responses := make([]*raftcmdpb.Response, 0, len(req.GetRequests()))
		for _, request := range req.GetRequests() {
			responses = append(responses, writeBatchResponse(request.GetCmdType(), 1))
		}
		return &raftcmdpb.RaftCmdResponse{Responses: responses}, nil
	}, 8, time.Millisecond)

	first, second := submitTwoWriteCommands(t, batcher, writeBatchTestHeader(7), writeBatchTestHeader(8), writeBatchAtomicRequest("k1"), writeBatchAtomicRequest("k2"))
	require.Equal(t, uint64(1), first.GetTryAtomicMutate().GetAppliedKeys())
	require.Equal(t, uint64(1), second.GetTryAtomicMutate().GetAppliedKeys())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, proposed, 2)
	for _, req := range proposed {
		require.Len(t, req.GetRequests(), 1)
	}
}

func TestWriteCommandBatcherCapsPreparedMVCCInstallBatchSize(t *testing.T) {
	var (
		mu       sync.Mutex
		proposed []*raftcmdpb.RaftCmdRequest
	)
	batcher := newWriteCommandBatcher(func(_ context.Context, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		mu.Lock()
		proposed = append(proposed, req)
		mu.Unlock()
		responses := make([]*raftcmdpb.Response, 0, len(req.GetRequests()))
		for range req.GetRequests() {
			responses = append(responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_InstallPreparedMvcc{InstallPreparedMvcc: &kvrpcpb.InstallPreparedMVCCEntriesResponse{}}})
		}
		return &raftcmdpb.RaftCmdResponse{Responses: responses}, nil
	}, 64, time.Hour)

	results := make(chan writeBatchTestResult, preparedMVCCInstallBatchMaxSize*2)
	for i := 0; i < cap(results); i++ {
		go func() {
			resp, regionErr, err := batcher.submit(context.Background(), writeBatchTestHeader(7), writeBatchPreparedMVCCInstallRequest())
			results <- writeBatchTestResult{response: resp, regionError: regionErr, err: err}
		}()
	}
	for i := 0; i < cap(results); i++ {
		result := receiveWriteBatchResult(t, results)
		require.NotNil(t, result.response.GetInstallPreparedMvcc())
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, proposed, 2)
	require.Len(t, proposed[0].GetRequests(), preparedMVCCInstallBatchMaxSize)
	require.Len(t, proposed[1].GetRequests(), preparedMVCCInstallBatchMaxSize)
	require.Equal(t, uint64(2), batcher.Stats()["write_command_batch_batches_by_command"].(map[string]uint64)["install_prepared_mvcc"])
	require.Equal(t, uint64(preparedMVCCInstallBatchMaxSize*2), batcher.Stats()["write_command_batch_batched_requests_by_command"].(map[string]uint64)["install_prepared_mvcc"])
}

func TestWriteCommandBatcherSplitsLargePreparedMVCCInstallBatchesByBytes(t *testing.T) {
	var (
		mu       sync.Mutex
		proposed []*raftcmdpb.RaftCmdRequest
	)
	batcher := newWriteCommandBatcher(func(_ context.Context, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		mu.Lock()
		proposed = append(proposed, req)
		mu.Unlock()
		responses := make([]*raftcmdpb.Response, 0, len(req.GetRequests()))
		for range req.GetRequests() {
			responses = append(responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_InstallPreparedMvcc{InstallPreparedMvcc: &kvrpcpb.InstallPreparedMVCCEntriesResponse{}}})
		}
		return &raftcmdpb.RaftCmdResponse{Responses: responses}, nil
	}, 64, time.Millisecond)

	results := make(chan writeBatchTestResult, 2)
	for i := 0; i < cap(results); i++ {
		go func() {
			resp, regionErr, err := batcher.submit(context.Background(), writeBatchTestHeader(7), writeBatchPreparedMVCCInstallRequestWithPayload(preparedMVCCInstallBatchMaxBytes/2+1))
			results <- writeBatchTestResult{response: resp, regionError: regionErr, err: err}
		}()
	}
	for i := 0; i < cap(results); i++ {
		result := receiveWriteBatchResult(t, results)
		require.NotNil(t, result.response.GetInstallPreparedMvcc())
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, proposed, 2)
	require.Len(t, proposed[0].GetRequests(), 1)
	require.Len(t, proposed[1].GetRequests(), 1)
}

func TestWriteCommandBatcherSkipsCanceledBeforeFlush(t *testing.T) {
	var proposed atomic.Uint64
	batcher := newWriteCommandBatcher(func(_ context.Context, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		proposed.Add(uint64(len(req.GetRequests())))
		return &raftcmdpb.RaftCmdResponse{}, nil
	}, 8, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := batcher.submit(ctx, writeBatchTestHeader(7), writeBatchAtomicRequest("k1"))
		done <- err
	}()
	cancel()
	require.Error(t, <-done)
	time.Sleep(5 * time.Millisecond)

	require.Zero(t, proposed.Load())
	require.Equal(t, uint64(1), batcher.Stats()["write_command_batch_canceled_before_flush_total"])
	require.Equal(t, uint64(0), batcher.Stats()["write_command_batch_batches_total"])
}

func TestWriteCommandBatcherBroadcastsRegionError(t *testing.T) {
	regionErr := &errorpb.RegionError{RegionNotFound: &errorpb.RegionNotFound{RegionId: 7}}
	batcher := newWriteCommandBatcher(func(context.Context, *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{RegionError: regionErr}, nil
	}, 2, time.Hour)

	first, second := submitTwoWriteResults(t, batcher, writeBatchTestHeader(7), writeBatchTestHeader(7), writeBatchPrewriteRequest("k1"), writeBatchPrewriteRequest("k2"))
	require.Same(t, regionErr, first.regionError)
	require.Same(t, regionErr, second.regionError)
	require.Nil(t, first.response)
	require.Nil(t, second.response)
}

func TestServicePrewriteUsesWriteCommandBatcher(t *testing.T) {
	var (
		mu       sync.Mutex
		proposed []*raftcmdpb.RaftCmdRequest
	)
	service := &Service{writeBatcher: newWriteCommandBatcher(func(_ context.Context, req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		mu.Lock()
		proposed = append(proposed, req)
		mu.Unlock()
		responses := make([]*raftcmdpb.Response, 0, len(req.GetRequests()))
		for range req.GetRequests() {
			responses = append(responses, writeBatchResponse(raftcmdpb.CmdType_CMD_PREWRITE, 0))
		}
		return &raftcmdpb.RaftCmdResponse{Responses: responses}, nil
	}, 2, time.Hour)}

	firstCh := make(chan error, 1)
	secondCh := make(chan error, 1)
	go func() {
		_, err := service.Prewrite(context.Background(), writeBatchPrewriteRPC("k1"))
		firstCh <- err
	}()
	go func() {
		_, err := service.Prewrite(context.Background(), writeBatchPrewriteRPC("k2"))
		secondCh <- err
	}()
	require.NoError(t, receiveError(t, firstCh))
	require.NoError(t, receiveError(t, secondCh))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, proposed, 1)
	require.Len(t, proposed[0].GetRequests(), 2)
	for _, req := range proposed[0].GetRequests() {
		require.Equal(t, raftcmdpb.CmdType_CMD_PREWRITE, req.GetCmdType())
	}
}

func submitTwoWriteCommands(t *testing.T, batcher *writeCommandBatcher, firstHeader, secondHeader *raftcmdpb.CmdHeader, firstRequest, secondRequest *raftcmdpb.Request) (*raftcmdpb.Response, *raftcmdpb.Response) {
	t.Helper()
	first, second := submitTwoWriteResults(t, batcher, firstHeader, secondHeader, firstRequest, secondRequest)
	return first.response, second.response
}

func submitTwoWriteResults(t *testing.T, batcher *writeCommandBatcher, firstHeader, secondHeader *raftcmdpb.CmdHeader, firstRequest, secondRequest *raftcmdpb.Request) (writeBatchTestResult, writeBatchTestResult) {
	t.Helper()
	firstCh := make(chan writeBatchTestResult, 1)
	secondCh := make(chan writeBatchTestResult, 1)
	go func() {
		resp, regionErr, err := batcher.submit(context.Background(), firstHeader, firstRequest)
		firstCh <- writeBatchTestResult{response: resp, regionError: regionErr, err: err}
	}()
	go func() {
		resp, regionErr, err := batcher.submit(context.Background(), secondHeader, secondRequest)
		secondCh <- writeBatchTestResult{response: resp, regionError: regionErr, err: err}
	}()
	first := receiveWriteBatchResult(t, firstCh)
	second := receiveWriteBatchResult(t, secondCh)
	return first, second
}

func receiveWriteBatchResult(t *testing.T, ch <-chan writeBatchTestResult) writeBatchTestResult {
	t.Helper()
	select {
	case result := <-ch:
		require.NoError(t, result.err)
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batched write command")
		return writeBatchTestResult{}
	}
}

func receiveError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for service write command")
		return nil
	}
}

func writeBatchTestHeader(region uint64) *raftcmdpb.CmdHeader {
	return &raftcmdpb.CmdHeader{
		RegionId: region,
		RegionEpoch: &metapb.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
		StoreId: 1,
		PeerId:  11,
	}
}

func writeBatchTestContext() *kvrpcpb.Context {
	header := writeBatchTestHeader(7)
	return &kvrpcpb.Context{
		RegionId:    header.GetRegionId(),
		RegionEpoch: header.GetRegionEpoch(),
		Peer:        &metapb.RegionPeer{StoreId: header.GetStoreId(), PeerId: header.GetPeerId()},
	}
}

func writeBatchPrewriteRPC(key string) *kvrpcpb.KvPrewriteRequest {
	return &kvrpcpb.KvPrewriteRequest{
		Context: writeBatchTestContext(),
		Request: &kvrpcpb.PrewriteRequest{
			PrimaryLock:  []byte(key),
			StartVersion: 10,
			Mutations: []*kvrpcpb.Mutation{{
				Op:    kvrpcpb.Mutation_Put,
				Key:   []byte(key),
				Value: []byte("value"),
			}},
		},
	}
}

func writeBatchPrewriteRequest(key string) *raftcmdpb.Request {
	return &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
		Cmd:     &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{PrimaryLock: []byte(key), StartVersion: 10}},
	}
}

func writeBatchCommitRequest(key string) *raftcmdpb.Request {
	return &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_COMMIT,
		Cmd:     &raftcmdpb.Request_Commit{Commit: &kvrpcpb.CommitRequest{Keys: [][]byte{[]byte(key)}, StartVersion: 10, CommitVersion: 11}},
	}
}

func writeBatchAtomicRequest(key string) *raftcmdpb.Request {
	return &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
		Cmd: &raftcmdpb.Request_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateRequest{
			StartVersion:  10,
			CommitVersion: 11,
			Predicates: []*kvrpcpb.AtomicPredicate{{
				Key:  []byte(key),
				Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS,
			}},
			Mutations: []*kvrpcpb.Mutation{{
				Op:                kvrpcpb.Mutation_Put,
				Key:               []byte(key),
				Value:             []byte("value"),
				AssertionNotExist: true,
			}},
		}},
	}
}

func writeBatchPreparedMVCCInstallRequest() *raftcmdpb.Request {
	return writeBatchPreparedMVCCInstallRequestWithPayload(0)
}

func writeBatchPreparedMVCCInstallRequestWithPayload(payloadSize int) *raftcmdpb.Request {
	return &raftcmdpb.Request{
		CmdType: raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC,
		Cmd: &raftcmdpb.Request_InstallPreparedMvcc{InstallPreparedMvcc: &kvrpcpb.InstallPreparedMVCCEntriesRequest{
			RoutingKey:    []byte("route"),
			CommitVersion: 10,
			Entries: []*kvrpcpb.PreparedMVCCEntry{{
				ColumnFamily: kvrpcpb.PreparedMVCCEntry_DEFAULT,
				Key:          []byte("key"),
				Version:      10,
				Value:        make([]byte, payloadSize),
				HasValue:     true,
			}},
		}},
	}
}

func writeBatchResponse(cmdType raftcmdpb.CmdType, value uint64) *raftcmdpb.Response {
	switch cmdType {
	case raftcmdpb.CmdType_CMD_PREWRITE:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Prewrite{Prewrite: &kvrpcpb.PrewriteResponse{}}}
	case raftcmdpb.CmdType_CMD_COMMIT:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Commit{Commit: &kvrpcpb.CommitResponse{}}}
	case raftcmdpb.CmdType_CMD_BATCH_ROLLBACK:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_BatchRollback{BatchRollback: &kvrpcpb.BatchRollbackResponse{}}}
	case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockResponse{}}}
	case raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_TryAtomicMutate{TryAtomicMutate: &kvrpcpb.TryAtomicMutateResponse{AppliedKeys: value}}}
	case raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC:
		return &raftcmdpb.Response{Cmd: &raftcmdpb.Response_InstallPreparedMvcc{InstallPreparedMvcc: &kvrpcpb.InstallPreparedMVCCEntriesResponse{AppliedEntries: value}}}
	default:
		return &raftcmdpb.Response{}
	}
}
