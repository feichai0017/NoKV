// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package kv

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	errorpb "github.com/feichai0017/NoKV/pb/error"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"google.golang.org/protobuf/proto"
)

const (
	defaultWriteCommandBatchMaxSize  = 64
	defaultWriteCommandBatchMaxWait  = 200 * time.Microsecond
	perasInstallSegmentBatchMaxSize  = 16
	perasInstallSegmentBatchMaxBytes = 8 << 20
)

var batchedWriteCommandTypes = []raftcmdpb.CmdType{
	raftcmdpb.CmdType_CMD_PREWRITE,
	raftcmdpb.CmdType_CMD_COMMIT,
	raftcmdpb.CmdType_CMD_BATCH_ROLLBACK,
	raftcmdpb.CmdType_CMD_RESOLVE_LOCK,
	raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE,
	raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC,
	raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT,
}

type writeCommandProposer func(context.Context, *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error)

type writeCommandBatchKey struct {
	cmdType          raftcmdpb.CmdType
	regionID         uint64
	epochVersion     uint64
	epochConfVersion uint64
	storeID          uint64
	peerID           uint64
}

type writeCommandBatcher struct {
	propose writeCommandProposer
	maxSize int
	maxWait time.Duration

	mu      sync.Mutex
	batches map[writeCommandBatchKey]*writeCommandBatch
	total   writeCommandCounters
	byCmd   map[raftcmdpb.CmdType]*writeCommandCounters
}

type writeCommandCounters struct {
	requestsTotal       atomic.Uint64
	batchesTotal        atomic.Uint64
	batchedRequests     atomic.Uint64
	canceledBeforeFlush atomic.Uint64
}

type writeCommandBatch struct {
	header   *raftcmdpb.CmdHeader
	cmdType  raftcmdpb.CmdType
	maxSize  int
	maxBytes int
	bytes    int
	items    []*writeCommandBatchItem
	timer    *time.Timer
}

type writeCommandBatchItem struct {
	ctx     context.Context
	request *raftcmdpb.Request
	result  chan writeCommandBatchResult
}

type writeCommandBatchResult struct {
	regionError *errorpb.RegionError
	response    *raftcmdpb.Response
	err         error
}

func newWriteCommandBatcher(propose writeCommandProposer, maxSize int, maxWait time.Duration) *writeCommandBatcher {
	if maxSize <= 1 || maxWait <= 0 {
		return nil
	}
	byCmd := make(map[raftcmdpb.CmdType]*writeCommandCounters, len(batchedWriteCommandTypes))
	for _, cmdType := range batchedWriteCommandTypes {
		byCmd[cmdType] = &writeCommandCounters{}
	}
	return &writeCommandBatcher{
		propose: propose,
		maxSize: maxSize,
		maxWait: maxWait,
		batches: make(map[writeCommandBatchKey]*writeCommandBatch),
		byCmd:   byCmd,
	}
}

func (b *writeCommandBatcher) submit(ctx context.Context, header *raftcmdpb.CmdHeader, request *raftcmdpb.Request) (*raftcmdpb.Response, *errorpb.RegionError, error) {
	if b == nil {
		return nil, nil, errStoreNotInitialized
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if request == nil {
		return nil, nil, rpcInvalidArgument("write command request missing payload")
	}
	if err := ctx.Err(); err != nil {
		b.addCanceled(request.GetCmdType())
		return nil, nil, rpcStatus(err)
	}
	item := &writeCommandBatchItem{
		ctx:     ctx,
		request: request,
		result:  make(chan writeCommandBatchResult, 1),
	}
	cmdType := request.GetCmdType()
	b.addRequest(cmdType)
	key := writeBatchKey(header, cmdType)
	requestBytes := writeCommandRequestBytes(cmdType, request)
	var flushes []*writeCommandBatch

	b.mu.Lock()
	batch := b.batches[key]
	if batch != nil && batch.shouldFlushBefore(requestBytes) {
		delete(b.batches, key)
		if batch.timer != nil {
			batch.timer.Stop()
		}
		flushes = append(flushes, batch)
		batch = nil
	}
	if batch == nil {
		maxSize, maxBytes := writeCommandBatchLimits(cmdType, b.maxSize)
		batch = &writeCommandBatch{
			header:   cloneCmdHeader(header),
			cmdType:  cmdType,
			maxSize:  maxSize,
			maxBytes: maxBytes,
			items:    make([]*writeCommandBatchItem, 0, maxSize),
		}
		b.batches[key] = batch
		batch.timer = time.AfterFunc(b.maxWait, func() {
			b.flushKey(key)
		})
	}
	batch.items = append(batch.items, item)
	batch.bytes += requestBytes
	if len(batch.items) >= batch.maxSize {
		delete(b.batches, key)
		if batch.timer != nil {
			batch.timer.Stop()
		}
		flushes = append(flushes, batch)
	}
	b.mu.Unlock()

	for _, flush := range flushes {
		b.flush(flush)
	}

	select {
	case result := <-item.result:
		return result.response, result.regionError, result.err
	case <-ctx.Done():
		return nil, nil, rpcStatus(ctx.Err())
	}
}

func writeCommandBatchLimits(cmdType raftcmdpb.CmdType, fallback int) (int, int) {
	if cmdType == raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT ||
		cmdType == raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC {
		if fallback > perasInstallSegmentBatchMaxSize {
			fallback = perasInstallSegmentBatchMaxSize
		}
		return fallback, perasInstallSegmentBatchMaxBytes
	}
	return fallback, 0
}

func writeCommandRequestBytes(cmdType raftcmdpb.CmdType, request *raftcmdpb.Request) int {
	if request == nil {
		return 0
	}
	if cmdType != raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT &&
		cmdType != raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC {
		return 0
	}
	size := proto.Size(request)
	if size <= 0 {
		return 1
	}
	return size
}

func (b *writeCommandBatch) shouldFlushBefore(nextBytes int) bool {
	if b == nil || len(b.items) == 0 || b.maxBytes <= 0 || nextBytes <= 0 {
		return false
	}
	return b.bytes+nextBytes > b.maxBytes
}

func (b *writeCommandBatcher) flushKey(key writeCommandBatchKey) {
	b.mu.Lock()
	batch := b.batches[key]
	if batch != nil {
		delete(b.batches, key)
	}
	b.mu.Unlock()
	if batch != nil {
		b.flush(batch)
	}
}

func (b *writeCommandBatcher) flush(batch *writeCommandBatch) {
	if batch == nil || len(batch.items) == 0 {
		return
	}
	items := make([]*writeCommandBatchItem, 0, len(batch.items))
	requests := make([]*raftcmdpb.Request, 0, len(batch.items))
	for _, item := range batch.items {
		if err := item.ctx.Err(); err != nil {
			b.addCanceled(batch.cmdType)
			item.complete(nil, nil, rpcStatus(err))
			continue
		}
		items = append(items, item)
		requests = append(requests, item.request)
	}
	if len(items) == 0 {
		return
	}
	b.addBatch(batch.cmdType, len(items))

	// Each caller has passed admission, but the raft proposal represents all
	// live items together. Use a detached context so one canceled RPC cannot
	// abort unrelated requests that share this raft batch.
	resp, err := b.propose(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header:   cloneCmdHeader(batch.header),
		Requests: requests,
	})
	if err != nil {
		err = rpcStatus(err)
		for _, item := range items {
			item.complete(nil, nil, err)
		}
		return
	}
	if regionErr := resp.GetRegionError(); regionErr != nil {
		for _, item := range items {
			item.complete(nil, regionErr, nil)
		}
		return
	}
	if len(resp.GetResponses()) != len(items) {
		err := raftPayloadError(writeCommandName(batch.cmdType)+" batch", fmt.Sprintf("expected %d raft responses, got %d", len(items), len(resp.GetResponses())))
		for _, item := range items {
			item.complete(nil, nil, err)
		}
		return
	}
	for i, item := range items {
		if resp.GetResponses()[i] == nil {
			item.complete(nil, nil, raftPayloadError(writeCommandName(batch.cmdType)+" batch", fmt.Sprintf("missing response %d", i)))
			continue
		}
		item.complete(resp.GetResponses()[i], nil, nil)
	}
}

func (i *writeCommandBatchItem) complete(response *raftcmdpb.Response, regionErr *errorpb.RegionError, err error) {
	i.result <- writeCommandBatchResult{response: response, regionError: regionErr, err: err}
}

func (b *writeCommandBatcher) addRequest(cmdType raftcmdpb.CmdType) {
	b.total.requestsTotal.Add(1)
	if counters := b.commandCounters(cmdType); counters != nil {
		counters.requestsTotal.Add(1)
	}
}

func (b *writeCommandBatcher) addBatch(cmdType raftcmdpb.CmdType, requests int) {
	b.total.batchesTotal.Add(1)
	b.total.batchedRequests.Add(uint64(requests))
	if counters := b.commandCounters(cmdType); counters != nil {
		counters.batchesTotal.Add(1)
		counters.batchedRequests.Add(uint64(requests))
	}
}

func (b *writeCommandBatcher) addCanceled(cmdType raftcmdpb.CmdType) {
	b.total.canceledBeforeFlush.Add(1)
	if counters := b.commandCounters(cmdType); counters != nil {
		counters.canceledBeforeFlush.Add(1)
	}
}

func (b *writeCommandBatcher) commandCounters(cmdType raftcmdpb.CmdType) *writeCommandCounters {
	if b == nil || b.byCmd == nil {
		return nil
	}
	return b.byCmd[cmdType]
}

func (b *writeCommandBatcher) Stats() map[string]any {
	if b == nil {
		zeroByCmd := make(map[raftcmdpb.CmdType]*writeCommandCounters, len(batchedWriteCommandTypes))
		for _, cmdType := range batchedWriteCommandTypes {
			zeroByCmd[cmdType] = &writeCommandCounters{}
		}
		return writeCommandStats(&writeCommandCounters{}, zeroByCmd)
	}
	return writeCommandStats(&b.total, b.byCmd)
}

func writeCommandStats(total *writeCommandCounters, byCmd map[raftcmdpb.CmdType]*writeCommandCounters) map[string]any {
	requestsByCommand := make(map[string]uint64, len(batchedWriteCommandTypes))
	batchesByCommand := make(map[string]uint64, len(batchedWriteCommandTypes))
	batchedByCommand := make(map[string]uint64, len(batchedWriteCommandTypes))
	canceledByCommand := make(map[string]uint64, len(batchedWriteCommandTypes))
	for _, cmdType := range batchedWriteCommandTypes {
		name := writeCommandName(cmdType)
		counters := byCmd[cmdType]
		if counters == nil {
			counters = &writeCommandCounters{}
		}
		requestsByCommand[name] = counters.requestsTotal.Load()
		batchesByCommand[name] = counters.batchesTotal.Load()
		batchedByCommand[name] = counters.batchedRequests.Load()
		canceledByCommand[name] = counters.canceledBeforeFlush.Load()
	}
	return map[string]any{
		"write_command_batch_requests_total":              total.requestsTotal.Load(),
		"write_command_batch_batches_total":               total.batchesTotal.Load(),
		"write_command_batch_batched_requests_total":      total.batchedRequests.Load(),
		"write_command_batch_canceled_before_flush_total": total.canceledBeforeFlush.Load(),
		"write_command_batch_requests_by_command":         requestsByCommand,
		"write_command_batch_batches_by_command":          batchesByCommand,
		"write_command_batch_batched_requests_by_command": batchedByCommand,
		"write_command_batch_canceled_by_command":         canceledByCommand,
	}
}

func writeBatchKey(header *raftcmdpb.CmdHeader, cmdType raftcmdpb.CmdType) writeCommandBatchKey {
	if header == nil {
		return writeCommandBatchKey{cmdType: cmdType}
	}
	epoch := header.GetRegionEpoch()
	return writeCommandBatchKey{
		cmdType:          cmdType,
		regionID:         header.GetRegionId(),
		epochVersion:     epoch.GetVersion(),
		epochConfVersion: epoch.GetConfVersion(),
		storeID:          header.GetStoreId(),
		peerID:           header.GetPeerId(),
	}
}

func writeCommandName(cmdType raftcmdpb.CmdType) string {
	switch cmdType {
	case raftcmdpb.CmdType_CMD_PREWRITE:
		return "prewrite"
	case raftcmdpb.CmdType_CMD_COMMIT:
		return "commit"
	case raftcmdpb.CmdType_CMD_BATCH_ROLLBACK:
		return "batch_rollback"
	case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
		return "resolve_lock"
	case raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE:
		return "atomic_mutate"
	case raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC:
		return "install_prepared_mvcc"
	case raftcmdpb.CmdType_CMD_PERAS_INSTALL_SEGMENT:
		return "peras_install_segment"
	default:
		return "unknown"
	}
}

func cloneCmdHeader(header *raftcmdpb.CmdHeader) *raftcmdpb.CmdHeader {
	if header == nil {
		return nil
	}
	out := &raftcmdpb.CmdHeader{
		RegionId:          header.GetRegionId(),
		PeerId:            header.GetPeerId(),
		ReadQuorum:        header.GetReadQuorum(),
		RequestId:         header.GetRequestId(),
		ReadConsistency:   header.GetReadConsistency(),
		ReadPreference:    header.GetReadPreference(),
		MaxStaleReadIndex: header.GetMaxStaleReadIndex(),
		MaxStaleReadMs:    header.GetMaxStaleReadMs(),
		StoreId:           header.GetStoreId(),
	}
	if epoch := header.GetRegionEpoch(); epoch != nil {
		out.RegionEpoch = &metapb.RegionEpoch{
			Version:     epoch.GetVersion(),
			ConfVersion: epoch.GetConfVersion(),
		}
	}
	return out
}
