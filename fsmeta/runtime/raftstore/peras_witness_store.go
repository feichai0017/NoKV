// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	rsperas "github.com/feichai0017/NoKV/raftstore/peras"
	"google.golang.org/grpc"
)

type remotePerasWitness struct {
	id     string
	client kvrpcpb.StoreKVClient
}

const remotePerasWitnessProbePageLimit = 32

func newRemotePerasWitness(id string, client kvrpcpb.StoreKVClient) (*remotePerasWitness, error) {
	if id == "" || client == nil {
		return nil, runtimeperas.ErrRuntimeInvalid
	}
	return &remotePerasWitness{id: id, client: client}, nil
}

func (w *remotePerasWitness) ID() string {
	if w == nil {
		return ""
	}
	return w.id
}

func (w *remotePerasWitness) AppendSegments(ctx context.Context, scope compile.AuthorityScope, records []fsperas.SegmentWitnessRecord) error {
	if w == nil || w.client == nil {
		return runtimeperas.ErrRuntimeInvalid
	}
	if len(records) == 0 {
		return nil
	}
	_, err := w.client.PerasWitnessSegments(ctx, &kvrpcpb.PerasWitnessSegmentsRequest{
		Scope:   rsperas.ScopeToProto(scope),
		Records: rsperas.SegmentWitnessRecordsToProto(records),
	})
	return err
}

func (w *remotePerasWitness) Probe(ctx context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	if w == nil || w.client == nil {
		return fsperas.WitnessSnapshot{}, runtimeperas.ErrRuntimeInvalid
	}
	var out fsperas.WitnessSnapshot
	var afterRoot []byte
	var afterDigest []byte
	for {
		resp, err := w.client.PerasWitnessProbe(ctx, &kvrpcpb.PerasWitnessProbeRequest{
			EpochId:                   epochID,
			Limit:                     remotePerasWitnessProbePageLimit,
			AfterSegmentRoot:          afterRoot,
			AfterSegmentPayloadDigest: afterDigest,
		})
		if err != nil {
			return fsperas.WitnessSnapshot{}, err
		}
		page, err := rsperas.SnapshotFromProto(resp)
		if err != nil {
			return fsperas.WitnessSnapshot{}, err
		}
		out.Segments = append(out.Segments, page.Segments...)
		if !resp.GetMore() {
			return out, nil
		}
		afterRoot = append(afterRoot[:0], resp.GetNextSegmentRoot()...)
		afterDigest = append(afterDigest[:0], resp.GetNextSegmentPayloadDigest()...)
		if len(afterRoot) != 32 || len(afterDigest) != 32 || len(page.Segments) == 0 {
			return fsperas.WitnessSnapshot{}, fmt.Errorf("peras witness probe returned invalid cursor: %w", runtimeperas.ErrRuntimeInvalid)
		}
	}
}

func (w *remotePerasWitness) ProbeSegment(ctx context.Context, ref fsperas.WitnessSegmentRef) (fsperas.SegmentWitnessRecord, bool, error) {
	if w == nil || w.client == nil {
		return fsperas.SegmentWitnessRecord{}, false, runtimeperas.ErrRuntimeInvalid
	}
	if !ref.Valid() {
		return fsperas.SegmentWitnessRecord{}, false, fsperas.ErrInvalidWitnessRecord
	}
	resp, err := w.client.PerasWitnessProbe(ctx, &kvrpcpb.PerasWitnessProbeRequest{
		EpochId:              ref.EpochID,
		SegmentRoot:          append([]byte(nil), ref.SegmentRoot[:]...),
		SegmentPayloadDigest: append([]byte(nil), ref.SegmentPayloadDigest[:]...),
	})
	if err != nil {
		return fsperas.SegmentWitnessRecord{}, false, err
	}
	if len(resp.GetSegments()) == 0 {
		return fsperas.SegmentWitnessRecord{}, false, nil
	}
	if len(resp.GetSegments()) != 1 {
		return fsperas.SegmentWitnessRecord{}, false, fmt.Errorf("peras witness targeted probe returned %d records: %w", len(resp.GetSegments()), runtimeperas.ErrRuntimeInvalid)
	}
	record, err := rsperas.SegmentWitnessRecordFromProto(resp.GetSegments()[0])
	if err != nil {
		return fsperas.SegmentWitnessRecord{}, false, err
	}
	if record.EpochID != ref.EpochID || record.SegmentRoot != ref.SegmentRoot || record.SegmentPayloadDigest != ref.SegmentPayloadDigest {
		return fsperas.SegmentWitnessRecord{}, false, fmt.Errorf("peras witness targeted probe returned mismatched record: %w", runtimeperas.ErrRuntimeInvalid)
	}
	return record, true, nil
}

const (
	perasWitnessDiscoveryTimeout = 45 * time.Second
	perasWitnessDiscoveryBackoff = 100 * time.Millisecond
)

type witnessStoreLister interface {
	ListStores(context.Context, *coordpb.ListStoresRequest) (*coordpb.ListStoresResponse, error)
}

type witnessConnections struct {
	witnesses []fsperas.WitnessReplica
	conns     []*grpc.ClientConn
}

func buildWitnessConnections(ctx context.Context, lister witnessStoreLister, dialOpts []grpc.DialOption, storeIDs []uint64) (*witnessConnections, error) {
	if lister == nil {
		return nil, errStoreListerRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, perasWitnessDiscoveryTimeout)
	defer cancel()

	allowed := make(map[uint64]struct{}, len(storeIDs))
	for _, id := range storeIDs {
		if id != 0 {
			allowed[id] = struct{}{}
		}
	}
	for {
		out, complete, err := tryBuildWitnessConnections(ctx, lister, dialOpts, allowed)
		if err != nil {
			return nil, err
		}
		if complete {
			return out, nil
		}
		if out != nil {
			_ = out.Close()
		}
		timer := time.NewTimer(perasWitnessDiscoveryBackoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, runtimeperas.ErrRuntimeInvalid
		case <-timer.C:
		}
	}
}

func tryBuildWitnessConnections(ctx context.Context, lister witnessStoreLister, dialOpts []grpc.DialOption, allowed map[uint64]struct{}) (*witnessConnections, bool, error) {
	resp, err := lister.ListStores(ctx, &coordpb.ListStoresRequest{})
	if err != nil {
		return nil, false, err
	}
	out := &witnessConnections{}
	seen := make(map[uint64]struct{}, len(allowed))
	for _, store := range resp.GetStores() {
		if !witnessStoreSelected(store, allowed) {
			continue
		}
		if len(allowed) > 0 {
			seen[store.GetStoreId()] = struct{}{}
		}
		conn, err := grpc.NewClient(store.GetClientAddr(), dialOpts...)
		if err != nil {
			_ = out.Close()
			return nil, false, fmt.Errorf("dial peras witness store %d: %w", store.GetStoreId(), err)
		}
		witness, err := newRemotePerasWitness(
			fmt.Sprintf("store-%d", store.GetStoreId()),
			kvrpcpb.NewStoreKVClient(conn),
		)
		if err != nil {
			_ = conn.Close()
			_ = out.Close()
			return nil, false, err
		}
		out.conns = append(out.conns, conn)
		out.witnesses = append(out.witnesses, witness)
	}
	complete := len(out.witnesses) > 0
	if len(allowed) > 0 {
		complete = len(seen) == len(allowed)
	}
	if !complete {
		return out, false, nil
	}
	slices.SortFunc(out.witnesses, func(left, right fsperas.WitnessReplica) int {
		if left.ID() < right.ID() {
			return -1
		}
		if left.ID() > right.ID() {
			return 1
		}
		return 0
	})
	return out, true, nil
}

func witnessStoreSelected(store *coordpb.StoreInfo, allowed map[uint64]struct{}) bool {
	if store == nil || store.GetState() != coordpb.StoreState_STORE_STATE_UP || store.GetClientAddr() == "" {
		return false
	}
	if len(allowed) == 0 {
		return true
	}
	_, ok := allowed[store.GetStoreId()]
	return ok
}

func (c *witnessConnections) Close() error {
	if c == nil {
		return nil
	}
	var first error
	for _, conn := range c.conns {
		if conn == nil {
			continue
		}
		if err := conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	c.conns = nil
	c.witnesses = nil
	return first
}
