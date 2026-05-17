// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package fsmeta

import (
	"context"
	"fmt"
	"slices"
	"time"

	perasraftstore "github.com/feichai0017/NoKV/experimental/peras/adapters/raftstore"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	runtimeperas "github.com/feichai0017/NoKV/experimental/peras/runtime"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"google.golang.org/grpc"
)

type remoteSegmentWitness struct {
	id     string
	client kvrpcpb.SegmentWitnessClient
}

const remoteProbeSegmentWitnessPageLimit = 32

func newRemoteSegmentWitness(id string, client kvrpcpb.SegmentWitnessClient) (*remoteSegmentWitness, error) {
	if id == "" || client == nil {
		return nil, runtimeperas.ErrRuntimeInvalid
	}
	return &remoteSegmentWitness{id: id, client: client}, nil
}

func (w *remoteSegmentWitness) ID() string {
	if w == nil {
		return ""
	}
	return w.id
}

func (w *remoteSegmentWitness) AppendSegments(ctx context.Context, scope compile.AuthorityScope, records []fsperas.SegmentWitnessRecord) error {
	if w == nil || w.client == nil {
		return runtimeperas.ErrRuntimeInvalid
	}
	if len(records) == 0 {
		return nil
	}
	_, err := w.client.AppendSegmentWitness(ctx, &kvrpcpb.AppendSegmentWitnessRequest{
		Scope:   perasraftstore.ScopeToProto(scope),
		Records: perasraftstore.SegmentWitnessRecordsToProto(records),
	})
	return err
}

func (w *remoteSegmentWitness) Probe(ctx context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	if w == nil || w.client == nil {
		return fsperas.WitnessSnapshot{}, runtimeperas.ErrRuntimeInvalid
	}
	var out fsperas.WitnessSnapshot
	var afterRoot []byte
	var afterDigest []byte
	for {
		resp, err := w.client.ProbeSegmentWitness(ctx, &kvrpcpb.ProbeSegmentWitnessRequest{
			EpochId:                   epochID,
			Limit:                     remoteProbeSegmentWitnessPageLimit,
			AfterSegmentRoot:          afterRoot,
			AfterSegmentPayloadDigest: afterDigest,
		})
		if err != nil {
			return fsperas.WitnessSnapshot{}, err
		}
		page, err := perasraftstore.SnapshotFromProto(resp)
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

func (w *remoteSegmentWitness) ProbeSegment(ctx context.Context, ref fsperas.WitnessSegmentRef) (fsperas.SegmentWitnessRecord, bool, error) {
	if w == nil || w.client == nil {
		return fsperas.SegmentWitnessRecord{}, false, runtimeperas.ErrRuntimeInvalid
	}
	if !ref.Valid() {
		return fsperas.SegmentWitnessRecord{}, false, fsperas.ErrInvalidWitnessRecord
	}
	resp, err := w.client.ProbeSegmentWitness(ctx, &kvrpcpb.ProbeSegmentWitnessRequest{
		EpochId:              ref.EpochID,
		SegmentRoot:          append([]byte(nil), ref.SegmentRoot[:]...),
		SegmentPayloadDigest: append([]byte(nil), ref.SegmentPayloadDigest[:]...),
		Limit:                1,
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
	record, err := perasraftstore.SegmentWitnessRecordFromProto(resp.GetSegments()[0])
	if err != nil {
		return fsperas.SegmentWitnessRecord{}, false, err
	}
	if record.EpochID != ref.EpochID || record.SegmentRoot != ref.SegmentRoot || record.SegmentPayloadDigest != ref.SegmentPayloadDigest {
		return fsperas.SegmentWitnessRecord{}, false, fmt.Errorf("peras witness targeted probe returned mismatched record: %w", runtimeperas.ErrRuntimeInvalid)
	}
	return record, true, nil
}

const (
	segmentWitnessDiscoveryTimeout = 45 * time.Second
	segmentWitnessDiscoveryBackoff = 100 * time.Millisecond
	segmentWitnessDiscoverySettle  = 2 * time.Second
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
	ctx, cancel := context.WithTimeout(ctx, segmentWitnessDiscoveryTimeout)
	defer cancel()

	allowed := make(map[uint64]struct{}, len(storeIDs))
	for _, id := range storeIDs {
		if id != 0 {
			allowed[id] = struct{}{}
		}
	}
	var stableIDs []string
	var stableSince time.Time
	for {
		out, complete, err := tryBuildWitnessConnections(ctx, lister, dialOpts, allowed)
		if err != nil {
			return nil, err
		}
		if len(allowed) == 0 {
			stableIDs, stableSince, complete = witnessDiscoverySettled(stableIDs, stableSince, witnessConnectionIDs(out), time.Now())
		}
		if complete {
			return out, nil
		}
		if out != nil {
			_ = out.Close()
		}
		timer := time.NewTimer(segmentWitnessDiscoveryBackoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, runtimeperas.ErrRuntimeInvalid
		case <-timer.C:
		}
	}
}

func witnessConnectionIDs(conns *witnessConnections) []string {
	if conns == nil || len(conns.witnesses) == 0 {
		return nil
	}
	ids := make([]string, 0, len(conns.witnesses))
	for _, witness := range conns.witnesses {
		if witness == nil {
			continue
		}
		ids = append(ids, witness.ID())
	}
	slices.Sort(ids)
	return ids
}

func witnessDiscoverySettled(previous []string, stableSince time.Time, current []string, now time.Time) ([]string, time.Time, bool) {
	if len(current) == 0 {
		return nil, time.Time{}, false
	}
	if len(previous) == 0 || !slices.Equal(previous, current) {
		return slices.Clone(current), now, false
	}
	return previous, stableSince, now.Sub(stableSince) >= segmentWitnessDiscoverySettle
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
		witness, err := newRemoteSegmentWitness(
			fmt.Sprintf("store-%d", store.GetStoreId()),
			kvrpcpb.NewSegmentWitnessClient(conn),
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
