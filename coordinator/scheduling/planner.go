// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package scheduling owns Coordinator-side cluster scheduling policy.
//
// The package consumes disposable Coordinator views and emits store-control
// operations. It must not import coordinator/server or raftstore packages:
// servers own RPC admission, stores own execution, and this package owns only
// policy selection.
package scheduling

import (
	"sync"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/meta/topology"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

const (
	defaultSchedulerCooldownTicks uint64 = 3
	defaultMaxOperationsPerStore         = 1
)

type PlanOptions struct {
	CooldownTicks         uint64
	MaxOperationsPerStore int
}

type Planner struct {
	mu       sync.Mutex
	opts     PlanOptions
	tick     uint64
	cooldown map[uint64]uint64
}

func NewPlanner(opts PlanOptions) *Planner {
	return &Planner{
		opts:     normalizeOptions(opts, true),
		cooldown: make(map[uint64]uint64),
	}
}

func (p *Planner) ConfigureOptions(opts PlanOptions) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.opts = mergeOptions(p.opts, opts)
	p.mu.Unlock()
}

// PlanStoreOperations builds bounded control-plane operations for the heartbeat
// source store. v1 keeps one raft group per mount, so automatic range split
// and merge are deliberately outside the main scheduling loop.
func PlanStoreOperations(heartbeatStoreID uint64, snapshot catalog.ClusterSnapshot) []*coordpb.SchedulerOperation {
	return PlanStoreOperationsWithOptions(heartbeatStoreID, snapshot, PlanOptions{})
}

func PlanStoreOperationsWithOptions(heartbeatStoreID uint64, snapshot catalog.ClusterSnapshot, opts PlanOptions) []*coordpb.SchedulerOperation {
	return planStoreOperationsImmediate(heartbeatStoreID, snapshot, normalizeOptions(opts, false))
}

func (p *Planner) PlanStoreOperationsWithOptions(heartbeatStoreID uint64, snapshot catalog.ClusterSnapshot, opts PlanOptions) []*coordpb.SchedulerOperation {
	if p == nil {
		return PlanStoreOperationsWithOptions(heartbeatStoreID, snapshot, opts)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tick++
	merged := mergeOptions(p.opts, opts)
	p.prune(snapshot)
	return p.plan(heartbeatStoreID, snapshot, merged)
}

func planStoreOperationsImmediate(heartbeatStoreID uint64, snapshot catalog.ClusterSnapshot, _ PlanOptions) []*coordpb.SchedulerOperation {
	if heartbeatStoreID == 0 || len(snapshot.Stores) < 2 {
		return nil
	}
	if op, ok := planLeaderTransfer(heartbeatStoreID, snapshot); ok {
		return []*coordpb.SchedulerOperation{op}
	}
	return nil
}

func (p *Planner) plan(heartbeatStoreID uint64, snapshot catalog.ClusterSnapshot, opts PlanOptions) []*coordpb.SchedulerOperation {
	if heartbeatStoreID == 0 || len(snapshot.Stores) < 2 {
		return nil
	}
	maxOps := opts.MaxOperationsPerStore
	if maxOps <= 0 {
		maxOps = defaultMaxOperationsPerStore
	}
	ops := make([]*coordpb.SchedulerOperation, 0, maxOps)
	if op, ok := planLeaderTransfer(heartbeatStoreID, snapshot); ok && !p.operationInCooldown(op) {
		ops = append(ops, op)
		p.markOperationCooldown(op, opts.CooldownTicks)
		return ops
	}
	return nil
}

func normalizeOptions(opts PlanOptions, stateful bool) PlanOptions {
	if opts.CooldownTicks == 0 && stateful {
		opts.CooldownTicks = defaultSchedulerCooldownTicks
	}
	if opts.MaxOperationsPerStore <= 0 {
		opts.MaxOperationsPerStore = defaultMaxOperationsPerStore
	}
	return opts
}

func mergeOptions(base, override PlanOptions) PlanOptions {
	out := base
	if override.CooldownTicks != 0 {
		out.CooldownTicks = override.CooldownTicks
	}
	if override.MaxOperationsPerStore != 0 {
		out.MaxOperationsPerStore = override.MaxOperationsPerStore
	}
	return normalizeOptions(out, true)
}

func planLeaderTransfer(heartbeatStoreID uint64, snapshot catalog.ClusterSnapshot) (*coordpb.SchedulerOperation, bool) {
	src, dst, ok := selectLeaderRebalancePair(snapshot.Stores, heartbeatStoreID)
	if !ok {
		return nil, false
	}
	return chooseLeaderTransferOperation(snapshot.Regions, src.StoreID, dst.StoreID)
}

func (p *Planner) operationInCooldown(op *coordpb.SchedulerOperation) bool {
	if op == nil {
		return false
	}
	switch op.GetType() {
	case coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER:
		return p.regionInCooldown(op.GetRegionId())
	default:
		return false
	}
}

func (p *Planner) markOperationCooldown(op *coordpb.SchedulerOperation, cooldownTicks uint64) {
	if op == nil || cooldownTicks == 0 {
		return
	}
	until := p.tick + cooldownTicks
	p.cooldown[op.GetRegionId()] = until
}

func (p *Planner) regionInCooldown(regionID uint64) bool {
	if regionID == 0 {
		return false
	}
	return p.cooldown[regionID] >= p.tick
}

func (p *Planner) prune(snapshot catalog.ClusterSnapshot) {
	active := make(map[uint64]struct{}, len(snapshot.Regions))
	for _, region := range snapshot.Regions {
		active[region.Descriptor.RegionID] = struct{}{}
	}
	for regionID, until := range p.cooldown {
		if _, ok := active[regionID]; !ok || until < p.tick {
			delete(p.cooldown, regionID)
		}
	}
}

func selectLeaderRebalancePair(stores []catalog.StoreStats, heartbeatStoreID uint64) (catalog.StoreStats, catalog.StoreStats, bool) {
	if len(stores) < 2 {
		return catalog.StoreStats{}, catalog.StoreStats{}, false
	}
	src := stores[0]
	dst := stores[0]
	for _, st := range stores[1:] {
		if st.LeaderNum > src.LeaderNum || (st.LeaderNum == src.LeaderNum && st.StoreID < src.StoreID) {
			src = st
		}
		if st.LeaderNum < dst.LeaderNum || (st.LeaderNum == dst.LeaderNum && st.StoreID < dst.StoreID) {
			dst = st
		}
	}
	if src.StoreID == 0 || dst.StoreID == 0 || src.StoreID == dst.StoreID {
		return catalog.StoreStats{}, catalog.StoreStats{}, false
	}
	if heartbeatStoreID != src.StoreID {
		return catalog.StoreStats{}, catalog.StoreStats{}, false
	}
	if src.LeaderNum <= dst.LeaderNum+1 {
		return catalog.StoreStats{}, catalog.StoreStats{}, false
	}
	return src, dst, true
}

func chooseLeaderTransferOperation(regions []catalog.RegionInfo, srcStoreID, dstStoreID uint64) (*coordpb.SchedulerOperation, bool) {
	if srcStoreID == 0 || dstStoreID == 0 {
		return nil, false
	}
	for _, region := range regions {
		if op, ok := buildLeaderTransfer(region.Descriptor, srcStoreID, dstStoreID, true); ok {
			return op, true
		}
	}
	for _, region := range regions {
		if op, ok := buildLeaderTransfer(region.Descriptor, srcStoreID, dstStoreID, false); ok {
			return op, true
		}
	}
	return nil, false
}

func buildLeaderTransfer(desc topology.Descriptor, srcStoreID, dstStoreID uint64, requireFirstPeerSrc bool) (*coordpb.SchedulerOperation, bool) {
	if desc.RegionID == 0 || len(desc.Peers) == 0 {
		return nil, false
	}
	if requireFirstPeerSrc && desc.Peers[0].StoreID != srcStoreID {
		return nil, false
	}
	srcPeerID, ok := peerIDOnStore(desc.Peers, srcStoreID)
	if !ok {
		return nil, false
	}
	dstPeerID, ok := peerIDOnStore(desc.Peers, dstStoreID)
	if !ok {
		return nil, false
	}
	if srcPeerID == dstPeerID {
		return nil, false
	}
	return &coordpb.SchedulerOperation{
		Type:         coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER,
		RegionId:     desc.RegionID,
		SourcePeerId: srcPeerID,
		TargetPeerId: dstPeerID,
	}, true
}

func peerIDOnStore(peers []metaregion.Peer, storeID uint64) (uint64, bool) {
	for _, p := range peers {
		if p.StoreID == storeID && p.PeerID != 0 {
			return p.PeerID, true
		}
	}
	return 0, false
}
