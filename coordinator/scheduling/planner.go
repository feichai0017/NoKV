// Package scheduling owns Coordinator-side cluster scheduling policy.
//
// The package consumes disposable Coordinator views and emits store-control
// operations. It must not import coordinator/server or raftstore packages:
// servers own RPC admission, stores own execution, and this package owns only
// policy selection.
package scheduling

import (
	"bytes"
	"sort"
	"sync"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/meta/topology"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

const (
	defaultHotRegionWriteQPS      uint64 = 2_000
	defaultHotRegionWriteBytesPS  uint64 = 8 << 20
	defaultColdRegionWriteQPS     uint64 = 10
	defaultColdRegionReadQPS      uint64 = 10
	defaultMergeMaxApproxBytes    uint64 = 128 << 20
	defaultHotRegionWindows              = 3
	defaultColdRegionWindows             = 5
	defaultSchedulerCooldownTicks uint64 = 3
	defaultMaxOperationsPerStore         = 1
)

type PlanOptions struct {
	HotRegionWriteQPS     uint64
	HotRegionWriteBytesPS uint64
	ColdRegionWriteQPS    uint64
	ColdRegionReadQPS     uint64
	MergeMaxApproxBytes   uint64
	NextID                func() (uint64, bool)
	SplitKey              func(topology.Descriptor) ([]byte, bool)
	HotRegionWindows      int
	ColdRegionWindows     int
	CooldownTicks         uint64
	MaxOperationsPerStore int
}

type Planner struct {
	mu       sync.Mutex
	opts     PlanOptions
	tick     uint64
	hot      map[uint64]int
	cold     map[regionPair]int
	cooldown map[uint64]uint64
}

type regionPair struct {
	left  uint64
	right uint64
}

func NewPlanner(opts PlanOptions) *Planner {
	return &Planner{
		opts:     normalizeOptions(opts, true),
		hot:      make(map[uint64]int),
		cold:     make(map[regionPair]int),
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
// source store. The ordering is deliberate: leader transfer is cheapest and
// does not disturb route epochs, while split and merge mutate rooted topology.
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

func planStoreOperationsImmediate(heartbeatStoreID uint64, snapshot catalog.ClusterSnapshot, opts PlanOptions) []*coordpb.SchedulerOperation {
	if heartbeatStoreID == 0 || len(snapshot.Stores) < 2 {
		return nil
	}
	if op, ok := planLeaderTransfer(heartbeatStoreID, snapshot); ok {
		return []*coordpb.SchedulerOperation{op}
	}
	if op, ok := planHotRegionSplit(heartbeatStoreID, snapshot, opts); ok {
		return []*coordpb.SchedulerOperation{op}
	}
	if op, ok := planColdRegionMerge(heartbeatStoreID, snapshot, opts); ok {
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
	if op, ok := p.planHotRegionSplit(heartbeatStoreID, snapshot, opts); ok {
		ops = append(ops, op)
		p.markOperationCooldown(op, opts.CooldownTicks)
		return ops
	}
	if op, ok := p.planColdRegionMerge(heartbeatStoreID, snapshot, opts); ok {
		ops = append(ops, op)
		p.markOperationCooldown(op, opts.CooldownTicks)
		return ops
	}
	return nil
}

func normalizeOptions(opts PlanOptions, stateful bool) PlanOptions {
	if opts.HotRegionWriteQPS == 0 {
		opts.HotRegionWriteQPS = defaultHotRegionWriteQPS
	}
	if opts.HotRegionWriteBytesPS == 0 {
		opts.HotRegionWriteBytesPS = defaultHotRegionWriteBytesPS
	}
	if opts.ColdRegionWriteQPS == 0 {
		opts.ColdRegionWriteQPS = defaultColdRegionWriteQPS
	}
	if opts.ColdRegionReadQPS == 0 {
		opts.ColdRegionReadQPS = defaultColdRegionReadQPS
	}
	if opts.MergeMaxApproxBytes == 0 {
		opts.MergeMaxApproxBytes = defaultMergeMaxApproxBytes
	}
	if opts.SplitKey == nil {
		opts.SplitKey = midpointSplitKey
	}
	if opts.HotRegionWindows <= 0 {
		if stateful {
			opts.HotRegionWindows = defaultHotRegionWindows
		} else {
			opts.HotRegionWindows = 1
		}
	}
	if opts.ColdRegionWindows <= 0 {
		if stateful {
			opts.ColdRegionWindows = defaultColdRegionWindows
		} else {
			opts.ColdRegionWindows = 1
		}
	}
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
	if override.HotRegionWriteQPS != 0 {
		out.HotRegionWriteQPS = override.HotRegionWriteQPS
	}
	if override.HotRegionWriteBytesPS != 0 {
		out.HotRegionWriteBytesPS = override.HotRegionWriteBytesPS
	}
	if override.ColdRegionWriteQPS != 0 {
		out.ColdRegionWriteQPS = override.ColdRegionWriteQPS
	}
	if override.ColdRegionReadQPS != 0 {
		out.ColdRegionReadQPS = override.ColdRegionReadQPS
	}
	if override.MergeMaxApproxBytes != 0 {
		out.MergeMaxApproxBytes = override.MergeMaxApproxBytes
	}
	if override.NextID != nil {
		out.NextID = override.NextID
	}
	if override.SplitKey != nil {
		out.SplitKey = override.SplitKey
	}
	if override.HotRegionWindows != 0 {
		out.HotRegionWindows = override.HotRegionWindows
	}
	if override.ColdRegionWindows != 0 {
		out.ColdRegionWindows = override.ColdRegionWindows
	}
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

func planHotRegionSplit(heartbeatStoreID uint64, snapshot catalog.ClusterSnapshot, opts PlanOptions) (*coordpb.SchedulerOperation, bool) {
	if opts.NextID == nil {
		return nil, false
	}
	stats := regionStatsByID(snapshot.Stores)
	for _, region := range snapshot.Regions {
		desc := region.Descriptor
		stat := stats[desc.RegionID]
		if stat.RegionID == 0 || stat.PendingAdmin || stat.LeaderStoreID != heartbeatStoreID {
			continue
		}
		if stat.WriteQPS < opts.HotRegionWriteQPS && stat.WriteBytesPerSecond < opts.HotRegionWriteBytesPS {
			continue
		}
		splitKey, ok := opts.SplitKey(desc)
		if !ok || !splitKeyInsideParent(desc, splitKey) {
			continue
		}
		child, ok := buildSplitChild(desc, splitKey, opts.NextID)
		if !ok {
			continue
		}
		return &coordpb.SchedulerOperation{
			Type:       coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_SPLIT_REGION,
			RegionId:   desc.RegionID,
			SplitKey:   splitKey,
			SplitChild: metawire.DescriptorToProto(child),
		}, true
	}
	return nil, false
}

func planColdRegionMerge(heartbeatStoreID uint64, snapshot catalog.ClusterSnapshot, opts PlanOptions) (*coordpb.SchedulerOperation, bool) {
	stats := regionStatsByID(snapshot.Stores)
	regions := append([]catalog.RegionInfo(nil), snapshot.Regions...)
	sortRegionsByStart(regions)
	for i := 0; i+1 < len(regions); i++ {
		left := regions[i].Descriptor
		right := regions[i+1].Descriptor
		leftStat := stats[left.RegionID]
		rightStat := stats[right.RegionID]
		if leftStat.RegionID == 0 || rightStat.RegionID == 0 {
			continue
		}
		if leftStat.PendingAdmin || rightStat.PendingAdmin {
			continue
		}
		if leftStat.LeaderStoreID != heartbeatStoreID {
			continue
		}
		if !bytes.Equal(left.EndKey, right.StartKey) || !samePeers(left.Peers, right.Peers) {
			continue
		}
		if leftStat.WriteQPS > opts.ColdRegionWriteQPS || rightStat.WriteQPS > opts.ColdRegionWriteQPS ||
			leftStat.ReadQPS > opts.ColdRegionReadQPS || rightStat.ReadQPS > opts.ColdRegionReadQPS {
			continue
		}
		if leftStat.ApproxRegionBytes == 0 && rightStat.ApproxRegionBytes == 0 {
			continue
		}
		if leftStat.ApproxRegionBytes+rightStat.ApproxRegionBytes > opts.MergeMaxApproxBytes {
			continue
		}
		return &coordpb.SchedulerOperation{
			Type:           coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_MERGE_REGION,
			RegionId:       left.RegionID,
			SourceRegionId: right.RegionID,
		}, true
	}
	return nil, false
}

func (p *Planner) planHotRegionSplit(heartbeatStoreID uint64, snapshot catalog.ClusterSnapshot, opts PlanOptions) (*coordpb.SchedulerOperation, bool) {
	stats := regionStatsByID(snapshot.Stores)
	for _, region := range snapshot.Regions {
		desc := region.Descriptor
		stat := stats[desc.RegionID]
		if !hotSplitCandidate(heartbeatStoreID, desc, stat, opts) || p.regionInCooldown(desc.RegionID) {
			p.hot[desc.RegionID] = 0
			continue
		}
		p.hot[desc.RegionID]++
		if p.hot[desc.RegionID] < opts.HotRegionWindows {
			continue
		}
		op, ok := buildHotRegionSplitOperation(desc, opts)
		if !ok {
			continue
		}
		p.hot[desc.RegionID] = 0
		return op, true
	}
	return nil, false
}

func (p *Planner) planColdRegionMerge(heartbeatStoreID uint64, snapshot catalog.ClusterSnapshot, opts PlanOptions) (*coordpb.SchedulerOperation, bool) {
	stats := regionStatsByID(snapshot.Stores)
	regions := append([]catalog.RegionInfo(nil), snapshot.Regions...)
	sortRegionsByStart(regions)
	for i := 0; i+1 < len(regions); i++ {
		left := regions[i].Descriptor
		right := regions[i+1].Descriptor
		pair := regionPair{left: left.RegionID, right: right.RegionID}
		if !coldMergeCandidate(heartbeatStoreID, left, right, stats[left.RegionID], stats[right.RegionID], opts) ||
			p.regionInCooldown(left.RegionID) || p.regionInCooldown(right.RegionID) {
			p.cold[pair] = 0
			continue
		}
		p.cold[pair]++
		if p.cold[pair] < opts.ColdRegionWindows {
			continue
		}
		p.cold[pair] = 0
		return buildColdRegionMergeOperation(left, right), true
	}
	return nil, false
}

func hotSplitCandidate(heartbeatStoreID uint64, desc topology.Descriptor, stat catalog.RegionStats, opts PlanOptions) bool {
	if stat.RegionID == 0 || stat.PendingAdmin || stat.LeaderStoreID != heartbeatStoreID {
		return false
	}
	return stat.WriteQPS >= opts.HotRegionWriteQPS || stat.WriteBytesPerSecond >= opts.HotRegionWriteBytesPS
}

func buildHotRegionSplitOperation(desc topology.Descriptor, opts PlanOptions) (*coordpb.SchedulerOperation, bool) {
	if opts.NextID == nil {
		return nil, false
	}
	splitKey, ok := opts.SplitKey(desc)
	if !ok || !splitKeyInsideParent(desc, splitKey) {
		return nil, false
	}
	child, ok := buildSplitChild(desc, splitKey, opts.NextID)
	if !ok {
		return nil, false
	}
	return &coordpb.SchedulerOperation{
		Type:       coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_SPLIT_REGION,
		RegionId:   desc.RegionID,
		SplitKey:   splitKey,
		SplitChild: metawire.DescriptorToProto(child),
	}, true
}

func coldMergeCandidate(heartbeatStoreID uint64, left, right topology.Descriptor, leftStat, rightStat catalog.RegionStats, opts PlanOptions) bool {
	if leftStat.RegionID == 0 || rightStat.RegionID == 0 {
		return false
	}
	if leftStat.PendingAdmin || rightStat.PendingAdmin {
		return false
	}
	if leftStat.LeaderStoreID != heartbeatStoreID {
		return false
	}
	if !bytes.Equal(left.EndKey, right.StartKey) || !samePeers(left.Peers, right.Peers) {
		return false
	}
	if leftStat.WriteQPS > opts.ColdRegionWriteQPS || rightStat.WriteQPS > opts.ColdRegionWriteQPS ||
		leftStat.ReadQPS > opts.ColdRegionReadQPS || rightStat.ReadQPS > opts.ColdRegionReadQPS {
		return false
	}
	if leftStat.ApproxRegionBytes == 0 && rightStat.ApproxRegionBytes == 0 {
		return false
	}
	return leftStat.ApproxRegionBytes+rightStat.ApproxRegionBytes <= opts.MergeMaxApproxBytes
}

func buildColdRegionMergeOperation(left, right topology.Descriptor) *coordpb.SchedulerOperation {
	return &coordpb.SchedulerOperation{
		Type:           coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_MERGE_REGION,
		RegionId:       left.RegionID,
		SourceRegionId: right.RegionID,
	}
}

func (p *Planner) operationInCooldown(op *coordpb.SchedulerOperation) bool {
	if op == nil {
		return false
	}
	switch op.GetType() {
	case coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER,
		coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_SPLIT_REGION:
		return p.regionInCooldown(op.GetRegionId())
	case coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_MERGE_REGION:
		return p.regionInCooldown(op.GetRegionId()) || p.regionInCooldown(op.GetSourceRegionId())
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
	if op.GetSourceRegionId() != 0 {
		p.cooldown[op.GetSourceRegionId()] = until
	}
}

func (p *Planner) regionInCooldown(regionID uint64) bool {
	if regionID == 0 {
		return false
	}
	return p.cooldown[regionID] > p.tick
}

func (p *Planner) prune(snapshot catalog.ClusterSnapshot) {
	active := make(map[uint64]struct{}, len(snapshot.Regions))
	for _, region := range snapshot.Regions {
		active[region.Descriptor.RegionID] = struct{}{}
	}
	for regionID := range p.hot {
		if _, ok := active[regionID]; !ok {
			delete(p.hot, regionID)
		}
	}
	for pair := range p.cold {
		if _, ok := active[pair.left]; !ok {
			delete(p.cold, pair)
			continue
		}
		if _, ok := active[pair.right]; !ok {
			delete(p.cold, pair)
		}
	}
	for regionID, until := range p.cooldown {
		if _, ok := active[regionID]; !ok || until <= p.tick {
			delete(p.cooldown, regionID)
		}
	}
}

func SplitKeyFromBoundaries(boundaries [][]byte) func(topology.Descriptor) ([]byte, bool) {
	clean := make([][]byte, 0, len(boundaries))
	for _, boundary := range boundaries {
		if len(boundary) == 0 {
			continue
		}
		clean = append(clean, append([]byte(nil), boundary...))
	}
	sort.Slice(clean, func(i, j int) bool { return bytes.Compare(clean[i], clean[j]) < 0 })
	return func(desc topology.Descriptor) ([]byte, bool) {
		candidates := make([][]byte, 0, len(clean))
		for _, boundary := range clean {
			if splitKeyInsideParent(desc, boundary) {
				candidates = append(candidates, boundary)
			}
		}
		if len(candidates) == 0 {
			return nil, false
		}
		return append([]byte(nil), candidates[len(candidates)/2]...), true
	}
}

func buildSplitChild(parent topology.Descriptor, splitKey []byte, nextID func() (uint64, bool)) (topology.Descriptor, bool) {
	regionID, ok := nextID()
	if !ok || regionID == 0 {
		return topology.Descriptor{}, false
	}
	child := parent.Clone()
	child.RegionID = regionID
	child.StartKey = append([]byte(nil), splitKey...)
	child.EndKey = append([]byte(nil), parent.EndKey...)
	child.Hash = nil
	child.Lineage = nil
	child.Peers = make([]metaregion.Peer, 0, len(parent.Peers))
	for _, peer := range parent.Peers {
		peerID, ok := nextID()
		if !ok || peerID == 0 {
			return topology.Descriptor{}, false
		}
		child.Peers = append(child.Peers, metaregion.Peer{StoreID: peer.StoreID, PeerID: peerID})
	}
	child.EnsureHash()
	return child, true
}

func splitKeyInsideParent(parent topology.Descriptor, splitKey []byte) bool {
	if len(splitKey) == 0 {
		return false
	}
	if bytes.Compare(splitKey, parent.StartKey) <= 0 {
		return false
	}
	return len(parent.EndKey) == 0 || bytes.Compare(splitKey, parent.EndKey) < 0
}

func regionStatsByID(stores []catalog.StoreStats) map[uint64]catalog.RegionStats {
	out := make(map[uint64]catalog.RegionStats)
	for _, store := range stores {
		for _, stat := range store.RegionStats {
			if stat.RegionID == 0 {
				continue
			}
			current := out[stat.RegionID]
			current.RegionID = stat.RegionID
			current.ReadQPS += stat.ReadQPS
			current.WriteQPS += stat.WriteQPS
			current.WriteBytesPerSecond += stat.WriteBytesPerSecond
			current.ApproxRegionBytes += stat.ApproxRegionBytes
			current.AtomicMutateQPS += stat.AtomicMutateQPS
			current.PendingAdmin = current.PendingAdmin || stat.PendingAdmin
			if stat.LeaderStoreID != 0 {
				current.LeaderStoreID = stat.LeaderStoreID
			}
			out[stat.RegionID] = current
		}
	}
	return out
}

func sortRegionsByStart(regions []catalog.RegionInfo) {
	for i := 1; i < len(regions); i++ {
		current := regions[i]
		j := i - 1
		for ; j >= 0 && bytes.Compare(regions[j].Descriptor.StartKey, current.Descriptor.StartKey) > 0; j-- {
			regions[j+1] = regions[j]
		}
		regions[j+1] = current
	}
}

func samePeers(a, b []metaregion.Peer) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].StoreID != b[i].StoreID {
			return false
		}
	}
	return true
}

func midpointSplitKey(desc topology.Descriptor) ([]byte, bool) {
	start, end := desc.StartKey, desc.EndKey
	if len(start) == 0 || len(end) == 0 || bytes.Compare(start, end) >= 0 {
		return nil, false
	}
	maxLen := max(len(start), len(end))
	left := make([]byte, maxLen+1)
	right := make([]byte, maxLen+1)
	copy(left, start)
	copy(right, end)
	var carry uint16
	mid := make([]byte, len(left))
	for i := len(left) - 1; i >= 0; i-- {
		sum := uint16(left[i]) + uint16(right[i]) + carry*256
		mid[i] = byte(sum / 2)
		carry = sum % 2
	}
	mid = bytes.TrimRight(mid, "\x00")
	if len(mid) == 0 || bytes.Compare(mid, start) <= 0 || bytes.Compare(mid, end) >= 0 {
		return nil, false
	}
	return mid, true
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
