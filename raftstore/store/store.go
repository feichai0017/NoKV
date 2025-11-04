package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/scheduler"
	raftpb "go.etcd.io/raft/v3/raftpb"
	proto "google.golang.org/protobuf/proto"
)

// Store hosts a collection of peers and provides helpers inspired by
// TinyKV's raftstore::Store structure. It wires peers to the router, exposes
// lifecycle hooks, and allows higher layers (RPC, schedulers, tests) to drive
// ticks or proposals without needing to keep global peer maps themselves.
type Store struct {
	mu                sync.RWMutex
	router            *Router
	peers             map[uint64]*peer.Peer
	peerFactory       PeerFactory
	peerBuilder       PeerBuilder
	hooks             LifecycleHooks
	regionHooks       RegionHooks
	regionMetrics     *RegionMetrics
	manifest          *manifest.Manager
	regions           *regionManager
	scheduler         scheduler.RegionSink
	heartbeatInterval time.Duration
	heartbeatStop     chan struct{}
	heartbeatWG       sync.WaitGroup
	storeID           uint64
	planner           scheduler.Planner
	operationHook     func(scheduler.Operation)
	operationInput    chan scheduler.Operation
	operationStop     chan struct{}
	operationWG       sync.WaitGroup
	operationCooldown time.Duration
	operationInterval time.Duration
	operationBurst    int
	operationMu       sync.Mutex
	lastApplied       map[operationKey]time.Time
	pendingOps        map[operationKey]struct{}
	commandApplier    func(*pb.RaftCmdRequest) (*pb.RaftCmdResponse, error)
	proposalSeq       uint64
	proposalMu        sync.Mutex
	proposals         map[uint64]*commandProposal
}

type operationKey struct {
	region uint64
	typeID scheduler.OperationType
}

// PeerHandle is a lightweight view of a peer registered with the store. It is
// designed for diagnostics and scheduling components so they can iterate over
// the cluster topology without touching the internal map directly.
type PeerHandle struct {
	ID     uint64
	Peer   *peer.Peer
	Region *manifest.RegionMeta
}

// RegionSnapshot provides an external view of the tracked Region metadata.
type RegionSnapshot struct {
	Regions []manifest.RegionMeta `json:"regions"`
}

// NewStore creates a Store with the provided router. When router is nil a new
// instance is allocated implicitly so callers can skip the explicit
// construction in tests.
func NewStore(router *Router) *Store {
	return NewStoreWithConfig(Config{Router: router})
}

// NewStoreWithConfig allows callers to supply a custom PeerFactory and
// LifecycleHooks when creating a store. This mirrors TinyKV's configurable
// raftstore bootstrap pipeline where schedulers wire themselves into peer
// lifecycle events.
func NewStoreWithConfig(cfg Config) *Store {
	router := cfg.Router
	if router == nil {
		router = NewRouter()
	}
	factory := cfg.PeerFactory
	if factory == nil {
		factory = peer.NewPeer
	}
	metrics := NewRegionMetrics()
	hookChain := []RegionHooks{metrics.Hooks()}
	if cfg.Scheduler != nil {
		hookChain = append(hookChain, RegionHooks{
			OnRegionUpdate: cfg.Scheduler.SubmitRegionHeartbeat,
			OnRegionRemove: cfg.Scheduler.RemoveRegion,
		})
	}
	hookChain = append(hookChain, cfg.RegionHooks)
	combinedHooks := mergeRegionHooks(hookChain...)
	planner := cfg.Planner
	if planner == nil {
		planner = scheduler.NoopPlanner{}
	}
	queueSize := cfg.OperationQueueSize
	if queueSize < 0 {
		queueSize = 0
	}
	operationCooldown := cfg.OperationCooldown
	if operationCooldown < 0 {
		operationCooldown = 0
	}
	operationInterval := cfg.OperationInterval
	if operationInterval <= 0 {
		operationInterval = cfg.HeartbeatInterval
	}
	if operationInterval <= 0 {
		operationInterval = 200 * time.Millisecond
	}
	operationBurst := cfg.OperationBurst
	if operationBurst < 0 {
		operationBurst = 0
	}
	if operationBurst == 0 {
		operationBurst = 4
	}
	if operationCooldown == 0 {
		operationCooldown = 5 * time.Second
	}
	s := &Store{
		router:            router,
		peers:             make(map[uint64]*peer.Peer),
		peerFactory:       factory,
		peerBuilder:       cfg.PeerBuilder,
		hooks:             cfg.Hooks,
		regionHooks:       combinedHooks,
		regionMetrics:     metrics,
		manifest:          cfg.Manifest,
		regions:           newRegionManager(),
		scheduler:         cfg.Scheduler,
		heartbeatInterval: cfg.HeartbeatInterval,
		storeID:           cfg.StoreID,
		planner:           planner,
		operationHook:     cfg.OperationObserver,
		operationCooldown: operationCooldown,
		operationInterval: operationInterval,
		operationBurst:    operationBurst,
		commandApplier:    cfg.CommandApplier,
		proposals:         make(map[uint64]*commandProposal),
	}
	if cfg.Manifest != nil {
		s.regions.loadSnapshot(cfg.Manifest.RegionSnapshot())
	}
	if s.scheduler != nil {
		if s.heartbeatInterval <= 0 {
			s.heartbeatInterval = 3 * time.Second
		}
	}
	s.heartbeatStop = make(chan struct{})
	if planner != nil && queueSize > 0 {
		s.operationInput = make(chan scheduler.Operation, queueSize)
		s.operationStop = make(chan struct{})
		s.lastApplied = make(map[operationKey]time.Time)
		s.pendingOps = make(map[operationKey]struct{})
		s.operationWG.Add(1)
		go s.operationWorker()
	}
	RegisterStore(s)
	if s.scheduler != nil {
		s.startHeartbeatLoop()
	}
	return s
}

type commandProposal struct {
	ch chan proposalResult
}

type proposalResult struct {
	resp *pb.RaftCmdResponse
	err  error
}

// SplitRegion updates the parent region metadata and bootstraps a new peer for
// the child region. The child metadata must describe the desired child region
// (key range, peers, epoch).
func (s *Store) SplitRegion(parentID uint64, childMeta manifest.RegionMeta) (*peer.Peer, error) {
	if s == nil {
		return nil, fmt.Errorf("raftstore: store is nil")
	}
	if parentID == 0 {
		return nil, fmt.Errorf("raftstore: parent region id is zero")
	}
	childMeta = manifest.CloneRegionMeta(childMeta)
	if childMeta.ID == 0 {
		return nil, fmt.Errorf("raftstore: child region id is zero")
	}
	if len(childMeta.StartKey) == 0 {
		return nil, fmt.Errorf("raftstore: child region start key required")
	}
	parentMeta, ok := s.RegionMetaByID(parentID)
	if !ok {
		return nil, fmt.Errorf("raftstore: parent region %d not found", parentID)
	}
	originalParent := manifest.CloneRegionMeta(parentMeta)
	if len(parentMeta.EndKey) > 0 && bytes.Compare(childMeta.StartKey, parentMeta.EndKey) >= 0 {
		return nil, fmt.Errorf("raftstore: split key >= parent end key")
	}
	if bytes.Compare(childMeta.StartKey, parentMeta.StartKey) <= 0 {
		return nil, fmt.Errorf("raftstore: split key must be greater than parent start key")
	}
	newParent := parentMeta
	newParent.EndKey = append([]byte(nil), childMeta.StartKey...)
	newParent.Epoch.Version++
	if err := s.UpdateRegion(newParent); err != nil {
		return nil, err
	}
	if childMeta.State == 0 {
		childMeta.State = manifest.RegionStateRunning
	}
	cfg, bootstrapPeers, err := s.buildChildPeerConfig(childMeta)
	if err != nil {
		_ = s.UpdateRegion(originalParent)
		return nil, err
	}
	childPeer, err := s.StartPeer(cfg, bootstrapPeers)
	if err != nil {
		_ = s.UpdateRegion(originalParent)
		return nil, err
	}
	return childPeer, nil
}

// ProposeSplit issues a split command through the raft log of the parent
// region. The child metadata must describe the new region configuration.
func (s *Store) ProposeSplit(parentID uint64, childMeta manifest.RegionMeta, splitKey []byte) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if parentID == 0 || childMeta.ID == 0 {
		return fmt.Errorf("raftstore: invalid region identifiers")
	}
	parentPeer := s.regions.peer(parentID)
	if parentPeer == nil {
		return fmt.Errorf("raftstore: region %d not hosted on this store", parentID)
	}
	if status := parentPeer.Status(); status.RaftState != myraft.StateLeader {
		return fmt.Errorf("raftstore: peer %d is not leader", parentPeer.ID())
	}
	cmd := &pb.AdminCommand{
		Type: pb.AdminCommand_SPLIT,
		Split: &pb.SplitCommand{
			ParentRegionId: parentID,
			SplitKey:       append([]byte(nil), splitKey...),
			Child:          regionMetaToPB(childMeta),
		},
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	return parentPeer.ProposeAdmin(data)
}

// ProposeMerge submits a merge admin command merging source region into target.
func (s *Store) ProposeMerge(targetRegionID, sourceRegionID uint64) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if targetRegionID == 0 || sourceRegionID == 0 {
		return fmt.Errorf("raftstore: invalid region identifiers")
	}
	peer := s.regions.peer(targetRegionID)
	if peer == nil {
		return fmt.Errorf("raftstore: region %d not hosted on this store", targetRegionID)
	}
	if status := peer.Status(); status.RaftState != myraft.StateLeader {
		return fmt.Errorf("raftstore: peer %d is not leader", peer.ID())
	}
	cmd := &pb.AdminCommand{
		Type: pb.AdminCommand_MERGE,
		Merge: &pb.MergeCommand{
			TargetRegionId: targetRegionID,
			SourceRegionId: sourceRegionID,
		},
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	return peer.ProposeAdmin(data)
}

func (s *Store) buildChildPeerConfig(child manifest.RegionMeta) (*peer.Config, []myraft.Peer, error) {
	if s.peerBuilder == nil {
		return nil, nil, fmt.Errorf("raftstore: peer builder not configured")
	}
	cfg, err := s.peerBuilder(child)
	if err != nil {
		return nil, nil, err
	}
	if cfg == nil {
		return nil, nil, fmt.Errorf("raftstore: peer builder returned nil config")
	}
	if cfg.Region == nil {
		cfg.Region = &child
	}
	peers := make([]myraft.Peer, 0, len(child.Peers))
	for _, peerMeta := range child.Peers {
		peers = append(peers, myraft.Peer{ID: peerMeta.PeerID})
	}
	return cfg, peers, nil
}

func (s *Store) handleAdminCommand(cmd *pb.AdminCommand) error {
	if s == nil || cmd == nil {
		return nil
	}
	switch cmd.Type {
	case pb.AdminCommand_SPLIT:
		return s.handleSplitCommand(cmd.Split)
	case pb.AdminCommand_MERGE:
		return s.handleMergeCommand(cmd.Merge)
	default:
		return nil
	}
}

func (s *Store) handleSplitCommand(split *pb.SplitCommand) error {
	if split == nil {
		return fmt.Errorf("raftstore: split command missing payload")
	}
	childMeta := pbRegionMetaToManifest(split.GetChild())
	childMeta.State = manifest.RegionStateRunning
	if len(childMeta.StartKey) == 0 {
		childMeta.StartKey = append([]byte(nil), split.GetSplitKey()...)
	}
	_, err := s.SplitRegion(split.GetParentRegionId(), childMeta)
	return err
}

func (s *Store) handleMergeCommand(merge *pb.MergeCommand) error {
	if merge == nil {
		return fmt.Errorf("raftstore: merge command missing payload")
	}
	parentMeta, ok := s.RegionMetaByID(merge.GetTargetRegionId())
	if !ok {
		return fmt.Errorf("raftstore: target region %d not found", merge.GetTargetRegionId())
	}
	sourceMeta, ok := s.RegionMetaByID(merge.GetSourceRegionId())
	if !ok {
		return fmt.Errorf("raftstore: source region %d not found", merge.GetSourceRegionId())
	}
	updated := parentMeta
	updated.Epoch.Version++
	if len(sourceMeta.EndKey) == 0 || bytes.Compare(sourceMeta.EndKey, updated.EndKey) > 0 {
		updated.EndKey = append([]byte(nil), sourceMeta.EndKey...)
	}
	if err := s.UpdateRegion(updated); err != nil {
		return err
	}
	if peer := s.regions.peer(sourceMeta.ID); peer != nil {
		s.StopPeer(peer.ID())
	}
	if err := s.RemoveRegion(sourceMeta.ID); err != nil {
		return err
	}
	return nil
}

func regionMetaToPB(meta manifest.RegionMeta) *pb.RegionMeta {
	peers := make([]*pb.RegionPeer, 0, len(meta.Peers))
	for _, p := range meta.Peers {
		peers = append(peers, &pb.RegionPeer{StoreId: p.StoreID, PeerId: p.PeerID})
	}
	return &pb.RegionMeta{
		Id:               meta.ID,
		StartKey:         append([]byte(nil), meta.StartKey...),
		EndKey:           append([]byte(nil), meta.EndKey...),
		EpochVersion:     meta.Epoch.Version,
		EpochConfVersion: meta.Epoch.ConfVersion,
		Peers:            peers,
	}
}

func pbRegionMetaToManifest(pbMeta *pb.RegionMeta) manifest.RegionMeta {
	if pbMeta == nil {
		return manifest.RegionMeta{}
	}
	meta := manifest.RegionMeta{
		ID:       pbMeta.GetId(),
		StartKey: append([]byte(nil), pbMeta.GetStartKey()...),
		EndKey:   append([]byte(nil), pbMeta.GetEndKey()...),
		Epoch: manifest.RegionEpoch{
			Version:     pbMeta.GetEpochVersion(),
			ConfVersion: pbMeta.GetEpochConfVersion(),
		},
	}
	for _, peerPB := range pbMeta.GetPeers() {
		meta.Peers = append(meta.Peers, manifest.PeerMeta{
			StoreID: peerPB.GetStoreId(),
			PeerID:  peerPB.GetPeerId(),
		})
	}
	return meta
}

// Router exposes the underlying router reference so transports can reuse the
// same registration hub.
func (s *Store) Router() *Router {
	if s == nil {
		return nil
	}
	return s.router
}

// SetPeerFactory overrides the peer constructor used for subsequent
// StartPeer calls. It is safe to invoke at runtime, enabling tests to inject
// failpoints or custom peer implementations.
func (s *Store) SetPeerFactory(factory PeerFactory) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.peerFactory = factory
	s.mu.Unlock()
}

// StartPeer builds and registers a peer according to the supplied
// configuration. The peer is automatically registered with the Store router.
// When bootstrapPeers is non-empty StartPeer will call Bootstrap with those
// peers after the peer is registered.
func (s *Store) StartPeer(cfg *peer.Config, bootstrapPeers []myraft.Peer) (*peer.Peer, error) {
	if s == nil {
		return nil, fmt.Errorf("raftstore: store is nil")
	}
	if cfg == nil {
		return nil, fmt.Errorf("raftstore: peer config is nil")
	}
	var regionMeta *manifest.RegionMeta
	if cfg.Region != nil {
		if cfg.Region.State == 0 {
			cfg.Region.State = manifest.RegionStateRunning
		}
		regionMeta = manifest.CloneRegionMetaPtr(cfg.Region)
	}
	factory := s.peerFactory
	if factory == nil {
		factory = peer.NewPeer
	}
	cfgCopy := *cfg
	cfgCopy.ConfChange = s.handlePeerConfChange
	if cfgCopy.AdminApply == nil {
		cfgCopy.AdminApply = s.handleAdminCommand
	}
	legacyApply := cfgCopy.Apply
	cfgCopy.Apply = func(entries []myraft.Entry) error {
		return s.applyEntries(entries, legacyApply)
	}
	p, err := factory(&cfgCopy)
	if err != nil {
		return nil, err
	}
	id := p.ID()
	s.mu.Lock()
	if _, exists := s.peers[id]; exists {
		s.mu.Unlock()
		p.Close()
		return nil, fmt.Errorf("raftstore: peer %d already exists", id)
	}
	s.peers[id] = p
	if regionMeta != nil && regionMeta.ID != 0 {
		s.regions.setPeer(regionMeta.ID, p)
	}
	s.mu.Unlock()

	if err := s.router.Register(p); err != nil {
		s.mu.Lock()
		delete(s.peers, id)
		s.mu.Unlock()
		p.Close()
		return nil, err
	}
	if regionMeta != nil {
		if err := s.UpdateRegion(*regionMeta); err != nil {
			s.router.Deregister(id)
			s.mu.Lock()
			delete(s.peers, id)
			s.mu.Unlock()
			s.regions.setPeer(regionMeta.ID, nil)
			p.Close()
			return nil, err
		}
	}
	if hook := s.hooks.OnPeerStart; hook != nil {
		hook(p)
	}
	if len(bootstrapPeers) > 0 {
		if err := p.Bootstrap(bootstrapPeers); err != nil {
			s.StopPeer(id)
			return nil, err
		}
	}
	return p, nil
}

// StopPeer removes the peer from the router and closes it.
func (s *Store) StopPeer(id uint64) {
	if s == nil || id == 0 {
		return
	}
	s.router.Deregister(id)
	var regionID uint64
	s.mu.Lock()
	p := s.peers[id]
	delete(s.peers, id)
	if p != nil {
		if meta := p.RegionMeta(); meta != nil {
			regionID = meta.ID
		}
	}
	s.mu.Unlock()
	if regionID != 0 {
		s.regions.setPeer(regionID, nil)
		_ = s.UpdateRegionState(regionID, manifest.RegionStateRemoving)
	}
	if hook := s.hooks.OnPeerStop; hook != nil && p != nil {
		hook(p)
	}
	if p != nil {
		p.Close()
	}
}

// Close stops background workers associated with the store.
func (s *Store) Close() {
	if s == nil {
		return
	}
	if s.heartbeatStop != nil {
		close(s.heartbeatStop)
	}
	s.heartbeatWG.Wait()
	if s.operationStop != nil {
		close(s.operationStop)
	}
	s.operationWG.Wait()
}

func (s *Store) startHeartbeatLoop() {
	if s == nil || s.scheduler == nil || s.heartbeatInterval <= 0 {
		return
	}
	for _, meta := range s.RegionMetas() {
		s.scheduler.SubmitRegionHeartbeat(meta)
	}
	if s.storeID != 0 {
		s.scheduler.SubmitStoreHeartbeat(s.storeStatsSnapshot())
	}
	s.processPlanner()
	s.heartbeatWG.Add(1)
	go func() {
		defer s.heartbeatWG.Done()
		ticker := time.NewTicker(s.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				metas := s.RegionMetas()
				for _, meta := range metas {
					s.scheduler.SubmitRegionHeartbeat(meta)
				}
				if s.storeID != 0 {
					s.scheduler.SubmitStoreHeartbeat(s.storeStatsSnapshot())
				}
				s.processPlanner()
			case <-s.heartbeatStop:
				return
			}
		}
	}()
}

func (s *Store) processPlanner() {
	if s == nil || s.planner == nil {
		return
	}
	snapshot := s.SchedulerSnapshot()
	ops := s.planner.Plan(snapshot)
	for _, op := range ops {
		s.enqueueOperation(op)
	}
}

func (s *Store) applyOperation(op scheduler.Operation) {
	if s == nil {
		return
	}
	var applied bool
	switch op.Type {
	case scheduler.OperationLeaderTransfer:
		if op.Source == 0 || op.Target == 0 {
			break
		}
		s.VisitPeers(func(p *peer.Peer) {
			if p.ID() == op.Source {
				_ = p.TransferLeader(op.Target)
			}
		})
		applied = true
	}
	if hook := s.operationHook; hook != nil && applied {
		hook(op)
	}
}

func (s *Store) applyEntries(entries []myraft.Entry, fallback peer.ApplyFunc) error {
	for _, entry := range entries {
		if entry.Type != myraft.EntryNormal {
			if fallback != nil {
				if err := fallback([]myraft.Entry{entry}); err != nil {
					return err
				}
			}
			continue
		}
		if len(entry.Data) == 0 {
			continue
		}
		req, isCmd, err := command.Decode(entry.Data)
		if err != nil {
			return err
		}
		if !isCmd {
			if fallback != nil {
				if err := fallback([]myraft.Entry{entry}); err != nil {
					return err
				}
			}
			continue
		}
		if s.commandApplier == nil {
			if fallback != nil {
				if err := fallback([]myraft.Entry{entry}); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("raftstore: command apply without handler")
		}
		resp, err := s.commandApplier(req)
		if err != nil {
			s.completeProposal(req.GetHeader().GetRequestId(), nil, err)
			continue
		}
		s.completeProposal(req.GetHeader().GetRequestId(), resp, nil)
	}
	return nil
}

type scheduledOperation struct {
	op    scheduler.Operation
	ready time.Time
}

func (s *Store) enqueueOperation(op scheduler.Operation) {
	if s == nil {
		return
	}
	if op.Type == scheduler.OperationNone || op.Region == 0 {
		return
	}
	if s.operationInput == nil {
		s.applyOperation(op)
		return
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	s.operationMu.Lock()
	if _, exists := s.pendingOps[key]; exists {
		s.operationMu.Unlock()
		return
	}
	s.pendingOps[key] = struct{}{}
	s.operationMu.Unlock()
	select {
	case s.operationInput <- op:
	default:
		s.operationMu.Lock()
		delete(s.pendingOps, key)
		s.operationMu.Unlock()
	}
}

func (s *Store) operationWorker() {
	defer s.operationWG.Done()
	interval := s.operationInterval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	pending := make([]scheduledOperation, 0)
	for {
		select {
		case <-s.operationStop:
			return
		case op := <-s.operationInput:
			pending = append(pending, scheduledOperation{op: op, ready: s.nextReadyTime(op)})
		case <-ticker.C:
			now := time.Now()
			limit := s.operationBurst
			if limit <= 0 {
				limit = len(pending)
			}
			executed := 0
			var remaining []scheduledOperation
			for _, item := range pending {
				if limit > 0 && executed >= limit {
					remaining = append(remaining, item)
					continue
				}
				if !item.ready.IsZero() && item.ready.After(now) {
					remaining = append(remaining, item)
					continue
				}
				s.applyOperation(item.op)
				s.markOperationApplied(item.op, now)
				executed++
			}
			pending = remaining
		}
	}
}

func (s *Store) nextReadyTime(op scheduler.Operation) time.Time {
	if s == nil {
		return time.Time{}
	}
	cooldown := s.operationCooldown
	if cooldown <= 0 {
		return time.Time{}
	}
	key := operationKey{region: op.Region, typeID: op.Type}
	s.operationMu.Lock()
	last := s.lastApplied[key]
	s.operationMu.Unlock()
	if last.IsZero() {
		return time.Time{}
	}
	ready := last.Add(cooldown)
	return ready
}

func (s *Store) markOperationApplied(op scheduler.Operation, appliedAt time.Time) {
	key := operationKey{region: op.Region, typeID: op.Type}
	if appliedAt.IsZero() {
		appliedAt = time.Now()
	}
	s.operationMu.Lock()
	s.lastApplied[key] = appliedAt
	delete(s.pendingOps, key)
	s.operationMu.Unlock()
}

func (s *Store) storeStatsSnapshot() scheduler.StoreStats {
	stats := scheduler.StoreStats{
		StoreID:   s.storeID,
		RegionNum: uint64(len(s.RegionMetas())),
		LeaderNum: s.countLeaders(),
	}
	if capacity, available, ok := s.diskStats(); ok {
		stats.Capacity = capacity
		stats.Available = available
	}
	return stats
}

func (s *Store) countLeaders() uint64 {
	if s == nil {
		return 0
	}
	var leaders uint64
	s.VisitPeers(func(p *peer.Peer) {
		if p.Status().RaftState == myraft.StateLeader {
			leaders++
		}
	})
	return leaders
}

// SchedulerSnapshot returns the scheduler snapshot if the store is connected to
// a coordinator that implements SnapshotProvider. When unavailable, an empty
// snapshot is returned.
func (s *Store) SchedulerSnapshot() scheduler.Snapshot {
	if s == nil {
		return scheduler.Snapshot{}
	}
	snap := scheduler.Snapshot{}
	if provider, ok := s.scheduler.(scheduler.SnapshotProvider); ok {
		regions := provider.RegionSnapshot()
		stores := provider.StoreSnapshot()
		snap.Stores = append(snap.Stores, stores...)
		for _, info := range regions {
			snap.Regions = append(snap.Regions, s.buildRegionDescriptor(info))
		}
	}
	return snap
}

func (s *Store) buildRegionDescriptor(info scheduler.RegionInfo) scheduler.RegionDescriptor {
	meta := info.Meta
	desc := scheduler.RegionDescriptor{
		ID:            meta.ID,
		StartKey:      append([]byte(nil), meta.StartKey...),
		EndKey:        append([]byte(nil), meta.EndKey...),
		Epoch:         meta.Epoch,
		LastHeartbeat: info.LastHeartbeat,
	}
	var leaderPeerID uint64
	if local := s.regions.peer(meta.ID); local != nil {
		if local.Status().RaftState == myraft.StateLeader {
			leaderPeerID = local.ID()
		}
	}
	if !info.LastHeartbeat.IsZero() {
		lag := time.Since(info.LastHeartbeat)
		if lag < 0 {
			lag = 0
		}
		desc.Lag = lag
	}
	for _, peerMeta := range meta.Peers {
		desc.Peers = append(desc.Peers, scheduler.PeerDescriptor{
			StoreID: peerMeta.StoreID,
			PeerID:  peerMeta.PeerID,
			Leader:  peerMeta.PeerID == leaderPeerID,
		})
	}
	return desc
}

func (s *Store) diskStats() (uint64, uint64, bool) {
	if s == nil || s.manifest == nil {
		return 0, 0, false
	}
	dir := s.manifest.Dir()
	if dir == "" {
		return 0, 0, false
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0, false
	}
	capacity := uint64(st.Blocks) * uint64(st.Bsize)
	available := uint64(st.Bavail) * uint64(st.Bsize)
	return capacity, available, true
}

func (s *Store) nextProposalID() uint64 {
	return atomic.AddUint64(&s.proposalSeq, 1)
}

func (s *Store) registerProposal(id uint64) *commandProposal {
	prop := &commandProposal{ch: make(chan proposalResult, 1)}
	s.proposalMu.Lock()
	s.proposals[id] = prop
	s.proposalMu.Unlock()
	return prop
}

func (s *Store) removeProposal(id uint64) {
	if id == 0 {
		return
	}
	s.proposalMu.Lock()
	delete(s.proposals, id)
	s.proposalMu.Unlock()
}

func (s *Store) completeProposal(id uint64, resp *pb.RaftCmdResponse, err error) {
	if id == 0 {
		return
	}
	s.proposalMu.Lock()
	prop, ok := s.proposals[id]
	if ok {
		delete(s.proposals, id)
	}
	s.proposalMu.Unlock()
	if ok && prop != nil {
		prop.ch <- proposalResult{resp: resp, err: err}
	}
}

func (s *Store) validateCommand(req *pb.RaftCmdRequest) (*peer.Peer, manifest.RegionMeta, *pb.RaftCmdResponse, error) {
	if s == nil {
		return nil, manifest.RegionMeta{}, nil, fmt.Errorf("raftstore: store is nil")
	}
	if req == nil {
		return nil, manifest.RegionMeta{}, nil, fmt.Errorf("raftstore: command is nil")
	}
	if req.Header == nil {
		req.Header = &pb.CmdHeader{}
	}
	regionID := req.Header.GetRegionId()
	if regionID == 0 {
		return nil, manifest.RegionMeta{}, nil, fmt.Errorf("raftstore: region id missing")
	}
	meta, ok := s.RegionMetaByID(regionID)
	if !ok {
		resp := &pb.RaftCmdResponse{Header: req.Header, RegionError: epochNotMatchError(nil)}
		return nil, manifest.RegionMeta{}, resp, nil
	}
	if err := validateRegionEpoch(req.Header.GetRegionEpoch(), meta); err != nil {
		resp := &pb.RaftCmdResponse{Header: req.Header, RegionError: err}
		return nil, meta, resp, nil
	}
	peer := s.regions.peer(regionID)
	if peer == nil {
		resp := &pb.RaftCmdResponse{Header: req.Header, RegionError: epochNotMatchError(&meta)}
		return nil, meta, resp, nil
	}
	status := peer.Status()
	if status.RaftState != myraft.StateLeader {
		resp := &pb.RaftCmdResponse{Header: req.Header, RegionError: notLeaderError(meta, status.Lead)}
		return nil, meta, resp, nil
	}
	req.Header.PeerId = peer.ID()
	return peer, meta, nil, nil
}

// ProposeCommand submits a raft command to the leader hosting the target
// region. When the store is not leader or the request header is invalid the
// returned response includes an appropriate RegionError.
func (s *Store) ProposeCommand(req *pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
	peer, _, resp, err := s.validateCommand(req)
	if err != nil {
		return nil, err
	}
	if resp != nil {
		return resp, nil
	}
	if req.Header.RequestId == 0 {
		req.Header.RequestId = s.nextProposalID()
	}
	id := req.Header.RequestId
	prop := s.registerProposal(id)
	if err := s.router.SendCommand(peer.ID(), req); err != nil {
		s.removeProposal(id)
		return nil, err
	}
	result := <-prop.ch
	if result.err != nil {
		return nil, result.err
	}
	if result.resp == nil {
		return &pb.RaftCmdResponse{Header: req.Header}, nil
	}
	return result.resp, nil
}

// ReadCommand executes the provided read-only raft command locally on the
// leader. The command must only include read operations (Get/Scan). The method
// returns a RegionError when the store is not leader for the target region.
func (s *Store) ReadCommand(req *pb.RaftCmdRequest) (*pb.RaftCmdResponse, error) {
	peer, _, regionResp, err := s.validateCommand(req)
	if err != nil {
		return nil, err
	}
	if regionResp != nil {
		return regionResp, nil
	}
	if len(req.GetRequests()) == 0 {
		return nil, fmt.Errorf("raftstore: read command missing requests")
	}
	if !isReadOnlyRequest(req) {
		return nil, fmt.Errorf("raftstore: read command must be read-only")
	}
	if s.commandApplier == nil {
		return nil, fmt.Errorf("raftstore: command apply without handler")
	}
	if req.Header == nil {
		req.Header = &pb.CmdHeader{}
	}
	if req.Header.GetRequestId() == 0 {
		req.Header.RequestId = s.nextProposalID()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	index, err := peer.LinearizableRead(ctx)
	if err != nil {
		return nil, err
	}
	if err := peer.WaitApplied(ctx, index); err != nil {
		return nil, err
	}
	out, err := s.commandApplier(req)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func isReadOnlyRequest(req *pb.RaftCmdRequest) bool {
	if req == nil {
		return false
	}
	for _, r := range req.GetRequests() {
		if r == nil {
			continue
		}
		switch r.GetCmdType() {
		case pb.CmdType_CMD_GET, pb.CmdType_CMD_SCAN:
			continue
		default:
			return false
		}
	}
	return true
}

func validateRegionEpoch(reqEpoch *pb.RegionEpoch, meta manifest.RegionMeta) *pb.RegionError {
	if reqEpoch == nil {
		return epochNotMatchError(&meta)
	}
	if reqEpoch.GetConfVer() != meta.Epoch.ConfVersion || reqEpoch.GetVersion() != meta.Epoch.Version {
		return epochNotMatchError(&meta)
	}
	return nil
}

func epochNotMatchError(meta *manifest.RegionMeta) *pb.RegionError {
	var current *pb.RegionEpoch
	var regions []*pb.RegionMeta
	if meta != nil {
		current = &pb.RegionEpoch{
			ConfVer: meta.Epoch.ConfVersion,
			Version: meta.Epoch.Version,
		}
		regions = append(regions, regionMetaToPB(*meta))
	}
	return &pb.RegionError{
		EpochNotMatch: &pb.EpochNotMatch{
			CurrentEpoch: current,
			Regions:      regions,
		},
	}
}

func notLeaderError(meta manifest.RegionMeta, leaderPeerID uint64) *pb.RegionError {
	var leader *pb.RegionPeer
	if leaderPeerID != 0 {
		for _, p := range meta.Peers {
			if p.PeerID == leaderPeerID {
				leader = &pb.RegionPeer{StoreId: p.StoreID, PeerId: p.PeerID}
				break
			}
		}
	}
	return &pb.RegionError{
		NotLeader: &pb.NotLeader{
			RegionId: meta.ID,
			Leader:   leader,
		},
	}
}

// UpdateRegion persists the region metadata (when a manifest manager is
// configured) and updates the in-memory catalog plus the running peer's
// snapshot, if any. Callers can use this to refresh epoch information,
// peer memberships, or lifecycle state transitions.
func (s *Store) UpdateRegion(meta manifest.RegionMeta) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if meta.ID == 0 {
		return fmt.Errorf("raftstore: region id is zero")
	}
	if meta.State == 0 {
		meta.State = manifest.RegionStateRunning
	}

	metaCopy := manifest.CloneRegionMeta(meta)

	current, exists := s.regions.meta(metaCopy.ID)
	var currentState manifest.RegionState
	if exists {
		currentState = current.State
	} else {
		currentState = manifest.RegionStateNew
	}

	if !validRegionStateTransition(currentState, metaCopy.State) {
		return fmt.Errorf("raftstore: invalid region %d state transition %v -> %v", metaCopy.ID, currentState, metaCopy.State)
	}

	if s.manifest != nil {
		if err := s.manifest.LogRegionUpdate(metaCopy); err != nil {
			return err
		}
	}

	peerRef := s.regions.updateMeta(metaCopy)

	if peerRef != nil {
		peerRef.SetRegionMeta(metaCopy)
	}
	if hook := s.regionHooks.OnRegionUpdate; hook != nil {
		hook(metaCopy)
	}
	return nil
}

// RemoveRegion tombstones the region metadata from the manifest (when present)
// and evicts it from the in-memory catalog. It is intended to be invoked after
// the corresponding peer has been stopped.
func (s *Store) RemoveRegion(regionID uint64) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if regionID == 0 {
		return fmt.Errorf("raftstore: region id is zero")
	}
	existing, ok := s.regions.meta(regionID)
	if !ok {
		return fmt.Errorf("raftstore: region %d not found", regionID)
	}
	if existing.State != manifest.RegionStateTombstone {
		existing.State = manifest.RegionStateTombstone
		if err := s.UpdateRegion(existing); err != nil {
			return err
		}
	}
	if s.manifest != nil {
		if err := s.manifest.LogRegionDelete(regionID); err != nil {
			return err
		}
	}
	s.regions.remove(regionID)
	if hook := s.regionHooks.OnRegionRemove; hook != nil {
		hook(regionID)
	}
	return nil
}

// UpdateRegionState loads the currently known metadata and advances the state
// machine to the requested value (Running/Removing/Tombstone) while validating
// legal transitions.
func (s *Store) UpdateRegionState(regionID uint64, state manifest.RegionState) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if regionID == 0 {
		return fmt.Errorf("raftstore: region id is zero")
	}
	meta, ok := s.regions.meta(regionID)
	if !ok {
		return fmt.Errorf("raftstore: region %d not found", regionID)
	}
	meta.State = state
	return s.UpdateRegion(meta)
}

func validRegionStateTransition(current, next manifest.RegionState) bool {
	if current == next {
		return true
	}
	switch current {
	case manifest.RegionStateNew:
		return next == manifest.RegionStateRunning
	case manifest.RegionStateRunning:
		return next == manifest.RegionStateRemoving || next == manifest.RegionStateTombstone
	case manifest.RegionStateRemoving:
		return next == manifest.RegionStateTombstone
	case manifest.RegionStateTombstone:
		return next == manifest.RegionStateTombstone
	default:
		return false
	}
}

func (s *Store) handlePeerConfChange(ev peer.ConfChangeEvent) error {
	if s == nil {
		return nil
	}
	region := ev.RegionMeta
	if region == nil && ev.Peer != nil {
		region = ev.Peer.RegionMeta()
	}
	if region == nil || region.ID == 0 {
		return nil
	}
	meta := manifest.CloneRegionMeta(*region)
	changed, err := applyConfChangeToMeta(&meta, ev.ConfChange)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if len(ev.ConfChange.Changes) > 0 {
		meta.Epoch.ConfVersion += uint64(len(ev.ConfChange.Changes))
	}
	return s.UpdateRegion(meta)
}

// VisitPeers executes the provided callback for every peer registered with the
// store. The callback receives a snapshot of the peer pointer so callers can
// perform operations without holding the store lock for extended periods.
func (s *Store) VisitPeers(fn func(*peer.Peer)) {
	if s == nil || fn == nil {
		return
	}
	s.mu.RLock()
	peers := make([]*peer.Peer, 0, len(s.peers))
	for _, p := range s.peers {
		peers = append(peers, p)
	}
	s.mu.RUnlock()
	for _, p := range peers {
		fn(p)
	}
}

// RegionMetas collects the known manifest.RegionMeta entries from registered
// peers. This mirrors the TinyKV store exposing region layout information to
// schedulers and debugging endpoints.
func (s *Store) RegionMetas() []manifest.RegionMeta {
	if s == nil {
		return nil
	}
	if s.regions == nil {
		return nil
	}
	return s.regions.listMetas()
}

// RegionMetaByID returns the stored metadata for the provided region, along
// with a boolean indicating whether it exists.
func (s *Store) RegionMetaByID(regionID uint64) (manifest.RegionMeta, bool) {
	if s == nil || regionID == 0 {
		return manifest.RegionMeta{}, false
	}
	if s.regions == nil {
		return manifest.RegionMeta{}, false
	}
	return s.regions.meta(regionID)
}

// RegionSnapshot returns a snapshot containing all region metadata currently
// known to the store. The resulting slice is safe for callers to modify.
func (s *Store) RegionSnapshot() RegionSnapshot {
	return RegionSnapshot{Regions: s.RegionMetas()}
}

// RegionMetrics returns the metrics recorder tracking region state counts.
func (s *Store) RegionMetrics() *RegionMetrics {
	if s == nil {
		return nil
	}
	return s.regionMetrics
}

// Peer returns the peer registered with the provided ID.
func (s *Store) Peer(id uint64) (*peer.Peer, bool) {
	if s == nil || id == 0 {
		return nil, false
	}
	s.mu.RLock()
	p, ok := s.peers[id]
	s.mu.RUnlock()
	return p, ok
}

// Step forwards the provided raft message to the target peer hosted on this
// store.
func (s *Store) Step(msg myraft.Message) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if msg.To == 0 {
		return fmt.Errorf("raftstore: raft message missing recipient")
	}
	if s.router == nil {
		return fmt.Errorf("raftstore: router is nil")
	}
	return s.router.SendRaft(msg.To, msg)
}

// ProposeAddPeer issues a configuration change to add the provided peer to the
// region's raft group. The manifest and in-memory region metadata are updated
// once the configuration change is committed and applied.
func (s *Store) ProposeAddPeer(regionID uint64, meta manifest.PeerMeta) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if regionID == 0 {
		return fmt.Errorf("raftstore: region id is zero")
	}
	if meta.PeerID == 0 {
		return fmt.Errorf("raftstore: peer id is zero")
	}
	peerRef := s.regions.peer(regionID)
	if peerRef == nil {
		return fmt.Errorf("raftstore: region %d not hosted on this store", regionID)
	}
	cc := raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{
			{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: meta.PeerID,
			},
		},
		Context: encodeConfChangeContext([]manifest.PeerMeta{meta}),
	}
	return peerRef.ProposeConfChange(cc)
}

// ProposeRemovePeer issues a configuration change removing the peer with the
// provided peer ID from the region's raft group.
func (s *Store) ProposeRemovePeer(regionID, peerID uint64) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if regionID == 0 || peerID == 0 {
		return fmt.Errorf("raftstore: invalid region (%d) or peer (%d) id", regionID, peerID)
	}
	peerRef := s.regions.peer(regionID)
	if peerRef == nil {
		return fmt.Errorf("raftstore: region %d not hosted on this store", regionID)
	}
	ctxMeta := manifest.PeerMeta{StoreID: peerID, PeerID: peerID}
	if meta, ok := s.RegionMetaByID(regionID); ok {
		if idx := peerIndexByID(meta.Peers, peerID); idx >= 0 {
			ctxMeta = meta.Peers[idx]
		}
	}
	cc := raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{
			{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: peerID,
			},
		},
		Context: encodeConfChangeContext([]manifest.PeerMeta{ctxMeta}),
	}
	return peerRef.ProposeConfChange(cc)
}

// TransferLeader initiates leadership transfer for the specified region to the
// provided peer ID.
func (s *Store) TransferLeader(regionID, targetPeerID uint64) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if regionID == 0 || targetPeerID == 0 {
		return fmt.Errorf("raftstore: invalid region (%d) or peer (%d) id", regionID, targetPeerID)
	}
	peerRef := s.regions.peer(regionID)
	if peerRef == nil {
		return fmt.Errorf("raftstore: region %d not hosted on this store", regionID)
	}
	return peerRef.TransferLeader(targetPeerID)
}

func applyConfChangeToMeta(meta *manifest.RegionMeta, cc raftpb.ConfChangeV2) (bool, error) {
	if meta == nil {
		return false, fmt.Errorf("raftstore: region meta is nil")
	}
	changed := false
	ctxPeers, err := decodeConfChangeContext(cc.Context)
	if err != nil {
		return false, err
	}
	ctxIndex := 0
	for _, change := range cc.Changes {
		peerMeta := manifest.PeerMeta{StoreID: change.NodeID, PeerID: change.NodeID}
		if ctxIndex < len(ctxPeers) {
			peerMeta = ctxPeers[ctxIndex]
		}
		ctxIndex++
		switch change.Type {
		case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
			if idx := peerIndexByID(meta.Peers, peerMeta.PeerID); idx == -1 {
				meta.Peers = append(meta.Peers, peerMeta)
				changed = true
			}
		case raftpb.ConfChangeRemoveNode:
			if idx := peerIndexByID(meta.Peers, change.NodeID); idx >= 0 {
				meta.Peers = append(meta.Peers[:idx], meta.Peers[idx+1:]...)
				changed = true
			}
		case raftpb.ConfChangeUpdateNode:
			if idx := peerIndexByID(meta.Peers, change.NodeID); idx >= 0 {
				meta.Peers[idx] = peerMeta
				changed = true
			}
		default:
			return false, fmt.Errorf("raftstore: unsupported conf change type %v", change.Type)
		}
	}
	return changed, nil
}

func encodeConfChangeContext(peers []manifest.PeerMeta) []byte {
	if len(peers) == 0 {
		return nil
	}
	buf := make([]byte, 0, len(peers)*16)
	for _, meta := range peers {
		buf = binary.AppendUvarint(buf, meta.StoreID)
		buf = binary.AppendUvarint(buf, meta.PeerID)
	}
	return buf
}

func decodeConfChangeContext(ctx []byte) ([]manifest.PeerMeta, error) {
	if len(ctx) == 0 {
		return nil, nil
	}
	peers := make([]manifest.PeerMeta, 0, 2)
	for len(ctx) > 0 {
		storeID, n := binary.Uvarint(ctx)
		if n <= 0 {
			return nil, fmt.Errorf("raftstore: invalid conf change context")
		}
		ctx = ctx[n:]
		peerID, m := binary.Uvarint(ctx)
		if m <= 0 {
			return nil, fmt.Errorf("raftstore: invalid conf change context")
		}
		ctx = ctx[m:]
		peers = append(peers, manifest.PeerMeta{StoreID: storeID, PeerID: peerID})
	}
	return peers, nil
}

func peerIndexByID(peers []manifest.PeerMeta, peerID uint64) int {
	for i, meta := range peers {
		if meta.PeerID == peerID {
			return i
		}
	}
	return -1
}

// Peers returns a snapshot describing every peer managed by the store.
func (s *Store) Peers() []PeerHandle {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	handles := make([]PeerHandle, 0, len(s.peers))
	for id, p := range s.peers {
		handles = append(handles, PeerHandle{
			ID:     id,
			Peer:   p,
			Region: manifest.CloneRegionMetaPtr(p.RegionMeta()),
		})
	}
	s.mu.RUnlock()
	return handles
}

func mergeRegionHooks(hooks ...RegionHooks) RegionHooks {
	update := func(meta manifest.RegionMeta) {
		for _, h := range hooks {
			if h.OnRegionUpdate != nil {
				h.OnRegionUpdate(meta)
			}
		}
	}
	remove := func(id uint64) {
		for _, h := range hooks {
			if h.OnRegionRemove != nil {
				h.OnRegionRemove(id)
			}
		}
	}
	return RegionHooks{
		OnRegionUpdate: func(meta manifest.RegionMeta) {
			if len(hooks) == 0 {
				return
			}
			update(meta)
		},
		OnRegionRemove: func(id uint64) {
			if len(hooks) == 0 {
				return
			}
			remove(id)
		},
	}
}
