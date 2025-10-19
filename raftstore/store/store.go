package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/scheduler"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
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
	RegisterStore(s)
	if s.scheduler != nil {
		s.startHeartbeatLoop()
	}
	return s
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
		s.applyOperation(op)
	}
}

func (s *Store) applyOperation(op scheduler.Operation) {
	if s == nil {
		return
	}
	switch op.Type {
	case scheduler.OperationLeaderTransfer:
		if op.Source == 0 || op.Target == 0 {
			return
		}
		s.VisitPeers(func(p *peer.Peer) {
			if p.ID() == op.Source {
				_ = p.TransferLeader(op.Target)
			}
		})
	}
}

func (s *Store) storeStatsSnapshot() scheduler.StoreStats {
	return scheduler.StoreStats{
		StoreID:   s.storeID,
		RegionNum: uint64(len(s.RegionMetas())),
		LeaderNum: s.countLeaders(),
	}
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
		for _, meta := range regions {
			snap.Regions = append(snap.Regions, s.buildRegionDescriptor(meta))
		}
	}
	return snap
}

func (s *Store) buildRegionDescriptor(meta manifest.RegionMeta) scheduler.RegionDescriptor {
	desc := scheduler.RegionDescriptor{
		ID:       meta.ID,
		StartKey: append([]byte(nil), meta.StartKey...),
		EndKey:   append([]byte(nil), meta.EndKey...),
		Epoch:    meta.Epoch,
	}
	var leaderPeerID uint64
	if local := s.regions.peer(meta.ID); local != nil {
		if local.Status().RaftState == myraft.StateLeader {
			leaderPeerID = local.ID()
		}
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
