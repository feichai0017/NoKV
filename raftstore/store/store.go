package store

import (
	"fmt"
	"sync"

	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

// Store hosts a collection of peers and provides helpers inspired by
// TinyKV's raftstore::Store structure. It wires peers to the router, exposes
// lifecycle hooks, and allows higher layers (RPC, schedulers, tests) to drive
// ticks or proposals without needing to keep global peer maps themselves.
type Store struct {
	mu          sync.RWMutex
	router      *Router
	peers       map[uint64]*peer.Peer
	peerFactory PeerFactory
	hooks       LifecycleHooks
	regionHooks RegionHooks
	manifest    *manifest.Manager
	regions     *regionManager
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
	s := &Store{
		router:      router,
		peers:       make(map[uint64]*peer.Peer),
		peerFactory: factory,
		hooks:       cfg.Hooks,
		regionHooks: cfg.RegionHooks,
		manifest:    cfg.Manifest,
		regions:     newRegionManager(),
	}
	if cfg.Manifest != nil {
		s.regions.loadSnapshot(cfg.Manifest.RegionSnapshot())
	}
	return s
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
	p, err := factory(cfg)
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
