package store

import (
	"fmt"

	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

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
	s.mu.RLock()
	factory := s.peerFactory
	s.mu.RUnlock()
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
	if err := s.peers.add(p); err != nil {
		p.Close()
		return nil, err
	}
	if regionMeta != nil && regionMeta.ID != 0 {
		s.regions.setPeer(regionMeta.ID, p)
	}

	if err := s.router.Register(p); err != nil {
		s.peers.remove(id)
		if regionMeta != nil {
			s.regions.setPeer(regionMeta.ID, nil)
		}
		p.Close()
		return nil, err
	}
	if regionMeta != nil {
		if err := s.UpdateRegion(*regionMeta); err != nil {
			s.router.Deregister(id)
			s.peers.remove(id)
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
	p := s.peers.remove(id)
	var regionID uint64
	if p != nil {
		if meta := p.RegionMeta(); meta != nil {
			regionID = meta.ID
		}
	}
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
	if s.heartbeat != nil {
		s.heartbeat.stopLoop()
	}
	if s.operations != nil {
		s.operations.stopLoop()
	}
}

// VisitPeers executes the provided callback for every peer registered with the
// store. The callback receives a snapshot of the peer pointer so callers can
// perform operations without holding the store lock for extended periods.
func (s *Store) VisitPeers(fn func(*peer.Peer)) {
	if s == nil || fn == nil {
		return
	}
	s.peers.visit(fn)
}

// Peer returns the peer registered with the provided ID.
func (s *Store) Peer(id uint64) (*peer.Peer, bool) {
	if s == nil || id == 0 {
		return nil, false
	}
	return s.peers.get(id)
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

// Peers returns a snapshot describing every peer managed by the store.
func (s *Store) Peers() []PeerHandle {
	if s == nil {
		return nil
	}
	raw := s.peers.list()
	handles := make([]PeerHandle, 0, len(raw))
	for _, p := range raw {
		handles = append(handles, PeerHandle{
			ID:     p.ID(),
			Peer:   p,
			Region: manifest.CloneRegionMetaPtr(p.RegionMeta()),
		})
	}
	return handles
}
