package store

import (
	"fmt"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"

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
	var regionMeta *raftmeta.RegionMeta
	if cfg.Region != nil {
		if cfg.Region.State == 0 {
			cfg.Region.State = raftmeta.RegionStateRunning
		}
		regionMeta = raftmeta.CloneRegionMetaPtr(cfg.Region)
	}
	cfgCopy := *cfg
	cfgCopy.ConfChange = s.handlePeerConfChange
	if cfgCopy.AdminApply == nil {
		cfgCopy.AdminApply = s.handleAdminCommand
	}
	cfgCopy.Apply = func(entries []myraft.Entry) error {
		return s.applyEntries(entries)
	}
	p, err := peer.NewPeer(&cfgCopy)
	if err != nil {
		return nil, err
	}
	id := p.ID()
	if err := s.router.add(p); err != nil {
		_ = p.Close()
		return nil, err
	}
	if regionMeta != nil && regionMeta.ID != 0 {
		s.regionMgr().setPeer(regionMeta.ID, p)
	}

	if regionMeta != nil {
		if err := s.applyRegionMeta(*regionMeta); err != nil {
			s.router.remove(id)
			s.regionMgr().setPeer(regionMeta.ID, nil)
			_ = p.Close()
			return nil, err
		}
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
	p := s.router.remove(id)
	var regionID uint64
	if p != nil {
		if meta := p.RegionMeta(); meta != nil {
			regionID = meta.ID
		}
	}
	if regionID != 0 {
		s.regionMgr().setPeer(regionID, nil)
		_ = s.applyRegionState(regionID, raftmeta.RegionStateRemoving)
	}
	if p != nil {
		_ = p.Close()
	}
}

// Close stops background workers associated with the store.
func (s *Store) Close() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.stopHeartbeatLoop()
	s.stopOperationLoop()
	if s.schedulerClient() != nil {
		_ = s.schedulerClient().Close()
	}
}

// VisitPeers executes the provided callback for every peer registered with the
// store. The callback receives a snapshot of the peer pointer so callers can
// perform operations without holding the store lock for extended periods.
func (s *Store) VisitPeers(fn func(*peer.Peer)) {
	if s == nil || fn == nil {
		return
	}
	s.router.visit(fn)
}

// Peer returns the peer registered with the provided ID.
func (s *Store) Peer(id uint64) (*peer.Peer, bool) {
	if s == nil || id == 0 {
		return nil, false
	}
	return s.router.get(id)
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
	raw := s.router.list()
	handles := make([]PeerHandle, 0, len(raw))
	for _, p := range raw {
		handles = append(handles, PeerHandle{
			ID:     p.ID(),
			Peer:   p,
			Region: raftmeta.CloneRegionMetaPtr(p.RegionMeta()),
		})
	}
	return handles
}
