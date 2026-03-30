package store

import (
	"fmt"

	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"

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
	if _, ok := s.router.Peer(msg.To); !ok && msg.Type == myraft.MsgSnapshot {
		if err := s.startPeerFromSnapshot(msg); err != nil {
			return err
		}
	}
	return s.router.SendRaft(msg.To, msg)
}

func (s *Store) startPeerFromSnapshot(msg myraft.Message) error {
	if s == nil {
		return fmt.Errorf("raftstore: store is nil")
	}
	if msg.Type != myraft.MsgSnapshot || msg.To == 0 || msg.Snapshot == nil || myraft.IsEmptySnap(*msg.Snapshot) {
		return fmt.Errorf("raftstore: snapshot peer bootstrap requires non-empty snapshot message")
	}
	if s.peerBuilder == nil {
		return fmt.Errorf("raftstore: snapshot peer bootstrap requires peer builder")
	}
	if _, ok := s.router.Peer(msg.To); ok {
		return nil
	}
	manifest, err := snapshotpkg.ReadPayloadManifest(msg.Snapshot.Data)
	if err != nil {
		return fmt.Errorf("raftstore: decode snapshot payload manifest: %w", err)
	}
	meta := manifest.Region
	if meta.ID == 0 {
		return fmt.Errorf("raftstore: snapshot payload missing region metadata")
	}
	var localPeer raftmeta.PeerMeta
	for _, peerMeta := range meta.Peers {
		if peerMeta.PeerID == msg.To {
			localPeer = peerMeta
			break
		}
	}
	if localPeer.PeerID == 0 {
		return fmt.Errorf("raftstore: snapshot payload missing peer %d", msg.To)
	}
	if s.storeID != 0 && localPeer.StoreID != 0 && localPeer.StoreID != s.storeID {
		return fmt.Errorf("raftstore: snapshot peer %d belongs to store %d, local store is %d", msg.To, localPeer.StoreID, s.storeID)
	}
	cfg, err := s.peerBuilder(meta)
	if err != nil {
		return fmt.Errorf("raftstore: build peer from snapshot region %d: %w", meta.ID, err)
	}
	if cfg == nil {
		return fmt.Errorf("raftstore: peer builder returned nil config for region %d", meta.ID)
	}
	if cfg.RaftConfig.ID != msg.To {
		return fmt.Errorf("raftstore: snapshot bootstrap peer mismatch want=%d got=%d", msg.To, cfg.RaftConfig.ID)
	}
	if _, err := s.StartPeer(cfg, nil); err != nil {
		return fmt.Errorf("raftstore: start snapshot peer %d for region %d: %w", msg.To, meta.ID, err)
	}
	return nil
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
