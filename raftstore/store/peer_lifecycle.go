package store

import (
	"fmt"
	metaregion "github.com/feichai0017/NoKV/meta/region"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/failpoints"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
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
	var regionMeta *localmeta.RegionMeta
	if cfg.Region != nil {
		if cfg.Region.State == 0 {
			cfg.Region.State = metaregion.ReplicaStateRunning
		}
		regionMeta = localmeta.CloneRegionMetaPtr(cfg.Region)
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
		_ = s.applyRegionState(regionID, metaregion.ReplicaStateRemoving)
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
	metaFile, err := snapshotpkg.ReadPayloadMeta(msg.Snapshot.Data)
	if err != nil {
		return fmt.Errorf("raftstore: decode sst snapshot payload meta: %w", err)
	}
	meta := metaFile.Region
	if meta.ID == 0 {
		return fmt.Errorf("raftstore: snapshot payload missing region metadata")
	}
	var localPeer metaregion.Peer
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

// InstallRegionSnapshot installs one leader-exported raft snapshot on the
// local store, bootstrapping the target peer on demand from the snapshot
// payload carried in Snapshot.Data.
func (s *Store) InstallRegionSnapshot(snap myraft.Snapshot) (localmeta.RegionMeta, error) {
	if s == nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: store is nil")
	}
	if myraft.IsEmptySnap(snap) {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: install region snapshot requires non-empty snapshot")
	}
	if len(snap.Data) == 0 {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: install region snapshot requires snapshot payload")
	}
	metaFile, err := snapshotpkg.ReadPayloadMeta(snap.Data)
	if err != nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: decode install sst snapshot payload: %w", err)
	}
	meta := metaFile.Region
	if meta.ID == 0 {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: install snapshot payload missing region metadata")
	}
	var localPeer metaregion.Peer
	for _, peerMeta := range meta.Peers {
		if peerMeta.StoreID == s.storeID {
			localPeer = peerMeta
			break
		}
	}
	if localPeer.PeerID == 0 {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: region %d does not assign a peer to store %d", meta.ID, s.storeID)
	}
	if existing, ok := s.Peer(localPeer.PeerID); ok && existing != nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: peer %d already hosted for region %d", localPeer.PeerID, meta.ID)
	}
	if s.peerBuilder == nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: install region snapshot requires peer builder")
	}
	cfg, err := s.peerBuilder(meta)
	if err != nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: build install peer for region %d: %w", meta.ID, err)
	}
	if cfg == nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: peer builder returned nil config for region %d", meta.ID)
	}
	if cfg.RaftConfig.ID != localPeer.PeerID {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: install snapshot peer mismatch want=%d got=%d", localPeer.PeerID, cfg.RaftConfig.ID)
	}
	cfgCopy := *cfg
	cfgCopy.ConfChange = s.handlePeerConfChange
	cfgCopy.AllowSnapshotInstallRetry = true
	if cfgCopy.AdminApply == nil {
		cfgCopy.AdminApply = s.handleAdminCommand
	}
	cfgCopy.Apply = func(entries []myraft.Entry) error {
		return s.applyEntries(entries)
	}
	p, err := peer.NewPeer(&cfgCopy)
	if err != nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: new install peer %d for region %d: %w", localPeer.PeerID, meta.ID, err)
	}
	msg := myraft.Message{Type: myraft.MsgSnapshot, To: localPeer.PeerID, Snapshot: &snap}
	if err := p.Step(msg); err != nil {
		_ = p.Close()
		return localmeta.RegionMeta{}, err
	}
	if failpoints.ShouldFailAfterSnapshotApplyBeforePublish() {
		_ = p.Close()
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: failpoint after snapshot apply before publish")
	}
	if err := s.router.add(p); err != nil {
		_ = p.Close()
		return localmeta.RegionMeta{}, err
	}
	s.regionMgr().setPeer(meta.ID, p)
	if err := s.applyRegionMeta(meta); err != nil {
		s.router.remove(localPeer.PeerID)
		s.regionMgr().setPeer(meta.ID, nil)
		_ = p.Close()
		return localmeta.RegionMeta{}, err
	}
	return meta, nil
}

// InstallRegionSSTSnapshot installs one migration-only SST snapshot payload on
// the local store. The raft snapshot metadata is applied to the peer storage
// first, then SST files are ingested before the peer is published.
func (s *Store) InstallRegionSSTSnapshot(snap myraft.Snapshot, meta localmeta.RegionMeta, install func() (func() error, error)) (localmeta.RegionMeta, error) {
	if s == nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: store is nil")
	}
	if myraft.IsEmptySnap(snap) {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: install region sst snapshot requires non-empty snapshot")
	}
	if meta.ID == 0 {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: install region sst snapshot requires region metadata")
	}
	if install == nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: install region sst snapshot requires install callback")
	}
	var localPeer metaregion.Peer
	for _, peerMeta := range meta.Peers {
		if peerMeta.StoreID == s.storeID {
			localPeer = peerMeta
			break
		}
	}
	if localPeer.PeerID == 0 {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: region %d does not assign a peer to store %d", meta.ID, s.storeID)
	}
	if existing, ok := s.Peer(localPeer.PeerID); ok && existing != nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: peer %d already hosted for region %d", localPeer.PeerID, meta.ID)
	}
	if s.peerBuilder == nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: install region sst snapshot requires peer builder")
	}
	cfg, err := s.peerBuilder(meta)
	if err != nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: build install peer for region %d: %w", meta.ID, err)
	}
	if cfg == nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: peer builder returned nil config for region %d", meta.ID)
	}
	if cfg.RaftConfig.ID != localPeer.PeerID {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: install sst snapshot peer mismatch want=%d got=%d", localPeer.PeerID, cfg.RaftConfig.ID)
	}
	cfgCopy := *cfg
	cfgCopy.ConfChange = s.handlePeerConfChange
	cfgCopy.AllowSnapshotInstallRetry = true
	if cfgCopy.AdminApply == nil {
		cfgCopy.AdminApply = s.handleAdminCommand
	}
	cfgCopy.Apply = func(entries []myraft.Entry) error {
		return s.applyEntries(entries)
	}
	p, err := peer.NewPeer(&cfgCopy)
	if err != nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: new sst install peer %d for region %d: %w", localPeer.PeerID, meta.ID, err)
	}
	snapNoData := snap
	snapNoData.Data = nil
	msg := myraft.Message{Type: myraft.MsgSnapshot, To: localPeer.PeerID, Snapshot: &snapNoData}
	if err := p.Step(msg); err != nil {
		_ = p.Close()
		return localmeta.RegionMeta{}, err
	}
	rollback, err := install()
	if err != nil {
		_ = p.Close()
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: install sst payload for region %d: %w", meta.ID, err)
	}
	cleanup := func() {
		if rollback != nil {
			_ = rollback()
		}
	}
	p.ApplyRegionMetaMirror(meta)
	if failpoints.ShouldFailAfterSnapshotApplyBeforePublish() {
		cleanup()
		_ = p.Close()
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: failpoint after snapshot apply before publish")
	}
	if err := s.router.add(p); err != nil {
		cleanup()
		_ = p.Close()
		return localmeta.RegionMeta{}, err
	}
	s.regionMgr().setPeer(meta.ID, p)
	if err := s.applyRegionMeta(meta); err != nil {
		cleanup()
		s.router.remove(localPeer.PeerID)
		s.regionMgr().setPeer(meta.ID, nil)
		_ = p.Close()
		return localmeta.RegionMeta{}, err
	}
	return meta, nil
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
			Region: localmeta.CloneRegionMetaPtr(p.RegionMeta()),
		})
	}
	return handles
}
