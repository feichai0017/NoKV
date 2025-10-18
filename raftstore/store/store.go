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
	mu     sync.RWMutex
	router *Router
	peers  map[uint64]*peer.Peer
}

// NewStore creates a Store with the provided router. When router is nil a new
// instance is allocated implicitly so callers can skip the explicit
// construction in tests.
func NewStore(router *Router) *Store {
	if router == nil {
		router = NewRouter()
	}
	return &Store{
		router: router,
		peers:  make(map[uint64]*peer.Peer),
	}
}

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
	p, err := peer.NewPeer(cfg)
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
	s.mu.Unlock()

	if err := s.router.Register(p); err != nil {
		s.mu.Lock()
		delete(s.peers, id)
		s.mu.Unlock()
		p.Close()
		return nil, err
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
	s.mu.Lock()
	p := s.peers[id]
	delete(s.peers, id)
	s.mu.Unlock()
	if p != nil {
		p.Close()
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
	metas := make([]manifest.RegionMeta, 0)
	s.VisitPeers(func(p *peer.Peer) {
		if p == nil {
			return
		}
		if meta := p.RegionMeta(); meta != nil {
			metas = append(metas, *meta)
		}
	})
	return metas
}