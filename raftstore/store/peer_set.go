package store

import (
	"fmt"
	"sync"

	"github.com/feichai0017/NoKV/raftstore/peer"
)

// peerSet tracks the peers hosted by a store and keeps the router registration
// in sync with the internal map.
type peerSet struct {
	mu     sync.RWMutex
	router *Router
	peers  map[uint64]*peer.Peer
}

func newPeerSet(router *Router) *peerSet {
	return &peerSet{
		router: router,
		peers:  make(map[uint64]*peer.Peer),
	}
}

func (ps *peerSet) add(p *peer.Peer) error {
	if ps == nil || p == nil {
		return fmt.Errorf("peerSet: invalid peer")
	}
	id := p.ID()
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if _, exists := ps.peers[id]; exists {
		return fmt.Errorf("raftstore: peer %d already exists", id)
	}
	ps.peers[id] = p
	return nil
}

func (ps *peerSet) remove(id uint64) *peer.Peer {
	if ps == nil || id == 0 {
		return nil
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	p := ps.peers[id]
	delete(ps.peers, id)
	return p
}

func (ps *peerSet) get(id uint64) (*peer.Peer, bool) {
	if ps == nil || id == 0 {
		return nil, false
	}
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	p, ok := ps.peers[id]
	return p, ok
}

func (ps *peerSet) visit(fn func(*peer.Peer)) {
	if ps == nil || fn == nil {
		return
	}
	ps.mu.RLock()
	snapshot := make([]*peer.Peer, 0, len(ps.peers))
	for _, p := range ps.peers {
		snapshot = append(snapshot, p)
	}
	ps.mu.RUnlock()
	for _, p := range snapshot {
		fn(p)
	}
}

func (ps *peerSet) list() []*peer.Peer {
	if ps == nil {
		return nil
	}
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	out := make([]*peer.Peer, 0, len(ps.peers))
	for _, p := range ps.peers {
		out = append(out, p)
	}
	return out
}
