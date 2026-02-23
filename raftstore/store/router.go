package store

import (
	"fmt"
	"sync"

	"github.com/feichai0017/NoKV/pb"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

// Router mimics TinyKV's raftstore router by providing an addressable
// abstraction for driving peer state machines. Each peer registers itself
// with the router so other components (store loops, RPC handlers, tests)
// can deliver raft traffic or administrative ticks without holding the peer
// reference directly.
type Router struct {
	mu    sync.RWMutex
	peers map[uint64]*peer.Peer
}

// NewRouter creates an empty router instance.
func NewRouter() *Router {
	return &Router{peers: make(map[uint64]*peer.Peer)}
}

// Register wires a peer into the router. Register must only be called once
// per peer ID.
func (r *Router) Register(p *peer.Peer) error {
	if p == nil {
		return fmt.Errorf("raftstore: router cannot register nil peer")
	}
	id := p.ID()
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.peers[id]; ok {
		return fmt.Errorf("raftstore: peer %d already registered", id)
	}
	r.peers[id] = p
	return nil
}

// Deregister removes a peer mapping. The caller is responsible for closing
// the peer after deregistration.
func (r *Router) Deregister(id uint64) {
	if id == 0 {
		return
	}
	r.mu.Lock()
	delete(r.peers, id)
	r.mu.Unlock()
}

// Peer returns a peer handle by ID.
func (r *Router) Peer(id uint64) (*peer.Peer, bool) {
	if id == 0 {
		return nil, false
	}
	r.mu.RLock()
	p, ok := r.peers[id]
	r.mu.RUnlock()
	return p, ok
}

// SendRaft delivers a raft protocol message to the registered peer.
func (r *Router) SendRaft(id uint64, msg myraft.Message) error {
	p, ok := r.Peer(id)
	if !ok {
		return fmt.Errorf("raftstore: peer %d not found", id)
	}
	return p.Step(msg)
}

// SendPropose submits an application proposal to the peer.
func (r *Router) SendPropose(id uint64, data []byte) error {
	p, ok := r.Peer(id)
	if !ok {
		return fmt.Errorf("raftstore: peer %d not found", id)
	}
	return p.Propose(data)
}

// SendCommand encodes the provided raft command and submits it to the peer.
func (r *Router) SendCommand(id uint64, req *pb.RaftCmdRequest) error {
	if req == nil {
		return fmt.Errorf("raftstore: nil raft command request")
	}
	p, ok := r.Peer(id)
	if !ok {
		return fmt.Errorf("raftstore: peer %d not found", id)
	}
	return p.ProposeCommand(req)
}

// SendTick drives a single logical clock tick for the target peer.
func (r *Router) SendTick(id uint64) error {
	p, ok := r.Peer(id)
	if !ok {
		return fmt.Errorf("raftstore: peer %d not found", id)
	}
	return p.Tick()
}

// BroadcastTick invokes Tick on every registered peer. The first error is
// returned to the caller.
func (r *Router) BroadcastTick() error {
	r.mu.RLock()
	peers := make([]*peer.Peer, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	r.mu.RUnlock()
	for _, p := range peers {
		if err := p.Tick(); err != nil {
			return err
		}
	}
	return nil
}

// BroadcastFlush forces processReady on all registered peers. This mirrors
// TinyKV's behavior of draining the ready queue when necessary.
func (r *Router) BroadcastFlush() error {
	r.mu.RLock()
	peers := make([]*peer.Peer, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	r.mu.RUnlock()
	for _, p := range peers {
		if err := p.Flush(); err != nil {
			return err
		}
	}
	return nil
}
