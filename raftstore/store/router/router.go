// Package router is the addressable peer registration hub for one raftstore
// node. Stores, transports, RPC handlers, and tests resolve a peer ID to a
// concrete *peer.Peer through this layer rather than holding peer references
// directly. The router is owned by store.Store but is split out so that the
// "where do raft messages go?" responsibility cannot grow tendrils into the
// rest of the store god-object.
package router

import (
	"errors"
	"fmt"
	"sync"

	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/peer"
)

var (
	// ErrRegisterNilPeer indicates Register received a nil peer.
	ErrRegisterNilPeer = errors.New("raftstore/router: cannot register nil peer")
	// ErrNilCommandRequest indicates SendCommand received a nil request.
	ErrNilCommandRequest = errors.New("raftstore/router: nil raft command request")
)

// ErrPeerNotFound is returned when no peer is registered under the given ID.
type ErrPeerNotFound struct{ PeerID uint64 }

func (e *ErrPeerNotFound) Error() string {
	return fmt.Sprintf("raftstore/router: peer %d not found", e.PeerID)
}

// ErrPeerAlreadyRegistered is returned when Register is invoked twice for the
// same peer ID.
type ErrPeerAlreadyRegistered struct{ PeerID uint64 }

func (e *ErrPeerAlreadyRegistered) Error() string {
	return fmt.Sprintf("raftstore/router: peer %d already registered", e.PeerID)
}

// Router mimics TinyKV's raftstore router by providing an addressable
// abstraction for driving peer state machines. Each peer registers itself
// with the router so other components (store loops, RPC handlers, tests)
// can deliver raft traffic or administrative ticks without holding the peer
// reference directly.
type Router struct {
	mu    sync.RWMutex
	peers map[uint64]*peer.Peer
}

// New creates an empty router instance.
func New() *Router {
	return &Router{peers: make(map[uint64]*peer.Peer)}
}

// Register wires a peer into the router. Register must only be called once
// per peer ID.
func (r *Router) Register(p *peer.Peer) error {
	if r == nil || p == nil {
		return ErrRegisterNilPeer
	}
	id := p.ID()
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.peers[id]; ok {
		return &ErrPeerAlreadyRegistered{PeerID: id}
	}
	r.peers[id] = p
	return nil
}

// Deregister removes a peer mapping and returns the dropped handle so the
// caller can finalize shutdown. Returns nil when the peer was unknown.
func (r *Router) Deregister(id uint64) *peer.Peer {
	if r == nil || id == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.peers[id]
	delete(r.peers, id)
	return p
}

// Peer returns a peer handle by ID.
func (r *Router) Peer(id uint64) (*peer.Peer, bool) {
	if r == nil || id == 0 {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.peers[id]
	return p, ok
}

// Visit invokes fn on every registered peer under a snapshot of the peer set.
// fn runs without the router lock held so it may safely call back into the
// router.
func (r *Router) Visit(fn func(*peer.Peer)) {
	if r == nil || fn == nil {
		return
	}
	r.mu.RLock()
	peers := make([]*peer.Peer, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	r.mu.RUnlock()
	for _, p := range peers {
		fn(p)
	}
}

// List returns a snapshot of currently registered peers.
func (r *Router) List() []*peer.Peer {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*peer.Peer, 0, len(r.peers))
	for _, p := range r.peers {
		out = append(out, p)
	}
	return out
}

// SendRaft delivers a raft protocol message to the registered peer.
func (r *Router) SendRaft(id uint64, msg myraft.Message) error {
	p, ok := r.Peer(id)
	if !ok {
		return &ErrPeerNotFound{PeerID: id}
	}
	return p.Step(msg)
}

// SendPropose submits an application proposal to the peer.
func (r *Router) SendPropose(id uint64, data []byte) error {
	p, ok := r.Peer(id)
	if !ok {
		return &ErrPeerNotFound{PeerID: id}
	}
	return p.Propose(data)
}

// SendCommand encodes the provided raft command and submits it to the peer.
func (r *Router) SendCommand(id uint64, req *raftcmdpb.RaftCmdRequest) error {
	if req == nil {
		return ErrNilCommandRequest
	}
	p, ok := r.Peer(id)
	if !ok {
		return &ErrPeerNotFound{PeerID: id}
	}
	return p.ProposeCommand(req)
}

// SendTick drives a single logical clock tick for the target peer.
func (r *Router) SendTick(id uint64) error {
	p, ok := r.Peer(id)
	if !ok {
		return &ErrPeerNotFound{PeerID: id}
	}
	return p.Tick()
}

// BroadcastTick invokes Tick on every registered peer. The first error is
// returned to the caller.
func (r *Router) BroadcastTick() error {
	for _, p := range r.List() {
		if err := p.Tick(); err != nil {
			return err
		}
	}
	return nil
}

// BroadcastFlush forces processReady on all registered peers. This mirrors
// TinyKV's behavior of draining the ready queue when necessary.
func (r *Router) BroadcastFlush() error {
	for _, p := range r.List() {
		if err := p.Flush(); err != nil {
			return err
		}
	}
	return nil
}
