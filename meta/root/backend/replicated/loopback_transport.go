package replicated

import (
	"fmt"
	"sync"

	myraft "github.com/feichai0017/NoKV/raft"
)

type loopbackTransport struct {
	mu      sync.RWMutex
	localID uint64
	addr    string
	peers   map[uint64]*loopbackTransport
	handler MessageHandler
	closed  bool
}

func newLoopbackTransportSet(ids ...uint64) map[uint64]Transport {
	out := make(map[uint64]Transport, len(ids))
	raw := make(map[uint64]*loopbackTransport, len(ids))
	for _, id := range ids {
		raw[id] = &loopbackTransport{
			localID: id,
			addr:    fmt.Sprintf("loopback://%d", id),
			peers:   make(map[uint64]*loopbackTransport),
		}
	}
	for _, id := range ids {
		for _, peerID := range ids {
			if peerID == id {
				continue
			}
			raw[id].peers[peerID] = raw[peerID]
		}
		out[id] = raw[id]
	}
	return out
}

func (t *loopbackTransport) LocalID() uint64 { return t.localID }

func (t *loopbackTransport) Addr() string { return t.addr }

func (t *loopbackTransport) SetHandler(handler MessageHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = handler
}

func (t *loopbackTransport) SetPeer(id uint64, _ string) {
	_ = id
}

func (t *loopbackTransport) SetPeers(_ map[uint64]string) {}

func (t *loopbackTransport) Send(msgs ...myraft.Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.closed {
		return fmt.Errorf("meta/root/backend/replicated: loopback transport %d closed", t.localID)
	}
	for _, msg := range msgs {
		peer, ok := t.peers[msg.To]
		if !ok {
			return fmt.Errorf("meta/root/backend/replicated: loopback peer %d unknown", msg.To)
		}
		peer.mu.RLock()
		handler := peer.handler
		peer.mu.RUnlock()
		if handler == nil {
			return fmt.Errorf("meta/root/backend/replicated: loopback peer %d missing handler", msg.To)
		}
		if err := handler(msg); err != nil {
			return err
		}
	}
	return nil
}

func (t *loopbackTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}
