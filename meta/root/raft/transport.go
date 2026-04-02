package rootraft

import (
	"fmt"
	"sync"

	myraft "github.com/feichai0017/NoKV/raft"
)

// Transport delivers raft messages between metadata-root nodes.
//
// The first implementation can stay minimal. Root replication only needs a
// tiny message path and should not depend on raftstore peer transport.
type Transport interface {
	Send(messages []myraft.Message) error
}

type nopTransport struct{}

func (nopTransport) Send(_ []myraft.Message) error { return nil }

// MessageHandler consumes incoming raft messages for one root node.
type MessageHandler interface {
	Step(msg myraft.Message) error
}

// MemoryTransport is a synchronous in-memory transport used by tests and
// embedded deployments. It preserves per-send message order and drains any
// recursively emitted messages before returning.
type MemoryTransport struct {
	mu        sync.Mutex
	handlers  map[uint64]MessageHandler
	queue     []myraft.Message
	delivering bool
}

func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{
		handlers: make(map[uint64]MessageHandler),
	}
}

func (t *MemoryTransport) Register(id uint64, handler MessageHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[id] = handler
}

func (t *MemoryTransport) Unregister(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.handlers, id)
}

func (t *MemoryTransport) Send(messages []myraft.Message) error {
	if len(messages) == 0 {
		return nil
	}

	t.mu.Lock()
	t.queue = append(t.queue, messages...)
	if t.delivering {
		t.mu.Unlock()
		return nil
	}
	t.delivering = true
	t.mu.Unlock()

	for {
		t.mu.Lock()
		if len(t.queue) == 0 {
			t.delivering = false
			t.mu.Unlock()
			return nil
		}
		msg := t.queue[0]
		t.queue = t.queue[1:]
		handler := t.handlers[msg.To]
		t.mu.Unlock()

		if handler == nil {
			return fmt.Errorf("meta/root/raft: no handler registered for peer %d", msg.To)
		}
		if err := handler.Step(msg); err != nil {
			return err
		}
	}
}
