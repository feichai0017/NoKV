package transport

import (
	"encoding/gob"
	"errors"
	"io"
	"net"
	"net/rpc"
	"sync"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
)

func init() {
	gob.Register(raftpb.Message{})
	gob.Register(raftpb.Entry{})
	gob.Register([]raftpb.Entry{})
	gob.Register(raftpb.Snapshot{})
}

// RPCTransport implements Transport using Go's net/rpc package. It allows
// peers to communicate across processes via TCP while providing helpers for
// tests to simulate partitions.
type RPCTransport struct {
	mu          sync.RWMutex
	localID     uint64
	addr        string
	peers       map[uint64]string
	blocked     map[uint64]struct{}
	clients     map[uint64]*rpc.Client
	conns       map[net.Conn]struct{}
	handler     func(myraft.Message) error
	server      *rpc.Server
	ln          net.Listener
	stopCh      chan struct{}
	wg          sync.WaitGroup
	dialTimeout time.Duration
}

// NewRPCTransport starts an RPC server bound to listenAddr (use "127.0.0.1:0"
// for automatic port selection). The returned transport must have SetHandler
// invoked before it begins processing messages.
func NewRPCTransport(localID uint64, listenAddr string) (*RPCTransport, error) {
	if localID == 0 {
		return nil, errors.New("raftstore: RPC transport requires non-zero local ID")
	}
	if listenAddr == "" {
		listenAddr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	srv := rpc.NewServer()
	t := &RPCTransport{
		localID:     localID,
		addr:        ln.Addr().String(),
		peers:       make(map[uint64]string),
		blocked:     make(map[uint64]struct{}),
		clients:     make(map[uint64]*rpc.Client),
		conns:       make(map[net.Conn]struct{}),
		server:      srv,
		ln:          ln,
		stopCh:      make(chan struct{}),
		dialTimeout: time.Second,
	}
	if err := srv.RegisterName("raft", &rpcService{transport: t}); err != nil {
		_ = ln.Close()
		return nil, err
	}
	t.wg.Add(1)
	go t.serve()
	return t, nil
}

// Addr returns the listener address.
func (t *RPCTransport) Addr() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.addr
}

// LocalID returns the ID associated with this transport.
func (t *RPCTransport) LocalID() uint64 {
	return t.localID
}

// SetHandler assigns the callback invoked when a message is delivered to this
// transport.
func (t *RPCTransport) SetHandler(fn func(myraft.Message) error) {
	t.mu.Lock()
	t.handler = fn
	t.mu.Unlock()
}

// SetPeer associates a remote peer ID with its address. Passing an empty
// address removes the peer mapping.
func (t *RPCTransport) SetPeer(id uint64, addr string) {
	if id == 0 || id == t.localID {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if addr == "" {
		delete(t.peers, id)
		if client, ok := t.clients[id]; ok {
			client.Close()
			delete(t.clients, id)
		}
		return
	}
	t.peers[id] = addr
	if client, ok := t.clients[id]; ok {
		client.Close()
		delete(t.clients, id)
	}
}

// BlockPeer drops outbound messages destined for the provided peer ID.
func (t *RPCTransport) BlockPeer(id uint64) {
	if id == 0 {
		return
	}
	t.mu.Lock()
	t.blocked[id] = struct{}{}
	t.mu.Unlock()
}

// UnblockPeer resumes delivery for the provided peer ID.
func (t *RPCTransport) UnblockPeer(id uint64) {
	if id == 0 {
		return
	}
	t.mu.Lock()
	delete(t.blocked, id)
	t.mu.Unlock()
}

// Send forwards the message to the remote peer using RPC. Errors are logged by
// dropping the cached client and allowing future attempts to redial.
func (t *RPCTransport) Send(msg myraft.Message) {
	if msg.To == 0 {
		return
	}
	addr := t.peerAddr(msg.To)
	if addr == "" || t.isBlocked(msg.To) {
		return
	}
	client, err := t.getClient(msg.To, addr)
	if err != nil {
		return
	}
	req := &RPCMessage{Message: raftpb.Message(msg)}
	var resp RPCResponse
	if err := client.Call("raft.Step", req, &resp); err != nil {
		t.handleSendError(msg.To, err)
	}
}

// Close shuts down the transport and releases any network resources.
func (t *RPCTransport) Close() error {
	close(t.stopCh)
	_ = t.ln.Close()
	var clients []*rpc.Client
	var conns []net.Conn
	t.mu.Lock()
	for id, client := range t.clients {
		clients = append(clients, client)
		delete(t.clients, id)
	}
	for conn := range t.conns {
		conns = append(conns, conn)
		delete(t.conns, conn)
	}
	t.mu.Unlock()
	for _, client := range clients {
		client.Close()
	}
	for _, conn := range conns {
		conn.Close()
	}
	t.wg.Wait()
	return nil
}

func (t *RPCTransport) serve() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			select {
			case <-t.stopCh:
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return
		}
		t.mu.Lock()
		t.conns[conn] = struct{}{}
		t.mu.Unlock()
		t.wg.Add(1)
		go func(c net.Conn) {
			defer func() {
				t.mu.Lock()
				delete(t.conns, c)
				t.mu.Unlock()
				t.wg.Done()
			}()
			t.server.ServeConn(c)
		}(conn)
	}
}

func (t *RPCTransport) deliver(msg raftpb.Message) error {
	t.mu.RLock()
	handler := t.handler
	t.mu.RUnlock()
	if handler == nil {
		return errors.New("raftstore: RPC transport has no handler")
	}
	return handler(myraft.Message(msg))
}

func (t *RPCTransport) peerAddr(id uint64) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peers[id]
}

func (t *RPCTransport) isBlocked(id uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.blocked[id]
	return ok
}

func (t *RPCTransport) getClient(id uint64, addr string) (*rpc.Client, error) {
	t.mu.RLock()
	client := t.clients[id]
	t.mu.RUnlock()
	if client != nil {
		return client, nil
	}
	conn, err := net.DialTimeout("tcp", addr, t.dialTimeout)
	if err != nil {
		return nil, err
	}
	newClient := rpc.NewClient(conn)
	t.mu.Lock()
	if old := t.clients[id]; old != nil {
		old.Close()
	}
	t.clients[id] = newClient
	t.mu.Unlock()
	return newClient, nil
}

func (t *RPCTransport) handleSendError(id uint64, err error) {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		t.mu.Lock()
		if client, ok := t.clients[id]; ok {
			client.Close()
			delete(t.clients, id)
		}
		t.mu.Unlock()
		return
	}
	t.mu.Lock()
	if client, ok := t.clients[id]; ok {
		client.Close()
		delete(t.clients, id)
	}
	t.mu.Unlock()
}

type rpcService struct {
	transport *RPCTransport
}

type RPCMessage struct {
	Message raftpb.Message
}

type RPCResponse struct{}

func (s *rpcService) Step(req *RPCMessage, resp *RPCResponse) error {
	if s == nil || s.transport == nil {
		return errors.New("raftstore: transport service not initialised")
	}
	return s.transport.deliver(req.Message)
}
