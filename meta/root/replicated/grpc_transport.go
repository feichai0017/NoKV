package replicated

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	rootTransportServiceName    = "nokv.meta.root.Transport"
	rootTransportStepMethod     = "Step"
	rootTransportStepFullMethod = "/" + rootTransportServiceName + "/" + rootTransportStepMethod
)

type rootTransportServer interface {
	Step(context.Context, *raftpb.Message) (*emptypb.Empty, error)
}

type rootTransportClientImpl struct {
	cc grpc.ClientConnInterface
}

func (c *rootTransportClientImpl) Step(ctx context.Context, in *raftpb.Message, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	if err := c.cc.Invoke(ctx, rootTransportStepFullMethod, in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

type grpcTransportService struct {
	transport *GRPCTransport
}

func (s *grpcTransportService) Step(ctx context.Context, msg *raftpb.Message) (*emptypb.Empty, error) {
	if s == nil || s.transport == nil {
		return &emptypb.Empty{}, nil
	}
	handler := s.transport.getHandler()
	if handler == nil {
		return &emptypb.Empty{}, nil
	}
	if err := handler(myraft.Message(*msg)); err != nil {
		if st, ok := status.FromError(err); ok {
			return nil, st.Err()
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &emptypb.Empty{}, nil
}

var rootTransportServiceDesc = grpc.ServiceDesc{
	ServiceName: rootTransportServiceName,
	HandlerType: (*rootTransportServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: rootTransportStepMethod,
			Handler:    rootTransportStepHandler,
		},
	},
}

func rootTransportStepHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(raftpb.Message)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(rootTransportServer).Step(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: rootTransportStepFullMethod,
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(rootTransportServer).Step(ctx, req.(*raftpb.Message))
	}
	return interceptor(ctx, in, info, handler)
}

// GRPCTransport is the first cross-process-capable transport skeleton for the
// replicated metadata root backend.
type GRPCTransport struct {
	mu          sync.RWMutex
	localID     uint64
	addr        string
	peers       map[uint64]string
	conns       map[uint64]*grpc.ClientConn
	clients     map[uint64]*rootTransportClientImpl
	handler     MessageHandler
	server      *grpc.Server
	ln          net.Listener
	dialTimeout time.Duration
	sendTimeout time.Duration
	closed      bool
}

// NewGRPCTransport starts one gRPC transport endpoint for one metadata root node.
func NewGRPCTransport(localID uint64, listenAddr string) (*GRPCTransport, error) {
	if localID == 0 {
		return nil, errTransportRequiresLocalID
	}
	if listenAddr == "" {
		listenAddr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	t := &GRPCTransport{
		localID:     localID,
		addr:        ln.Addr().String(),
		peers:       make(map[uint64]string),
		conns:       make(map[uint64]*grpc.ClientConn),
		clients:     make(map[uint64]*rootTransportClientImpl),
		server:      grpc.NewServer(),
		ln:          ln,
		dialTimeout: time.Second,
		sendTimeout: time.Second,
	}
	t.server.RegisterService(&rootTransportServiceDesc, &grpcTransportService{transport: t})
	go func() {
		_ = t.server.Serve(ln)
	}()
	return t, nil
}

func (t *GRPCTransport) Addr() string { return t.addr }

func (t *GRPCTransport) SetHandler(handler MessageHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = handler
}

func (t *GRPCTransport) getHandler() MessageHandler {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.handler
}

func (t *GRPCTransport) SetPeer(id uint64, addr string) {
	if t == nil || id == 0 || id == t.localID || addr == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if prev, ok := t.peers[id]; ok && prev == addr {
		return
	}
	if conn := t.conns[id]; conn != nil {
		_ = conn.Close()
		delete(t.conns, id)
		delete(t.clients, id)
	}
	t.peers[id] = addr
}

func (t *GRPCTransport) SetPeers(peers map[uint64]string) {
	if t == nil {
		return
	}
	for id, addr := range peers {
		t.SetPeer(id, addr)
	}
}

func (t *GRPCTransport) Send(msgs ...myraft.Message) error {
	if t == nil || len(msgs) == 0 {
		return nil
	}
	// etcd/raft batches outbound messages per tick. We MUST try every message
	// even when one fails — otherwise an unreachable peer (dead leader, or a
	// peer with a stale cached gRPC conn) silently drops MsgPreVote /
	// MsgVote / MsgHeartbeat to the live peers, and the cluster never
	// converges on a new leader.
	//
	// Sends fan out concurrently so one dead peer's sendTimeout does not
	// serialize the others. First error wins for observability.
	var wg sync.WaitGroup
	var firstErrMu sync.Mutex
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		firstErrMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		firstErrMu.Unlock()
	}
	for _, msg := range msgs {
		if msg.To == 0 || msg.To == t.localID {
			continue
		}
		wg.Add(1)
		go func(m myraft.Message) {
			defer wg.Done()
			client, err := t.clientFor(m.To)
			if err != nil {
				recordErr(err)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), t.sendTimeout)
			_, err = client.Step(ctx, (*raftpb.Message)(&m))
			cancel()
			if err != nil {
				// Drop the cached connection so the next Send re-dials. A
				// peer that restarted (new container IP, or a conn stuck
				// in TRANSIENT_FAILURE) is otherwise permanently
				// unreachable through the stale cached client.
				t.invalidatePeer(m.To)
				recordErr(err)
				return
			}
		}(msg)
	}
	wg.Wait()
	return firstErr
}

// invalidatePeer tears down the cached gRPC client for id so the next Send
// re-dials. Safe to call even if there is no cached client.
func (t *GRPCTransport) invalidatePeer(id uint64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	conn := t.conns[id]
	delete(t.conns, id)
	delete(t.clients, id)
	t.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (t *GRPCTransport) clientFor(id uint64) (*rootTransportClientImpl, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, errTransportClosed
	}
	if client, ok := t.clients[id]; ok {
		return client, nil
	}
	addr, ok := t.peers[id]
	if !ok || addr == "" {
		return nil, errPeerAddressUnknown(id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.dialTimeout)
	defer cancel()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	if err := waitForTransportReady(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	client := &rootTransportClientImpl{cc: conn}
	t.conns[id] = conn
	t.clients[id] = client
	return client, nil
}

func (t *GRPCTransport) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	conns := t.conns
	server := t.server
	ln := t.ln
	t.conns = make(map[uint64]*grpc.ClientConn)
	t.clients = make(map[uint64]*rootTransportClientImpl)
	t.mu.Unlock()

	var firstErr error
	for _, conn := range conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	server.GracefulStop()
	if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func waitForTransportReady(ctx context.Context, conn *grpc.ClientConn) error {
	conn.Connect()
	for {
		state := conn.GetState()
		switch state {
		case connectivity.Ready:
			return nil
		case connectivity.Shutdown:
			return errTransportConnectionShutdown
		}
		if !conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}
