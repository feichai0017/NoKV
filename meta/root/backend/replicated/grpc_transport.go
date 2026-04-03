package replicated

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
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

type rootTransportClient interface {
	Step(ctx context.Context, in *raftpb.Message, opts ...grpc.CallOption) (*emptypb.Empty, error)
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
		return nil, status.Error(status.Code(err), err.Error())
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
	clients     map[uint64]rootTransportClient
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
		return nil, errors.New("meta/root/backend/replicated: gRPC transport requires non-zero local id")
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
		clients:     make(map[uint64]rootTransportClient),
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

func (t *GRPCTransport) LocalID() uint64 { return t.localID }

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
	for _, msg := range msgs {
		if msg.To == 0 || msg.To == t.localID {
			continue
		}
		client, err := t.clientFor(msg.To)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), t.sendTimeout)
		_, err = client.Step(ctx, (*raftpb.Message)(&msg))
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *GRPCTransport) clientFor(id uint64) (rootTransportClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, errors.New("meta/root/backend/replicated: transport closed")
	}
	if client, ok := t.clients[id]; ok {
		return client, nil
	}
	addr, ok := t.peers[id]
	if !ok || addr == "" {
		return nil, fmt.Errorf("meta/root/backend/replicated: peer %d address unknown", id)
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
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	var firstErr error
	for id, conn := range t.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(t.conns, id)
		delete(t.clients, id)
	}
	t.server.GracefulStop()
	if err := t.ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) && firstErr == nil {
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
			return errors.New("meta/root/backend/replicated: transport connection shutdown")
		}
		if !conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}
