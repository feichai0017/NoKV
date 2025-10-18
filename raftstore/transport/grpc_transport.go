package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	myraft "github.com/feichai0017/NoKV/raft"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	raftServiceName    = "nokv.raft.Transport"
	raftStepMethod     = "Step"
	raftStepFullMethod = "/" + raftServiceName + "/" + raftStepMethod
)

type raftServiceServer interface {
	Step(context.Context, *raftpb.Message) (*emptypb.Empty, error)
}

type raftService struct {
	transport *GRPCTransport
}

func (s *raftService) Step(ctx context.Context, msg *raftpb.Message) (*emptypb.Empty, error) {
	if s == nil || s.transport == nil {
		return &emptypb.Empty{}, nil
	}
	handler := s.transport.getHandler()
	if handler == nil {
		return &emptypb.Empty{}, nil
	}
	if err := handler(myraft.Message(*msg)); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &emptypb.Empty{}, nil
}

var raftServiceDesc = grpc.ServiceDesc{
	ServiceName: raftServiceName,
	HandlerType: (*raftServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: raftStepMethod,
			Handler:    raftStepHandler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "",
}

func raftStepHandler(srv any, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(raftpb.Message)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(raftServiceServer).Step(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: raftStepFullMethod,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(raftServiceServer).Step(ctx, req.(*raftpb.Message))
	}
	return interceptor(ctx, in, info, handler)
}

type raftServiceClient interface {
	Step(ctx context.Context, in *raftpb.Message, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type raftServiceClientImpl struct {
	cc grpc.ClientConnInterface
}

func (c *raftServiceClientImpl) Step(ctx context.Context, in *raftpb.Message, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	if err := c.cc.Invoke(ctx, raftStepFullMethod, in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

// GRPCTransport implements Transport backed by gRPC connections.
type GRPCTransport struct {
	mu          sync.RWMutex
	localID     uint64
	addr        string
	peers       map[uint64]string
	blocked     map[uint64]struct{}
	conns       map[uint64]*grpc.ClientConn
	clients     map[uint64]raftServiceClient
	handler     func(myraft.Message) error
	server      *grpc.Server
	ln          net.Listener
	stopCh      chan struct{}
	wg          sync.WaitGroup
	dialTimeout time.Duration
}

// NewGRPCTransport starts a gRPC server bound to listenAddr.
func NewGRPCTransport(localID uint64, listenAddr string) (*GRPCTransport, error) {
	if localID == 0 {
		return nil, errors.New("raftstore: gRPC transport requires non-zero local ID")
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
		blocked:     make(map[uint64]struct{}),
		conns:       make(map[uint64]*grpc.ClientConn),
		clients:     make(map[uint64]raftServiceClient),
		server:      grpc.NewServer(),
		ln:          ln,
		stopCh:      make(chan struct{}),
		dialTimeout: time.Second,
	}
	raftSrv := &raftService{transport: t}
	t.server.RegisterService(&raftServiceDesc, raftSrv)
	t.wg.Add(1)
	go t.serve()
	return t, nil
}

func (t *GRPCTransport) serve() {
	defer t.wg.Done()
	if err := t.server.Serve(t.ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		select {
		case <-t.stopCh:
		default:
		}
	}
}

// Addr returns the listener address.
func (t *GRPCTransport) Addr() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.addr
}

// LocalID returns the ID associated with this transport.
func (t *GRPCTransport) LocalID() uint64 {
	return t.localID
}

// SetHandler assigns the callback invoked when a message is delivered to this transport.
func (t *GRPCTransport) SetHandler(fn func(myraft.Message) error) {
	t.mu.Lock()
	t.handler = fn
	t.mu.Unlock()
}

func (t *GRPCTransport) getHandler() func(myraft.Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.handler
}

// SetPeer associates a remote peer ID with its address.
func (t *GRPCTransport) SetPeer(id uint64, addr string) {
	if id == 0 || id == t.localID {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if addr == "" {
		delete(t.peers, id)
		if conn, ok := t.conns[id]; ok {
			conn.Close()
			delete(t.conns, id)
			delete(t.clients, id)
		}
		return
	}
	if conn, ok := t.conns[id]; ok {
		conn.Close()
		delete(t.conns, id)
		delete(t.clients, id)
	}
	delete(t.blocked, id)
	t.peers[id] = addr
}

// BlockPeer drops outbound messages destined for the provided peer ID.
func (t *GRPCTransport) BlockPeer(id uint64) {
	if id == 0 {
		return
	}
	t.mu.Lock()
	t.blocked[id] = struct{}{}
	t.mu.Unlock()
}

// UnblockPeer resumes delivery for the provided peer ID.
func (t *GRPCTransport) UnblockPeer(id uint64) {
	if id == 0 {
		return
	}
	t.mu.Lock()
	delete(t.blocked, id)
	t.mu.Unlock()
}

// Send forwards the message to the remote peer using gRPC.
func (t *GRPCTransport) Send(msg myraft.Message) {
	if msg.To == 0 {
		return
	}
	client, err := t.getClient(msg.To)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.dialTimeout)
	defer cancel()
	pbMsg := raftpb.Message(msg)
	if _, err := client.Step(ctx, &pbMsg); err != nil {
		t.handleSendError(msg.To, err)
	}
}

func (t *GRPCTransport) getClient(id uint64) (raftServiceClient, error) {
	t.mu.RLock()
	addr, ok := t.peers[id]
	if ok {
		if _, blocked := t.blocked[id]; blocked {
			t.mu.RUnlock()
			return nil, errors.New("raftstore: peer blocked")
		}
		if client, exists := t.clients[id]; exists {
			t.mu.RUnlock()
			return client, nil
		}
	}
	t.mu.RUnlock()
	if !ok || addr == "" {
		return nil, errors.New("raftstore: peer address unknown")
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	client := &raftServiceClientImpl{cc: conn}
	t.mu.Lock()
	t.conns[id] = conn
	t.clients[id] = client
	t.mu.Unlock()
	return client, nil
}

func (t *GRPCTransport) handleSendError(id uint64, err error) {
	t.mu.Lock()
	if conn, ok := t.conns[id]; ok {
		conn.Close()
		delete(t.conns, id)
		delete(t.clients, id)
	}
	t.mu.Unlock()
}

// Close shuts down the transport and releases resources.
func (t *GRPCTransport) Close() error {
	close(t.stopCh)
	t.server.GracefulStop()
	_ = t.ln.Close()
	var conns []*grpc.ClientConn
	t.mu.Lock()
	for id, conn := range t.conns {
		conns = append(conns, conn)
		delete(t.conns, id)
		delete(t.clients, id)
	}
	t.mu.Unlock()
	for _, conn := range conns {
		conn.Close()
	}
	t.wg.Wait()
	return nil
}
