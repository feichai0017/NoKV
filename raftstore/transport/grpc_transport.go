package transport

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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	raftServiceName    = "nokv.raft.Transport"
	raftStepMethod     = "Step"
	raftStepFullMethod = "/" + raftServiceName + "/" + raftStepMethod
)

type GRPCOption func(*grpcTransportConfig)

type grpcTransportConfig struct {
	serverCreds  credentials.TransportCredentials
	clientCreds  credentials.TransportCredentials
	dialTimeout  time.Duration
	sendTimeout  time.Duration
	maxRetries   int
	retryBackoff time.Duration
	registrars   []func(grpc.ServiceRegistrar)
}

func defaultGRPCConfig() grpcTransportConfig {
	return grpcTransportConfig{
		clientCreds:  insecure.NewCredentials(),
		dialTimeout:  time.Second,
		sendTimeout:  time.Second,
		maxRetries:   0,
		retryBackoff: 0,
	}
}

func WithServerCredentials(creds credentials.TransportCredentials) GRPCOption {
	return func(cfg *grpcTransportConfig) {
		cfg.serverCreds = creds
	}
}

func WithClientCredentials(creds credentials.TransportCredentials) GRPCOption {
	return func(cfg *grpcTransportConfig) {
		cfg.clientCreds = creds
	}
}

func WithDialTimeout(d time.Duration) GRPCOption {
	return func(cfg *grpcTransportConfig) {
		if d > 0 {
			cfg.dialTimeout = d
		}
	}
}

func WithSendTimeout(d time.Duration) GRPCOption {
	return func(cfg *grpcTransportConfig) {
		if d >= 0 {
			cfg.sendTimeout = d
		}
	}
}

func WithRetry(maxRetries int, backoff time.Duration) GRPCOption {
	return func(cfg *grpcTransportConfig) {
		if maxRetries < 0 {
			maxRetries = 0
		}
		cfg.maxRetries = maxRetries
		if backoff > 0 {
			cfg.retryBackoff = backoff
		}
	}
}

// WithServerRegistrar registers additional gRPC services on the transport
// before it starts serving. Callers can pass multiple functions to register
// several services.
func WithServerRegistrar(regs ...func(grpc.ServiceRegistrar)) GRPCOption {
	return func(cfg *grpcTransportConfig) {
		cfg.registrars = append(cfg.registrars, regs...)
	}
}

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

var (
	errPeerBlocked = errors.New("raftstore: peer blocked")
	errPeerUnknown = errors.New("raftstore: peer address unknown")
)

func raftStepHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
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
	mu           sync.RWMutex
	localID      uint64
	addr         string
	peers        map[uint64]string
	blocked      map[uint64]struct{}
	conns        map[uint64]*grpc.ClientConn
	clients      map[uint64]raftServiceClient
	handler      func(myraft.Message) error
	server       *grpc.Server
	ln           net.Listener
	stopCh       chan struct{}
	wg           sync.WaitGroup
	dialTimeout  time.Duration
	sendTimeout  time.Duration
	maxRetries   int
	retryBackoff time.Duration
	clientCreds  credentials.TransportCredentials
}

// NewGRPCTransport starts a gRPC server bound to listenAddr.
func NewGRPCTransport(localID uint64, listenAddr string, opts ...GRPCOption) (*GRPCTransport, error) {
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
	cfg := defaultGRPCConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	clientCreds := cfg.clientCreds
	if clientCreds == nil {
		clientCreds = insecure.NewCredentials()
	}
	serverOpts := make([]grpc.ServerOption, 0, 1)
	if cfg.serverCreds != nil {
		serverOpts = append(serverOpts, grpc.Creds(cfg.serverCreds))
	}
	t := &GRPCTransport{
		localID:      localID,
		addr:         ln.Addr().String(),
		peers:        make(map[uint64]string),
		blocked:      make(map[uint64]struct{}),
		conns:        make(map[uint64]*grpc.ClientConn),
		clients:      make(map[uint64]raftServiceClient),
		server:       grpc.NewServer(serverOpts...),
		ln:           ln,
		stopCh:       make(chan struct{}),
		dialTimeout:  cfg.dialTimeout,
		sendTimeout:  cfg.sendTimeout,
		maxRetries:   cfg.maxRetries,
		retryBackoff: cfg.retryBackoff,
		clientCreds:  clientCreds,
	}
	raftSrv := &raftService{transport: t}
	t.server.RegisterService(&raftServiceDesc, raftSrv)
	for _, register := range cfg.registrars {
		if register != nil {
			register(t.server)
		}
	}
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
	var blockedDelta int64
	t.mu.Lock()
	if addr == "" {
		delete(t.peers, id)
		if conn, ok := t.conns[id]; ok {
			_ = conn.Close()
			delete(t.conns, id)
			delete(t.clients, id)
		}
		if _, ok := t.blocked[id]; ok {
			delete(t.blocked, id)
			blockedDelta--
		}
		t.mu.Unlock()
		if blockedDelta != 0 {
			grpcMetrics().recordBlocked(blockedDelta)
		}
		return
	}
	if conn, ok := t.conns[id]; ok {
		_ = conn.Close()
		delete(t.conns, id)
		delete(t.clients, id)
	}
	if _, ok := t.blocked[id]; ok {
		delete(t.blocked, id)
		blockedDelta--
	}
	t.peers[id] = addr
	t.mu.Unlock()
	if blockedDelta != 0 {
		grpcMetrics().recordBlocked(blockedDelta)
	}
}

// BlockPeer drops outbound messages destined for the provided peer ID.
func (t *GRPCTransport) BlockPeer(id uint64) {
	if id == 0 {
		return
	}
	t.mu.Lock()
	_, existed := t.blocked[id]
	if !existed {
		t.blocked[id] = struct{}{}
	}
	t.mu.Unlock()
	if !existed {
		grpcMetrics().recordBlocked(1)
	}
}

// UnblockPeer resumes delivery for the provided peer ID.
func (t *GRPCTransport) UnblockPeer(id uint64) {
	if id == 0 {
		return
	}
	t.mu.Lock()
	_, existed := t.blocked[id]
	if existed {
		delete(t.blocked, id)
	}
	t.mu.Unlock()
	if existed {
		grpcMetrics().recordBlocked(-1)
	}
}

// Send forwards the message to the remote peer using gRPC.
func (t *GRPCTransport) Send(msg myraft.Message) {
	if msg.To == 0 {
		return
	}
	attempts := t.maxRetries + 1
	if attempts <= 0 {
		attempts = 1
	}
	for attempt := 0; attempt < attempts; attempt++ {
		client, err := t.getClient(msg.To)
		if err != nil {
			if errors.Is(err, errPeerBlocked) || errors.Is(err, errPeerUnknown) {
				return
			}
			if attempt < attempts-1 {
				t.backoff()
			}
			continue
		}
		metrics := grpcMetrics()
		metrics.recordSendAttempt(attempt > 0)
		var (
			ctx    context.Context
			cancel context.CancelFunc
		)
		if t.sendTimeout > 0 {
			ctx, cancel = context.WithTimeout(context.Background(), t.sendTimeout)
		} else {
			ctx, cancel = context.WithCancel(context.Background())
		}
		pbMsg := raftpb.Message(msg)
		_, err = client.Step(ctx, &pbMsg)
		cancel()
		if err == nil {
			metrics.recordSendSuccess()
			return
		}
		metrics.recordSendFailure(err, attempt == attempts-1)
		t.handleSendError(msg.To, err)
		if attempt == attempts-1 {
			return
		}
		t.backoff()
	}
}

func (t *GRPCTransport) getClient(id uint64) (raftServiceClient, error) {
	t.mu.RLock()
	addr, ok := t.peers[id]
	if ok {
		if _, blocked := t.blocked[id]; blocked {
			t.mu.RUnlock()
			return nil, errPeerBlocked
		}
		if client, exists := t.clients[id]; exists {
			t.mu.RUnlock()
			return client, nil
		}
	}
	t.mu.RUnlock()
	if !ok || addr == "" {
		return nil, errPeerUnknown
	}
	ctx := context.Background()
	cancel := func() {}
	if t.dialTimeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), t.dialTimeout)
	}
	defer cancel()
	metrics := grpcMetrics()
	metrics.recordDialAttempt()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(t.clientCreds))
	if err != nil {
		metrics.recordDialFailure(err)
		return nil, err
	}
	if err := t.waitForClientReady(ctx, conn); err != nil {
		metrics.recordDialFailure(err)
		_ = conn.Close()
		return nil, err
	}
	metrics.recordDialSuccess()
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
		_ = conn.Close()
		delete(t.conns, id)
		delete(t.clients, id)
	}
	t.mu.Unlock()
}

func (t *GRPCTransport) backoff() {
	if t.retryBackoff <= 0 {
		return
	}
	time.Sleep(t.retryBackoff)
}

func (t *GRPCTransport) waitForClientReady(ctx context.Context, conn *grpc.ClientConn) error {
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if state == connectivity.Shutdown {
			return fmt.Errorf("raftstore: connection shutdown while dialing")
		}
		if !conn.WaitForStateChange(ctx, state) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("raftstore: wait for state change failed")
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// Close shuts down the transport and releases resources.
func (t *GRPCTransport) Close() error {
	close(t.stopCh)
	t.server.GracefulStop()
	_ = t.ln.Close()
	var conns []*grpc.ClientConn
	var blockedCount int64
	t.mu.Lock()
	for id, conn := range t.conns {
		conns = append(conns, conn)
		delete(t.conns, id)
		delete(t.clients, id)
	}
	if len(t.blocked) > 0 {
		blockedCount = int64(len(t.blocked))
		t.blocked = make(map[uint64]struct{})
	}
	t.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	t.wg.Wait()
	if blockedCount > 0 {
		grpcMetrics().recordBlocked(-blockedCount)
	}
	return nil
}
