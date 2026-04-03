package rootraft

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	myraft "github.com/feichai0017/NoKV/raft"
	pbraft "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Service struct {
	metapb.UnimplementedMetadataRootServiceServer

	cfg        Config
	node       *Node
	transport  *GRPCTransport
	grpcServer *grpc.Server
	listener   net.Listener

	stopCh chan struct{}
	doneCh chan struct{}
	once   sync.Once
}

func OpenService(cfg Config) (*Service, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	addr := localPeerAddress(cfg)
	if addr == "" {
		return nil, status.Error(codes.InvalidArgument, "meta/root/raft: local peer address is required for grpc service")
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	transport := NewGRPCTransport(cfg.NodeID, cfg.Peers)
	node, err := OpenNode(cfg, Checkpoint{}, transport)
	if err != nil {
		_ = lis.Close()
		return nil, err
	}

	svc := &Service{
		cfg:        cfg,
		node:       node,
		transport:  transport,
		grpcServer: grpc.NewServer(),
		listener:   lis,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
	metapb.RegisterMetadataRootServiceServer(svc.grpcServer, svc)
	go func() {
		defer close(svc.doneCh)
		ticker := time.NewTicker(cfg.TickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-svc.stopCh:
				return
			case <-ticker.C:
				_ = svc.node.Tick()
			}
		}
	}()
	go func() {
		_ = svc.grpcServer.Serve(lis)
	}()
	return svc, nil
}

func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		close(s.stopCh)
		s.grpcServer.GracefulStop()
		_ = s.transport.Close()
		_ = s.listener.Close()
		<-s.doneCh
	})
	return nil
}

func (s *Service) currentState() (rootpkg.State, error) {
	if s == nil || s.node == nil {
		return rootpkg.State{}, nil
	}
	return s.node.Current(), nil
}

func (s *Service) readSince(cursor rootpkg.Cursor) ([]rootpkg.Event, rootpkg.Cursor, error) {
	if s == nil || s.node == nil {
		return nil, rootpkg.Cursor{}, nil
	}
	events, tail := s.node.ReadSince(cursor)
	return events, tail, nil
}

func (s *Service) appendEvents(events ...rootpkg.Event) (rootpkg.CommitInfo, error) {
	if s == nil || s.node == nil {
		return rootpkg.CommitInfo{}, nil
	}
	var commit rootpkg.CommitInfo
	for _, event := range events {
		ci, err := s.node.ProposeEvent(event)
		if err != nil {
			return rootpkg.CommitInfo{}, err
		}
		commit = ci
	}
	if len(events) == 0 {
		state := s.node.Current()
		return rootpkg.CommitInfo{Cursor: state.LastCommitted, State: state}, nil
	}
	return commit, nil
}

func (s *Service) fenceAllocator(kind rootpkg.AllocatorKind, min uint64) (uint64, error) {
	if s == nil || s.node == nil {
		return 0, nil
	}
	ci, err := s.node.ProposeFence(kind, min)
	if err != nil {
		return 0, err
	}
	switch kind {
	case rootpkg.AllocatorKindID:
		return ci.State.IDFence, nil
	case rootpkg.AllocatorKindTSO:
		return ci.State.TSOFence, nil
	default:
		return 0, nil
	}
}

func (s *Service) Current(_ context.Context, _ *metapb.RootCurrentRequest) (*metapb.RootCurrentResponse, error) {
	state, err := s.currentState()
	if err != nil {
		return nil, toStatusError(err)
	}
	return &metapb.RootCurrentResponse{State: metacodec.RootStateToProto(state)}, nil
}

func (s *Service) ReadSince(_ context.Context, req *metapb.RootReadSinceRequest) (*metapb.RootReadSinceResponse, error) {
	var cursor rootpkg.Cursor
	if req != nil && req.Cursor != nil {
		cursor = rootpkg.Cursor{Term: req.Cursor.Term, Index: req.Cursor.Index}
	}
	events, tail, err := s.readSince(cursor)
	if err != nil {
		return nil, toStatusError(err)
	}
	resp := &metapb.RootReadSinceResponse{Tail: &metapb.RootCursor{Term: tail.Term, Index: tail.Index}}
	if len(events) > 0 {
		resp.Events = make([]*metapb.RootEvent, 0, len(events))
		for _, event := range events {
			resp.Events = append(resp.Events, metacodec.RootEventToProto(event))
		}
	}
	return resp, nil
}

func (s *Service) Append(_ context.Context, req *metapb.RootAppendRequest) (*metapb.RootCommitInfo, error) {
	var events []rootpkg.Event
	if req != nil && len(req.Events) > 0 {
		events = make([]rootpkg.Event, 0, len(req.Events))
		for _, event := range req.Events {
			events = append(events, metacodec.RootEventFromProto(event))
		}
	}
	commit, err := s.appendEvents(events...)
	if err != nil {
		return nil, toStatusError(err)
	}
	return metacodec.RootCommitInfoToProto(commit), nil
}

func (s *Service) FenceAllocator(_ context.Context, req *metapb.RootFenceAllocatorRequest) (*metapb.RootFenceAllocatorResponse, error) {
	if req == nil {
		return &metapb.RootFenceAllocatorResponse{}, nil
	}
	fence, err := s.fenceAllocator(metacodec.RootAllocatorKindFromProto(req.Kind), req.Min)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &metapb.RootFenceAllocatorResponse{Fence: fence}, nil
}

func (s *Service) Step(_ context.Context, req *metapb.RootRaftWireMessage) (*emptypb.Empty, error) {
	if req == nil || len(req.Payload) == 0 {
		return &emptypb.Empty{}, nil
	}
	var msg pbraft.Message
	if err := msg.Unmarshal(req.Payload); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.node.Step(myraft.Message(msg)); err != nil {
		return nil, toStatusError(err)
	}
	return &emptypb.Empty{}, nil
}

type Client struct {
	cc  *grpc.ClientConn
	rpc metapb.MetadataRootServiceClient
}

var _ rootpkg.Root = (*Client)(nil)

func Dial(ctx context.Context, addr string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	cc, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{cc: cc, rpc: metapb.NewMetadataRootServiceClient(cc)}, nil
}

func (c *Client) Close() error {
	if c == nil || c.cc == nil {
		return nil
	}
	return c.cc.Close()
}

func (c *Client) Current() (rootpkg.State, error) {
	resp, err := c.rpc.Current(context.Background(), &metapb.RootCurrentRequest{})
	if err != nil {
		return rootpkg.State{}, err
	}
	return metacodec.RootStateFromProto(resp.GetState()), nil
}

func (c *Client) ReadSince(cursor rootpkg.Cursor) ([]rootpkg.Event, rootpkg.Cursor, error) {
	resp, err := c.rpc.ReadSince(context.Background(), &metapb.RootReadSinceRequest{
		Cursor: &metapb.RootCursor{Term: cursor.Term, Index: cursor.Index},
	})
	if err != nil {
		return nil, rootpkg.Cursor{}, err
	}
	events := make([]rootpkg.Event, 0, len(resp.GetEvents()))
	for _, event := range resp.GetEvents() {
		events = append(events, metacodec.RootEventFromProto(event))
	}
	var tail rootpkg.Cursor
	if resp.Tail != nil {
		tail = rootpkg.Cursor{Term: resp.Tail.Term, Index: resp.Tail.Index}
	}
	return events, tail, nil
}

func (c *Client) Append(events ...rootpkg.Event) (rootpkg.CommitInfo, error) {
	req := &metapb.RootAppendRequest{}
	if len(events) > 0 {
		req.Events = make([]*metapb.RootEvent, 0, len(events))
		for _, event := range events {
			req.Events = append(req.Events, metacodec.RootEventToProto(event))
		}
	}
	resp, err := c.rpc.Append(context.Background(), req)
	if err != nil {
		return rootpkg.CommitInfo{}, err
	}
	return metacodec.RootCommitInfoFromProto(resp), nil
}

func (c *Client) FenceAllocator(kind rootpkg.AllocatorKind, min uint64) (uint64, error) {
	resp, err := c.rpc.FenceAllocator(context.Background(), &metapb.RootFenceAllocatorRequest{
		Kind: metacodec.RootAllocatorKindToProto(kind),
		Min:  min,
	})
	if err != nil {
		return 0, err
	}
	return resp.Fence, nil
}

func localPeerAddress(cfg Config) string {
	for _, peer := range cfg.Peers {
		if peer.ID == cfg.NodeID {
			return peer.Address
		}
	}
	return ""
}

func toStatusError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotLeader) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
