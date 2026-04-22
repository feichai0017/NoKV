// Package client is the coordinator-side gRPC client for the meta-root
// service. It mirrors the layout used by raftstore/client and
// coordinator/client: callers dial the Service registered by meta/root/server
// via Dial/DialCluster and get back a Client that implements the same
// authority surface (Snapshot/Append/FenceAllocator/ObserveTail/...) used by
// coordinator/rootview.
package client

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const defaultCallTimeout = 3 * time.Second

const errMetadataRootNotLeader = "metadata root not leader"

// Client is a remote metadata-root backend client. It implements the same
// authority surface consumed by coordinator/rootview.OpenRootRemoteStore.
type Client struct {
	mu          sync.Mutex
	endpoints   []clientEndpoint
	byID        map[uint64]int
	preferred   int
	callTimeout time.Duration
}

type clientEndpoint struct {
	id   uint64
	rpc  metapb.MetadataRootClient
	conn *grpc.ClientConn
}

// NewClient wraps an existing gRPC client connection.
func NewClient(conn grpc.ClientConnInterface) *Client {
	return &Client{
		endpoints:   []clientEndpoint{{rpc: metapb.NewMetadataRootClient(conn)}},
		byID:        make(map[uint64]int),
		callTimeout: defaultCallTimeout,
	}
}

// Dial opens a metadata-root client connection.
func Dial(ctx context.Context, target string, opts ...grpc.DialOption) (*Client, error) {
	if strings.TrimSpace(target) == "" {
		return nil, errEmptyTarget
	}
	conn, err := dialEndpoint(ctx, target, opts...)
	if err != nil {
		return nil, err
	}
	client := NewClient(conn)
	client.endpoints[0].conn = conn
	return client, nil
}

// DialCluster opens a multi-endpoint metadata-root client. The map key is the
// stable metadata-root node id returned as leader_id by Status and not-leader
// errors.
func DialCluster(ctx context.Context, targets map[uint64]string, opts ...grpc.DialOption) (*Client, error) {
	ids := make([]uint64, 0, len(targets))
	for id, target := range targets {
		if id == 0 || strings.TrimSpace(target) == "" {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, errEmptyTargetSet
	}
	slices.Sort(ids)
	client := &Client{
		endpoints:   make([]clientEndpoint, 0, len(ids)),
		byID:        make(map[uint64]int, len(ids)),
		callTimeout: defaultCallTimeout,
	}
	for _, id := range ids {
		conn, err := dialEndpoint(ctx, targets[id], opts...)
		if err != nil {
			_ = client.Close()
			return nil, err
		}
		client.byID[id] = len(client.endpoints)
		client.endpoints = append(client.endpoints, clientEndpoint{
			id:   id,
			rpc:  metapb.NewMetadataRootClient(conn),
			conn: conn,
		})
	}
	// Probe each endpoint for the raft leader so reads land on the leader from
	// the first call and the cached cursor/lease state stays fresh. Without
	// this, the first reads (catch-up, ObserveCommitted) go to whichever
	// endpoint has index 0 — typically a follower — and a lagging follower
	// can surface state that regressed behind what the local coordinator has
	// already observed on writes, which causes visible thrash in multi-peer
	// deployments.
	client.pinLeaderPreferred(ctx)
	return client, nil
}

// pinLeaderPreferred asks each endpoint for its current Status view and
// switches the preferred index to the raft leader if any peer reports one.
// Best-effort: failures leave preferred at its sorted-order default.
func (c *Client) pinLeaderPreferred(ctx context.Context) {
	if c == nil || len(c.endpoints) == 0 {
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for _, endpoint := range c.endpoints {
		resp, err := endpoint.rpc.Status(probeCtx, &metapb.MetadataRootStatusRequest{})
		if err != nil {
			continue
		}
		leaderID := resp.GetLeaderId()
		if leaderID == 0 {
			continue
		}
		c.markPreferred(leaderID)
		return
	}
}

// Close closes the underlying connections owned by this client.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for _, endpoint := range c.endpoints {
		if endpoint.conn == nil {
			continue
		}
		if err := endpoint.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *Client) Snapshot() (rootstate.Snapshot, error) {
	resp, err := invokeRead(c, context.Background(), func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootSnapshotResponse, error) {
		return rpc.Snapshot(ctx, &metapb.MetadataRootSnapshotRequest{})
	})
	if err != nil {
		return rootstate.Snapshot{}, err
	}
	snapshot, _ := metawire.RootSnapshotFromProto(resp.GetCheckpoint())
	return snapshot, nil
}

func (c *Client) Append(ctx context.Context, events ...rootevent.Event) (rootstate.CommitInfo, error) {
	if c == nil || len(events) == 0 {
		snapshot, err := c.Snapshot()
		return rootstate.CommitInfo{Cursor: snapshot.State.LastCommitted, State: snapshot.State}, err
	}
	pbEvents := make([]*metapb.RootEvent, 0, len(events))
	for _, event := range events {
		pbEvents = append(pbEvents, metawire.RootEventToProto(event))
	}
	resp, err := invokeWrite(c, ctx, func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootAppendResponse, error) {
		return rpc.Append(ctx, &metapb.MetadataRootAppendRequest{Events: pbEvents})
	})
	if err != nil {
		return rootstate.CommitInfo{}, err
	}
	return rootstate.CommitInfo{
		Cursor: metawire.RootCursorFromProto(resp.GetCursor()),
		State:  metawire.RootStateFromProto(resp.GetState()),
	}, nil
}

func (c *Client) FenceAllocator(ctx context.Context, kind rootstate.AllocatorKind, min uint64) (uint64, error) {
	resp, err := invokeWrite(c, ctx, func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootFenceAllocatorResponse, error) {
		return rpc.FenceAllocator(ctx, &metapb.MetadataRootFenceAllocatorRequest{
			Kind:    metawire.RootAllocatorKindToProto(kind),
			Minimum: min,
		})
	})
	if err != nil {
		return 0, err
	}
	return resp.GetCurrent(), nil
}

func (c *Client) IsLeader() bool {
	statusResp, err := c.status()
	return err == nil && statusResp.GetIsLeader()
}

func (c *Client) LeaderID() uint64 {
	statusResp, err := c.status()
	if err != nil {
		return 0
	}
	return statusResp.GetLeaderId()
}

func (c *Client) ApplyCoordinatorLease(ctx context.Context, cmd rootproto.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error) {
	if !validCoordinatorLeaseCommandKind(cmd.Kind) {
		return rootstate.CoordinatorProtocolState{}, rootstate.ErrInvalidCoordinatorLease
	}
	resp, err := invokeWrite(c, ctx, func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootApplyCoordinatorLeaseResponse, error) {
		return rpc.ApplyCoordinatorLease(ctx, &metapb.MetadataRootApplyCoordinatorLeaseRequest{
			Command: metawire.RootCoordinatorLeaseCommandToProto(cmd),
		})
	})
	if err != nil {
		return rootstate.CoordinatorProtocolState{}, err
	}
	protocolState := metawire.RootCoordinatorProtocolStateFromProto(resp.GetState())
	if cmd.Kind == rootproto.CoordinatorLeaseCommandIssue &&
		resp.GetStatus() == metapb.RootCoordinatorLeaseApplyStatus_ROOT_COORDINATOR_LEASE_APPLY_STATUS_HELD {
		return protocolState, rootstate.ErrCoordinatorLeaseHeld
	}
	return protocolState, nil
}

func (c *Client) ApplyCoordinatorClosure(ctx context.Context, cmd rootproto.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error) {
	if !validCoordinatorClosureCommandKind(cmd.Kind) {
		return rootstate.CoordinatorProtocolState{}, rootstate.ErrCoordinatorLeaseAudit
	}
	resp, err := invokeWrite(c, ctx, func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootApplyCoordinatorClosureResponse, error) {
		return rpc.ApplyCoordinatorClosure(ctx, &metapb.MetadataRootApplyCoordinatorClosureRequest{
			Command: metawire.RootCoordinatorClosureCommandToProto(cmd),
		})
	})
	if err != nil {
		return rootstate.CoordinatorProtocolState{}, err
	}
	return metawire.RootCoordinatorProtocolStateFromProto(resp.GetState()), nil
}

func (c *Client) ObserveCommitted() (rootstorage.ObservedCommitted, error) {
	resp, err := invokeRead(c, context.Background(), func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootObserveCommittedResponse, error) {
		return rpc.ObserveCommitted(ctx, &metapb.MetadataRootObserveCommittedRequest{})
	})
	if err != nil {
		return rootstorage.ObservedCommitted{}, err
	}
	return metawire.RootObservedFromProto(resp.GetCheckpoint(), resp.GetTail()), nil
}

func (c *Client) ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	resp, err := invokeRead(c, context.Background(), func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootObserveTailResponse, error) {
		return rpc.ObserveTail(ctx, &metapb.MetadataRootObserveTailRequest{After: metawire.RootTailTokenToProto(after)})
	})
	if err != nil {
		return rootstorage.TailAdvance{}, err
	}
	return metawire.RootTailAdvanceFromProto(resp.GetAfter(), resp.GetToken(), resp.GetCheckpoint(), resp.GetTail()), nil
}

func (c *Client) WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error) {
	resp, err := invokeRead(c, context.Background(), func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootWaitTailResponse, error) {
		return rpc.WaitTail(ctx, &metapb.MetadataRootWaitTailRequest{
			After:         metawire.RootTailTokenToProto(after),
			TimeoutMillis: uint64(timeout / time.Millisecond),
		})
	})
	if err != nil {
		return rootstorage.TailAdvance{}, err
	}
	return metawire.RootTailAdvanceFromProto(resp.GetAfter(), resp.GetToken(), resp.GetCheckpoint(), resp.GetTail()), nil
}

func (c *Client) status() (*metapb.MetadataRootStatusResponse, error) {
	return invokeRead(c, context.Background(), func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootStatusResponse, error) {
		return rpc.Status(ctx, &metapb.MetadataRootStatusRequest{})
	})
}

func invokeRead[T any](c *Client, ctx context.Context, call func(context.Context, metapb.MetadataRootClient) (T, error)) (T, error) {
	return invoke(c, ctx, false, call)
}

func invokeWrite[T any](c *Client, ctx context.Context, call func(context.Context, metapb.MetadataRootClient) (T, error)) (T, error) {
	return invoke(c, ctx, true, call)
}

func invoke[T any](c *Client, parent context.Context, write bool, call func(context.Context, metapb.MetadataRootClient) (T, error)) (T, error) {
	var zero T
	if c == nil {
		return zero, errNilClient
	}
	if parent == nil {
		parent = context.Background()
	}
	if err := parent.Err(); err != nil {
		return zero, err
	}
	endpoints := c.orderedEndpoints()
	if len(endpoints) == 0 {
		return zero, errNoEndpoints
	}
	var lastErr error
	tried := make(map[uint64]struct{}, len(endpoints))
	maxAttempts := len(endpoints)
	for attempts := 0; attempts < maxAttempts && len(endpoints) > 0; {
		endpoint := endpoints[0]
		endpoints = endpoints[1:]
		if endpoint.id != 0 {
			if _, ok := tried[endpoint.id]; ok {
				continue
			}
			tried[endpoint.id] = struct{}{}
		}
		attempts++
		ctx, cancel := c.context(parent)
		resp, err := call(ctx, endpoint.rpc)
		cancel()
		if err == nil {
			c.markPreferred(endpoint.id)
			return resp, nil
		}
		lastErr = err
		if !retryableRemoteError(err, write) {
			return zero, err
		}
		if write {
			if leaderID, ok := leaderHint(err); ok {
				if hinted, ok := c.endpointByID(leaderID); ok {
					endpoints = append([]clientEndpoint{hinted}, endpoints...)
				}
			}
		}
	}
	if lastErr == nil {
		lastErr = errNoReachableEndpoint
	}
	return zero, lastErr
}

func (c *Client) orderedEndpoints() []clientEndpoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.endpoints) == 0 {
		return nil
	}
	start := c.preferred
	if start < 0 || start >= len(c.endpoints) {
		start = 0
	}
	out := make([]clientEndpoint, 0, len(c.endpoints))
	for i := 0; i < len(c.endpoints); i++ {
		out = append(out, c.endpoints[(start+i)%len(c.endpoints)])
	}
	return out
}

func (c *Client) endpointByID(id uint64) (clientEndpoint, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx, ok := c.byID[id]
	if !ok || idx < 0 || idx >= len(c.endpoints) {
		return clientEndpoint{}, false
	}
	return c.endpoints[idx], true
}

func (c *Client) markPreferred(id uint64) {
	if id == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx, ok := c.byID[id]; ok {
		c.preferred = idx
	}
}

func (c *Client) context(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := defaultCallTimeout
	if c != nil && c.callTimeout > 0 {
		timeout = c.callTimeout
	}
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

func dialEndpoint(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	if err := waitForReady(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func validCoordinatorLeaseCommandKind(kind rootproto.CoordinatorLeaseCommandKind) bool {
	switch kind {
	case rootproto.CoordinatorLeaseCommandIssue, rootproto.CoordinatorLeaseCommandRelease:
		return true
	default:
		return false
	}
}

func validCoordinatorClosureCommandKind(kind rootproto.CoordinatorClosureCommandKind) bool {
	switch kind {
	case rootproto.CoordinatorClosureCommandSeal,
		rootproto.CoordinatorClosureCommandConfirm,
		rootproto.CoordinatorClosureCommandClose,
		rootproto.CoordinatorClosureCommandReattach:
		return true
	default:
		return false
	}
}

func waitForReady(ctx context.Context, conn *grpc.ClientConn) error {
	if conn == nil {
		return nil
	}
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

func retryableRemoteError(err error, write bool) bool {
	code := status.Code(err)
	if code == codes.Unavailable || code == codes.DeadlineExceeded {
		return true
	}
	return write && code == codes.FailedPrecondition && strings.Contains(err.Error(), errMetadataRootNotLeader)
}

func leaderHint(err error) (uint64, bool) {
	msg := err.Error()
	idx := strings.Index(msg, "leader_id=")
	if idx < 0 {
		return 0, false
	}
	start := idx + len("leader_id=")
	end := start
	for end < len(msg) && msg[end] >= '0' && msg[end] <= '9' {
		end++
	}
	if end == start {
		return 0, false
	}
	id, parseErr := strconv.ParseUint(msg[start:end], 10, 64)
	if parseErr != nil || id == 0 {
		return 0, false
	}
	return id, true
}
