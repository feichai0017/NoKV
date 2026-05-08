// Package client is the coordinator-side gRPC client for the meta-root
// service. It mirrors the layout used by raftstore/client and
// coordinator/client: callers dial the Service registered by meta/root/server
// via Dial/DialCluster and get back a Client that implements the same
// authority surface (Snapshot/Append/FenceAllocator/ObserveTail/...) used by
// coordinator/rootview.
package client

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultCallTimeout            = 3 * time.Second
	defaultMaxRootRPCMessageBytes = 64 << 20
)

const errMetadataRootNotLeader = "metadata root not leader"

const (
	metaRootReasonMetadata = "meta_root_reason"
	leaderIDMetadata       = "leader_id"
	reasonNotLeader        = "not_leader"
)

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

func (c *Client) ApplyGrant(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error) {
	if !validGrantAct(cmd.Kind) {
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, rootstate.ErrInvalidGrant
	}
	resp, err := invokeWrite(c, ctx, func(ctx context.Context, rpc metapb.MetadataRootClient) (*metapb.MetadataRootApplyGrantResponse, error) {
		return rpc.ApplyGrant(ctx, &metapb.MetadataRootApplyGrantRequest{
			Command: metawire.RootGrantCommandToProto(cmd),
		})
	})
	if err != nil {
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, err
	}
	protocolState := metawire.RootEunomiaStateFromProto(resp.GetState())
	cert := metawire.RootGrantCertificateFromProto(resp.GetCertificate())
	if cmd.Kind == rootproto.GrantActIssue &&
		resp.GetStatus() == metapb.RootGrantApplyStatus_ROOT_GRANT_APPLY_STATUS_HELD {
		return protocolState, cert, rootstate.ErrPrimacy
	}
	return protocolState, cert, nil
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
	dialOpts := make([]grpc.DialOption, 0, len(opts)+2)
	if len(opts) == 0 {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		dialOpts = append(dialOpts, opts...)
	}
	// Metadata-root checkpoints are compact state, but long-running fsmeta
	// tests can legitimately grow them past gRPC's 4 MiB default. The root RPC
	// boundary owns that transport budget so coordinator bootstrap does not
	// depend on an implicit library limit.
	dialOpts = append(dialOpts,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(defaultMaxRootRPCMessageBytes),
			grpc.MaxCallSendMsgSize(defaultMaxRootRPCMessageBytes),
		),
	)
	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, err
	}
	if err := waitForReady(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func validGrantAct(kind rootproto.GrantAct) bool {
	switch kind {
	case rootproto.GrantActIssue,
		rootproto.GrantActSeal,
		rootproto.GrantActRetireExpired,
		rootproto.GrantActInherit:
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
	if transientConnectionClosing(err) {
		return true
	}
	switch nokverrors.KindOf(err) {
	case nokverrors.KindUnavailable,
		nokverrors.KindRouteUnavailable,
		nokverrors.KindRetryable:
		return true
	case nokverrors.KindNotLeader:
		return write
	default:
		return false
	}
}

func transientConnectionClosing(err error) bool {
	// gRPC does not expose a non-deprecated replacement that distinguishes a
	// local ClientConn shutdown from ordinary application-level Canceled. The
	// retry path needs that distinction, so keep the transport sentinel here and
	// keep protocol errors on nokverrors.Kind/RPC metadata elsewhere.
	//nolint:staticcheck
	return errors.Is(err, grpc.ErrClientConnClosing)
}

func leaderHint(err error) (uint64, bool) {
	kind, metadata, ok := nokverrors.RPCErrorInfo(err)
	if !ok || kind != nokverrors.KindNotLeader || metadata[metaRootReasonMetadata] != reasonNotLeader {
		return 0, false
	}
	raw := metadata[leaderIDMetadata]
	if raw == "" {
		return 0, false
	}
	id, parseErr := strconv.ParseUint(raw, 10, 64)
	if parseErr != nil || id == 0 {
		return 0, false
	}
	return id, true
}
