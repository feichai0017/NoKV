package client

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Client is the typed fsmeta client surface consumed by demos and benchmarks.
type Client interface {
	Create(ctx context.Context, req fsmeta.CreateRequest, inode fsmeta.InodeRecord) error
	Lookup(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error)
	ReadDir(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error)
	ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error)
	WatchSubtree(ctx context.Context, req fsmeta.WatchRequest) (WatchSubscription, error)
	SnapshotSubtree(ctx context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error)
	RetireSnapshotSubtree(ctx context.Context, token fsmeta.SnapshotSubtreeToken) error
	RenameSubtree(ctx context.Context, req fsmeta.RenameSubtreeRequest) error
	Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error
	Close() error
}

// GRPCClient wraps the generated FSMetadata client with typed fsmeta records.
type GRPCClient struct {
	conn *grpc.ClientConn
	rpc  fsmetapb.FSMetadataClient
}

// New wraps an existing generated FSMetadata client.
func New(rpc fsmetapb.FSMetadataClient) *GRPCClient {
	return &GRPCClient{rpc: rpc}
}

// NewGRPCClient dials one FSMetadata endpoint and returns a typed client.
func NewGRPCClient(ctx context.Context, addr string, dialOpts ...grpc.DialOption) (*GRPCClient, error) {
	if addr == "" {
		return nil, errors.New("fsmeta/client: address is required")
	}
	opts := normalizeDialOptions(dialOpts)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	if err := waitForReady(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &GRPCClient{
		conn: conn,
		rpc:  fsmetapb.NewFSMetadataClient(conn),
	}, nil
}

func (c *GRPCClient) Create(ctx context.Context, req fsmeta.CreateRequest, inode fsmeta.InodeRecord) error {
	if err := c.requireRPC(); err != nil {
		return err
	}
	_, err := c.rpc.Create(ctx, createRequestToProto(req, inode))
	return translateRPCError(err)
}

func (c *GRPCClient) Lookup(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	if err := c.requireRPC(); err != nil {
		return fsmeta.DentryRecord{}, err
	}
	resp, err := c.rpc.Lookup(ctx, lookupRequestToProto(req))
	if err != nil {
		return fsmeta.DentryRecord{}, translateRPCError(err)
	}
	return dentryFromProto(resp.GetDentry()), nil
}

func (c *GRPCClient) ReadDir(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	if err := c.requireRPC(); err != nil {
		return nil, err
	}
	resp, err := c.rpc.ReadDir(ctx, readDirRequestToProto(req))
	if err != nil {
		return nil, translateRPCError(err)
	}
	out := make([]fsmeta.DentryRecord, 0, len(resp.GetEntries()))
	for _, entry := range resp.GetEntries() {
		out = append(out, dentryFromProto(entry))
	}
	return out, nil
}

func (c *GRPCClient) ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	if err := c.requireRPC(); err != nil {
		return nil, err
	}
	resp, err := c.rpc.ReadDirPlus(ctx, readDirRequestToProto(req))
	if err != nil {
		return nil, translateRPCError(err)
	}
	out := make([]fsmeta.DentryAttrPair, 0, len(resp.GetEntries()))
	for _, entry := range resp.GetEntries() {
		out = append(out, pairFromProto(entry))
	}
	return out, nil
}

// WatchSubtree opens a prefix watch stream. When ResumeCursor is set, the
// server replays retained events after that cursor before switching to live
// delivery.
func (c *GRPCClient) WatchSubtree(ctx context.Context, req fsmeta.WatchRequest) (WatchSubscription, error) {
	if err := c.requireRPC(); err != nil {
		return nil, err
	}
	stream, err := c.rpc.WatchSubtree(ctx)
	if err != nil {
		return nil, translateRPCError(err)
	}
	if err := stream.Send(&fsmetapb.WatchAckOrSubscribe{
		Body: &fsmetapb.WatchAckOrSubscribe_Subscribe{Subscribe: watchRequestToProto(req)},
	}); err != nil {
		return nil, translateRPCError(err)
	}
	ready, err := waitForWatchReady(stream)
	if err != nil {
		_ = stream.CloseSend()
		return nil, err
	}
	return &WatchStream{stream: stream, ready: ready}, nil
}

func (c *GRPCClient) SnapshotSubtree(ctx context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	if err := c.requireRPC(); err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	resp, err := c.rpc.SnapshotSubtree(ctx, snapshotSubtreeRequestToProto(req))
	if err != nil {
		return fsmeta.SnapshotSubtreeToken{}, translateRPCError(err)
	}
	return snapshotSubtreeTokenFromProto(resp), nil
}

func (c *GRPCClient) RetireSnapshotSubtree(ctx context.Context, token fsmeta.SnapshotSubtreeToken) error {
	if err := c.requireRPC(); err != nil {
		return err
	}
	_, err := c.rpc.RetireSnapshotSubtree(ctx, retireSnapshotSubtreeRequestToProto(token))
	return translateRPCError(err)
}

func (c *GRPCClient) RenameSubtree(ctx context.Context, req fsmeta.RenameSubtreeRequest) error {
	if err := c.requireRPC(); err != nil {
		return err
	}
	_, err := c.rpc.RenameSubtree(ctx, renameSubtreeRequestToProto(req))
	return translateRPCError(err)
}

func (c *GRPCClient) Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error {
	if err := c.requireRPC(); err != nil {
		return err
	}
	_, err := c.rpc.Unlink(ctx, unlinkRequestToProto(req))
	return translateRPCError(err)
}

// WatchSubscription is one typed WatchSubtree client stream.
type WatchSubscription interface {
	Recv() (fsmeta.WatchEvent, error)
	ReadyCursor() fsmeta.WatchCursor
	Ack(fsmeta.WatchCursor) error
	Close() error
}

// WatchStream is the gRPC-backed WatchSubtree stream implementation.
type WatchStream struct {
	stream fsmetapb.FSMetadata_WatchSubtreeClient
	ready  fsmeta.WatchCursor
}

// Recv blocks until the next watch event arrives.
func (s *WatchStream) Recv() (fsmeta.WatchEvent, error) {
	if s == nil || s.stream == nil {
		return fsmeta.WatchEvent{}, errors.New("fsmeta/client: watch stream is not configured")
	}
	for {
		resp, err := s.stream.Recv()
		if err != nil {
			return fsmeta.WatchEvent{}, translateRPCError(err)
		}
		if event := resp.GetEvent(); event != nil {
			return watchEventFromProto(event), nil
		}
		if throttle := resp.GetThrottle(); throttle != nil {
			return fsmeta.WatchEvent{}, fmt.Errorf("%w: %s", fsmeta.ErrWatchOverflow, throttle.GetReason())
		}
		// Ready and catch-up markers are stream-control frames, not user events.
	}
}

func waitForWatchReady(stream fsmetapb.FSMetadata_WatchSubtreeClient) (fsmeta.WatchCursor, error) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			return fsmeta.WatchCursor{}, translateRPCError(err)
		}
		if ready := resp.GetReady(); ready != nil {
			return watchCursorFromProto(ready.GetCursor()), nil
		}
		if throttle := resp.GetThrottle(); throttle != nil {
			return fsmeta.WatchCursor{}, fmt.Errorf("%w: %s", fsmeta.ErrWatchOverflow, throttle.GetReason())
		}
		if resp.GetEvent() != nil {
			return fsmeta.WatchCursor{}, errors.New("fsmeta/client: watch stream delivered event before ready")
		}
	}
}

// ReadyCursor returns the server frontier after the subscription's initial
// catch-up replay was queued.
func (s *WatchStream) ReadyCursor() fsmeta.WatchCursor {
	if s == nil {
		return fsmeta.WatchCursor{}
	}
	return s.ready
}

// Ack releases back-pressure budget for a received event.
func (s *WatchStream) Ack(cursor fsmeta.WatchCursor) error {
	if s == nil || s.stream == nil {
		return errors.New("fsmeta/client: watch stream is not configured")
	}
	return translateRPCError(s.stream.Send(&fsmetapb.WatchAckOrSubscribe{
		Body: &fsmetapb.WatchAckOrSubscribe_Ack{Ack: &fsmetapb.WatchAck{Cursor: watchCursorToProto(cursor)}},
	}))
}

// AckEvent releases back-pressure budget for a received event.
func (s *WatchStream) AckEvent(evt fsmeta.WatchEvent) error {
	return s.Ack(evt.Cursor)
}

// Close closes the sending side of the watch stream.
func (s *WatchStream) Close() error {
	if s == nil || s.stream == nil {
		return nil
	}
	return s.stream.CloseSend()
}

// Close closes the underlying connection when this client owns one.
func (c *GRPCClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *GRPCClient) requireRPC() error {
	if c == nil || c.rpc == nil {
		return errors.New("fsmeta/client: rpc client is not configured")
	}
	return nil
}

func translateRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.AlreadyExists:
		return fmt.Errorf("%w: %v", fsmeta.ErrExists, err)
	case codes.NotFound:
		return fmt.Errorf("%w: %v", fsmeta.ErrNotFound, err)
	case codes.OutOfRange:
		return fmt.Errorf("%w: %v", fsmeta.ErrWatchCursorExpired, err)
	case codes.ResourceExhausted:
		msg := status.Convert(err).Message()
		if strings.Contains(msg, fsmeta.ErrQuotaExceeded.Error()) {
			return fmt.Errorf("%w: %v", fsmeta.ErrQuotaExceeded, err)
		}
		if strings.Contains(msg, fsmeta.ErrWatchOverflow.Error()) {
			return fmt.Errorf("%w: %v", fsmeta.ErrWatchOverflow, err)
		}
		return err
	default:
		return err
	}
}

// WatchSession wraps a WatchSubscription with event-based ack helpers.
type WatchSession struct {
	sub WatchSubscription
}

// NewWatchSession constructs a helper around one watch subscription.
func NewWatchSession(sub WatchSubscription) *WatchSession {
	return &WatchSession{sub: sub}
}

// Recv receives the next watch event.
func (s *WatchSession) Recv() (fsmeta.WatchEvent, error) {
	if s == nil || s.sub == nil {
		return fsmeta.WatchEvent{}, errors.New("fsmeta/client: watch session is not configured")
	}
	return s.sub.Recv()
}

// Ack acknowledges the cursor carried by event.
func (s *WatchSession) Ack(event fsmeta.WatchEvent) error {
	if s == nil || s.sub == nil {
		return errors.New("fsmeta/client: watch session is not configured")
	}
	return s.sub.Ack(event.Cursor)
}

// ReadyCursor returns the server frontier after initial replay.
func (s *WatchSession) ReadyCursor() fsmeta.WatchCursor {
	if s == nil || s.sub == nil {
		return fsmeta.WatchCursor{}
	}
	return s.sub.ReadyCursor()
}

// Close closes the wrapped subscription.
func (s *WatchSession) Close() error {
	if s == nil || s.sub == nil {
		return nil
	}
	return s.sub.Close()
}

func normalizeDialOptions(opts []grpc.DialOption) []grpc.DialOption {
	out := make([]grpc.DialOption, 0, len(opts)+1)
	if len(opts) == 0 {
		out = append(out, grpc.WithTransportCredentials(insecure.NewCredentials()))
		return out
	}
	out = append(out, opts...)
	return out
}

func waitForReady(ctx context.Context, conn *grpc.ClientConn) error {
	if ctx == nil {
		return nil
	}
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(ctx, state) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("fsmeta/client: connection did not become ready")
		}
	}
}
