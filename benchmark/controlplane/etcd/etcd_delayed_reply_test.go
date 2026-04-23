package etcd

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	"github.com/stretchr/testify/require"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type delayedRangeProxy struct {
	etcdserverpb.UnimplementedKVServer

	listener    net.Listener
	server      *grpc.Server
	backendConn *grpc.ClientConn
	backend     etcdserverpb.KVClient
	captured    chan *etcdserverpb.RangeResponse
	release     chan struct{}
}

func openDelayedRangeProxy(t *testing.T, target string) *delayedRangeProxy {
	t.Helper()

	backendTarget, err := grpcTarget(target)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	backendConn, err := grpc.DialContext(
		ctx,
		backendTarget,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)

	listener, err := net.Listen("tcp", mustFreeAddr(t))
	require.NoError(t, err)

	proxy := &delayedRangeProxy{
		listener:    listener,
		server:      grpc.NewServer(),
		backendConn: backendConn,
		backend:     etcdserverpb.NewKVClient(backendConn),
		captured:    make(chan *etcdserverpb.RangeResponse, 1),
		release:     make(chan struct{}),
	}
	etcdserverpb.RegisterKVServer(proxy.server, proxy)

	go func() { _ = proxy.server.Serve(listener) }()
	waitForGRPCReady(t, listener.Addr().String())
	t.Cleanup(func() { proxy.close() })
	return proxy
}

func grpcTarget(endpoint string) (string, error) {
	if parsed, err := url.Parse(endpoint); err == nil && parsed.Host != "" {
		return parsed.Host, nil
	}
	if endpoint == "" {
		return "", fmt.Errorf("empty grpc target")
	}
	return endpoint, nil
}

func (p *delayedRangeProxy) endpoint() string {
	return p.listener.Addr().String()
}

func (p *delayedRangeProxy) close() {
	if p.server != nil {
		p.server.GracefulStop()
	}
	if p.listener != nil {
		_ = p.listener.Close()
	}
	if p.backendConn != nil {
		_ = p.backendConn.Close()
	}
}

func (p *delayedRangeProxy) waitCaptured(ctx context.Context) (*etcdserverpb.RangeResponse, error) {
	select {
	case resp := <-p.captured:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *delayedRangeProxy) releaseResponse() {
	select {
	case <-p.release:
	default:
		close(p.release)
	}
}

func (p *delayedRangeProxy) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	resp, err := p.backend.Range(ctx, req)
	if err != nil {
		return nil, err
	}
	select {
	case p.captured <- resp:
	default:
		return nil, fmt.Errorf("delayed range proxy only supports one in-flight capture")
	}
	select {
	case <-p.release:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type delayedRangeResult struct {
	resp *etcdserverpb.RangeResponse
	err  error
}

func TestControlPlaneEtcdReadIndexRealDelayedInFlightReply(t *testing.T) {
	h := openEtcdClusterHarness(t, []string{"n1", "n2", "n3"})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	oldLeader, err := h.currentLeaderName(ctx)
	require.NoError(t, err)

	const key = "/nokv/pilot/real-delayed-reply"
	_, err = h.members[oldLeader].client.Put(ctx, key, "v1")
	require.NoError(t, err)

	proxy := openDelayedRangeProxy(t, h.members[oldLeader].clientURL)

	rangeCtx, rangeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rangeCancel()

	proxyConn, err := grpc.DialContext(
		rangeCtx,
		proxy.endpoint(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxyConn.Close()) })

	rangeClient := etcdserverpb.NewKVClient(proxyConn)
	delayed := make(chan delayedRangeResult, 1)
	go func() {
		resp, err := rangeClient.Range(rangeCtx, &etcdserverpb.RangeRequest{Key: []byte(key)})
		delayed <- delayedRangeResult{resp: resp, err: err}
	}()

	captured, err := proxy.waitCaptured(ctx)
	require.NoError(t, err)
	require.NotNil(t, captured)
	require.NotNil(t, captured.Header)

	require.NoError(t, h.pauseMember(oldLeader))
	t.Cleanup(func() { _ = h.resumeMember(oldLeader) })

	successor, err := h.waitForLeaderChange(ctx, oldLeader)
	require.NoError(t, err)
	successorWrite, err := h.members[successor].client.Put(ctx, key, "v2")
	require.NoError(t, err)
	require.NotNil(t, successorWrite.Header)

	proxy.releaseResponse()

	var delayedResp delayedRangeResult
	select {
	case delayedResp = <-delayed:
	case <-ctx.Done():
		t.Fatal("timed out waiting for delayed in-flight range reply")
	}
	require.NoError(t, delayedResp.err)
	require.NotNil(t, delayedResp.resp)
	require.NotNil(t, delayedResp.resp.Header)
	require.Equal(t, captured.Header.Revision, delayedResp.resp.Header.Revision)
	require.Less(t, uint64(delayedResp.resp.Header.Revision), uint64(successorWrite.Header.Revision))

	walSummaries, err := h.loadWALSummaries()
	require.NoError(t, err)
	require.Len(t, walSummaries, 3)
	for _, summary := range walSummaries {
		require.NotZero(t, summary.EntryCount)
		t.Logf(
			"etcd_wal scenario=real_inflight_reply member=%s hard_state_term=%d hard_state_vote=%d hard_state_commit=%d entry_count=%d last_entry_index=%d last_entry_term=%d snapshot_count=%d",
			summary.Member,
			summary.HardStateTerm,
			summary.HardStateVote,
			summary.HardStateCommit,
			summary.EntryCount,
			summary.LastEntryIndex,
			summary.LastEntryTerm,
			summary.SnapshotCount,
		)
	}

	records := mustDecodeEtcdReadIndexTrace(t, []etcdReadIndexTraceRecord{
		{
			MemberID:            oldLeader,
			Duty:                "read_index",
			ReadStateGeneration: uint64(delayedResp.resp.Header.Revision),
			SuccessorEpoch:      uint64(successorWrite.Header.Revision),
			Accepted:            true,
		},
	})
	anomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, records)
	require.Equal(t, []string{"accepted_read_index_behind_successor"}, pilotAnomalyKinds(anomalies))
	t.Logf(
		"etcd_delayed_capture scenario=real_inflight_reply old_leader=%s successor=%s reply_revision=%d successor_revision=%d anomalies=%d kinds=%s",
		oldLeader,
		successor,
		delayedResp.resp.Header.Revision,
		successorWrite.Header.Revision,
		len(anomalies),
		formatPilotAnomalyKinds(anomalies),
	)
}
