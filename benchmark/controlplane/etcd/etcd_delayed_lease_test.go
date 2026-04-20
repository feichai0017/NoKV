package etcd

import (
	"context"
	"testing"
	"time"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type etcdLeaseRenewTraceRecord struct {
	MemberID         string `json:"member_id,omitempty"`
	Duty             string `json:"duty,omitempty"`
	ResponseRevision uint64 `json:"response_revision"`
	RevokeRevision   uint64 `json:"revoke_revision"`
	Accepted         bool   `json:"accepted"`
}

func TestControlPlaneEtcdLeaseKeepAliveBufferedSuccessAfterRevoke(t *testing.T) {
	h := openEtcdClusterHarness(t, []string{"n1"})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	member := h.members["n1"]
	require.NotNil(t, member)

	grantResp, err := member.client.Grant(ctx, 3)
	require.NoError(t, err)
	require.NotNil(t, grantResp.ResponseHeader)
	leaseID := clientv3.LeaseID(grantResp.ID)

	const key = "/nokv/pilot/lease-renew-buffered-success"
	putResp, err := member.client.Put(ctx, key, "v1", clientv3.WithLease(leaseID))
	require.NoError(t, err)
	require.NotNil(t, putResp.Header)

	keepAliveCtx, keepAliveCancel := context.WithCancel(context.Background())
	defer keepAliveCancel()

	ch, err := member.client.KeepAlive(keepAliveCtx, leaseID)
	require.NoError(t, err)

	var bufferedCount int
	require.Eventually(t, func() bool {
		bufferedCount = len(ch)
		return bufferedCount > 0
	}, 5*time.Second, 10*time.Millisecond, "timed out waiting for buffered keepalive response")

	revokeResp, err := member.client.Revoke(ctx, leaseID)
	require.NoError(t, err)
	require.NotNil(t, revokeResp.Header)

	var bufferedResp *clientv3.LeaseKeepAliveResponse
	select {
	case bufferedResp = <-ch:
	case <-ctx.Done():
		t.Fatal("timed out waiting for buffered keepalive response after revoke")
	}
	require.NotNil(t, bufferedResp)
	require.NotNil(t, bufferedResp.ResponseHeader)
	require.Greater(t, bufferedResp.TTL, int64(0))
	require.Less(t, uint64(bufferedResp.ResponseHeader.Revision), uint64(revokeResp.Header.Revision))

	t.Logf(
		"etcd_lease_buffered scenario=keepalive_buffered_success_after_revoke member=%s lease_id=%x buffered_count=%d success_revision=%d revoke_revision=%d ttl=%d",
		member.name,
		uint64(leaseID),
		bufferedCount,
		bufferedResp.ResponseHeader.Revision,
		revokeResp.Header.Revision,
		bufferedResp.TTL,
	)

	records := mustDecodeEtcdLeaseRenewTrace(t, []etcdLeaseRenewTraceRecord{
		{
			MemberID:         member.name,
			Duty:             "lease_renew",
			ResponseRevision: uint64(bufferedResp.ResponseHeader.Revision),
			RevokeRevision:   uint64(revokeResp.Header.Revision),
			Accepted:         true,
		},
	})
	anomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, records)
	require.Equal(t, []string{"accepted_keepalive_success_after_revoke"}, pilotAnomalyKinds(anomalies))
	t.Logf(
		"etcd_lease_buffered scenario=keepalive_buffered_success_after_revoke member=%s lease_id=%x records=%d anomalies=%d kinds=%s success_revision=%d revoke_revision=%d",
		member.name,
		uint64(leaseID),
		len(records),
		len(anomalies),
		formatPilotAnomalyKinds(anomalies),
		bufferedResp.ResponseHeader.Revision,
		revokeResp.Header.Revision,
	)
}

func keepAliveResponseAllowedAfterRevoke(resp *clientv3.LeaseKeepAliveResponse, revokeRevision int64) bool {
	if resp == nil || resp.ResponseHeader == nil {
		return false
	}
	if revokeRevision <= 0 {
		return true
	}
	return resp.ResponseHeader.Revision >= revokeRevision
}

func TestControlPlaneEtcdLeaseKeepAliveBufferedSuccessAfterRevokeWithWitnessGate(t *testing.T) {
	h := openEtcdClusterHarness(t, []string{"n1"})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	member := h.members["n1"]
	require.NotNil(t, member)

	grantResp, err := member.client.Grant(ctx, 3)
	require.NoError(t, err)
	require.NotNil(t, grantResp.ResponseHeader)
	leaseID := clientv3.LeaseID(grantResp.ID)

	const key = "/nokv/pilot/lease-renew-buffered-success-witness-gate"
	putResp, err := member.client.Put(ctx, key, "v1", clientv3.WithLease(leaseID))
	require.NoError(t, err)
	require.NotNil(t, putResp.Header)

	keepAliveCtx, keepAliveCancel := context.WithCancel(context.Background())
	defer keepAliveCancel()

	ch, err := member.client.KeepAlive(keepAliveCtx, leaseID)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(ch) > 0
	}, 5*time.Second, 10*time.Millisecond, "timed out waiting for buffered keepalive response")

	revokeResp, err := member.client.Revoke(ctx, leaseID)
	require.NoError(t, err)
	require.NotNil(t, revokeResp.Header)

	var bufferedResp *clientv3.LeaseKeepAliveResponse
	select {
	case bufferedResp = <-ch:
	case <-ctx.Done():
		t.Fatal("timed out waiting for buffered keepalive response after revoke")
	}
	require.NotNil(t, bufferedResp)
	require.NotNil(t, bufferedResp.ResponseHeader)
	require.Greater(t, bufferedResp.TTL, int64(0))
	require.Less(t, uint64(bufferedResp.ResponseHeader.Revision), uint64(revokeResp.Header.Revision))

	accepted := keepAliveResponseAllowedAfterRevoke(bufferedResp, revokeResp.Header.Revision)
	require.False(t, accepted)

	records := mustDecodeEtcdLeaseRenewTrace(t, []etcdLeaseRenewTraceRecord{
		{
			MemberID:         member.name,
			Duty:             "lease_renew",
			ResponseRevision: uint64(bufferedResp.ResponseHeader.Revision),
			RevokeRevision:   uint64(revokeResp.Header.Revision),
			Accepted:         accepted,
		},
	})
	anomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, records)
	require.Empty(t, anomalies)
	t.Logf(
		"etcd_lease_buffered_with_gate scenario=keepalive_buffered_success_after_revoke member=%s lease_id=%x filtered=%t success_revision=%d revoke_revision=%d anomalies=%d",
		member.name,
		uint64(leaseID),
		!accepted,
		bufferedResp.ResponseHeader.Revision,
		revokeResp.Header.Revision,
		len(anomalies),
	)
}
