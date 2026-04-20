package controlplane

import (
	"context"
	"slices"
	"strings"
	"testing"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestControlPlaneNoKVLateReplyControl(t *testing.T) {
	h := openDetachedAblationHarness(t, detachedAblationVariant{}.predecessorConfig, detachedAblationVariant{}.successorConfig)
	t.Cleanup(func() { h.close(t) })

	ctx := context.Background()
	oldResp, err := h.predecessor.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	require.NoError(t, h.predecessor.SealCoordinatorLease())
	require.NoError(t, h.successor.RefreshFromStorage())

	freshResp1, err := h.successor.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.NoError(t, h.successor.ConfirmCoordinatorClosure())
	require.NoError(t, h.successor.CloseCoordinatorClosure())
	require.NoError(t, h.successor.ReattachCoordinatorClosure())
	freshResp2, err := h.successor.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	freshPrimary := &allocSequenceServer{
		steps: []allocStep{
			{resp: freshResp1},
			{err: status.Error(codes.Unavailable, "fresh primary temporarily unavailable")},
		},
	}
	lateReply := &allocSequenceServer{
		steps: []allocStep{
			{resp: oldResp},
		},
	}
	freshSecondary := &allocSequenceServer{
		steps: []allocStep{
			{resp: freshResp2},
		},
	}

	client := openDetachedAblationClient(t, []string{"fresh-primary", "late-reply", "fresh-secondary"}, map[string]coordpb.CoordinatorServer{
		"fresh-primary":   freshPrimary,
		"late-reply":      lateReply,
		"fresh-secondary": freshSecondary,
	})

	resp, err := client.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, freshResp1.GetFirstId(), resp.GetFirstId())

	resp, err = client.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, freshResp2.GetFirstId(), resp.GetFirstId())
	require.Equal(t, uint64(2), resp.GetCertGeneration())
	require.Equal(t, 1, lateReply.calls)
	require.Equal(t, 1, freshSecondary.calls)

	records := []coordaudit.ReplyTraceRecord{
		{
			Source:                      "nokv-control",
			Duty:                        "alloc_id",
			CertGeneration:              oldResp.GetCertGeneration(),
			ObservedSuccessorGeneration: freshResp2.GetCertGeneration(),
			Accepted:                    false,
		},
		{
			Source:                      "nokv-control",
			Duty:                        "alloc_id",
			CertGeneration:              freshResp2.GetCertGeneration(),
			ObservedSuccessorGeneration: freshResp2.GetCertGeneration(),
			Accepted:                    true,
		},
	}
	anomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, records)
	require.Empty(t, anomalies)
	t.Logf(
		"control_late_reply scenario=late_reply_after_successor records=%d anomalies=%d kinds=%s rejected_generation=%d accepted_generation=%d",
		len(records),
		len(anomalies),
		formatAnomalyKinds(anomalies),
		oldResp.GetCertGeneration(),
		freshResp2.GetCertGeneration(),
	)
}

func anomalyKinds(anomalies []coordaudit.ReplyTraceAnomaly) []string {
	if len(anomalies) == 0 {
		return nil
	}
	out := make([]string, 0, len(anomalies))
	for _, anomaly := range anomalies {
		out = append(out, anomaly.Kind)
	}
	slices.Sort(out)
	return out
}

func formatAnomalyKinds(anomalies []coordaudit.ReplyTraceAnomaly) string {
	kinds := anomalyKinds(anomalies)
	if len(kinds) == 0 {
		return "none"
	}
	return strings.Join(kinds, ",")
}
