package integration_test

import (
	"context"
	"testing"
	"time"

	coordfailpoints "github.com/feichai0017/NoKV/coordinator/failpoints"
	rootfailpoints "github.com/feichai0017/NoKV/meta/root/failpoints"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pdtestcluster "github.com/feichai0017/NoKV/coordinator/testcluster"
)

func TestControlPlaneFailpointBeforeApplyTenureRejectsDutyAdmission(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureTenures(10*time.Second, 3*time.Second)
	_, leader := cluster.LeaderService()

	rootfailpoints.Set(rootfailpoints.BeforeApplyTenure)
	t.Cleanup(func() { rootfailpoints.Set(rootfailpoints.None) })

	_, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), rootfailpoints.ErrBeforeApplyTenure.Error())
}

func TestControlPlaneFailpointBeforeTenureStorageReadRejectsSeal(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureTenures(10*time.Second, 3*time.Second)
	_, leader := cluster.LeaderService()

	_, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	rootfailpoints.Set(rootfailpoints.BeforeTenureStorageRead)
	t.Cleanup(func() { rootfailpoints.Set(rootfailpoints.None) })

	err = leader.SealTenure()
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), rootfailpoints.ErrBeforeTenureStorageRead.Error())
}

func TestControlPlaneFailpointBeforeApplyHandoverRejectsSeal(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureTenures(10*time.Second, 3*time.Second)
	_, leader := cluster.LeaderService()

	_, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	rootfailpoints.Set(rootfailpoints.BeforeApplyHandover)
	t.Cleanup(func() { rootfailpoints.Set(rootfailpoints.None) })

	err = leader.SealTenure()
	require.ErrorIs(t, err, rootfailpoints.ErrBeforeApplyHandover)
}

func TestControlPlaneFailpointAfterApplyHandoverBeforeReloadPreservesSealAcrossRestart(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureTenures(10*time.Second, 3*time.Second)
	leaderID, leader := cluster.LeaderService()

	_, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 4})
	require.NoError(t, err)

	coordfailpoints.Set(coordfailpoints.AfterApplyHandoverBeforeReload)
	t.Cleanup(func() { coordfailpoints.Set(coordfailpoints.None) })

	err = leader.SealTenure()
	require.ErrorIs(t, err, coordfailpoints.ErrAfterApplyHandoverBeforeReload)

	rootState, err := cluster.Roots[leaderID].Current()
	require.NoError(t, err)
	require.Equal(t, rootState.Tenure.Era, rootState.Legacy.Era)
	require.NotZero(t, rootState.Legacy.Era)

	coordfailpoints.Set(coordfailpoints.None)
	restarted := cluster.RestartService(leaderID)
	restarted.ConfigureTenure("c-next", 10*time.Second, 3*time.Second)
	require.NoError(t, restarted.ReloadFromStorage())

	allocResp, err := restarted.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Greater(t, allocResp.GetEra(), rootState.Legacy.Era)
	require.NoError(t, restarted.ConfirmHandover())
	require.NoError(t, restarted.CloseHandover())
	require.NoError(t, restarted.ReattachHandover())

	audit := restarted.DiagnosticsSnapshot()["audit"].(map[string]any)
	require.Equal(t, "reattached", audit["handover_stage"])
	require.Equal(t, true, audit["finality_satisfied"])
}

func TestControlPlaneFailpointAfterApplyHandoverBeforeReloadPreservesConfirmedClosureAcrossRestart(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureTenures(10*time.Second, 3*time.Second)
	leaderID, leader := cluster.LeaderService()

	_, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.NoError(t, leader.SealTenure())

	leader.ConfigureTenure("c-successor", 10*time.Second, 3*time.Second)
	require.NoError(t, leader.ReloadFromStorage())
	_, err = leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	coordfailpoints.Set(coordfailpoints.AfterApplyHandoverBeforeReload)
	t.Cleanup(func() { coordfailpoints.Set(coordfailpoints.None) })

	err = leader.ConfirmHandover()
	require.ErrorIs(t, err, coordfailpoints.ErrAfterApplyHandoverBeforeReload)

	rootState, err := cluster.Roots[leaderID].Current()
	require.NoError(t, err)
	require.Equal(t, rootproto.HandoverStageConfirmed, rootState.Handover.Stage)
	require.Equal(t, rootState.Legacy.Era, rootState.Handover.LegacyEra)
	require.Equal(t, rootState.Tenure.Era, rootState.Handover.SuccessorEra)

	coordfailpoints.Set(coordfailpoints.None)
	restarted := cluster.RestartService(leaderID)
	restarted.ConfigureTenure("c-successor", 10*time.Second, 3*time.Second)
	require.NoError(t, restarted.ReloadFromStorage())

	require.NoError(t, restarted.CloseHandover())
	require.NoError(t, restarted.ReattachHandover())

	audit := restarted.DiagnosticsSnapshot()["audit"].(map[string]any)
	require.Equal(t, "reattached", audit["handover_stage"])
	require.Equal(t, true, audit["finality_satisfied"])
}
