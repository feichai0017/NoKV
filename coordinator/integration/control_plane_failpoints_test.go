package integration_test

import (
	"context"
	"testing"
	"time"

	coordfailpoints "github.com/feichai0017/NoKV/coordinator/failpoints"
	rootfailpoints "github.com/feichai0017/NoKV/meta/root/failpoints"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pdtestcluster "github.com/feichai0017/NoKV/coordinator/testcluster"
)

func TestControlPlaneFailpointBeforeApplyGrantIssueRejectsDutyAdmission(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureAuthorityGrants(10*time.Second, 3*time.Second)
	_, leader := cluster.LeaderService()

	rootfailpoints.Set(rootfailpoints.BeforeApplyGrantIssue)
	t.Cleanup(func() { rootfailpoints.Set(rootfailpoints.None) })

	_, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), rootfailpoints.ErrBeforeApplyGrantIssue.Error())
}

func TestControlPlaneFailpointBeforeGrantStorageReadRejectsSeal(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureAuthorityGrants(10*time.Second, 3*time.Second)
	_, leader := cluster.LeaderService()

	_, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	rootfailpoints.Set(rootfailpoints.BeforeGrantStorageRead)
	t.Cleanup(func() { rootfailpoints.Set(rootfailpoints.None) })

	err = leader.SealGrant()
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), rootfailpoints.ErrBeforeGrantStorageRead.Error())
}

func TestControlPlaneFailpointBeforeApplyGrantRetirementRejectsSeal(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureAuthorityGrants(10*time.Second, 3*time.Second)
	_, leader := cluster.LeaderService()

	_, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	rootfailpoints.Set(rootfailpoints.BeforeApplyGrantRetirement)
	t.Cleanup(func() { rootfailpoints.Set(rootfailpoints.None) })

	err = leader.SealGrant()
	require.ErrorIs(t, err, rootfailpoints.ErrBeforeApplyGrantRetirement)
}

func TestControlPlaneFailpointAfterSealGrantBeforeReloadPreservesSealAcrossRestart(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureAuthorityGrants(10*time.Second, 3*time.Second)
	leaderID, leader := cluster.LeaderService()

	firstResp, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 4})
	require.NoError(t, err)

	coordfailpoints.Set(coordfailpoints.AfterSealGrantBeforeReload)
	t.Cleanup(func() { coordfailpoints.Set(coordfailpoints.None) })

	err = leader.SealGrant()
	require.ErrorIs(t, err, coordfailpoints.ErrAfterSealGrantBeforeReload)

	rootState, err := cluster.Roots[leaderID].Current()
	require.NoError(t, err)
	require.Len(t, rootState.RetiredGrants, 1)
	retiredEra := rootState.RetiredGrants[0].Era
	require.Equal(t, firstResp.GetEra(), retiredEra)
	require.NotZero(t, retiredEra)

	coordfailpoints.Set(coordfailpoints.None)
	restarted := cluster.RestartService(leaderID)
	restarted.ConfigureAuthorityGrant("c-next", 10*time.Second, 3*time.Second)
	require.NoError(t, restarted.ReloadFromStorage())

	allocResp, err := restarted.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Greater(t, allocResp.GetEra(), retiredEra)
	require.NoError(t, restarted.InheritRetiredGrants(context.Background()))

	audit := restarted.DiagnosticsSnapshot()["audit"].(map[string]any)
	require.Equal(t, true, audit["sealed_exact_completed"])
	require.Equal(t, false, audit["retired_not_inherited"])
}
