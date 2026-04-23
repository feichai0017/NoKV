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

func TestControlPlaneFailpointBeforeApplyCoordinatorLeaseRejectsDutyAdmission(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureCoordinatorLeases(10*time.Second, 3*time.Second)
	_, leader := cluster.LeaderService()

	rootfailpoints.Set(rootfailpoints.BeforeApplyCoordinatorLease)
	t.Cleanup(func() { rootfailpoints.Set(rootfailpoints.None) })

	_, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), rootfailpoints.ErrBeforeApplyCoordinatorLease.Error())
}

func TestControlPlaneFailpointBeforeCoordinatorLeaseStorageReadRejectsSeal(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureCoordinatorLeases(10*time.Second, 3*time.Second)
	_, leader := cluster.LeaderService()

	_, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	rootfailpoints.Set(rootfailpoints.BeforeCoordinatorLeaseStorageRead)
	t.Cleanup(func() { rootfailpoints.Set(rootfailpoints.None) })

	err = leader.SealCoordinatorLease()
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), rootfailpoints.ErrBeforeCoordinatorLeaseStorageRead.Error())
}

func TestControlPlaneFailpointBeforeApplyCoordinatorClosureRejectsSeal(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureCoordinatorLeases(10*time.Second, 3*time.Second)
	_, leader := cluster.LeaderService()

	_, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	rootfailpoints.Set(rootfailpoints.BeforeApplyCoordinatorClosure)
	t.Cleanup(func() { rootfailpoints.Set(rootfailpoints.None) })

	err = leader.SealCoordinatorLease()
	require.ErrorIs(t, err, rootfailpoints.ErrBeforeApplyCoordinatorClosure)
}

func TestControlPlaneFailpointAfterApplyCoordinatorClosureBeforeReloadPreservesSealAcrossRestart(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureCoordinatorLeases(10*time.Second, 3*time.Second)
	leaderID, leader := cluster.LeaderService()

	_, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 4})
	require.NoError(t, err)

	coordfailpoints.Set(coordfailpoints.AfterApplyCoordinatorClosureBeforeReload)
	t.Cleanup(func() { coordfailpoints.Set(coordfailpoints.None) })

	err = leader.SealCoordinatorLease()
	require.ErrorIs(t, err, coordfailpoints.ErrAfterApplyCoordinatorClosureBeforeReload)

	rootState, err := cluster.Roots[leaderID].Current()
	require.NoError(t, err)
	require.Equal(t, rootState.CoordinatorLease.CertGeneration, rootState.CoordinatorSeal.CertGeneration)
	require.NotZero(t, rootState.CoordinatorSeal.CertGeneration)

	coordfailpoints.Set(coordfailpoints.None)
	restarted := cluster.RestartService(leaderID)
	restarted.ConfigureCoordinatorLease("c-next", 10*time.Second, 3*time.Second)
	require.NoError(t, restarted.ReloadFromStorage())

	allocResp, err := restarted.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Greater(t, allocResp.GetCertGeneration(), rootState.CoordinatorSeal.CertGeneration)
	require.NoError(t, restarted.ConfirmCoordinatorClosure())
	require.NoError(t, restarted.CloseCoordinatorClosure())
	require.NoError(t, restarted.ReattachCoordinatorClosure())

	audit := restarted.DiagnosticsSnapshot()["audit"].(map[string]any)
	require.Equal(t, "reattached", audit["closure_stage"])
	require.Equal(t, true, audit["closure_satisfied"])
}

func TestControlPlaneFailpointAfterApplyCoordinatorClosureBeforeReloadPreservesConfirmedClosureAcrossRestart(t *testing.T) {
	cluster := pdtestcluster.OpenReplicated(t)
	cluster.ConfigureCoordinatorLeases(10*time.Second, 3*time.Second)
	leaderID, leader := cluster.LeaderService()

	_, err := leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.NoError(t, leader.SealCoordinatorLease())

	leader.ConfigureCoordinatorLease("c-successor", 10*time.Second, 3*time.Second)
	require.NoError(t, leader.ReloadFromStorage())
	_, err = leader.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	coordfailpoints.Set(coordfailpoints.AfterApplyCoordinatorClosureBeforeReload)
	t.Cleanup(func() { coordfailpoints.Set(coordfailpoints.None) })

	err = leader.ConfirmCoordinatorClosure()
	require.ErrorIs(t, err, coordfailpoints.ErrAfterApplyCoordinatorClosureBeforeReload)

	rootState, err := cluster.Roots[leaderID].Current()
	require.NoError(t, err)
	require.Equal(t, rootproto.CoordinatorClosureStageConfirmed, rootState.CoordinatorClosure.Stage)
	require.Equal(t, rootState.CoordinatorSeal.CertGeneration, rootState.CoordinatorClosure.SealGeneration)
	require.Equal(t, rootState.CoordinatorLease.CertGeneration, rootState.CoordinatorClosure.SuccessorGeneration)

	coordfailpoints.Set(coordfailpoints.None)
	restarted := cluster.RestartService(leaderID)
	restarted.ConfigureCoordinatorLease("c-successor", 10*time.Second, 3*time.Second)
	require.NoError(t, restarted.ReloadFromStorage())

	require.NoError(t, restarted.CloseCoordinatorClosure())
	require.NoError(t, restarted.ReattachCoordinatorClosure())

	audit := restarted.DiagnosticsSnapshot()["audit"].(map[string]any)
	require.Equal(t, "reattached", audit["closure_stage"])
	require.Equal(t, true, audit["closure_satisfied"])
}
