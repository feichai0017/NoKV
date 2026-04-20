package controlplane_test

import (
	"testing"

	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	"github.com/stretchr/testify/require"
)

func TestValidateLeaseStartCoverage(t *testing.T) {
	summary := controlplane.NewReadSummary(
		controlplane.ServedRead{Key: "k", Timestamp: 105},
		controlplane.ServedRead{Key: "m", Timestamp: 101},
	)

	require.NoError(t, controlplane.ValidateLeaseStartCoverage(
		controlplane.LeaseView{HolderID: "c2", LeaseStart: 106},
		summary,
	))

	err := controlplane.ValidateLeaseStartCoverage(
		controlplane.LeaseView{HolderID: "c2", LeaseStart: 103},
		summary,
	)
	require.ErrorIs(t, err, controlplane.ErrLeaseStartCoverage)
	require.Contains(t, err.Error(), `key="k"`)
}

func TestEvaluateLeaseStartCoverage(t *testing.T) {
	status := controlplane.EvaluateLeaseStartCoverage(
		controlplane.LeaseView{HolderID: "c2", LeaseStart: 103},
		controlplane.NewReadSummary(
			controlplane.ServedRead{Key: "k", Timestamp: 105},
			controlplane.ServedRead{Key: "m", Timestamp: 101},
		),
	)

	require.False(t, status.Covered())
	require.Len(t, status.Violations(), 1)
	require.Equal(t, "k", status.Violations()[0].Key)
	require.Equal(t, uint64(105), status.Violations()[0].ServedTimestamp)
}
