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

func TestReadSummaryHelpers(t *testing.T) {
	require.Equal(t, "fresh", controlplane.LeaseAcquisitionFresh.String())
	require.Equal(t, "transfer", controlplane.LeaseAcquisitionTransfer.String())
	require.Equal(t, "unknown", controlplane.LeaseAcquisitionUnknown.String())

	summary := controlplane.NewReadSummary(
		controlplane.ServedRead{Key: "b", Timestamp: 4},
		controlplane.ServedRead{Key: "", Timestamp: 99},
		controlplane.ServedRead{Key: "a", Timestamp: 3},
		controlplane.ServedRead{Key: "a", Timestamp: 1},
	)
	require.Len(t, summary.Reads, 3)
	require.Equal(t, "a", summary.Reads[0].Key)
	require.Equal(t, uint64(1), summary.Reads[0].Timestamp)
	require.Equal(t, uint64(4), summary.MaxTimestamp())

	summary = summary.WithRead("c", 8)
	maxTs, ok := summary.MaxTimestampForKey("c")
	require.True(t, ok)
	require.Equal(t, uint64(8), maxTs)
	_, ok = summary.MaxTimestampForKey("missing")
	require.False(t, ok)

	status := controlplane.EvaluateLeaseStartCoverage(controlplane.LeaseView{LeaseStart: 9}, controlplane.ReadSummary{})
	require.True(t, status.Covered())
	require.Nil(t, status.Violations())
}
