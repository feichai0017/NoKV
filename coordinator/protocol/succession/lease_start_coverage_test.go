package succession_test

import (
	"testing"

	succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"
	"github.com/stretchr/testify/require"
)

func TestValidateLeaseStartCoverage(t *testing.T) {
	summary := succession.NewReadSummary(
		succession.ServedRead{Key: "k", Timestamp: 105},
		succession.ServedRead{Key: "m", Timestamp: 101},
	)

	require.NoError(t, succession.ValidateLeaseStartCoverage(
		succession.LeaseView{HolderID: "c2", LeaseStart: 106},
		summary,
	))

	err := succession.ValidateLeaseStartCoverage(
		succession.LeaseView{HolderID: "c2", LeaseStart: 103},
		summary,
	)
	require.ErrorIs(t, err, succession.ErrLeaseStartCoverage)
	require.Contains(t, err.Error(), `key="k"`)
}

func TestEvaluateLeaseStartCoverage(t *testing.T) {
	status := succession.EvaluateLeaseStartCoverage(
		succession.LeaseView{HolderID: "c2", LeaseStart: 103},
		succession.NewReadSummary(
			succession.ServedRead{Key: "k", Timestamp: 105},
			succession.ServedRead{Key: "m", Timestamp: 101},
		),
	)

	require.False(t, status.Covered())
	require.Len(t, status.Violations(), 1)
	require.Equal(t, "k", status.Violations()[0].Key)
	require.Equal(t, uint64(105), status.Violations()[0].ServedTimestamp)
}

func TestReadSummaryHelpers(t *testing.T) {
	require.Equal(t, "fresh", succession.LeaseAcquisitionFresh.String())
	require.Equal(t, "transfer", succession.LeaseAcquisitionTransfer.String())
	require.Equal(t, "unknown", succession.LeaseAcquisitionUnknown.String())

	summary := succession.NewReadSummary(
		succession.ServedRead{Key: "b", Timestamp: 4},
		succession.ServedRead{Key: "", Timestamp: 99},
		succession.ServedRead{Key: "a", Timestamp: 3},
		succession.ServedRead{Key: "a", Timestamp: 1},
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

	status := succession.EvaluateLeaseStartCoverage(succession.LeaseView{LeaseStart: 9}, succession.ReadSummary{})
	require.True(t, status.Covered())
	require.Nil(t, status.Violations())
}
