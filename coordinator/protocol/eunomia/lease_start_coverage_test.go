package eunomia_test

import (
	"testing"

	eunomia "github.com/feichai0017/NoKV/coordinator/protocol/eunomia"
	"github.com/stretchr/testify/require"
)

func TestValidateLeaseStartCoverage(t *testing.T) {
	summary := eunomia.NewReadSummary(
		eunomia.ServedRead{Key: "k", Timestamp: 105},
		eunomia.ServedRead{Key: "m", Timestamp: 101},
	)

	require.NoError(t, eunomia.ValidateLeaseStartCoverage(
		eunomia.LeaseView{HolderID: "c2", LeaseStart: 106},
		summary,
	))

	err := eunomia.ValidateLeaseStartCoverage(
		eunomia.LeaseView{HolderID: "c2", LeaseStart: 103},
		summary,
	)
	require.ErrorIs(t, err, eunomia.ErrLeaseStartCoverage)
	require.Contains(t, err.Error(), `key="k"`)
}

func TestEvaluateLeaseStartCoverage(t *testing.T) {
	status := eunomia.EvaluateLeaseStartCoverage(
		eunomia.LeaseView{HolderID: "c2", LeaseStart: 103},
		eunomia.NewReadSummary(
			eunomia.ServedRead{Key: "k", Timestamp: 105},
			eunomia.ServedRead{Key: "m", Timestamp: 101},
		),
	)

	require.False(t, status.Covered())
	require.Len(t, status.Violations(), 1)
	require.Equal(t, "k", status.Violations()[0].Key)
	require.Equal(t, uint64(105), status.Violations()[0].ServedTimestamp)
}

func TestReadSummaryHelpers(t *testing.T) {
	require.Equal(t, "fresh", eunomia.LeaseAcquisitionFresh.String())
	require.Equal(t, "transfer", eunomia.LeaseAcquisitionTransfer.String())
	require.Equal(t, "unknown", eunomia.LeaseAcquisitionUnknown.String())

	summary := eunomia.NewReadSummary(
		eunomia.ServedRead{Key: "b", Timestamp: 4},
		eunomia.ServedRead{Key: "", Timestamp: 99},
		eunomia.ServedRead{Key: "a", Timestamp: 3},
		eunomia.ServedRead{Key: "a", Timestamp: 1},
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

	status := eunomia.EvaluateLeaseStartCoverage(eunomia.LeaseView{LeaseStart: 9}, eunomia.ReadSummary{})
	require.True(t, status.Covered())
	require.Nil(t, status.Violations())
}
