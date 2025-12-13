package benchmark

import (
	"bytes"
	"path/filepath"
	"testing"
	"text/tabwriter"

	"github.com/stretchr/testify/require"
)

func TestEngineDir(t *testing.T) {
	opts := ycsbEngineOptions{BaseDir: "/tmp/ycsb"}
	require.Equal(t, filepath.Join("/tmp/ycsb", "nokv_ycsb"), opts.engineDir("nokv"))
}

func TestLatencyRecorder(t *testing.T) {
	rec := newLatencyRecorder(4)
	rec.Record(10)
	rec.Record(20)
	rec.Record(30)

	require.InDelta(t, 0.0, rec.Percentile(0), 0.001)
	require.InDelta(t, 10.0, rec.Percentile(1), 0.001)
	require.InDelta(t, 20.0, rec.Percentile(50), 0.001)
	require.InDelta(t, 30.0, rec.Percentile(100), 0.001)

	samples := rec.Samples()
	require.ElementsMatch(t, []int64{10, 20, 30}, samples)
}

func TestIntRecorder(t *testing.T) {
	rec := newIntRecorder(3)
	rec.Record(5)
	rec.Record(15)
	rec.Record(25)

	require.InDelta(t, 5.0, rec.Percentile(10), 0.001)
	require.InDelta(t, 15.0, rec.Percentile(50), 0.001)
	require.InDelta(t, 25.0, rec.Percentile(100), 0.001)
	require.InDelta(t, (5+15+25)/3.0, rec.Average(), 0.001)

	var nilRecorder *intRecorder
	require.Zero(t, nilRecorder.Average())
	require.Zero(t, nilRecorder.Percentile(50))
}

func TestWriteSummaryTableOutputsRows(t *testing.T) {
	results := []BenchmarkResult{{
		Engine:          "nokv",
		Operation:       "YCSB-A",
		Mode:            "standalone",
		TotalOperations: 0, // verify zero guard paths
		TotalDuration:   0, // nanoseconds
	}}
	results[0].Finalize()

	buf := &bytes.Buffer{}
	tw := tabwriter.NewWriter(buf, 0, 4, 2, ' ', 0)
	writeSummaryTable(tw, results)

	require.Contains(t, buf.String(), "nokv")
	require.Contains(t, buf.String(), "YCSB-A")
}

func TestLatencyRecorderEdgeCases(t *testing.T) {
	rec := newLatencyRecorder(2)
	require.Equal(t, 0.0, rec.Percentile(0))
	require.Equal(t, 0.0, rec.Percentile(200)) // >100 clamps

	rec.Record(5)
	require.Equal(t, float64(5), rec.Percentile(100))

	rec.Record(10)
	require.InDelta(t, 5.0, rec.Percentile(1), 0.001)
	require.InDelta(t, 10.0, rec.Percentile(100), 0.001)
}
