package plot

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	bench "github.com/feichai0017/NoKV/benchmark"
	"github.com/stretchr/testify/require"
)

func TestReadYCSBResultsCSV(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "results.csv")
	err := os.WriteFile(path, []byte("name,engine,workload,ops,ops_per_sec,avg_ns,p50_ns,p95_ns,p99_ns,duration_ns,data_bytes,data_mb,reads,updates,inserts,scans,scan_items,rmw,val_avg,val_p50,val_p95,val_p99\n"+
		"runA,nokv,A,1000,50000,20000,15000,40000,60000,20000000,1048576,1.00,500,500,0,0,0,0,1024,1024,1024,1024\n"), 0o644)
	require.NoError(t, err)

	results, err := ReadYCSBResultsCSV(path)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "nokv", results[0].Engine)
	require.Equal(t, "A", results[0].Operation)
	require.Equal(t, int64(1000), results[0].TotalOperations)
	require.Equal(t, 50000.0, results[0].Throughput)
	require.Equal(t, 20000.0, results[0].AvgLatencyNS)
}

func TestWriteGroupedBarChartFromResults(t *testing.T) {
	results := []bench.BenchmarkResult{
		{
			Name:            "A-nokv",
			Engine:          "nokv",
			Operation:       "A",
			TotalOperations: 1000,
			TotalDuration:   20 * time.Millisecond,
			Throughput:      50000,
			AvgLatencyNS:    20000,
			P95LatencyNS:    40000,
		},
		{
			Name:            "A-pebble",
			Engine:          "pebble",
			Operation:       "A",
			TotalOperations: 1000,
			TotalDuration:   25 * time.Millisecond,
			Throughput:      40000,
			AvgLatencyNS:    25000,
			P95LatencyNS:    50000,
		},
	}

	output := filepath.Join(t.TempDir(), "throughput.svg")
	err := WriteGroupedBarChartFromResults(results, ResultGroupedBarChartConfig{
		Metric: MetricThroughputOpsPerSec,
		GroupedBarChartConfig: GroupedBarChartConfig{
			Title:  "YCSB Throughput",
			Output: output,
		},
	})
	require.NoError(t, err)

	info, err := os.Stat(output)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(0))
}
