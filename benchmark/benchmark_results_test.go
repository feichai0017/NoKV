package benchmark

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBenchmarkResultFinalizeAndHelpers(t *testing.T) {
	res := BenchmarkResult{
		TotalDuration:   2 * time.Second,
		TotalOperations: 4,
		DataBytes:       10 << 20, // 10 MiB
		AvgLatencyNS:    0,        // should be populated by Finalize
	}

	res.Finalize()

	require.InDelta(t, 2.0, res.opsPerSecond(), 0.001)
	require.Equal(t, 10.0, res.DataSize)
	require.Equal(t, 500*time.Millisecond, res.avgPerOp())

	zeroOps := BenchmarkResult{}
	require.Zero(t, zeroOps.avgPerOp())
	require.Zero(t, latencyFromNS(0))
	require.Equal(t, 5*time.Nanosecond, latencyFromNS(5))
}

func TestWriteResultsCreatesReport(t *testing.T) {
	tmpDir := t.TempDir()
	cwd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(tmpDir))
	defer func() {
		_ = os.Chdir(cwd)
	}()

	start := time.Date(2024, 1, 2, 15, 4, 5, 0, time.UTC)
	end := start.Add(1500 * time.Millisecond)

	results := []BenchmarkResult{{
		Name:            "sample-run",
		Engine:          "nokv",
		Operation:       "ycsbA",
		Mode:            "standalone",
		StartTime:       start,
		EndTime:         end,
		TotalDuration:   end.Sub(start),
		TotalOperations: 1500,
		DataBytes:       1 << 20,
		P50LatencyNS:    10_000,
		P95LatencyNS:    20_000,
		P99LatencyNS:    30_000,
		ReadOps:         900,
		UpdateOps:       600,
	}}
	results[0].Finalize()

	require.NoError(t, WriteResults(results))

	files, err := os.ReadDir("benchmark_results")
	require.NoError(t, err)
	require.Len(t, files, 1)

	reportPath := filepath.Join("benchmark_results", files[0].Name())
	content, err := os.ReadFile(reportPath)
	require.NoError(t, err)

	body := string(content)
	require.Contains(t, body, "=== Benchmark Results ===")
	require.Contains(t, body, "nokv")
	require.Contains(t, body, "sample-run")
	require.Contains(t, body, "Throughput")
	require.True(t, strings.HasPrefix(files[0].Name(), "benchmark_results_"))
}

func TestEnsureCleanDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "workdir")
	require.NoError(t, os.MkdirAll(dir, 0o755))

	testFile := filepath.Join(dir, "temp.txt")
	require.NoError(t, os.WriteFile(testFile, []byte("stale"), 0o644))

	require.NoError(t, ensureCleanDir(dir))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}
