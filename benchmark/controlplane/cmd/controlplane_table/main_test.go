package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSamplesParsesBenchmarkOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bench.txt")
	err := os.WriteFile(path, []byte(`
BenchmarkControlPlaneAllocIDLocalWindowDefault-12    	 1000	  1500 ns/op	  64 B/op	  1 allocs/op
BenchmarkControlPlaneAllocIDLocalWindowDefault-12    	 1000	  1700 ns/op	  66 B/op	  1 allocs/op
BenchmarkControlPlaneAllocIDLocalWindowOne-12        	   10	10000000 ns/op	3000 B/op	40 allocs/op
`), 0o644)
	require.NoError(t, err)

	samples, err := parseSamples(path)
	require.NoError(t, err)
	require.Len(t, samples["BenchmarkControlPlaneAllocIDLocalWindowDefault"], 2)
	require.Len(t, samples["BenchmarkControlPlaneAllocIDLocalWindowOne"], 1)

	mean := meanSample(samples["BenchmarkControlPlaneAllocIDLocalWindowDefault"])
	require.Equal(t, 1600.0, mean.nsPerOp)
	require.Equal(t, 65.0, mean.bytesOp)
	require.Equal(t, 1.0, mean.allocsOp)
}

func TestBuildRowsValidatesRequiredBenchmarks(t *testing.T) {
	_, err := buildRows("inprocess", map[string][]sample{
		"BenchmarkControlPlaneAllocIDLocalWindowDefault": {{nsPerOp: 1}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing benchmark")

	rows, err := buildRows("process", map[string][]sample{
		"BenchmarkControlPlaneProcessNoKVRemoteTCPWindowDefault": {{nsPerOp: 1}},
		"BenchmarkControlPlaneProcessNoKVRemoteTCPWindowOne":     {{nsPerOp: 2}},
		"BenchmarkControlPlaneProcessEtcdCASWindowDefault":       {{nsPerOp: 3}},
		"BenchmarkControlPlaneProcessEtcdCASWindowOne":           {{nsPerOp: 4}},
	})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "NoKV Remote (process TCP)", rows[0].label)
	require.Equal(t, "etcd CAS (process)", rows[1].label)
}

func TestFormatLatency(t *testing.T) {
	require.Equal(t, "999.0 ns", formatLatency(999))
	require.Equal(t, "1.500 us", formatLatency(1500))
	require.Equal(t, "2.500 ms", formatLatency(2_500_000))
}

func TestRenderTable(t *testing.T) {
	rows := []row{
		{
			label:   "NoKV Local",
			steady:  "BenchmarkControlPlaneAllocIDLocalWindowDefault",
			degrade: "BenchmarkControlPlaneAllocIDLocalWindowOne",
		},
	}
	samples := map[string][]sample{
		"BenchmarkControlPlaneAllocIDLocalWindowDefault": {{nsPerOp: 1500, bytesOp: 64, allocsOp: 1}},
		"BenchmarkControlPlaneAllocIDLocalWindowOne":     {{nsPerOp: 9_000_000, bytesOp: 3000, allocsOp: 40}},
	}

	var buf bytes.Buffer
	err := renderTable(&buf, rows, samples)
	require.NoError(t, err)

	out := buf.String()
	require.Contains(t, out, "| System | Window=10k | Window=1 | Slowdown |")
	require.Contains(t, out, "| NoKV Local | 1.500 us | 9.000 ms | 6000.0x |")
	require.Contains(t, out, "| Benchmark | Mean ns/op | Mean B/op | Mean allocs/op | Samples |")
	require.Contains(t, out, "| BenchmarkControlPlaneAllocIDLocalWindowDefault | 1500.0 | 64.0 | 1.0 | 1 |")
}
