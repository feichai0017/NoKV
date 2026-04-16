package plot

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	bench "github.com/feichai0017/NoKV/benchmark/ycsb"
)

// ResultMetric names a commonly plotted benchmark metric.
type ResultMetric string

const (
	MetricThroughputOpsPerSec ResultMetric = "throughput_ops_per_sec"
	MetricAvgLatencyUS        ResultMetric = "avg_latency_us"
	MetricP50LatencyUS        ResultMetric = "p50_latency_us"
	MetricP95LatencyUS        ResultMetric = "p95_latency_us"
	MetricP99LatencyUS        ResultMetric = "p99_latency_us"
	MetricDataSizeMB          ResultMetric = "data_size_mb"
)

// ResultGroupedBarChartConfig derives a grouped chart from BenchmarkResult rows.
type ResultGroupedBarChartConfig struct {
	GroupedBarChartConfig
	Metric       ResultMetric
	CategoryFunc func(bench.BenchmarkResult) string
	SeriesFunc   func(bench.BenchmarkResult) string
}

// WriteGroupedBarChartFromResults renders a grouped bar chart directly from
// benchmark results.
func WriteGroupedBarChartFromResults(results []bench.BenchmarkResult, cfg ResultGroupedBarChartConfig) error {
	spec, err := metricSpec(cfg.Metric)
	if err != nil {
		return err
	}
	categoryFn := cfg.CategoryFunc
	if categoryFn == nil {
		categoryFn = func(r bench.BenchmarkResult) string {
			if r.Mode != "" {
				return r.Operation + " / " + r.Mode
			}
			return r.Operation
		}
	}
	seriesFn := cfg.SeriesFunc
	if seriesFn == nil {
		seriesFn = func(r bench.BenchmarkResult) string { return r.Engine }
	}

	obs := make([]Observation, 0, len(results))
	for _, r := range results {
		obs = append(obs, Observation{
			Category: categoryFn(r),
			Series:   seriesFn(r),
			Value:    spec.Value(r),
		})
	}
	if cfg.GroupedBarChartConfig.YLabel == "" {
		cfg.GroupedBarChartConfig.YLabel = spec.Label
	}
	return WriteGroupedBarChart(obs, cfg.GroupedBarChartConfig)
}

// ReadYCSBResultsCSV parses the benchmark CSV emitted by writeYCSBSummary.
func ReadYCSBResultsCSV(path string) ([]bench.BenchmarkResult, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("open results csv: %w", err)
	}
	defer func() { _ = f.Close() }()

	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read results csv: %w", err)
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("results csv %q has no data rows", path)
	}

	header := make(map[string]int, len(rows[0]))
	for i, name := range rows[0] {
		header[name] = i
	}
	required := []string{"name", "engine", "workload", "ops", "ops_per_sec", "avg_ns", "p50_ns", "p95_ns", "p99_ns", "duration_ns", "data_bytes", "data_mb"}
	for _, name := range required {
		if _, ok := header[name]; !ok {
			return nil, fmt.Errorf("results csv missing column %q", name)
		}
	}

	results := make([]bench.BenchmarkResult, 0, len(rows)-1)
	for _, row := range rows[1:] {
		parseInt := func(col string) (int64, error) {
			return strconv.ParseInt(row[header[col]], 10, 64)
		}
		parseFloat := func(col string) (float64, error) {
			return strconv.ParseFloat(row[header[col]], 64)
		}

		ops, err := parseInt("ops")
		if err != nil {
			return nil, fmt.Errorf("parse ops: %w", err)
		}
		durationNS, err := parseInt("duration_ns")
		if err != nil {
			return nil, fmt.Errorf("parse duration_ns: %w", err)
		}
		dataBytes, err := parseInt("data_bytes")
		if err != nil {
			return nil, fmt.Errorf("parse data_bytes: %w", err)
		}
		throughput, err := parseFloat("ops_per_sec")
		if err != nil {
			return nil, fmt.Errorf("parse ops_per_sec: %w", err)
		}
		avgNS, err := parseFloat("avg_ns")
		if err != nil {
			return nil, fmt.Errorf("parse avg_ns: %w", err)
		}
		p50NS, err := parseFloat("p50_ns")
		if err != nil {
			return nil, fmt.Errorf("parse p50_ns: %w", err)
		}
		p95NS, err := parseFloat("p95_ns")
		if err != nil {
			return nil, fmt.Errorf("parse p95_ns: %w", err)
		}
		p99NS, err := parseFloat("p99_ns")
		if err != nil {
			return nil, fmt.Errorf("parse p99_ns: %w", err)
		}
		dataMB, err := parseFloat("data_mb")
		if err != nil {
			return nil, fmt.Errorf("parse data_mb: %w", err)
		}

		result := bench.BenchmarkResult{
			Name:            row[header["name"]],
			Engine:          row[header["engine"]],
			Operation:       row[header["workload"]],
			TotalOperations: ops,
			TotalDuration:   time.Duration(durationNS),
			DataBytes:       dataBytes,
			DataSize:        dataMB,
			Throughput:      throughput,
			AvgLatencyNS:    avgNS,
			P50LatencyNS:    p50NS,
			P95LatencyNS:    p95NS,
			P99LatencyNS:    p99NS,
		}
		results = append(results, result)
	}
	return results, nil
}

type metricDefinition struct {
	Label string
	Value func(bench.BenchmarkResult) float64
}

func metricSpec(metric ResultMetric) (metricDefinition, error) {
	switch metric {
	case MetricThroughputOpsPerSec:
		return metricDefinition{
			Label: "Throughput (ops/s)",
			Value: func(r bench.BenchmarkResult) float64 { return r.Throughput },
		}, nil
	case MetricAvgLatencyUS:
		return metricDefinition{
			Label: "Average Latency (µs)",
			Value: func(r bench.BenchmarkResult) float64 { return r.AvgLatencyNS / 1_000.0 },
		}, nil
	case MetricP50LatencyUS:
		return metricDefinition{
			Label: "P50 Latency (µs)",
			Value: func(r bench.BenchmarkResult) float64 { return r.P50LatencyNS / 1_000.0 },
		}, nil
	case MetricP95LatencyUS:
		return metricDefinition{
			Label: "P95 Latency (µs)",
			Value: func(r bench.BenchmarkResult) float64 { return r.P95LatencyNS / 1_000.0 },
		}, nil
	case MetricP99LatencyUS:
		return metricDefinition{
			Label: "P99 Latency (µs)",
			Value: func(r bench.BenchmarkResult) float64 { return r.P99LatencyNS / 1_000.0 },
		}, nil
	case MetricDataSizeMB:
		return metricDefinition{
			Label: "Data Size (MB)",
			Value: func(r bench.BenchmarkResult) float64 { return r.DataSize },
		}, nil
	default:
		return metricDefinition{}, fmt.Errorf("unsupported result metric %q", metric)
	}
}
