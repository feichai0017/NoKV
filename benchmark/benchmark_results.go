package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"
)

// BenchmarkResult captures metrics for a single benchmark run.
type BenchmarkResult struct {
	Name            string        `json:"name"`
	Engine          string        `json:"engine"`
	Operation       string        `json:"operation"`
	Mode            string        `json:"mode,omitempty"`
	StartTime       time.Time     `json:"start_time"`
	EndTime         time.Time     `json:"end_time"`
	TotalDuration   time.Duration `json:"total_duration_ns"`
	TotalOperations int64         `json:"total_ops"`
	DataSize        float64       `json:"data_mb"`
	Throughput      float64       `json:"ops_per_sec"`
	AvgLatencyNS    float64       `json:"avg_latency_ns"`
}

// Finalize computes derived metrics after the raw totals are filled in.
func (r *BenchmarkResult) Finalize() {
	sec := r.TotalDuration.Seconds()
	if sec > 0 {
		r.Throughput = float64(r.TotalOperations) / sec
	}
	if r.TotalOperations > 0 {
		r.AvgLatencyNS = float64(r.TotalDuration.Nanoseconds()) / float64(r.TotalOperations)
	}
}

// avgPerOp returns the average latency per operation as a duration.
func (r BenchmarkResult) avgPerOp() time.Duration {
	if r.TotalOperations == 0 {
		return 0
	}
	return time.Duration(r.AvgLatencyNS) * time.Nanosecond
}

// opsPerSecond returns the throughput in operations per second.
func (r BenchmarkResult) opsPerSecond() float64 {
	return r.Throughput
}

// writeSummaryTable renders a table of benchmark results to the provided writer.
func writeSummaryTable(w *tabwriter.Writer, results []BenchmarkResult) {
	fmt.Fprintln(w, "ENGINE\tOPERATION\tMODE\tOPS/S\tAVG LATENCY\tTOTAL OPS\tDATA (MB)\tDURATION")
	for _, result := range results {
		fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%.0f\t%v\t%d\t%.2f\t%v\n",
			result.Engine,
			result.Operation,
			result.Mode,
			result.opsPerSecond(),
			result.avgPerOp(),
			result.TotalOperations,
			result.DataSize,
			result.TotalDuration,
		)
	}
	w.Flush()
}

// WriteResults writes benchmark results to a timestamped text report for convenience.
func WriteResults(results []BenchmarkResult) error {
	resultsDir := "benchmark_results"
	if err := os.MkdirAll(resultsDir, 0o755); err != nil {
		return fmt.Errorf("failed to create results directory: %w", err)
	}

	filename := fmt.Sprintf("benchmark_results_%s.txt", time.Now().Format("20060102_150405"))
	filepath := filepath.Join(resultsDir, filename)

	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create results file: %w", err)
	}
	defer file.Close()

	fmt.Fprintf(file, "=== Benchmark Results ===\n")
	fmt.Fprintf(file, "Generated at: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))

	fmt.Fprintf(file, "Summary:\n")
	tw := tabwriter.NewWriter(file, 0, 4, 2, ' ', 0)
	writeSummaryTable(tw, results)
	fmt.Fprintln(file)

	for _, result := range results {
		fmt.Fprintf(file, "=== %s ===\n", result.Name)
		fmt.Fprintf(file, "Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))
		fmt.Fprintf(file, "End Time: %s\n", result.EndTime.Format("2006-01-02 15:04:05"))
		fmt.Fprintf(file, "Total Duration: %v\n", result.TotalDuration)
		fmt.Fprintf(file, "Average Time per Operation: %v\n", result.avgPerOp())
		fmt.Fprintf(file, "Total Operations: %d\n", result.TotalOperations)
		fmt.Fprintf(file, "Total Data Size: %.2f MB\n", result.DataSize)
		fmt.Fprintf(file, "Throughput: %.0f ops/s\n", result.opsPerSecond())
		fmt.Fprintf(file, "Average Latency: %.0f ns\n", result.AvgLatencyNS)
		fmt.Fprintln(file)
	}

	return nil
}