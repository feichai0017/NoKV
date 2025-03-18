package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// BenchmarkResult represents a single benchmark result
type BenchmarkResult struct {
	Name            string
	StartTime       time.Time
	EndTime         time.Time
	TotalDuration   time.Duration
	TotalOperations int64
	DataSize        float64
	MemoryStats     struct {
		Allocations int64
		Bytes       int64
	}
}

// WriteResults writes benchmark results to a file
func WriteResults(results []BenchmarkResult) error {
	// Create results directory if it doesn't exist
	resultsDir := "benchmark_results"
	if err := os.MkdirAll(resultsDir, 0755); err != nil {
		return fmt.Errorf("failed to create results directory: %v", err)
	}

	// Generate filename with timestamp
	filename := fmt.Sprintf("benchmark_results_%s.txt", time.Now().Format("20060102_150405"))
	filepath := filepath.Join(resultsDir, filename)

	// Create file
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create results file: %v", err)
	}
	defer file.Close()

	// Write header
	fmt.Fprintf(file, "=== Benchmark Results ===\n")
	fmt.Fprintf(file, "Generated at: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))

	// Write each result
	for _, result := range results {
		fmt.Fprintf(file, "=== %s ===\n", result.Name)
		fmt.Fprintf(file, "Start Time: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))
		fmt.Fprintf(file, "End Time: %s\n", result.EndTime.Format("2006-01-02 15:04:05"))
		fmt.Fprintf(file, "Total Duration: %v\n", result.TotalDuration)
		fmt.Fprintf(file, "Average Time per Operation: %v\n", result.TotalDuration/time.Duration(result.TotalOperations))
		fmt.Fprintf(file, "Total Operations: %d\n", result.TotalOperations)
		fmt.Fprintf(file, "Total Data Size: %.2f MB\n", result.DataSize)
		fmt.Fprintf(file, "Memory Allocations: %d\n", result.MemoryStats.Allocations)
		fmt.Fprintf(file, "Memory Bytes: %d\n", result.MemoryStats.Bytes)
		fmt.Fprintf(file, "\n")
	}

	return nil
}
