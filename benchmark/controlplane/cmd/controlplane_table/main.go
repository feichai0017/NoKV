package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var benchLinePattern = regexp.MustCompile(`^(Benchmark\S+)-\d+\s+\d+\s+([0-9.]+)\s+ns/op\s+([0-9.]+)\s+B/op\s+([0-9.]+)\s+allocs/op$`)

type sample struct {
	nsPerOp  float64
	bytesOp  float64
	allocsOp float64
}

type row struct {
	label   string
	steady  string
	degrade string
}

func main() {
	input := flag.String("input", "", "path to raw go test benchmark output")
	suite := flag.String("suite", "", "table suite: inprocess or process")
	flag.Parse()

	if strings.TrimSpace(*input) == "" || strings.TrimSpace(*suite) == "" {
		fmt.Fprintln(os.Stderr, "usage: controlplane_table -input <raw.txt> -suite <inprocess|process>")
		os.Exit(2)
	}

	samples, err := parseSamples(*input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse benchmark samples: %v\n", err)
		os.Exit(1)
	}

	rows, err := buildRows(*suite, samples)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build table: %v\n", err)
		os.Exit(1)
	}

	if err := renderTable(os.Stdout, rows, samples); err != nil {
		fmt.Fprintf(os.Stderr, "render table: %v\n", err)
		os.Exit(1)
	}
}

func parseSamples(path string) (map[string][]sample, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	out := make(map[string][]sample)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		match := benchLinePattern.FindStringSubmatch(line)
		if len(match) == 0 {
			continue
		}
		nsPerOp, err := strconv.ParseFloat(match[2], 64)
		if err != nil {
			return nil, fmt.Errorf("parse ns/op for %q: %w", match[1], err)
		}
		bytesOp, err := strconv.ParseFloat(match[3], 64)
		if err != nil {
			return nil, fmt.Errorf("parse B/op for %q: %w", match[1], err)
		}
		allocsOp, err := strconv.ParseFloat(match[4], 64)
		if err != nil {
			return nil, fmt.Errorf("parse allocs/op for %q: %w", match[1], err)
		}
		out[match[1]] = append(out[match[1]], sample{
			nsPerOp:  nsPerOp,
			bytesOp:  bytesOp,
			allocsOp: allocsOp,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func buildRows(suite string, samples map[string][]sample) ([]row, error) {
	switch suite {
	case "inprocess":
		rows := []row{
			{label: "NoKV Local", steady: "BenchmarkControlPlaneAllocIDLocalWindowDefault", degrade: "BenchmarkControlPlaneAllocIDLocalWindowOne"},
			{label: "NoKV Remote (bufconn)", steady: "BenchmarkControlPlaneAllocIDRemoteWindowDefault", degrade: "BenchmarkControlPlaneAllocIDRemoteWindowOne"},
			{label: "NoKV Remote (TCP)", steady: "BenchmarkControlPlaneAllocIDRemoteTCPWindowDefault", degrade: "BenchmarkControlPlaneAllocIDRemoteTCPWindowOne"},
		}
		return validateRows(rows, samples)
	case "process":
		rows := []row{
			{label: "NoKV Remote (process TCP)", steady: "BenchmarkControlPlaneProcessNoKVRemoteTCPWindowDefault", degrade: "BenchmarkControlPlaneProcessNoKVRemoteTCPWindowOne"},
			{label: "etcd CAS (process)", steady: "BenchmarkControlPlaneProcessEtcdCASWindowDefault", degrade: "BenchmarkControlPlaneProcessEtcdCASWindowOne"},
		}
		return validateRows(rows, samples)
	default:
		return nil, fmt.Errorf("unknown suite %q", suite)
	}
}

func validateRows(rows []row, samples map[string][]sample) ([]row, error) {
	for _, row := range rows {
		if len(samples[row.steady]) == 0 {
			return nil, fmt.Errorf("missing benchmark %s", row.steady)
		}
		if len(samples[row.degrade]) == 0 {
			return nil, fmt.Errorf("missing benchmark %s", row.degrade)
		}
	}
	return rows, nil
}

func meanSample(samples []sample) sample {
	var out sample
	for _, s := range samples {
		out.nsPerOp += s.nsPerOp
		out.bytesOp += s.bytesOp
		out.allocsOp += s.allocsOp
	}
	if len(samples) == 0 {
		return out
	}
	n := float64(len(samples))
	out.nsPerOp /= n
	out.bytesOp /= n
	out.allocsOp /= n
	return out
}

func renderTable(w io.Writer, rows []row, samples map[string][]sample) error {
	if w == nil {
		return fmt.Errorf("table writer is nil")
	}
	_, _ = fmt.Fprintf(w, "| System | Window=10k | Window=1 | Slowdown |\n")
	_, _ = fmt.Fprintf(w, "| --- | --- | --- | --- |\n")
	for _, row := range rows {
		steady := meanSample(samples[row.steady])
		degrade := meanSample(samples[row.degrade])
		_, _ = fmt.Fprintf(w, "| %s | %s | %s | %.1fx |\n",
			row.label,
			formatLatency(steady.nsPerOp),
			formatLatency(degrade.nsPerOp),
			degrade.nsPerOp/steady.nsPerOp,
		)
	}

	_, _ = fmt.Fprintf(w, "\n")
	_, _ = fmt.Fprintf(w, "| Benchmark | Mean ns/op | Mean B/op | Mean allocs/op | Samples |\n")
	_, _ = fmt.Fprintf(w, "| --- | --- | --- | --- | --- |\n")

	keys := make([]string, 0, len(samples))
	for key := range samples {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		mean := meanSample(samples[key])
		_, _ = fmt.Fprintf(w, "| %s | %.1f | %.1f | %.1f | %d |\n",
			key, mean.nsPerOp, mean.bytesOp, mean.allocsOp, len(samples[key]),
		)
	}
	return nil
}

func formatLatency(ns float64) string {
	switch {
	case ns >= 1_000_000:
		return fmt.Sprintf("%.3f ms", ns/1_000_000)
	case ns >= 1_000:
		return fmt.Sprintf("%.3f us", ns/1_000)
	default:
		return fmt.Sprintf("%.1f ns", ns)
	}
}
