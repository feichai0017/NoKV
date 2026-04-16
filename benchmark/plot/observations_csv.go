package plot

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// ReadObservationsCSV parses a generic CSV with the default columns:
// category,series,value
func ReadObservationsCSV(path string) ([]Observation, error) {
	return ReadObservationsCSVWithConfig(path, ObservationCSVConfig{})
}

// ReadObservationsCSVWithConfig parses a generic CSV and maps selected columns
// into category/series/value triples.
func ReadObservationsCSVWithConfig(path string, cfg ObservationCSVConfig) ([]Observation, error) {
	cfg = cfg.withDefaults()
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("open observations csv: %w", err)
	}
	defer func() { _ = f.Close() }()

	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read observations csv: %w", err)
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("observations csv %q has no data rows", path)
	}

	header := make(map[string]int, len(rows[0]))
	for i, name := range rows[0] {
		header[name] = i
	}
	required := []string{cfg.CategoryColumn, cfg.SeriesColumn, cfg.ValueColumn}
	for _, name := range required {
		if _, ok := header[name]; !ok {
			return nil, fmt.Errorf("observations csv missing column %q", name)
		}
	}

	out := make([]Observation, 0, len(rows)-1)
	for _, row := range rows[1:] {
		value, err := strconv.ParseFloat(row[header[cfg.ValueColumn]], 64)
		if err != nil {
			return nil, fmt.Errorf("parse observation value: %w", err)
		}
		out = append(out, Observation{
			Category: row[header[cfg.CategoryColumn]],
			Series:   row[header[cfg.SeriesColumn]],
			Value:    value,
		})
	}
	return out, nil
}
