package plot

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gonum.org/v1/plot/vg"
)

// PlotConfig bundles generic chart styling with optional observation CSV column
// mappings. This keeps the plotting entrypoint parameter-driven rather than
// tied to hardcoded figure presets.
type PlotConfig struct {
	Chart        GroupedBarChartConfig
	Observations ObservationCSVConfig
}

// ReadPlotConfigCSV loads plot configuration from a simple CSV with either
// `key,value` rows or `key,value1,value2,...` rows for list fields.
//
// Supported chart keys:
// - title
// - xlabel
// - ylabel
// - width_in
// - height_in
// - ymin
// - show_grid
// - hide_legend
// - category_order
// - series_order
//
// Supported observation keys:
// - category_column
// - series_column
// - value_column
func ReadPlotConfigCSV(path string) (PlotConfig, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return PlotConfig{}, fmt.Errorf("open plot config csv: %w", err)
	}
	defer func() { _ = f.Close() }()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	rows, err := reader.ReadAll()
	if err != nil {
		return PlotConfig{}, fmt.Errorf("read plot config csv: %w", err)
	}
	cfg := PlotConfig{
		Chart: GroupedBarChartConfig{Theme: DefaultTheme()},
	}
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		key := strings.TrimSpace(strings.ToLower(row[0]))
		if key == "" || strings.HasPrefix(key, "#") {
			continue
		}
		values := make([]string, 0, len(row))
		for _, value := range row[1:] {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			values = append(values, value)
		}
		switch key {
		case "title":
			if len(values) > 0 {
				cfg.Chart.Title = strings.Join(values, ",")
			}
		case "xlabel":
			if len(values) > 0 {
				cfg.Chart.XLabel = strings.Join(values, ",")
			}
		case "ylabel":
			if len(values) > 0 {
				cfg.Chart.YLabel = strings.Join(values, ",")
			}
		case "width_in":
			if len(values) == 0 {
				continue
			}
			v, err := strconv.ParseFloat(values[0], 64)
			if err != nil {
				return PlotConfig{}, fmt.Errorf("parse width_in: %w", err)
			}
			cfg.Chart.Width = vg.Length(v) * vg.Inch
		case "height_in":
			if len(values) == 0 {
				continue
			}
			v, err := strconv.ParseFloat(values[0], 64)
			if err != nil {
				return PlotConfig{}, fmt.Errorf("parse height_in: %w", err)
			}
			cfg.Chart.Height = vg.Length(v) * vg.Inch
		case "ymin":
			if len(values) == 0 {
				continue
			}
			v, err := strconv.ParseFloat(values[0], 64)
			if err != nil {
				return PlotConfig{}, fmt.Errorf("parse ymin: %w", err)
			}
			cfg.Chart.YMin = v
		case "show_grid":
			if len(values) == 0 {
				continue
			}
			v, err := strconv.ParseBool(values[0])
			if err != nil {
				return PlotConfig{}, fmt.Errorf("parse show_grid: %w", err)
			}
			cfg.Chart.ShowGrid = v
			cfg.Chart.GridConfigured = true
		case "hide_legend":
			if len(values) == 0 {
				continue
			}
			v, err := strconv.ParseBool(values[0])
			if err != nil {
				return PlotConfig{}, fmt.Errorf("parse hide_legend: %w", err)
			}
			cfg.Chart.HideLegend = v
		case "category_order":
			cfg.Chart.CategoryOrder = append([]string(nil), values...)
		case "series_order":
			cfg.Chart.SeriesOrder = append([]string(nil), values...)
		case "category_column":
			if len(values) > 0 {
				cfg.Observations.CategoryColumn = values[0]
			}
		case "series_column":
			if len(values) > 0 {
				cfg.Observations.SeriesColumn = values[0]
			}
		case "value_column":
			if len(values) > 0 {
				cfg.Observations.ValueColumn = values[0]
			}
		default:
			return PlotConfig{}, fmt.Errorf("unsupported plot config key %q", key)
		}
	}
	cfg.Observations = cfg.Observations.withDefaults()
	return cfg, nil
}

// ReadGroupedBarChartConfigCSV keeps backward compatibility for callers that
// only care about chart styling.
func ReadGroupedBarChartConfigCSV(path string) (GroupedBarChartConfig, error) {
	cfg, err := ReadPlotConfigCSV(path)
	if err != nil {
		return GroupedBarChartConfig{}, err
	}
	return cfg.Chart, nil
}
