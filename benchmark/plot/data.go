package plot

import "gonum.org/v1/plot/vg"

// Observation is one scalar value plotted for a series/category pair.
type Observation struct {
	Category string
	Series   string
	Value    float64
}

// ObservationCSVConfig controls how generic observation CSV rows are mapped
// into category/series/value triples.
type ObservationCSVConfig struct {
	CategoryColumn string
	SeriesColumn   string
	ValueColumn    string
}

// DefaultObservationCSVConfig returns the default category/series/value column
// mapping used by generic observation CSV parsing.
func DefaultObservationCSVConfig() ObservationCSVConfig {
	return ObservationCSVConfig{}.withDefaults()
}

// NormalizeObservationCSVConfig fills missing observation CSV columns with
// defaults while preserving explicitly provided fields.
func NormalizeObservationCSVConfig(cfg ObservationCSVConfig) ObservationCSVConfig {
	return cfg.withDefaults()
}

func (cfg ObservationCSVConfig) withDefaults() ObservationCSVConfig {
	if cfg.CategoryColumn == "" {
		cfg.CategoryColumn = "category"
	}
	if cfg.SeriesColumn == "" {
		cfg.SeriesColumn = "series"
	}
	if cfg.ValueColumn == "" {
		cfg.ValueColumn = "value"
	}
	return cfg
}

// GroupedBarChartConfig controls grouped bar chart rendering.
type GroupedBarChartConfig struct {
	Title          string
	XLabel         string
	YLabel         string
	Output         string
	Width          vg.Length
	Height         vg.Length
	YMin           float64
	ShowGrid       bool
	GridConfigured bool
	HideLegend     bool
	CategoryOrder  []string
	SeriesOrder    []string
	Theme          Theme
}

func (cfg GroupedBarChartConfig) withDefaults() GroupedBarChartConfig {
	if cfg.Width <= 0 {
		cfg.Width = 7.2 * vg.Inch
	}
	if cfg.Height <= 0 {
		cfg.Height = 4.2 * vg.Inch
	}
	if !cfg.GridConfigured {
		cfg.ShowGrid = true
	}
	if len(cfg.Theme.Palette) == 0 {
		cfg.Theme = DefaultTheme()
	}
	return cfg
}
