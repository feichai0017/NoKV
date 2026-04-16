package plot

import (
	"fmt"
	"image/color"
	"os"
	"path/filepath"
	"slices"

	goplot "gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

// WriteGroupedBarChart renders a grouped bar chart to the output path
// configured in cfg. The output format is inferred from the file extension
// supported by gonum/plot (for example .png, .svg, .pdf).
func WriteGroupedBarChart(observations []Observation, cfg GroupedBarChartConfig) error {
	cfg = cfg.withDefaults()
	if cfg.Output == "" {
		return fmt.Errorf("plot output path is required")
	}
	if len(observations) == 0 {
		return fmt.Errorf("no observations to plot")
	}

	categories := orderedValues(observations, cfg.CategoryOrder, func(o Observation) string { return o.Category })
	series := orderedValues(observations, cfg.SeriesOrder, func(o Observation) string { return o.Series })
	if len(categories) == 0 || len(series) == 0 {
		return fmt.Errorf("plot requires at least one category and one series")
	}

	p := goplot.New()
	p.Title.Text = cfg.Title
	p.X.Label.Text = cfg.XLabel
	p.Y.Label.Text = cfg.YLabel
	cfg.Theme.Apply(p)
	p.Y.Min = cfg.YMin

	if cfg.ShowGrid {
		grid := plotter.NewGrid()
		grid.Vertical.Color = color.Transparent
		grid.Horizontal.Color = cfg.Theme.Grid
		grid.Horizontal.Width = vg.Points(0.7)
		p.Add(grid)
	}

	valueMatrix := make(map[string]map[string]float64, len(series))
	for _, s := range series {
		valueMatrix[s] = make(map[string]float64, len(categories))
	}
	for _, obs := range observations {
		valueMatrix[obs.Series][obs.Category] = obs.Value
	}

	barWidth := groupedBarWidth(len(series))
	for i, s := range series {
		vals := make(plotter.Values, len(categories))
		for j, c := range categories {
			vals[j] = valueMatrix[s][c]
		}
		bars, err := plotter.NewBarChart(vals, barWidth)
		if err != nil {
			return fmt.Errorf("build bar chart for %q: %w", s, err)
		}
		bars.LineStyle.Width = vg.Points(0.4)
		bars.LineStyle.Color = color.White
		bars.Color = cfg.Theme.Palette[i%len(cfg.Theme.Palette)]
		bars.Offset = groupedBarOffset(i, len(series), barWidth)
		p.Add(bars)
		if !cfg.HideLegend {
			p.Legend.Add(s, bars)
		}
	}

	p.NominalX(categories...)

	if err := os.MkdirAll(filepath.Dir(cfg.Output), 0o755); err != nil {
		return fmt.Errorf("create plot output dir: %w", err)
	}
	if err := p.Save(cfg.Width, cfg.Height, cfg.Output); err != nil {
		return fmt.Errorf("save plot: %w", err)
	}
	return nil
}

func groupedBarWidth(seriesCount int) vg.Length {
	if seriesCount <= 1 {
		return vg.Points(24)
	}
	width := 30.0 / float64(seriesCount)
	if width < 8 {
		width = 8
	}
	if width > 20 {
		width = 20
	}
	return vg.Points(width)
}

func groupedBarOffset(index, seriesCount int, width vg.Length) vg.Length {
	center := float64(seriesCount-1) / 2
	return vg.Length((float64(index) - center) * float64(width))
}

func orderedValues[T any](items []T, preferred []string, project func(T) string) []string {
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, v := range preferred {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	for _, item := range items {
		key := project(item)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	if len(preferred) == 0 {
		slices.Sort(out)
	}
	return out
}
