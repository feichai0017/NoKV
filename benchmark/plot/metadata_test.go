package plot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadObservationsCSV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "obs.csv")
	err := os.WriteFile(path, []byte("category,series,value\npaginated,read-plane,198.2\npaginated,secondary-index,209.4\n"), 0o644)
	require.NoError(t, err)

	obs, err := ReadObservationsCSV(path)
	require.NoError(t, err)
	require.Len(t, obs, 2)
	require.Equal(t, "paginated", obs[0].Category)
	require.Equal(t, "read-plane", obs[0].Series)
	require.Equal(t, 198.2, obs[0].Value)
}

func TestReadObservationsCSVWithConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "obs.csv")
	err := os.WriteFile(path, []byte("mode,engine,latency_us\npaginated,read-plane,198.2\npaginated,secondary-index,209.4\n"), 0o644)
	require.NoError(t, err)

	obs, err := ReadObservationsCSVWithConfig(path, ObservationCSVConfig{
		CategoryColumn: "mode",
		SeriesColumn:   "engine",
		ValueColumn:    "latency_us",
	})
	require.NoError(t, err)
	require.Len(t, obs, 2)
	require.Equal(t, "paginated", obs[0].Category)
	require.Equal(t, "read-plane", obs[0].Series)
	require.Equal(t, 198.2, obs[0].Value)
}

func TestReadPlotConfigCSV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.csv")
	err := os.WriteFile(path, []byte(
		"title,Steady-State Paginated Listing\n"+
			"xlabel,Mode\n"+
			"ylabel,Latency (us)\n"+
			"width_in,6.8\n"+
			"height_in,3.9\n"+
			"show_grid,false\n"+
			"hide_legend,true\n"+
			"category_order,steady-paginated\n"+
			"series_order,secondary-index,repairing-read-plane,strict-read-plane\n"+
			"category_column,mode\n"+
			"series_column,implementation\n"+
			"value_column,latency_us\n",
	), 0o644)
	require.NoError(t, err)

	cfg, err := ReadPlotConfigCSV(path)
	require.NoError(t, err)
	require.Equal(t, "Steady-State Paginated Listing", cfg.Chart.Title)
	require.Equal(t, "Mode", cfg.Chart.XLabel)
	require.Equal(t, "Latency (us)", cfg.Chart.YLabel)
	require.False(t, cfg.Chart.ShowGrid)
	require.True(t, cfg.Chart.GridConfigured)
	require.True(t, cfg.Chart.HideLegend)
	require.Equal(t, []string{"steady-paginated"}, cfg.Chart.CategoryOrder)
	require.Equal(t, []string{"secondary-index", "repairing-read-plane", "strict-read-plane"}, cfg.Chart.SeriesOrder)
	require.Equal(t, "mode", cfg.Observations.CategoryColumn)
	require.Equal(t, "implementation", cfg.Observations.SeriesColumn)
	require.Equal(t, "latency_us", cfg.Observations.ValueColumn)
}

func TestWriteMetadataFigure(t *testing.T) {
	output := filepath.Join(t.TempDir(), "namespace.svg")
	err := WriteMetadataFigure([]Observation{
		{Category: "paginated", Series: "read-plane", Value: 198.2},
		{Category: "paginated", Series: "secondary-index", Value: 209.4},
		{Category: "single-page", Series: "read-plane", Value: 29.4},
		{Category: "single-page", Series: "secondary-index", Value: 35.0},
	}, MetadataFigureConfig{
		Preset: MetadataPresetNamespaceSteadyState,
		Output: output,
	})
	require.NoError(t, err)
	info, err := os.Stat(output)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(0))
}
