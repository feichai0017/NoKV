package runtime

import (
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/metrics"
	"github.com/stretchr/testify/require"
)

type testModule struct {
	closed *int
}

func (m *testModule) Close() {
	if m == nil || m.closed == nil {
		return
	}
	*m.closed = *m.closed + 1
}

func TestRegistryRegisterCountAndClear(t *testing.T) {
	var r Registry
	require.Equal(t, 0, r.Count())
	require.True(t, r.Cleared())

	count1 := 0
	count2 := 0
	r.Register(&testModule{closed: &count1})
	r.Register(&testModule{closed: &count2})

	require.Equal(t, 2, r.Count())
	require.False(t, r.Cleared())

	r.CloseAll()

	require.Equal(t, 1, count1)
	require.Equal(t, 1, count2)
	require.Equal(t, 0, r.Count())
	require.True(t, r.Cleared())
}

func TestRegistryUnregister(t *testing.T) {
	var r Registry
	count := 0
	module := &testModule{closed: &count}

	r.Register(module)
	require.Equal(t, 1, r.Count())

	r.Unregister(module)
	require.Equal(t, 0, r.Count())

	r.CloseAll()
	require.Equal(t, 0, count)
}

type testStatsCollector struct {
	started      int
	closeErr     error
	closed       int
	regionMetric *metrics.RegionMetrics
}

func (s *testStatsCollector) StartStats() {
	s.started++
}

func (s *testStatsCollector) SetRegionMetrics(rm *metrics.RegionMetrics) {
	s.regionMetric = rm
}

func (s *testStatsCollector) Close() error {
	s.closed++
	return s.closeErr
}

func TestBackgroundServicesLifecycle(t *testing.T) {
	stats := &testStatsCollector{}
	var services BackgroundServices
	services.Init(stats)

	compacterStarts := 0
	valueLogGCStarts := 0
	services.Start(BackgroundConfig{
		StartCompacter: func() {
			compacterStarts++
		},
		StartValueLogGC: func() {
			valueLogGCStarts++
		},
		EnableWALWatchdog: true,
	})

	require.Equal(t, 1, compacterStarts)
	require.Equal(t, 1, valueLogGCStarts)
	require.Equal(t, 1, stats.started)
	require.Same(t, stats, services.StatsCollector())
	require.Empty(t, services.WALWatchdogs())

	rm := metrics.NewRegionMetrics()
	services.SetRegionMetrics(rm)
	require.Same(t, rm, stats.regionMetric)

	require.NoError(t, services.Close())
	require.Equal(t, 1, stats.closed)
	require.Nil(t, services.StatsCollector())
	require.Empty(t, services.WALWatchdogs())
}

func TestBackgroundServicesClosePropagatesStatsError(t *testing.T) {
	stats := &testStatsCollector{closeErr: errors.New("close failed")}
	services := &BackgroundServices{}
	services.Init(stats)

	err := services.Close()
	require.Error(t, err)
	require.Contains(t, err.Error(), "stats close: close failed")
	require.Equal(t, 1, stats.closed)
	require.Nil(t, services.StatsCollector())
}
