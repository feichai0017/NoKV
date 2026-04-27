package runtime

import (
	stderrors "errors"
	"fmt"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/metrics"
)

// StatsCollector is the narrow observer surface required by background
// DB services. The concrete implementation lives in runtime/stats while
// the lifecycle orchestration lives here.
type StatsCollector interface {
	StartStats()
	SetRegionMetrics(*metrics.RegionMetrics)
	Close() error
}

// BackgroundConfig describes the runtime hooks needed to start
// DB-scoped background services without importing the root DB package.
//
// WALWatchdogConfigs supports the LSM data plane's per-shard WAL
// Managers: one watchdog per Manager keeps backlog metrics and auto-GC
// scoped to that shard's segments.
type BackgroundConfig struct {
	StartCompacter     func()
	StartValueLogGC    func()
	EnableWALWatchdog  bool
	WALWatchdogConfigs []wal.WatchdogConfig
}

// BackgroundServices owns DB-scoped background runtime services that
// are not part of the DB's truth or public API surface.
type BackgroundServices struct {
	stats        StatsCollector
	walWatchdogs []*wal.Watchdog
}

func (s *BackgroundServices) Init(stats StatsCollector) {
	if s == nil {
		return
	}
	s.stats = stats
}

func (s *BackgroundServices) Start(cfg BackgroundConfig) {
	if s == nil {
		return
	}
	if cfg.StartCompacter != nil {
		cfg.StartCompacter()
	}
	if cfg.EnableWALWatchdog {
		for _, wcfg := range cfg.WALWatchdogConfigs {
			wd := wal.NewWatchdog(wcfg)
			if wd == nil {
				continue
			}
			wd.Start()
			s.walWatchdogs = append(s.walWatchdogs, wd)
		}
	}
	if s.stats != nil {
		s.stats.StartStats()
	}
	if cfg.StartValueLogGC != nil {
		cfg.StartValueLogGC()
	}
}

func (s *BackgroundServices) Close() error {
	if s == nil {
		return nil
	}
	var errs []error
	if s.stats != nil {
		if err := s.stats.Close(); err != nil {
			errs = append(errs, fmt.Errorf("stats close: %w", err))
		}
		s.stats = nil
	}
	for i, wd := range s.walWatchdogs {
		if wd != nil {
			wd.Stop()
			s.walWatchdogs[i] = nil
		}
	}
	s.walWatchdogs = nil
	if len(errs) > 0 {
		return stderrors.Join(errs...)
	}
	return nil
}

func (s *BackgroundServices) StatsCollector() StatsCollector {
	if s == nil {
		return nil
	}
	return s.stats
}

func (s *BackgroundServices) SetRegionMetrics(rm *metrics.RegionMetrics) {
	if s == nil || s.stats == nil {
		return
	}
	s.stats.SetRegionMetrics(rm)
}

// WALWatchdogs returns every running WAL watchdog (one per LSM
// data-plane shard). Stats collectors that surface backlog metrics
// aggregate across the slice.
func (s *BackgroundServices) WALWatchdogs() []*wal.Watchdog {
	if s == nil {
		return nil
	}
	return s.walWatchdogs
}
