package runtime

import (
	stderrors "errors"
	"fmt"
	"sync"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/metrics"
)

// Module is the minimum lifecycle hook a runtime or research module must
// implement before it can be attached to the DB composition root.
//
// The interface is intentionally tiny: attached modules own their own state
// and cleanup, while the DB only coordinates lifecycle boundaries.
type Module interface {
	Close()
}

// Registry tracks optional runtime or research modules attached to one DB
// instance without baking module-specific fields into DB itself.
//
// This is a platform boundary, not a plugin framework. It exists to keep DB
// from accumulating one-off subsystem maps and locks as the repository grows.
type Registry struct {
	mu      sync.Mutex
	modules map[Module]struct{}
}

func (r *Registry) Register(m Module) {
	if r == nil || m == nil {
		return
	}
	r.mu.Lock()
	if r.modules == nil {
		r.modules = make(map[Module]struct{})
	}
	r.modules[m] = struct{}{}
	r.mu.Unlock()
}

func (r *Registry) Unregister(m Module) {
	if r == nil || m == nil {
		return
	}
	r.mu.Lock()
	delete(r.modules, m)
	r.mu.Unlock()
}

func (r *Registry) CloseAll() {
	if r == nil {
		return
	}
	r.mu.Lock()
	modules := make([]Module, 0, len(r.modules))
	for m := range r.modules {
		modules = append(modules, m)
	}
	r.modules = nil
	r.mu.Unlock()
	for _, m := range modules {
		if m != nil {
			m.Close()
		}
	}
}

func (r *Registry) Count() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.modules)
}

func (r *Registry) Cleared() bool {
	if r == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.modules == nil
}

// StatsCollector is the narrow observer surface required by background DB
// services. The concrete implementation may live in the root package while the
// lifecycle orchestration lives in internal/runtime.
type StatsCollector interface {
	StartStats()
	SetRegionMetrics(*metrics.RegionMetrics)
	Close() error
}

// BackgroundConfig describes the runtime hooks needed to start DB-scoped
// background services without importing the root DB package into internal code.
type BackgroundConfig struct {
	StartCompacter    func()
	StartValueLogGC   func()
	EnableWALWatchdog bool
	WALWatchdogConfig wal.WatchdogConfig
}

// BackgroundServices owns DB-scoped background runtime services that are not
// part of the DB's truth or public API surface.
type BackgroundServices struct {
	stats       StatsCollector
	walWatchdog *wal.Watchdog
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
		s.walWatchdog = wal.NewWatchdog(cfg.WALWatchdogConfig)
		if s.walWatchdog != nil {
			s.walWatchdog.Start()
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
	if s.walWatchdog != nil {
		s.walWatchdog.Stop()
		s.walWatchdog = nil
	}
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

func (s *BackgroundServices) WALWatchdog() *wal.Watchdog {
	if s == nil {
		return nil
	}
	return s.walWatchdog
}
