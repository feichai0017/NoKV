package runtime

import (
	"context"
	"sync"
	"time"
)

// PeriodicTaskConfig configures one DB-scoped periodic runtime task.
type PeriodicTaskConfig struct {
	Name     string
	Interval time.Duration
	Run      func(context.Context) error
}

func (c PeriodicTaskConfig) enabled() bool {
	return c.Name != "" && c.Interval > 0 && c.Run != nil
}

// PeriodicTaskSnapshot exposes the last execution state of one periodic task.
type PeriodicTaskSnapshot struct {
	Enabled        bool
	Runs           uint64
	LastUnix       int64
	LastDurationMs float64
	LastError      string
}

type PeriodicTask struct {
	name     string
	interval time.Duration
	runFn    func(context.Context) error

	ctx    context.Context
	cancel context.CancelFunc
	stop   chan struct{}
	done   chan struct{}

	closeOnce sync.Once
	mu        sync.RWMutex
	snap      PeriodicTaskSnapshot
}

func NewPeriodicTask(cfg PeriodicTaskConfig) *PeriodicTask {
	if !cfg.enabled() {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &PeriodicTask{
		name:     cfg.Name,
		interval: cfg.Interval,
		runFn:    cfg.Run,
		ctx:      ctx,
		cancel:   cancel,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		snap: PeriodicTaskSnapshot{
			Enabled: true,
		},
	}
}

func (t *PeriodicTask) Name() string {
	if t == nil {
		return ""
	}
	return t.name
}

func (t *PeriodicTask) Start() {
	if t == nil {
		return
	}
	go t.loop()
}

func (t *PeriodicTask) Close() {
	if t == nil {
		return
	}
	t.closeOnce.Do(func() {
		t.cancel()
		close(t.stop)
	})
	<-t.done
}

func (t *PeriodicTask) Snapshot() PeriodicTaskSnapshot {
	if t == nil {
		return PeriodicTaskSnapshot{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.snap
}

func (t *PeriodicTask) loop() {
	defer close(t.done)
	t.runOnce()
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.runOnce()
		case <-t.stop:
			return
		}
	}
}

func (t *PeriodicTask) runOnce() {
	start := time.Now()
	if t.isStopped() {
		return
	}
	err := t.runFn(t.ctx)
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	t.mu.Lock()
	t.snap.Enabled = true
	t.snap.Runs++
	t.snap.LastUnix = time.Now().Unix()
	t.snap.LastDurationMs = float64(time.Since(start)) / 1e6
	t.snap.LastError = errText
	t.mu.Unlock()
}

func (t *PeriodicTask) isStopped() bool {
	select {
	case <-t.stop:
		return true
	default:
		return false
	}
}
