package flush

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/internal/metrics"
)

// Stage represents the state of a flush task.
type Stage int32

const (
	StagePrepare Stage = iota
	StageBuild
	StageInstall
	StageRelease
)

type Task struct {
	ID         uint64
	SegmentID  uint32
	Stage      Stage
	Data       any
	Err        error
	queuedAt   time.Time
	buildStart time.Time
	installAt  time.Time
}

type Metrics = metrics.FlushMetrics

// Manager coordinates flush tasks.
type Manager struct {
	mu      sync.Mutex
	cond    *sync.Cond
	closed  bool
	counter uint64

	queue         []*Task
	active        map[uint64]*Task
	pending       int64
	queueLen      int64
	activeCt      int64
	waitSumNs     int64
	waitCount     int64
	waitLastNs    int64
	waitMaxNs     int64
	buildSumNs    int64
	buildCount    int64
	buildLastNs   int64
	buildMaxNs    int64
	releaseSumNs  int64
	releaseCount  int64
	releaseLastNs int64
	releaseMaxNs  int64
	completed     int64
}

func NewManager() *Manager {
	m := &Manager{
		queue:  make([]*Task, 0),
		active: make(map[uint64]*Task),
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

func (m *Manager) Submit(task *Task) (*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, errors.New("flush manager closed")
	}
	task.ID = atomic.AddUint64(&m.counter, 1)
	task.Stage = StagePrepare
	task.queuedAt = time.Now()
	m.queue = append(m.queue, task)
	atomic.AddInt64(&m.pending, 1)
	atomic.StoreInt64(&m.queueLen, int64(len(m.queue)))
	m.cond.Signal()
	return task, nil
}

func (m *Manager) Next() (*Task, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for !m.closed && len(m.queue) == 0 {
		m.cond.Wait()
	}
	if len(m.queue) == 0 {
		return nil, false
	}
	task := m.queue[0]
	m.queue = m.queue[1:]
	atomic.StoreInt64(&m.queueLen, int64(len(m.queue)))
	task.Stage = StageBuild
	task.buildStart = time.Now()
	if !task.queuedAt.IsZero() {
		waitNs := time.Since(task.queuedAt).Nanoseconds()
		atomic.AddInt64(&m.waitSumNs, waitNs)
		atomic.AddInt64(&m.waitCount, 1)
		atomic.StoreInt64(&m.waitLastNs, waitNs)
		updateMaxInt64(&m.waitMaxNs, waitNs)
	}
	m.active[task.ID] = task
	atomic.AddInt64(&m.activeCt, 1)
	return task, true
}

func (m *Manager) Update(taskID uint64, stage Stage, data any, err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, ok := m.active[taskID]
	if !ok {
		return errors.New("flush task not found")
	}
	task.Stage = stage
	task.Data = data
	task.Err = err

	if stage == StageInstall && err == nil {
		if !task.buildStart.IsZero() {
			buildNs := time.Since(task.buildStart).Nanoseconds()
			atomic.AddInt64(&m.buildSumNs, buildNs)
			atomic.AddInt64(&m.buildCount, 1)
			atomic.StoreInt64(&m.buildLastNs, buildNs)
			updateMaxInt64(&m.buildMaxNs, buildNs)
		}
		task.installAt = time.Now()
	}

	released := stage == StageRelease || err != nil
	if released {
		if !task.installAt.IsZero() {
			releaseNs := time.Since(task.installAt).Nanoseconds()
			atomic.AddInt64(&m.releaseSumNs, releaseNs)
			atomic.AddInt64(&m.releaseCount, 1)
			atomic.StoreInt64(&m.releaseLastNs, releaseNs)
			updateMaxInt64(&m.releaseMaxNs, releaseNs)
		}
		delete(m.active, taskID)
		atomic.AddInt64(&m.activeCt, -1)
		atomic.AddInt64(&m.pending, -1)
		atomic.AddInt64(&m.completed, 1)
		task.Stage = StageRelease
	}
	return err
}

func (m *Manager) Stats() Metrics {
	return Metrics{
		Pending:       atomic.LoadInt64(&m.pending),
		Queue:         atomic.LoadInt64(&m.queueLen),
		Active:        atomic.LoadInt64(&m.activeCt),
		WaitNs:        atomic.LoadInt64(&m.waitSumNs),
		WaitCount:     atomic.LoadInt64(&m.waitCount),
		WaitLastNs:    atomic.LoadInt64(&m.waitLastNs),
		WaitMaxNs:     atomic.LoadInt64(&m.waitMaxNs),
		BuildNs:       atomic.LoadInt64(&m.buildSumNs),
		BuildCount:    atomic.LoadInt64(&m.buildCount),
		BuildLastNs:   atomic.LoadInt64(&m.buildLastNs),
		BuildMaxNs:    atomic.LoadInt64(&m.buildMaxNs),
		ReleaseNs:     atomic.LoadInt64(&m.releaseSumNs),
		ReleaseCount:  atomic.LoadInt64(&m.releaseCount),
		ReleaseLastNs: atomic.LoadInt64(&m.releaseLastNs),
		ReleaseMaxNs:  atomic.LoadInt64(&m.releaseMaxNs),
		Completed:     atomic.LoadInt64(&m.completed),
	}
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.cond.Broadcast()
	return nil
}

func updateMaxInt64(target *int64, val int64) {
	for {
		current := atomic.LoadInt64(target)
		if val <= current {
			return
		}
		if atomic.CompareAndSwapInt64(target, current, val) {
			return
		}
	}
}
