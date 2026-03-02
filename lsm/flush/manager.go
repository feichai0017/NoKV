package flush

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/metrics"
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
	counter atomic.Uint64

	queue         []*Task
	active        map[uint64]*Task
	pending       atomic.Int64
	queueLen      atomic.Int64
	activeCt      atomic.Int64
	waitSumNs     atomic.Int64
	waitCount     atomic.Int64
	waitLastNs    atomic.Int64
	waitMaxNs     atomic.Int64
	buildSumNs    atomic.Int64
	buildCount    atomic.Int64
	buildLastNs   atomic.Int64
	buildMaxNs    atomic.Int64
	releaseSumNs  atomic.Int64
	releaseCount  atomic.Int64
	releaseLastNs atomic.Int64
	releaseMaxNs  atomic.Int64
	completed     atomic.Int64
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
	task.ID = m.counter.Add(1)
	task.Stage = StagePrepare
	task.queuedAt = time.Now()
	m.queue = append(m.queue, task)
	m.pending.Add(1)
	m.queueLen.Store(int64(len(m.queue)))
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
	m.queueLen.Store(int64(len(m.queue)))
	task.Stage = StageBuild
	task.buildStart = time.Now()
	if !task.queuedAt.IsZero() {
		waitNs := time.Since(task.queuedAt).Nanoseconds()
		m.waitSumNs.Add(waitNs)
		m.waitCount.Add(1)
		m.waitLastNs.Store(waitNs)
		updateMaxInt64(&m.waitMaxNs, waitNs)
	}
	m.active[task.ID] = task
	m.activeCt.Add(1)
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
			m.buildSumNs.Add(buildNs)
			m.buildCount.Add(1)
			m.buildLastNs.Store(buildNs)
			updateMaxInt64(&m.buildMaxNs, buildNs)
		}
		task.installAt = time.Now()
	}

	released := stage == StageRelease || err != nil
	if released {
		if !task.installAt.IsZero() {
			releaseNs := time.Since(task.installAt).Nanoseconds()
			m.releaseSumNs.Add(releaseNs)
			m.releaseCount.Add(1)
			m.releaseLastNs.Store(releaseNs)
			updateMaxInt64(&m.releaseMaxNs, releaseNs)
		}
		delete(m.active, taskID)
		m.activeCt.Add(-1)
		m.pending.Add(-1)
		m.completed.Add(1)
		task.Stage = StageRelease
	}
	return err
}

func (m *Manager) Stats() Metrics {
	return Metrics{
		Pending:       m.pending.Load(),
		Queue:         m.queueLen.Load(),
		Active:        m.activeCt.Load(),
		WaitNs:        m.waitSumNs.Load(),
		WaitCount:     m.waitCount.Load(),
		WaitLastNs:    m.waitLastNs.Load(),
		WaitMaxNs:     m.waitMaxNs.Load(),
		BuildNs:       m.buildSumNs.Load(),
		BuildCount:    m.buildCount.Load(),
		BuildLastNs:   m.buildLastNs.Load(),
		BuildMaxNs:    m.buildMaxNs.Load(),
		ReleaseNs:     m.releaseSumNs.Load(),
		ReleaseCount:  m.releaseCount.Load(),
		ReleaseLastNs: m.releaseLastNs.Load(),
		ReleaseMaxNs:  m.releaseMaxNs.Load(),
		Completed:     m.completed.Load(),
	}
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.cond.Broadcast()
	return nil
}

func updateMaxInt64(target *atomic.Int64, val int64) {
	for {
		current := target.Load()
		if val <= current {
			return
		}
		if target.CompareAndSwap(current, val) {
			return
		}
	}
}
