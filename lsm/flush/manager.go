package flush

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
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

type Metrics struct {
	Pending      int64
	Queue        int64
	Active       int64
	WaitNs       int64
	WaitCount    int64
	BuildNs      int64
	BuildCount   int64
	ReleaseNs    int64
	ReleaseCount int64
	Completed    int64
}

// Manager coordinates flush tasks.
type Manager struct {
	mu      sync.Mutex
	cond    *sync.Cond
	closed  bool
	counter uint64

	queue        []*Task
	active       map[uint64]*Task
	pending      int64
	queueLen     int64
	activeCt     int64
	waitSumNs    int64
	waitCount    int64
	buildSumNs   int64
	buildCount   int64
	releaseSumNs int64
	releaseCount int64
	completed    int64
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
		atomic.AddInt64(&m.waitSumNs, time.Since(task.queuedAt).Nanoseconds())
		atomic.AddInt64(&m.waitCount, 1)
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
			atomic.AddInt64(&m.buildSumNs, time.Since(task.buildStart).Nanoseconds())
			atomic.AddInt64(&m.buildCount, 1)
		}
		task.installAt = time.Now()
	}

	released := stage == StageRelease || err != nil
	if released {
		if !task.installAt.IsZero() {
			atomic.AddInt64(&m.releaseSumNs, time.Since(task.installAt).Nanoseconds())
			atomic.AddInt64(&m.releaseCount, 1)
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
		Pending:      atomic.LoadInt64(&m.pending),
		Queue:        atomic.LoadInt64(&m.queueLen),
		Active:       atomic.LoadInt64(&m.activeCt),
		WaitNs:       atomic.LoadInt64(&m.waitSumNs),
		WaitCount:    atomic.LoadInt64(&m.waitCount),
		BuildNs:      atomic.LoadInt64(&m.buildSumNs),
		BuildCount:   atomic.LoadInt64(&m.buildCount),
		ReleaseNs:    atomic.LoadInt64(&m.releaseSumNs),
		ReleaseCount: atomic.LoadInt64(&m.releaseCount),
		Completed:    atomic.LoadInt64(&m.completed),
	}
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.cond.Broadcast()
	return nil
}
