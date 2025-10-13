package flush

import (
	"errors"
	"sync"
	"sync/atomic"
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
	ID        uint64
	SegmentID uint32
	Stage     Stage
	Data      any
	Err       error
}

type Metrics struct {
	Pending int64
	Active  int64
	Stage   Stage
}

// Manager coordinates flush tasks.
type Manager struct {
	mu      sync.Mutex
	cond    *sync.Cond
	closed  bool
	counter uint64

	queue   []*Task
	active  map[uint64]*Task
	pending int64
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
	m.queue = append(m.queue, task)
	atomic.AddInt64(&m.pending, 1)
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
	task.Stage = StageBuild
	m.active[task.ID] = task
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

	if stage == StageRelease || err != nil {
		delete(m.active, taskID)
		atomic.AddInt64(&m.pending, -1)
		task.Stage = StageRelease
	}
	return err
}

func (m *Manager) Stats() Metrics {
	return Metrics{
		Pending: atomic.LoadInt64(&m.pending),
	}
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.cond.Broadcast()
	return nil
}
