package replicated

import (
	"sync"

	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
)

// MemoryDriver is one in-memory committed-log plus checkpoint carrier for the
// replicated root backend. It is a single-process prototype driver used to
// validate backend lifecycle before wiring a real consensus log underneath.
type MemoryDriver struct {
	mu         sync.RWMutex
	checkpoint rootstorage.Checkpoint
	records    []rootstorage.CommittedEvent
}

// NewMemoryDriver returns one empty in-memory replicated-root driver.
func NewMemoryDriver() *MemoryDriver {
	return &MemoryDriver{}
}

// Config returns one backend config wired to this driver's log and checkpoint
// views.
func (d *MemoryDriver) Config(maxRetainedRecords int) Config {
	return Config{
		Log:                memoryEventLog{driver: d},
		Checkpoint:         memoryCheckpointStore{driver: d},
		MaxRetainedRecords: maxRetainedRecords,
	}
}

// OpenMemory opens one replicated root store backed by an in-memory committed
// log and checkpoint driver.
func OpenMemory(maxRetainedRecords int) (*Store, *MemoryDriver, error) {
	driver := NewMemoryDriver()
	store, err := Open(driver.Config(maxRetainedRecords))
	if err != nil {
		return nil, nil, err
	}
	return store, driver, nil
}

type memoryEventLog struct{ driver *MemoryDriver }

func (l memoryEventLog) Load(offset int64) ([]rootstorage.CommittedEvent, error) {
	l.driver.mu.RLock()
	defer l.driver.mu.RUnlock()
	if offset <= 0 || int(offset) > len(l.driver.records) {
		return rootstorage.CloneCommittedEvents(l.driver.records), nil
	}
	return rootstorage.CloneCommittedEvents(l.driver.records[int(offset):]), nil
}

func (l memoryEventLog) Append(records ...rootstorage.CommittedEvent) (int64, error) {
	l.driver.mu.Lock()
	defer l.driver.mu.Unlock()
	l.driver.records = append(l.driver.records, rootstorage.CloneCommittedEvents(records)...)
	return int64(len(l.driver.records)), nil
}

func (l memoryEventLog) Compact(records []rootstorage.CommittedEvent) error {
	l.driver.mu.Lock()
	defer l.driver.mu.Unlock()
	l.driver.records = rootstorage.CloneCommittedEvents(records)
	return nil
}

func (l memoryEventLog) Size() (int64, error) {
	l.driver.mu.RLock()
	defer l.driver.mu.RUnlock()
	return int64(len(l.driver.records)), nil
}

type memoryCheckpointStore struct{ driver *MemoryDriver }

func (s memoryCheckpointStore) Load() (rootstorage.Checkpoint, error) {
	s.driver.mu.RLock()
	defer s.driver.mu.RUnlock()
	return rootstorage.CloneCheckpoint(s.driver.checkpoint), nil
}

func (s memoryCheckpointStore) Save(checkpoint rootstorage.Checkpoint) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	s.driver.checkpoint = rootstorage.CloneCheckpoint(checkpoint)
	return nil
}
