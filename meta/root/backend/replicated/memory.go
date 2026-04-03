package replicated

import (
	"sync"

	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
)

// DriverState is one detached view of the memory driver's current bootstrap
// boundary and retained committed tail.
type DriverState struct {
	Checkpoint rootstorage.Checkpoint
	Records    []rootstorage.CommittedEvent
}

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
	return ConfigFromDriver(d, maxRetainedRecords)
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

func (d *MemoryDriver) Log() rootstorage.EventLog { return memoryEventLog{driver: d} }

func (d *MemoryDriver) CheckpointStore() rootstorage.CheckpointStore {
	return memoryCheckpointStore{driver: d}
}

func (d *MemoryDriver) BootstrapInstaller() rootstorage.BootstrapInstaller { return d }

func (d *MemoryDriver) IsLeader() bool { return true }

func (d *MemoryDriver) LeaderID() uint64 { return 1 }

// State returns one detached view of the driver's current checkpoint and
// retained committed tail.
func (d *MemoryDriver) State() DriverState {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return DriverState{
		Checkpoint: rootstorage.CloneCheckpoint(d.checkpoint),
		Records:    rootstorage.CloneCommittedEvents(d.records),
	}
}

type memoryEventLog struct{ driver *MemoryDriver }

func (l memoryEventLog) Load(offset int64) ([]rootstorage.CommittedEvent, error) {
	return l.driver.loadRecords(offset), nil
}

func (l memoryEventLog) Append(records ...rootstorage.CommittedEvent) (int64, error) {
	return l.driver.appendCommitted(records), nil
}

func (l memoryEventLog) Compact(records []rootstorage.CommittedEvent) error {
	l.driver.compactTail(records)
	return nil
}

func (l memoryEventLog) Size() (int64, error) {
	return l.driver.logSize(), nil
}

type memoryCheckpointStore struct{ driver *MemoryDriver }

func (s memoryCheckpointStore) Load() (rootstorage.Checkpoint, error) {
	return s.driver.loadCheckpoint(), nil
}

func (s memoryCheckpointStore) Save(checkpoint rootstorage.Checkpoint) error {
	s.driver.saveCheckpoint(checkpoint)
	return nil
}

// InstallBootstrap replaces the in-memory checkpoint and retained committed
// tail. It is the single-process reference implementation of snapshot install.
func (d *MemoryDriver) InstallBootstrap(checkpoint rootstorage.Checkpoint, records []rootstorage.CommittedEvent) error {
	d.installBootstrap(checkpoint, records)
	return nil
}

func (d *MemoryDriver) loadCheckpoint() rootstorage.Checkpoint {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return rootstorage.CloneCheckpoint(d.checkpoint)
}

func (d *MemoryDriver) saveCheckpoint(checkpoint rootstorage.Checkpoint) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.checkpoint = rootstorage.CloneCheckpoint(checkpoint)
}

func (d *MemoryDriver) loadRecords(offset int64) []rootstorage.CommittedEvent {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if offset <= 0 || int(offset) > len(d.records) {
		return rootstorage.CloneCommittedEvents(d.records)
	}
	return rootstorage.CloneCommittedEvents(d.records[int(offset):])
}

func (d *MemoryDriver) appendCommitted(records []rootstorage.CommittedEvent) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.records = append(d.records, rootstorage.CloneCommittedEvents(records)...)
	return int64(len(d.records))
}

func (d *MemoryDriver) compactTail(records []rootstorage.CommittedEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.records = rootstorage.CloneCommittedEvents(records)
}

func (d *MemoryDriver) installBootstrap(checkpoint rootstorage.Checkpoint, records []rootstorage.CommittedEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.checkpoint = rootstorage.CloneCheckpoint(checkpoint)
	d.records = rootstorage.CloneCommittedEvents(records)
}

func (d *MemoryDriver) logSize() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return int64(len(d.records))
}
