package replicated

import (
	"fmt"
	"time"

	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	myraft "github.com/feichai0017/NoKV/raft"
)

func (d *NetworkDriver) ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.adapter.observeLocked(after)
}

func (d *NetworkDriver) TailNotify() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.adapter.watchChannel()
}

func (d *NetworkDriver) WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error) {
	if timeout <= 0 {
		timeout = 200 * time.Millisecond
	}
	advance, err := d.ObserveTail(after)
	if err != nil {
		return rootstorage.TailAdvance{}, err
	}
	if advance.Advanced() {
		return advance, nil
	}
	notify := d.TailNotify()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-d.stopCh:
		d.mu.Lock()
		defer d.mu.Unlock()
		advance, tailErr := d.adapter.observeLocked(after)
		if tailErr != nil {
			return d.adapter.closedAdvance(after), fmt.Errorf("meta/root/backend/replicated: network driver is closed")
		}
		return advance, fmt.Errorf("meta/root/backend/replicated: network driver is closed")
	case <-notify:
		return d.ObserveTail(after)
	case <-timer.C:
		return d.ObserveTail(after)
	}
}

func (d *NetworkDriver) InstallBootstrap(observed rootstorage.ObservedCommitted) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.adapter.installBootstrapLocked(observed)
}

func (d *NetworkDriver) LoadCheckpoint() (rootstorage.Checkpoint, error) {
	return d.adapter.loadCheckpoint()
}

func (d *NetworkDriver) SaveCheckpoint(checkpoint rootstorage.Checkpoint) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.adapter.saveCheckpointLocked(checkpoint)
}

func (d *NetworkDriver) ReadCommitted(offset int64) (rootstorage.CommittedTail, error) {
	return d.adapter.readCommitted(offset)
}

func (d *NetworkDriver) AppendCommitted(records ...rootstorage.CommittedEvent) (int64, error) {
	d.mu.Lock()
	if d.node == nil {
		d.mu.Unlock()
		return 0, fmt.Errorf("meta/root/backend/replicated: network driver is closed")
	}
	if d.node.raw.Status().RaftState != myraft.StateLeader {
		d.mu.Unlock()
		return 0, fmt.Errorf("meta/root/backend/replicated: node %d is not leader", d.id)
	}
	for _, rec := range records {
		payload, err := marshalCommittedEvent(rec)
		if err != nil {
			d.mu.Unlock()
			return 0, err
		}
		if err := d.node.raw.Propose(payload); err != nil {
			d.mu.Unlock()
			return 0, err
		}
		_, outbound, err := d.drainLocked()
		if err != nil {
			d.mu.Unlock()
			return 0, err
		}
		d.mu.Unlock()
		if err := d.sendMessages(outbound); err != nil {
			return 0, err
		}
		d.mu.Lock()
	}
	size, err := d.adapter.size()
	d.mu.Unlock()
	return size, err
}

func (d *NetworkDriver) CompactCommitted(stream rootstorage.CommittedTail) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.adapter.compactCommittedLocked(stream)
}

func (d *NetworkDriver) Size() (int64, error) {
	return d.adapter.size()
}

