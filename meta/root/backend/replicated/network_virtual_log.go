package replicated

import (
	"time"

	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	myraft "github.com/feichai0017/NoKV/raft"
)

func (d *NetworkDriver) ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	return d.adapter.observe(after)
}

func (d *NetworkDriver) TailNotify() <-chan struct{} {
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
		advance, tailErr := d.adapter.observe(after)
		if tailErr != nil {
			return d.adapter.closedAdvance(after), errNetworkDriverClosed
		}
		return advance, errNetworkDriverClosed
	case <-notify:
		return d.ObserveTail(after)
	case <-timer.C:
		return d.ObserveTail(after)
	}
}

func (d *NetworkDriver) InstallBootstrap(observed rootstorage.ObservedCommitted) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.adapter.installBootstrap(observed)
}

func (d *NetworkDriver) LoadCheckpoint() (rootstorage.Checkpoint, error) {
	return d.adapter.loadCheckpoint()
}

func (d *NetworkDriver) SaveCheckpoint(checkpoint rootstorage.Checkpoint) error {
	return d.adapter.saveCheckpoint(checkpoint)
}

func (d *NetworkDriver) ReadCommitted(offset int64) (rootstorage.CommittedTail, error) {
	return d.adapter.readCommitted(offset)
}

func (d *NetworkDriver) AppendCommitted(records ...rootstorage.CommittedEvent) (int64, error) {
	if len(records) == 0 {
		return d.Size()
	}
	before, err := d.ObserveTail(rootstorage.TailToken{})
	if err != nil {
		return 0, err
	}
	target := records[len(records)-1].Cursor
	d.mu.Lock()
	if d.node == nil {
		d.mu.Unlock()
		return 0, errNetworkDriverClosed
	}
	if d.node.raw.Status().RaftState != myraft.StateLeader {
		d.mu.Unlock()
		return 0, errNodeNotLeader(d.id)
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
	d.mu.Unlock()
	if err := d.waitForCommittedCursor(before.Token, target, d.appendWaitTimeout); err != nil {
		return 0, err
	}
	return d.adapter.size()
}

func (d *NetworkDriver) waitForCommittedCursor(after rootstorage.TailToken, target rootstate.Cursor, timeout time.Duration) error {
	if target == (rootstate.Cursor{}) {
		return nil
	}
	if timeout <= 0 {
		timeout = defaultAppendWaitTimeout
	}
	advance, err := d.ObserveTail(after)
	if err != nil {
		return err
	}
	if committedCursorReached(advance.LastCursor(), target) {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return errAppendWaitTimedOut(target)
		}
		wait := min(remaining, 200*time.Millisecond)
		advance, err := d.WaitForTail(after, wait)
		if err != nil {
			return err
		}
		if committedCursorReached(advance.LastCursor(), target) {
			return nil
		}
		if advance.Advanced() {
			after = advance.Token
		}
	}
}

func committedCursorReached(current, target rootstate.Cursor) bool {
	return current == target || rootstate.CursorAfter(current, target)
}

func (d *NetworkDriver) CompactCommitted(stream rootstorage.CommittedTail) error {
	return d.adapter.compactCommitted(stream)
}

func (d *NetworkDriver) Size() (int64, error) {
	return d.adapter.size()
}
