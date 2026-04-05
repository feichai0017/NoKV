package replicated

import (
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
)

// substrateAdapter owns the rooted virtual-log view that sits beneath the
// replicated protocol driver. Callers must hold the enclosing driver mutex
// before invoking the mutating helpers.
type substrateAdapter struct {
	storage  rootstorage.Substrate
	notifyCh chan struct{}
	latest   rootstorage.TailToken
}

func newSubstrateAdapter(storage rootstorage.Substrate) (*substrateAdapter, error) {
	adapter := &substrateAdapter{
		storage:  storage,
		notifyCh: make(chan struct{}, 1),
	}
	if err := adapter.bootstrap(); err != nil {
		return nil, err
	}
	return adapter, nil
}

func (a *substrateAdapter) bootstrap() error {
	if a == nil || a.storage == nil {
		return nil
	}
	observed, err := rootstorage.ObserveCommitted(a.storage, 0)
	if err != nil {
		return err
	}
	a.latest.Cursor = observed.LastCursor()
	if a.latest.Cursor != (rootstate.Cursor{}) || observed.Checkpoint.TailOffset != 0 || len(observed.Tail.Records) > 0 {
		a.latest.Revision = 1
	}
	return nil
}

func (a *substrateAdapter) watchChannel() <-chan struct{} {
	if a == nil {
		return nil
	}
	return a.notifyCh
}

func (a *substrateAdapter) observeLocked(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	observed, err := rootstorage.ObserveCommitted(a.storage, 0)
	if err != nil {
		return rootstorage.TailAdvance{}, err
	}
	a.latest.Cursor = observed.LastCursor()
	return observed.Advance(after, a.latest), nil
}

func (a *substrateAdapter) closedAdvance(after rootstorage.TailToken) rootstorage.TailAdvance {
	if a == nil {
		return rootstorage.ObservedCommitted{}.Advance(after, rootstorage.TailToken{})
	}
	return rootstorage.ObservedCommitted{}.Advance(after, a.latest)
}

func (a *substrateAdapter) installBootstrapLocked(observed rootstorage.ObservedCommitted) error {
	if a == nil {
		return nil
	}
	if err := a.storage.InstallBootstrap(observed); err != nil {
		return err
	}
	a.bumpLocked(observed.LastCursor())
	a.signalLocked()
	return nil
}

func (a *substrateAdapter) appendCommittedLocked(records []rootstorage.CommittedEvent) error {
	if a == nil || len(records) == 0 {
		return nil
	}
	if _, err := a.storage.AppendCommitted(records...); err != nil {
		return err
	}
	a.bumpLocked(records[len(records)-1].Cursor)
	a.signalLocked()
	return nil
}

func (a *substrateAdapter) loadCheckpoint() (rootstorage.Checkpoint, error) {
	return a.storage.LoadCheckpoint()
}

func (a *substrateAdapter) saveCheckpointLocked(checkpoint rootstorage.Checkpoint) error {
	if err := a.storage.SaveCheckpoint(checkpoint); err != nil {
		return err
	}
	observed, err := rootstorage.ObserveCommitted(a.storage, 0)
	if err != nil {
		return err
	}
	a.bumpLocked(observed.LastCursor())
	a.signalLocked()
	return nil
}

func (a *substrateAdapter) readCommitted(offset int64) (rootstorage.CommittedTail, error) {
	return a.storage.ReadCommitted(offset)
}

func (a *substrateAdapter) compactCommittedLocked(stream rootstorage.CommittedTail) error {
	if err := a.storage.CompactCommitted(stream); err != nil {
		return err
	}
	a.bumpLocked(stream.TailCursor(a.latest.Cursor))
	a.signalLocked()
	return nil
}

func (a *substrateAdapter) size() (int64, error) {
	return a.storage.Size()
}

func (a *substrateAdapter) bumpLocked(cursor rootstate.Cursor) {
	a.latest.Cursor = cursor
	a.latest.Revision++
}

func (a *substrateAdapter) signalLocked() {
	select {
	case a.notifyCh <- struct{}{}:
	default:
	}
}
