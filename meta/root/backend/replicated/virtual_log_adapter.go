package replicated

import (
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"sync"
)

// virtualLogAdapter owns the rooted virtual-log view that sits beneath the
// replicated protocol driver. Callers must hold the enclosing driver mutex
// before invoking the mutating helpers.
type virtualLogAdapter struct {
	mu       sync.Mutex
	log      rootstorage.VirtualLog
	notifyCh chan struct{}
	latest   rootstorage.TailToken
}

func newVirtualLogAdapter(log rootstorage.VirtualLog) (*virtualLogAdapter, error) {
	adapter := &virtualLogAdapter{
		log:      log,
		notifyCh: make(chan struct{}, 1),
	}
	if err := adapter.bootstrap(); err != nil {
		return nil, err
	}
	return adapter, nil
}

func (a *virtualLogAdapter) bootstrap() error {
	if a == nil || a.log == nil {
		return nil
	}
	observed, err := rootstorage.ObserveCommitted(a.log, 0)
	if err != nil {
		return err
	}
	a.latest.Cursor = observed.LastCursor()
	if a.latest.Cursor != (rootstate.Cursor{}) || observed.Checkpoint.TailOffset != 0 || len(observed.Tail.Records) > 0 {
		a.latest.Revision = 1
	}
	return nil
}

func (a *virtualLogAdapter) watchChannel() <-chan struct{} {
	if a == nil {
		return nil
	}
	return a.notifyCh
}

func (a *virtualLogAdapter) observe(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	observed, err := rootstorage.ObserveCommitted(a.log, 0)
	if err != nil {
		return rootstorage.TailAdvance{}, err
	}
	a.latest.Cursor = observed.LastCursor()
	return observed.Advance(after, a.latest), nil
}

func (a *virtualLogAdapter) closedAdvance(after rootstorage.TailToken) rootstorage.TailAdvance {
	if a == nil {
		return rootstorage.ObservedCommitted{}.Advance(after, rootstorage.TailToken{})
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return rootstorage.ObservedCommitted{}.Advance(after, a.latest)
}

func (a *virtualLogAdapter) installBootstrap(observed rootstorage.ObservedCommitted) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.log.InstallBootstrap(observed); err != nil {
		return err
	}
	a.bump(observed.LastCursor())
	a.signal()
	return nil
}

func (a *virtualLogAdapter) appendCommitted(records []rootstorage.CommittedEvent) error {
	if a == nil || len(records) == 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.log.AppendCommitted(records...); err != nil {
		return err
	}
	a.bump(records[len(records)-1].Cursor)
	a.signal()
	return nil
}

func (a *virtualLogAdapter) loadCheckpoint() (rootstorage.Checkpoint, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.log.LoadCheckpoint()
}

func (a *virtualLogAdapter) saveCheckpoint(checkpoint rootstorage.Checkpoint) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.log.SaveCheckpoint(checkpoint); err != nil {
		return err
	}
	observed, err := rootstorage.ObserveCommitted(a.log, 0)
	if err != nil {
		return err
	}
	a.bump(observed.LastCursor())
	a.signal()
	return nil
}

func (a *virtualLogAdapter) readCommitted(offset int64) (rootstorage.CommittedTail, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.log.ReadCommitted(offset)
}

func (a *virtualLogAdapter) compactCommitted(stream rootstorage.CommittedTail) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.log.CompactCommitted(stream); err != nil {
		return err
	}
	a.bump(stream.TailCursor(a.latest.Cursor))
	a.signal()
	return nil
}

func (a *virtualLogAdapter) size() (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.log.Size()
}

func (a *virtualLogAdapter) bump(cursor rootstate.Cursor) {
	a.latest.Cursor = cursor
	a.latest.Revision++
}

func (a *virtualLogAdapter) signal() {
	select {
	case a.notifyCh <- struct{}{}:
	default:
	}
}
