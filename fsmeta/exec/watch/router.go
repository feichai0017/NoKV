package watch

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
)

const defaultWindow uint32 = 256
const defaultRecentEvents = 4096

// Router fans committed raftstore key events out to fsmeta prefix subscribers.
type Router struct {
	mu         sync.RWMutex
	next       uint64
	subs       map[uint64]*Subscription
	recent     map[eventKey]struct{}
	recentRing [defaultRecentEvents]eventKey
	recentNext int
	recentLen  int

	published atomic.Uint64
	delivered atomic.Uint64
	dropped   atomic.Uint64
	overflow  atomic.Uint64
}

// NewRouter constructs an empty watch router.
func NewRouter() *Router {
	return &Router{
		subs:   make(map[uint64]*Subscription),
		recent: make(map[eventKey]struct{}),
	}
}

// Subscribe registers one prefix watch.
func (r *Router) Subscribe(ctx context.Context, req fsmeta.WatchRequest) (fsmeta.WatchSubscription, error) {
	if r == nil {
		return nil, fsmeta.ErrInvalidRequest
	}
	prefix, err := fsmeta.WatchPrefix(req)
	if err != nil {
		return nil, err
	}
	window := req.BackPressureWindow
	if window == 0 {
		window = defaultWindow
	}
	sub := &Subscription{
		router:  r,
		prefix:  prefix,
		events:  make(chan fsmeta.WatchEvent, window),
		window:  window,
		pending: make(map[fsmeta.WatchCursor]uint32),
	}
	r.mu.Lock()
	r.next++
	sub.id = r.next
	r.subs[sub.id] = sub
	r.mu.Unlock()
	if ctx != nil {
		go func() {
			<-ctx.Done()
			sub.Close()
		}()
	}
	return sub, nil
}

// Publish fans one committed key event out to matching subscribers.
func (r *Router) Publish(evt fsmeta.WatchEvent) {
	if r == nil || len(evt.Key) == 0 {
		return
	}
	id := eventID(evt)
	r.mu.Lock()
	if _, ok := r.recent[id]; ok {
		r.mu.Unlock()
		return
	}
	r.rememberLocked(id)
	subs := make([]*Subscription, 0, len(r.subs))
	for _, sub := range r.subs {
		subs = append(subs, sub)
	}
	r.mu.Unlock()
	r.published.Add(1)
	for _, sub := range subs {
		if bytes.HasPrefix(evt.Key, sub.prefix) {
			sub.enqueue(evt)
		}
	}
}

// OnApply implements raftstore/store.ApplyObserver.
func (r *Router) OnApply(evt storepkg.ApplyEvent) {
	source := fsmeta.WatchEventSource(0)
	switch evt.Source {
	case storepkg.ApplyEventSourceCommit:
		source = fsmeta.WatchEventSourceCommit
	case storepkg.ApplyEventSourceResolveLock:
		source = fsmeta.WatchEventSourceResolveLock
	default:
		return
	}
	for _, key := range evt.Keys {
		r.Publish(fsmeta.WatchEvent{
			Cursor: fsmeta.WatchCursor{
				RegionID: evt.RegionID,
				Term:     evt.Term,
				Index:    evt.Index,
			},
			CommitVersion: evt.CommitVersion,
			Source:        source,
			Key:           key,
		})
	}
}

// Dropped returns how many matching events were dropped due to slow subscribers.
func (r *Router) Dropped() uint64 {
	if r == nil {
		return 0
	}
	return r.dropped.Load()
}

// Stats returns a point-in-time router metrics snapshot.
func (r *Router) Stats() map[string]any {
	if r == nil {
		return map[string]any{
			"subscribers":     0,
			"events_total":    uint64(0),
			"delivered_total": uint64(0),
			"dropped_total":   uint64(0),
			"overflow_total":  uint64(0),
		}
	}
	r.mu.RLock()
	subscribers := len(r.subs)
	recent := r.recentLen
	r.mu.RUnlock()
	return map[string]any{
		"subscribers":     subscribers,
		"recent_events":   recent,
		"events_total":    r.published.Load(),
		"delivered_total": r.delivered.Load(),
		"dropped_total":   r.dropped.Load(),
		"overflow_total":  r.overflow.Load(),
	}
}

func (r *Router) rememberLocked(id eventKey) {
	r.recent[id] = struct{}{}
	if r.recentLen < defaultRecentEvents {
		r.recentRing[r.recentNext] = id
		r.recentNext = (r.recentNext + 1) % defaultRecentEvents
		r.recentLen++
		return
	}
	old := r.recentRing[r.recentNext]
	delete(r.recent, old)
	r.recentRing[r.recentNext] = id
	r.recentNext = (r.recentNext + 1) % defaultRecentEvents
}

func (r *Router) unregister(id uint64, sub *Subscription) {
	r.mu.Lock()
	if current := r.subs[id]; current == sub {
		delete(r.subs, id)
	}
	r.mu.Unlock()
}

// Subscription is one router subscriber.
type Subscription struct {
	router *Router
	id     uint64
	prefix []byte
	events chan fsmeta.WatchEvent
	window uint32

	mu          sync.Mutex
	outstanding uint32
	pending     map[fsmeta.WatchCursor]uint32
	closed      bool
	err         error
}

// Events returns the event stream.
func (s *Subscription) Events() <-chan fsmeta.WatchEvent {
	if s == nil {
		return nil
	}
	return s.events
}

// Ack releases outstanding event budget up to cursor within the same region.
func (s *Subscription) Ack(cursor fsmeta.WatchCursor) {
	if s == nil {
		return
	}
	s.mu.Lock()
	var released uint32
	for pending, count := range s.pending {
		if cursorCovers(pending, cursor) {
			released += count
			delete(s.pending, pending)
		}
	}
	if released > s.outstanding {
		s.outstanding = 0
	} else {
		s.outstanding -= released
	}
	s.mu.Unlock()
}

// Err returns the terminal subscription error, if any.
func (s *Subscription) Err() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close unregisters the subscription.
func (s *Subscription) Close() {
	s.closeWith(nil)
}

func (s *Subscription) enqueue(evt fsmeta.WatchEvent) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if s.outstanding >= s.window {
		s.mu.Unlock()
		s.closeWith(fsmeta.ErrWatchOverflow)
		if s.router != nil {
			s.router.dropped.Add(1)
			s.router.overflow.Add(1)
		}
		return
	}
	s.outstanding++
	s.pending[evt.Cursor]++
	s.mu.Unlock()

	select {
	case s.events <- cloneEvent(evt):
		if s.router != nil {
			s.router.delivered.Add(1)
		}
	default:
		s.closeWith(fsmeta.ErrWatchOverflow)
		if s.router != nil {
			s.router.dropped.Add(1)
			s.router.overflow.Add(1)
		}
	}
}

func (s *Subscription) closeWith(err error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.err = err
	close(s.events)
	id := s.id
	router := s.router
	s.mu.Unlock()
	if router != nil {
		router.unregister(id, s)
	}
}

func cloneEvent(evt fsmeta.WatchEvent) fsmeta.WatchEvent {
	evt.Key = append([]byte(nil), evt.Key...)
	return evt
}

type eventKey struct {
	regionID      uint64
	term          uint64
	index         uint64
	source        fsmeta.WatchEventSource
	commitVersion uint64
	key           string
}

func eventID(evt fsmeta.WatchEvent) eventKey {
	return eventKey{
		regionID:      evt.Cursor.RegionID,
		term:          evt.Cursor.Term,
		index:         evt.Cursor.Index,
		source:        evt.Source,
		commitVersion: evt.CommitVersion,
		key:           string(evt.Key),
	}
}

func cursorCovers(pending, ack fsmeta.WatchCursor) bool {
	if pending.RegionID != ack.RegionID || ack.RegionID == 0 {
		return false
	}
	if pending.Term < ack.Term {
		return true
	}
	return pending.Term == ack.Term && pending.Index <= ack.Index
}
