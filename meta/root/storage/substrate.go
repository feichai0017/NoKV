package storage

import (
	"context"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"sync"
	"time"
)

// Checkpoint is one compact rooted snapshot plus the retained-log offset to
// continue bootstrap replay from.
type Checkpoint struct {
	Snapshot   rootstate.Snapshot
	TailOffset int64
}

// CloneCheckpoint returns a detached rooted checkpoint.
func CloneCheckpoint(in Checkpoint) Checkpoint {
	return Checkpoint{
		Snapshot:   rootstate.CloneSnapshot(in.Snapshot),
		TailOffset: in.TailOffset,
	}
}

// CommittedEvent is one rooted metadata event paired with its committed cursor.
type CommittedEvent struct {
	Cursor rootstate.Cursor
	Event  rootevent.Event
}

// CloneCommittedEvents returns a detached committed-event slice.
func CloneCommittedEvents(in []CommittedEvent) []CommittedEvent {
	if len(in) == 0 {
		return nil
	}
	out := make([]CommittedEvent, 0, len(in))
	for _, rec := range in {
		out = append(out, CommittedEvent{
			Cursor: rec.Cursor,
			Event:  rootevent.CloneEvent(rec.Event),
		})
	}
	return out
}

// CommittedTail is one retained committed rooted tail returned by one read.
type CommittedTail struct {
	RequestedOffset int64
	StartOffset     int64
	EndOffset       int64
	Records         []CommittedEvent
}

// TailWindow describes one retained committed-tail window observed from the
// virtual log substrate.
type TailWindow struct {
	RequestedOffset int64
	StartOffset     int64
	EndOffset       int64
}

// TailCompactionPlan is one retention/compaction decision derived from the
// current committed rooted tail.
type TailCompactionPlan struct {
	Tail       CommittedTail
	RetainFrom rootstate.Cursor
	Compacted  bool
}

// TailToken identifies one observed committed-tail view.
type TailToken struct {
	Cursor   rootstate.Cursor
	Revision uint64
}

// TailWaitFunc is one virtual-log wait primitive used to drive one follower
// subscription from one previously acknowledged tail token.
type TailWaitFunc func(after TailToken, timeout time.Duration) (TailAdvance, error)

// TailObserveFunc reads the current rooted tail view relative to one token
// without blocking for new commits.
type TailObserveFunc func(after TailToken) (TailAdvance, error)

// TailAdvanceKind classifies one observed committed-tail change.
type TailAdvanceKind uint8

const (
	TailAdvanceUnchanged TailAdvanceKind = iota
	TailAdvanceCursorAdvanced
	TailAdvanceWindowShifted
)

// TailCatchUpAction classifies what a follower/view consumer should do with one
// observed tail advance.
type TailCatchUpAction uint8

const (
	TailCatchUpIdle TailCatchUpAction = iota
	TailCatchUpRefreshState
	TailCatchUpAcknowledgeWindow
	TailCatchUpInstallBootstrap
)

// AdvancedSince reports whether the observed tail view changed since prev.
func (t TailToken) AdvancedSince(prev TailToken) bool {
	return t.Revision > prev.Revision || rootstate.CursorAfter(t.Cursor, prev.Cursor)
}

// TailAdvance is one observed committed-tail read paired with its change token.
type TailAdvance struct {
	After    TailToken
	Token    TailToken
	Observed ObservedCommitted
}

// TailSubscription is one watch-like follower subscription over the rooted
// virtual log. Callers wait from one acknowledged token and explicitly advance
// the subscription only after they have consumed the returned view.
type TailSubscription struct {
	mu      sync.Mutex
	token   TailToken
	wait    TailWaitFunc
	observe TailObserveFunc
	watch   <-chan struct{}
}

// NewTailSubscription constructs one watch-like rooted tail subscription.
func NewTailSubscription(after TailToken, wait TailWaitFunc) *TailSubscription {
	if wait == nil {
		return nil
	}
	return &TailSubscription{token: after, wait: wait}
}

// NewWatchedTailSubscription constructs one watch-first rooted tail
// subscription. It first re-observes the current tail and then blocks on watch
// notifications, falling back to wait when needed.
func NewWatchedTailSubscription(after TailToken, observe TailObserveFunc, watch <-chan struct{}, wait TailWaitFunc) *TailSubscription {
	if observe == nil && wait == nil {
		return nil
	}
	return &TailSubscription{
		token:   after,
		wait:    wait,
		observe: observe,
		watch:   watch,
	}
}

// Token returns the last acknowledged tail token for this subscription.
func (s *TailSubscription) Token() TailToken {
	if s == nil {
		return TailToken{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token
}

// Wait blocks until the rooted tail changes past the current acknowledged
// token, or until timeout elapses.
func (s *TailSubscription) Wait(timeout time.Duration) (TailAdvance, error) {
	if s == nil || s.wait == nil {
		if s == nil || s.observe == nil {
			return TailAdvance{}, nil
		}
		return s.observe(s.Token())
	}
	return s.wait(s.Token(), timeout)
}

// Next waits for the next rooted tail advance using watch notifications when
// available and falling back to the wait primitive otherwise.
func (s *TailSubscription) Next(ctx context.Context, fallback time.Duration) (TailAdvance, error) {
	if s == nil {
		return TailAdvance{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s.observe == nil {
		return s.waitWithContext(ctx, fallback)
	}
	for {
		advance, err := s.observe(s.Token())
		if err != nil {
			return TailAdvance{}, err
		}
		if advance.Advanced() || s.watch == nil {
			if advance.Advanced() {
				return advance, nil
			}
			return s.waitWithContext(ctx, fallback)
		}
		select {
		case <-ctx.Done():
			return TailAdvance{}, ctx.Err()
		case <-s.watch:
		}
	}
}

// Acknowledge advances the subscription token after the caller has consumed
// one observed tail advance.
func (s *TailSubscription) Acknowledge(advance TailAdvance) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if advance.Token.AdvancedSince(s.token) {
		s.token = advance.Token
	}
}

func (s *TailSubscription) waitWithContext(ctx context.Context, fallback time.Duration) (TailAdvance, error) {
	if s == nil {
		return TailAdvance{}, nil
	}
	if s.wait == nil {
		if s.observe != nil {
			return s.observe(s.Token())
		}
		return TailAdvance{}, nil
	}
	if deadline, ok := ctx.Deadline(); ok {
		timeout := max(time.Until(deadline), 0)
		if fallback <= 0 || timeout < fallback {
			fallback = timeout
		}
	}
	if fallback <= 0 {
		fallback = 200 * time.Millisecond
	}
	return s.wait(s.Token(), fallback)
}

// ObservedCommitted is one compact checkpoint observed together with one
// retained committed tail view.
type ObservedCommitted struct {
	Checkpoint Checkpoint
	Tail       CommittedTail
}

// CloneObservedCommitted returns a detached observed committed view.
func CloneObservedCommitted(in ObservedCommitted) ObservedCommitted {
	return ObservedCommitted{
		Checkpoint: CloneCheckpoint(in.Checkpoint),
		Tail:       CloneCommittedTail(in.Tail),
	}
}

// CloneCommittedTail returns a detached committed-stream view.
func CloneCommittedTail(in CommittedTail) CommittedTail {
	return CommittedTail{
		RequestedOffset: in.RequestedOffset,
		StartOffset:     in.StartOffset,
		EndOffset:       in.EndOffset,
		Records:         CloneCommittedEvents(in.Records),
	}
}

// Installable normalizes one retained tail for bootstrap installation into a
// fresh substrate instance. Installation rewrites the retained stream as a new
// origin, so byte offsets are reset.
func (s CommittedTail) Installable() CommittedTail {
	out := CloneCommittedTail(s)
	out.RequestedOffset = 0
	out.StartOffset = 0
	out.EndOffset = 0
	return out
}

// Installable normalizes one checkpoint for bootstrap installation into a
// fresh substrate instance. Installation rewrites the retained stream as a new
// origin, so the next retained-tail offset resets to zero.
func (c Checkpoint) Installable() Checkpoint {
	out := CloneCheckpoint(c)
	out.TailOffset = 0
	return out
}

// Installable normalizes one observed view for bootstrap installation into a
// fresh substrate instance.
func (o ObservedCommitted) Installable() ObservedCommitted {
	out := CloneObservedCommitted(o)
	out.Checkpoint = out.Checkpoint.Installable()
	out.Tail = out.Tail.Installable()
	return out
}

// PlanTailCompaction trims the retained committed tail down to at most
// maxRetained records and reports the resulting retain-from cursor.
func PlanTailCompaction(records []CommittedEvent, lastCommitted rootstate.Cursor, maxRetained int) TailCompactionPlan {
	if maxRetained <= 0 || len(records) <= maxRetained {
		tail := CommittedTail{Records: CloneCommittedEvents(records)}
		return TailCompactionPlan{
			Tail:       tail,
			RetainFrom: tail.RetainFrom(lastCommitted),
		}
	}
	start := len(records) - maxRetained
	tail := CommittedTail{Records: CloneCommittedEvents(records[start:])}
	return TailCompactionPlan{
		Tail:       tail,
		RetainFrom: tail.RetainFrom(lastCommitted),
		Compacted:  true,
	}
}

// Observed packages one compaction decision into one installable observed view
// so callers can publish compaction through the same install contract used by
// bootstrap and recovery.
func (p TailCompactionPlan) Observed(snapshot rootstate.Snapshot) ObservedCommitted {
	return ObservedCommitted{
		Checkpoint: Checkpoint{Snapshot: rootstate.CloneSnapshot(snapshot), TailOffset: 0},
		Tail:       p.Tail.Installable(),
	}
}

// Window returns the retained committed-tail boundaries without record payloads.
func (s CommittedTail) Window() TailWindow {
	return TailWindow{
		RequestedOffset: s.RequestedOffset,
		StartOffset:     s.StartOffset,
		EndOffset:       s.EndOffset,
	}
}

// FellBehind reports whether the requested offset is already behind the
// current retained tail boundary.
func (s CommittedTail) FellBehind() bool {
	return s.Window().FellBehind()
}

// Empty reports whether the retained window currently contains no bytes.
func (w TailWindow) Empty() bool {
	return w.StartOffset >= w.EndOffset
}

// FellBehind reports whether the requested offset is already behind the
// current retained tail boundary.
func (w TailWindow) FellBehind() bool {
	return w.RequestedOffset < w.StartOffset
}

// RetainFrom returns the cursor immediately before the first retained event.
// When the stream is empty, fallback is returned unchanged.
func (s CommittedTail) RetainFrom(fallback rootstate.Cursor) rootstate.Cursor {
	if len(s.Records) == 0 {
		return fallback
	}
	first := s.Records[0].Cursor
	if first.Index <= 1 {
		return rootstate.Cursor{}
	}
	return rootstate.Cursor{Term: first.Term, Index: first.Index - 1}
}

// TailCursor returns the last committed cursor visible in this retained
// stream. When the stream is empty, fallback is returned unchanged.
func (s CommittedTail) TailCursor(fallback rootstate.Cursor) rootstate.Cursor {
	if len(s.Records) == 0 {
		return fallback
	}
	return s.Records[len(s.Records)-1].Cursor
}

// LastCursor returns the last committed cursor visible in the observed view.
func (o ObservedCommitted) LastCursor() rootstate.Cursor {
	return o.Tail.TailCursor(o.Checkpoint.Snapshot.State.LastCommitted)
}

// RetainFrom returns the cursor immediately before the retained tail in the
// observed view.
func (o ObservedCommitted) RetainFrom() rootstate.Cursor {
	return o.Tail.RetainFrom(o.Checkpoint.Snapshot.State.LastCommitted)
}

// Window returns the retained committed-tail window in the observed view.
func (o ObservedCommitted) Window() TailWindow {
	return o.Tail.Window()
}

// Advance packages one observed view together with tail tokens into a
// classified tail-advance result.
func (o ObservedCommitted) Advance(after, token TailToken) TailAdvance {
	return TailAdvance{
		After:    after,
		Token:    token,
		Observed: o,
	}
}

// LastCursor returns the last committed cursor visible in the observed tail.
func (a TailAdvance) LastCursor() rootstate.Cursor {
	return a.Observed.LastCursor()
}

// Window returns the retained committed-tail window visible in this advance.
func (a TailAdvance) Window() TailWindow {
	return a.Observed.Window()
}

// Advanced reports whether the observed tail view changed past the requested token.
func (a TailAdvance) Advanced() bool {
	return a.Token.AdvancedSince(a.After)
}

// CursorAdvanced reports whether the committed cursor frontier advanced.
func (a TailAdvance) CursorAdvanced() bool {
	return rootstate.CursorAfter(a.Token.Cursor, a.After.Cursor)
}

// WindowShifted reports whether the retained tail window changed without
// advancing the committed cursor frontier.
func (a TailAdvance) WindowShifted() bool {
	return a.Advanced() && !a.CursorAdvanced()
}

// Kind classifies the observed tail change relative to the requested token.
func (a TailAdvance) Kind() TailAdvanceKind {
	if !a.Advanced() {
		return TailAdvanceUnchanged
	}
	if a.CursorAdvanced() {
		return TailAdvanceCursorAdvanced
	}
	return TailAdvanceWindowShifted
}

// CatchUpAction classifies whether the caller should reload rooted state or
// only acknowledge a retained-window shift.
func (a TailAdvance) CatchUpAction() TailCatchUpAction {
	if a.NeedsBootstrapInstall() {
		return TailCatchUpInstallBootstrap
	}
	switch a.Kind() {
	case TailAdvanceCursorAdvanced:
		return TailCatchUpRefreshState
	case TailAdvanceWindowShifted:
		return TailCatchUpAcknowledgeWindow
	default:
		return TailCatchUpIdle
	}
}

// NeedsBootstrapInstall reports whether the caller has already fallen behind
// the retained committed tail and therefore must reload/install one bootstrap
// image instead of replaying tail deltas.
func (a TailAdvance) NeedsBootstrapInstall() bool {
	return a.Advanced() && rootstate.CursorAfter(a.Observed.RetainFrom(), a.After.Cursor)
}

// ShouldReloadState reports whether the observed tail advance carries rooted
// truth that should be reloaded into a follower view.
func (a TailAdvance) ShouldReloadState() bool {
	switch a.CatchUpAction() {
	case TailCatchUpRefreshState, TailCatchUpInstallBootstrap:
		return true
	default:
		return false
	}
}

// FellBehind reports whether the observed retained tail had to fall back past
// the requested offset due to compaction.
func (a TailAdvance) FellBehind() bool {
	return a.Window().FellBehind()
}

// ObserveCommitted loads one compact checkpoint together with one retained
// committed tail view starting at requestedOffset.
func ObserveCommitted(storage Substrate, requestedOffset int64) (ObservedCommitted, error) {
	checkpoint, err := storage.LoadCheckpoint()
	if err != nil {
		return ObservedCommitted{}, err
	}
	tail, err := storage.ReadCommitted(requestedOffset)
	if err != nil {
		return ObservedCommitted{}, err
	}
	return ObservedCommitted{
		Checkpoint: checkpoint,
		Tail:       tail,
	}, nil
}

// Substrate is the rooted metadata virtual-log surface consumed by root
// backends. It combines one compact checkpoint boundary with one retained
// committed stream and bootstrap installation semantics.
type Substrate interface {
	LoadCheckpoint() (checkpoint Checkpoint, err error)
	SaveCheckpoint(checkpoint Checkpoint) error
	ReadCommitted(requestedOffset int64) (CommittedTail, error)
	AppendCommitted(records ...CommittedEvent) (logEnd int64, err error)
	CompactCommitted(stream CommittedTail) error
	InstallBootstrap(observed ObservedCommitted) error
	Size() (int64, error)
}
