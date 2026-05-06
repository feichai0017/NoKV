package peer

import (
	"sync"
	"time"
)

const (
	// defaultBatchMaxSize is the default number of proposals collected before
	// the batcher flushes them as a single Ready cycle.
	defaultBatchMaxSize = 64
	// defaultBatchMaxWait is the maximum time the batcher waits before flushing
	// a non-full batch.
	defaultBatchMaxWait = time.Millisecond
)

// proposalBatcher collects proposals and flushes them as a batch, calling
// processReady once for the entire batch instead of once per proposal.
//
// The batcher is the single proposal path for Peer. It batches proposals up
// to maxSize or maxWait, whichever comes first, then flushes them under a
// single lock acquisition + processReady call. This amortizes the cost of
// raft Ready processing across multiple proposals, dramatically improving
// throughput under I/O-bound workloads.
type proposalBatcher struct {
	peer    *Peer
	ch      chan *proposalItem
	maxSize int
	maxWait time.Duration
	wg      sync.WaitGroup
	mu      sync.Mutex
	closed  bool
}

func newProposalBatcher(p *Peer, maxSize int, maxWait time.Duration) *proposalBatcher {
	if maxSize <= 0 {
		maxSize = defaultBatchMaxSize
	}
	if maxWait <= 0 {
		maxWait = defaultBatchMaxWait
	}
	b := &proposalBatcher{
		peer:    p,
		ch:      make(chan *proposalItem, maxSize*2),
		maxSize: maxSize,
		maxWait: maxWait,
	}
	b.wg.Add(1)
	go b.run()
	return b
}

// propose submits a proposal to the batcher and returns a proposalResult
// whose Done channel receives the error once the proposal is flushed.
func (b *proposalBatcher) propose(data []byte) *proposalResult {
	r := &proposalResult{Done: make(chan error, 1)}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		r.complete(errPeerStopped)
		return r
	}
	b.ch <- &proposalItem{data: data, result: r}
	return r
}

// proposalResult carries the outcome of an asynchronously submitted proposal.
type proposalResult struct {
	Done chan error
}

// Wait blocks until the proposal has been flushed and returns its error.
func (r *proposalResult) Wait() error {
	return <-r.Done
}

func (r *proposalResult) complete(err error) {
	r.Done <- err
}

// close signals the batcher goroutine to stop and waits for it to drain
// all pending proposals.
func (b *proposalBatcher) close() {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		close(b.ch)
	}
	b.mu.Unlock()
	b.wg.Wait()
}

func (b *proposalBatcher) run() {
	defer b.wg.Done()
	batch := make([]*proposalItem, 0, b.maxSize)
	timer := time.NewTimer(b.maxWait)
	if !timer.Stop() {
		<-timer.C
	}
	timerActive := false
	defer func() {
		if timerActive {
			timer.Stop()
		}
	}()
	resetTimer := func() {
		if timerActive {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		timer.Reset(b.maxWait)
		timerActive = true
	}
	stopTimer := func() {
		if !timerActive {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerActive = false
	}

	for {
		select {
		case item, ok := <-b.ch:
			if !ok {
				stopTimer()
				if len(batch) > 0 {
					b.flush(batch)
				}
				return
			}
			batch = append(batch, item)
			if len(batch) >= b.maxSize {
				stopTimer()
				b.flush(batch)
				batch = batch[:0]
			} else if len(batch) == 1 {
				// First item: start the wait timer.
				resetTimer()
			}

		case <-timer.C:
			timerActive = false
			if len(batch) > 0 {
				b.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

func (b *proposalBatcher) flush(batch []*proposalItem) {
	p := b.peer
	succeeded := make([]*proposalItem, 0, len(batch))
	p.mu.Lock()
	// Propose all items under a single lock hold. node.Propose only
	// appends to the pending list, so this is safe.
	for _, item := range batch {
		if err := p.node.Propose(item.data); err != nil {
			item.result.complete(err)
			continue
		}
		succeeded = append(succeeded, item)
	}
	p.mu.Unlock()
	if len(succeeded) == 0 {
		return
	}

	// Process Ready once for the entire batch. If this fails, propagate
	// the error to any item that hasn't already received a per-propose error.
	if err := p.processReady(); err != nil {
		for _, item := range succeeded {
			item.result.complete(err)
		}
		return
	}

	for _, item := range succeeded {
		item.result.complete(nil)
	}
}

// proposalItem is a single proposal submitted to the batcher.
type proposalItem struct {
	data   []byte
	result *proposalResult
}
