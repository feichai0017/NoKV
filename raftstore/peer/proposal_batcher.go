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
	stopCh  chan struct{}
	wg      sync.WaitGroup
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
		stopCh:  make(chan struct{}),
	}
	b.wg.Add(1)
	go b.run()
	return b
}

// propose submits a proposal to the batcher and returns a proposalResult
// whose Done channel receives the error once the proposal is flushed.
func (b *proposalBatcher) propose(data []byte) *proposalResult {
	r := &proposalResult{Done: make(chan error, 1)}
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

// close signals the batcher goroutine to stop and waits for it to drain
// all pending proposals.
func (b *proposalBatcher) close() {
	close(b.stopCh)
	b.wg.Wait()
}

func (b *proposalBatcher) run() {
	defer b.wg.Done()
	batch := make([]*proposalItem, 0, b.maxSize)
	timer := time.NewTimer(b.maxWait)
	defer timer.Stop()

	for {
		select {
		case item := <-b.ch:
			batch = append(batch, item)
			if len(batch) >= b.maxSize {
				b.flush(batch)
				batch = batch[:0]
				timer.Reset(b.maxWait)
			} else if len(batch) == 1 {
				// First item: start the wait timer.
				timer.Reset(b.maxWait)
			}

		case <-timer.C:
			if len(batch) > 0 {
				b.flush(batch)
				batch = batch[:0]
			}
			timer.Reset(b.maxWait)

		case <-b.stopCh:
			// Flush any partial batch that was already collected.
			if len(batch) > 0 {
				b.flush(batch)
			}
			// Drain any remaining items in the channel.
			for {
				select {
				case item := <-b.ch:
					b.flush([]*proposalItem{item})
				default:
					return
				}
			}
		}
	}
}

func (b *proposalBatcher) flush(batch []*proposalItem) {
	p := b.peer
	p.mu.Lock()
	// Propose all items under a single lock hold. node.Propose only
	// appends to the pending list, so this is safe.
	for _, item := range batch {
		if err := p.node.Propose(item.data); err != nil {
			item.result.Done <- err
		}
	}
	p.mu.Unlock()

	// Process Ready once for the entire batch. If this fails, propagate
	// the error to any item that hasn't already received a per-propose error.
	if err := p.processReady(); err != nil {
		for _, item := range batch {
			select {
			case item.result.Done <- err:
			default:
			}
		}
		return
	}

	for _, item := range batch {
		item.result.Done <- nil
	}
}

// proposalItem is a single proposal submitted to the batcher.
type proposalItem struct {
	data   []byte
	result *proposalResult
}
