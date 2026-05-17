// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

type WitnessNodeConfig struct {
	NodeID           string
	Log              *WALWitnessLog
	AuthorityView    AuthorityView
	AuthorityRefresh func(context.Context) error
	Now              func() time.Time
}

type WitnessNode struct {
	nodeID        string
	log           *WALWitnessLog
	authorityView AuthorityView
	refresh       func(context.Context) error
	now           func() time.Time
	metrics       witnessNodeMetrics

	mu       sync.Mutex
	segments map[witnessSegmentKey]struct{}
	records  map[witnessSegmentKey]fsperas.SegmentWitnessRecord
	inflight map[witnessSegmentKey]*witnessAppendCall
	loaded   map[uint64]struct{}
}

type witnessSegmentKey struct {
	epochID uint64
	root    [32]byte
	digest  [32]byte
}

type witnessAppendCall struct {
	done chan struct{}
	err  error
}

func NewWitnessNode(cfg WitnessNodeConfig) (*WitnessNode, error) {
	if cfg.NodeID == "" || cfg.Log == nil || cfg.AuthorityView == nil {
		return nil, ErrWitnessNodeConfigInvalid
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &WitnessNode{
		nodeID:        cfg.NodeID,
		log:           cfg.Log,
		authorityView: cfg.AuthorityView,
		refresh:       cfg.AuthorityRefresh,
		now:           now,
		segments:      make(map[witnessSegmentKey]struct{}),
		records:       make(map[witnessSegmentKey]fsperas.SegmentWitnessRecord),
		inflight:      make(map[witnessSegmentKey]*witnessAppendCall),
		loaded:        make(map[uint64]struct{}),
	}, nil
}

func (n *WitnessNode) ID() string {
	if n == nil {
		return ""
	}
	return n.nodeID
}

func (n *WitnessNode) Stats() map[string]any {
	if n == nil {
		stats := emptyWitnessNodeStats()
		maps.Copy(stats, emptyWitnessLogStats())
		return stats
	}
	stats := n.metrics.Stats()
	if n.log != nil {
		maps.Copy(stats, n.log.Stats())
	}
	return stats
}

func (n *WitnessNode) AppendSegments(ctx context.Context, scope compile.AuthorityScope, records []fsperas.SegmentWitnessRecord) (err error) {
	start := time.Now()
	recordCount := len(records)
	defer func() {
		if n != nil {
			n.metrics.recordAppend(recordCount, time.Since(start), err)
		}
	}()
	if n == nil || n.log == nil || n.authorityView == nil {
		return ErrWitnessNodeConfigInvalid
	}
	if err = ctxErr(ctx); err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	authorityStart := time.Now()
	authorityChecks := 0
	for _, record := range records {
		authorityChecks++
		if err = n.validateAuthority(ctx, scope, record); err != nil {
			n.metrics.recordAuthorityCheck(authorityChecks, time.Since(authorityStart))
			return err
		}
	}
	n.metrics.recordAuthorityCheck(authorityChecks, time.Since(authorityStart))

	type pendingAppend struct {
		key    witnessSegmentKey
		record fsperas.SegmentWitnessRecord
		call   *witnessAppendCall
	}
	pending := make([]pendingAppend, 0, len(records))
	waiting := make([]pendingAppend, 0)
	skipped := 0
	n.mu.Lock()
	for _, record := range records {
		key := witnessSegmentKey{epochID: record.EpochID, root: record.SegmentRoot, digest: record.SegmentPayloadDigest}
		if _, ok := n.segments[key]; ok {
			skipped++
			continue
		}
		if call := n.inflight[key]; call != nil {
			waiting = append(waiting, pendingAppend{key: key, call: call})
			continue
		}
		if _, loaded := n.loaded[record.EpochID]; !loaded {
			if err = n.loadEpochLocked(ctx, record.EpochID); err != nil {
				n.mu.Unlock()
				return err
			}
		}
		if _, ok := n.segments[key]; ok {
			skipped++
			continue
		}
		call := &witnessAppendCall{done: make(chan struct{})}
		n.inflight[key] = call
		pending = append(pending, pendingAppend{key: key, record: record, call: call})
	}
	n.mu.Unlock()
	n.metrics.recordDedupe(skipped, len(waiting), len(pending))

	if len(pending) > 0 {
		appendRecords := make([]fsperas.SegmentWitnessRecord, 0, len(pending))
		for _, item := range pending {
			appendRecords = append(appendRecords, item.record)
		}
		_, err = n.log.AppendSegments(ctx, appendRecords)

		n.mu.Lock()
		for _, item := range pending {
			if err == nil {
				n.segments[item.key] = struct{}{}
				n.records[item.key] = item.record
			}
			item.call.err = err
			delete(n.inflight, item.key)
			close(item.call.done)
		}
		n.mu.Unlock()
		if err != nil {
			return err
		}
	}
	for _, item := range waiting {
		if err = n.waitAppendCall(ctx, item.key, item.call); err != nil {
			return err
		}
	}
	return nil
}

func (n *WitnessNode) Probe(ctx context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	if n == nil || n.log == nil {
		return fsperas.WitnessSnapshot{}, ErrWitnessNodeConfigInvalid
	}
	return n.log.Probe(ctx, epochID)
}

func (n *WitnessNode) ProbeSegment(ctx context.Context, ref fsperas.WitnessSegmentRef) (fsperas.SegmentWitnessRecord, bool, error) {
	if n == nil || n.log == nil {
		return fsperas.SegmentWitnessRecord{}, false, ErrWitnessNodeConfigInvalid
	}
	if !ref.Valid() {
		return fsperas.SegmentWitnessRecord{}, false, fsperas.ErrInvalidWitnessRecord
	}
	if err := ctx.Err(); err != nil {
		return fsperas.SegmentWitnessRecord{}, false, err
	}
	key := witnessSegmentKey{epochID: ref.EpochID, root: ref.SegmentRoot, digest: ref.SegmentPayloadDigest}
	n.mu.Lock()
	if record, ok := n.records[key]; ok {
		n.mu.Unlock()
		return record, true, nil
	}
	_, loaded := n.loaded[ref.EpochID]
	n.mu.Unlock()
	if loaded {
		return fsperas.SegmentWitnessRecord{}, false, nil
	}
	record, found, err := n.log.ProbeSegment(ctx, ref)
	if err != nil || !found {
		return record, found, err
	}
	n.mu.Lock()
	if current, ok := n.records[key]; ok {
		n.mu.Unlock()
		return current, true, nil
	}
	n.segments[key] = struct{}{}
	n.records[key] = record
	n.mu.Unlock()
	return record, true, nil
}

func (n *WitnessNode) validateAuthority(ctx context.Context, scope compile.AuthorityScope, record fsperas.SegmentWitnessRecord) error {
	err := n.checkAuthority(scope, record)
	if err == nil || n.refresh == nil || ctx.Err() != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	if !errors.Is(err, ErrWitnessAuthorityMissing) && !errors.Is(err, ErrWitnessAuthorityMismatch) {
		return err
	}
	if refreshErr := n.refresh(ctx); refreshErr != nil {
		return refreshErr
	}
	return n.checkAuthority(scope, record)
}

func (n *WitnessNode) checkAuthority(scope compile.AuthorityScope, record fsperas.SegmentWitnessRecord) error {
	grant, ok, err := n.authorityView.Find(scope, n.now())
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: want epoch=%d holder=%q", ErrWitnessAuthorityMissing, record.EpochID, record.HolderID)
	}
	// A holder may drain segments from an older local visible log after root
	// has renewed the same holder into a later epoch. Reject transferred
	// ownership, future epochs, and records from a different predecessor
	// frontier; accept same-holder predecessor drain only on the same frontier.
	if grant.HolderID != record.HolderID || grant.EpochID < record.EpochID || grant.PredecessorDigest != record.PredecessorDigest {
		return fmt.Errorf("%w: have grant=%q epoch=%d holder=%q want epoch=%d holder=%q",
			ErrWitnessAuthorityMismatch, grant.GrantID, grant.EpochID, grant.HolderID, record.EpochID, record.HolderID)
	}
	return nil
}

func (n *WitnessNode) loadEpochLocked(ctx context.Context, epochID uint64) error {
	if _, loaded := n.loaded[epochID]; loaded {
		return nil
	}
	snapshot, err := n.log.Probe(ctx, epochID)
	if err != nil {
		return err
	}
	for _, segment := range snapshot.Segments {
		key := witnessSegmentKey{epochID: segment.EpochID, root: segment.SegmentRoot, digest: segment.SegmentPayloadDigest}
		n.segments[key] = struct{}{}
		n.records[key] = segment
	}
	n.loaded[epochID] = struct{}{}
	return nil
}

func (n *WitnessNode) waitAppendCall(ctx context.Context, key witnessSegmentKey, call *witnessAppendCall) error {
	select {
	case <-call.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if call.err != nil {
		return call.err
	}
	n.mu.Lock()
	_, ok := n.segments[key]
	n.mu.Unlock()
	if ok {
		return nil
	}
	return fsperas.ErrInvalidWitnessRecord
}
