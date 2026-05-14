// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
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

	mu       sync.Mutex
	segments map[witnessSegmentKey]struct{}
	inflight map[witnessSegmentKey]*witnessAppendCall
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
		inflight:      make(map[witnessSegmentKey]*witnessAppendCall),
	}, nil
}

func (n *WitnessNode) ID() string {
	if n == nil {
		return ""
	}
	return n.nodeID
}

func (n *WitnessNode) AppendSegment(ctx context.Context, scope compile.AuthorityScope, record fsperas.SegmentWitnessRecord) error {
	if n == nil || n.log == nil || n.authorityView == nil {
		return ErrWitnessNodeConfigInvalid
	}
	if err := n.validateAuthority(ctx, scope, record.EpochID, record.HolderID); err != nil {
		return err
	}
	key := witnessSegmentKey{epochID: record.EpochID, root: record.SegmentRoot, digest: record.SegmentPayloadDigest}

	n.mu.Lock()
	if _, ok := n.segments[key]; ok {
		n.mu.Unlock()
		return nil
	}
	if call := n.inflight[key]; call != nil {
		n.mu.Unlock()
		return n.waitAppendCall(ctx, key, call)
	}
	if err := n.loadEpochLocked(ctx, record.EpochID); err != nil {
		n.mu.Unlock()
		return err
	}
	if _, ok := n.segments[key]; ok {
		n.mu.Unlock()
		return nil
	}
	call := &witnessAppendCall{done: make(chan struct{})}
	n.inflight[key] = call
	n.mu.Unlock()

	_, err := n.log.AppendSegment(ctx, record)

	n.mu.Lock()
	if err == nil {
		n.segments[key] = struct{}{}
	}
	call.err = err
	delete(n.inflight, key)
	close(call.done)
	n.mu.Unlock()
	return err
}

func (n *WitnessNode) Probe(ctx context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	if n == nil || n.log == nil {
		return fsperas.WitnessSnapshot{}, ErrWitnessNodeConfigInvalid
	}
	return n.log.Probe(ctx, epochID)
}

func (n *WitnessNode) validateAuthority(ctx context.Context, scope compile.AuthorityScope, epochID uint64, holderID string) error {
	err := n.checkAuthority(scope, epochID, holderID)
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
	return n.checkAuthority(scope, epochID, holderID)
}

func (n *WitnessNode) checkAuthority(scope compile.AuthorityScope, epochID uint64, holderID string) error {
	grant, ok, err := n.authorityView.Find(scope, n.now())
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: want epoch=%d holder=%q", ErrWitnessAuthorityMissing, epochID, holderID)
	}
	// A holder may drain segments from an older local visible log after root
	// has renewed the same holder into a later epoch. Reject transferred
	// ownership and future epochs; accept same-holder predecessor drain.
	if grant.HolderID != holderID || grant.EpochID < epochID {
		return fmt.Errorf("%w: have grant=%q epoch=%d holder=%q want epoch=%d holder=%q",
			ErrWitnessAuthorityMismatch, grant.GrantID, grant.EpochID, grant.HolderID, epochID, holderID)
	}
	return nil
}

func (n *WitnessNode) loadEpochLocked(ctx context.Context, epochID uint64) error {
	snapshot, err := n.log.Probe(ctx, epochID)
	if err != nil {
		return err
	}
	for _, segment := range snapshot.Segments {
		n.segments[witnessSegmentKey{epochID: segment.EpochID, root: segment.SegmentRoot, digest: segment.SegmentPayloadDigest}] = struct{}{}
	}
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
