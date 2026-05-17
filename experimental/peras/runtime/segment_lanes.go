// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"sync"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
)

type perasInstallLane struct {
	owner  *Runtime
	jobs   chan perasInstallRequest
	closed chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

type perasInstallRequest struct {
	ctx  context.Context
	job  perasFlushJob
	done chan perasInstallResult
}

type perasInstallResult struct {
	cursor InstallCursor
	err    error
}

func newPerasInstallLane(owner *Runtime, workers int) *perasInstallLane {
	if workers <= 0 {
		workers = 1
	}
	lane := &perasInstallLane{
		owner:  owner,
		jobs:   make(chan perasInstallRequest, workers*4),
		closed: make(chan struct{}),
	}
	lane.wg.Add(workers)
	for range workers {
		go lane.worker()
	}
	return lane
}

func (l *perasInstallLane) install(ctx context.Context, job perasFlushJob) (InstallCursor, error) {
	if l == nil || l.owner == nil {
		return InstallCursor{}, ErrRuntimeInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-l.closed:
		return InstallCursor{}, ErrRuntimeClosed
	default:
	}
	done := make(chan perasInstallResult, 1)
	req := perasInstallRequest{ctx: ctx, job: job, done: done}
	select {
	case l.jobs <- req:
	case <-ctx.Done():
		return InstallCursor{}, ctx.Err()
	case <-l.closed:
		return InstallCursor{}, ErrRuntimeClosed
	}
	select {
	case result := <-done:
		return result.cursor, result.err
	case <-ctx.Done():
		return InstallCursor{}, ctx.Err()
	case <-l.closed:
		return InstallCursor{}, ErrRuntimeClosed
	}
}

func (l *perasInstallLane) close() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		close(l.closed)
	})
	l.wg.Wait()
}

func (l *perasInstallLane) depth() int {
	if l == nil {
		return 0
	}
	return len(l.jobs)
}

func (l *perasInstallLane) capacity() int {
	if l == nil {
		return 0
	}
	return cap(l.jobs)
}

func (l *perasInstallLane) worker() {
	defer l.wg.Done()
	for {
		select {
		case req := <-l.jobs:
			l.run(req)
		case <-l.closed:
			return
		}
	}
}

func (l *perasInstallLane) run(req perasInstallRequest) {
	if err := req.ctx.Err(); err != nil {
		req.done <- perasInstallResult{err: err}
		return
	}
	ctx, cancel := context.WithCancel(req.ctx)
	defer cancel()
	stop := make(chan struct{})
	go func() {
		select {
		case <-l.closed:
			cancel()
		case <-ctx.Done():
		case <-stop:
		}
	}()
	defer close(stop)
	start := time.Now()
	cursor, err := l.owner.installSegmentWithRetry(ctx, req.job)
	if err == nil {
		l.owner.recordInstallLatency(time.Since(start))
	}
	req.done <- perasInstallResult{cursor: cursor, err: err}
}

func (c *Runtime) installSegmentWithRetry(ctx context.Context, job perasFlushJob) (InstallCursor, error) {
	c.recordInstallJobShape(job)
	var last error
	for attempt := 0; attempt <= defaultPerasSegmentInstallRetries; attempt++ {
		cursor, err := c.installer.InstallSegment(ctx, SegmentInstallRequest{
			Scope:           job.scope,
			Segment:         job.segment,
			Payload:         job.payload,
			PayloadDigest:   job.digest,
			Install:         job.install,
			MaterializeMVCC: job.materialize,
		})
		if err == nil {
			return cursor, nil
		}
		last = err
		if !nokverrors.Retryable(err) || attempt == defaultPerasSegmentInstallRetries {
			break
		}
		c.recordInstallRetry(err)
		delay := perasSegmentInstallRetryDelay(err, attempt)
		if !sleepContext(ctx, delay) {
			return InstallCursor{}, ctx.Err()
		}
	}
	return InstallCursor{}, last
}

func perasSegmentInstallRetryDelay(err error, attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	base := defaultPerasSegmentInstallRetryBackoff
	maxDelay := defaultPerasSegmentInstallMaxBackoff
	switch nokverrors.KindOf(err) {
	case nokverrors.KindStaleEpoch, nokverrors.KindRegionRouting, nokverrors.KindNotLeader:
		base = defaultPerasSegmentInstallStaleBackoff
		maxDelay = defaultPerasSegmentInstallStaleMaxBackoff
	}
	delay := min(base<<attempt, maxDelay)
	return delay
}

func (c *Runtime) recordInstallJobShape(job perasFlushJob) {
	if c == nil {
		return
	}
	if len(job.install.RoutingKeys) > 0 {
		c.recordInstallShape(len(job.payload), len(job.install.RoutingKeys))
		return
	}
	routeKeys, err := SegmentInstallRoutingKeys(job.segment, job.materialize)
	if err != nil {
		c.recordInstallShape(len(job.payload), 0)
		return
	}
	c.recordInstallShape(len(job.payload), len(routeKeys))
}

func (c *Runtime) submitInstallJob(ctx context.Context, job perasFlushJob) (InstallCursor, error) {
	if c == nil || c.installer == nil {
		return InstallCursor{}, ErrRuntimeInvalid
	}
	if c.installQ != nil {
		return c.installQ.install(ctx, job)
	}
	start := time.Now()
	cursor, err := c.installSegmentWithRetry(ctx, job)
	if err == nil {
		c.recordInstallLatency(time.Since(start))
	}
	return cursor, err
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

type visibleSealLane struct {
	owner  *Runtime
	jobs   chan visibleSealRequest
	closed chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

type visibleSealRequest struct {
	ctx    context.Context
	holder *fsperas.Holder
	job    perasFlushJob
	done   chan error
}

func newVisibleSealLane(owner *Runtime, workers int) *visibleSealLane {
	if workers <= 0 {
		workers = 1
	}
	lane := &visibleSealLane{
		owner:  owner,
		jobs:   make(chan visibleSealRequest, workers*4),
		closed: make(chan struct{}),
	}
	lane.wg.Add(workers)
	for range workers {
		go lane.worker()
	}
	return lane
}

func (l *visibleSealLane) publish(ctx context.Context, holder *fsperas.Holder, job perasFlushJob) error {
	if l == nil || l.owner == nil {
		return ErrRuntimeInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-l.closed:
		return ErrRuntimeClosed
	default:
	}
	done := make(chan error, 1)
	req := visibleSealRequest{ctx: ctx, holder: holder, job: job, done: done}
	select {
	case l.jobs <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-l.closed:
		return ErrRuntimeClosed
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-l.closed:
		return ErrRuntimeClosed
	}
}

func (l *visibleSealLane) close() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		close(l.closed)
	})
	l.wg.Wait()
}

func (l *visibleSealLane) depth() int {
	if l == nil {
		return 0
	}
	return len(l.jobs)
}

func (l *visibleSealLane) capacity() int {
	if l == nil {
		return 0
	}
	return cap(l.jobs)
}

func (l *visibleSealLane) worker() {
	defer l.wg.Done()
	for {
		select {
		case req := <-l.jobs:
			l.run(req)
		case <-l.closed:
			return
		}
	}
}

func (l *visibleSealLane) run(req visibleSealRequest) {
	if err := req.ctx.Err(); err != nil {
		req.done <- err
		return
	}
	req.done <- l.owner.publishSegmentSeal(req.ctx, req.holder, req.job)
}
