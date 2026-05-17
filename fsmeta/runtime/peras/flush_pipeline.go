// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/utils"
)

func (c *Runtime) flushLocked(ctx context.Context, scope *compile.AuthorityScope, level fsperas.SegmentPersistenceLevel) error {
	pipeline := flushPipeline{runtime: c, level: level, materialize: c.materialize}
	batches, err := pipeline.freeze(scope)
	if err != nil {
		return err
	}
	return pipeline.run(ctx, batches)
}

type flushPipeline struct {
	runtime                 *Runtime
	level                   fsperas.SegmentPersistenceLevel
	materialize             bool
	allowDurableOldEpochRun bool
}

type publishDecision uint8

const (
	publishDecisionDenied publishDecision = iota
	publishDecisionNow
	publishDecisionOldEpochDrain
)

func (p flushPipeline) freeze(scope *compile.AuthorityScope) ([]perasFlushBatch, error) {
	c := p.runtime
	c.commitMu.Lock()
	plans, err := c.freezeReplayPlansLocked(scope, 0)
	c.commitMu.Unlock()
	if err != nil {
		return nil, err
	}
	return c.buildFlushBatches(plans, p.materialize)
}

func (p flushPipeline) run(ctx context.Context, batches []perasFlushBatch) error {
	c := p.runtime
	if len(batches) > 0 && c.installer == nil {
		return c.recordError(ErrRuntimeInvalid)
	}
	prepared, err := p.prepareBatches(ctx, batches)
	if err != nil {
		return err
	}
	for _, batch := range prepared {
		if err := p.commitBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (p flushPipeline) prepareBatches(ctx context.Context, batches []perasFlushBatch) ([]perasFlushBatch, error) {
	if len(batches) == 0 {
		return nil, nil
	}
	prepared := make([]perasFlushBatch, len(batches))
	if err := p.runBatchJobs(ctx, batches, func(ctx context.Context, idx int, batch perasFlushBatch) error {
		installed, err := p.prepareBatch(ctx, batch)
		if err != nil {
			return err
		}
		prepared[idx] = installed
		return nil
	}); err != nil {
		return nil, err
	}
	return prepared, nil
}

func (p flushPipeline) prepareBatch(ctx context.Context, batch perasFlushBatch) (perasFlushBatch, error) {
	c := p.runtime
	c.recordFlushBatch(len(batch.jobs))
	started := make([]time.Time, len(batch.jobs))
	for idx := range started {
		started[idx] = time.Now()
	}
	if err := p.renewBatchAuthority(ctx, batch); err != nil {
		return perasFlushBatch{}, err
	}
	if err := p.witnessBatch(ctx, batch); err != nil {
		return perasFlushBatch{}, err
	}
	jobs, err := p.installBatch(ctx, batch, started)
	if err != nil {
		return perasFlushBatch{}, err
	}
	batch.jobs = jobs
	if p.level.RequiresPublish() {
		decision, err := p.publishDecision(ctx, batch)
		if err != nil {
			return perasFlushBatch{}, err
		}
		batch.publishDecision = decision
		if decision == publishDecisionOldEpochDrain && !p.allowDurableOldEpochRun {
			batch.publishErr = c.recordError(ErrPublishRequired)
		}
		if decision == publishDecisionDenied {
			batch.publishErr = c.recordError(ErrPublishRequired)
		}
	} else {
		batch.publishDecision = publishDecisionDenied
	}
	return batch, nil
}

func (p flushPipeline) publishDecision(ctx context.Context, batch perasFlushBatch) (publishDecision, error) {
	c := p.runtime
	if c == nil || batch.holder == nil {
		return publishDecisionDenied, ErrRuntimeInvalid
	}
	grant, owned, err := c.authority.Acquire(ctx, batch.scope)
	if err != nil {
		return publishDecisionDenied, c.recordErrorf("check peras publish authority: %w", err)
	}
	if !owned || grant.HolderID != batch.holder.HolderID() {
		return publishDecisionDenied, nil
	}
	if grant.EpochID == batch.holder.EpochID() {
		return publishDecisionNow, nil
	}
	if grant.EpochID > batch.holder.EpochID() {
		return publishDecisionOldEpochDrain, nil
	}
	return publishDecisionDenied, nil
}

func (p flushPipeline) renewBatchAuthority(ctx context.Context, batch perasFlushBatch) error {
	c := p.runtime
	if c == nil || c.authority == nil || batch.holder == nil {
		return ErrRuntimeInvalid
	}
	grant, owned, err := c.authority.Acquire(ctx, batch.scope)
	if err != nil {
		return c.recordErrorf("renew peras authority for flush: %w", err)
	}
	if !owned {
		return c.recordError(ErrNotHeld)
	}
	if grant.HolderID != batch.holder.HolderID() {
		return c.recordError(ErrNotHeld)
	}
	if grant.EpochID == batch.holder.EpochID() {
		c.epochs.updateGrant(grant)
		return nil
	}
	if grant.EpochID > batch.holder.EpochID() {
		// Visible-log recovery can rebuild an older same-holder epoch after
		// root has moved to a newer grant. The witness still verifies the
		// predecessor frontier, so allow durable drain without rewriting the
		// old holder's grant.
		older, ok := c.epochs.grant(batch.holder.EpochID())
		if ok && older.HolderID == batch.holder.HolderID() && older.PredecessorDigest == grant.PredecessorDigest {
			return nil
		}
	}
	return c.recordErrorf("renew peras authority for flush: %w", ErrInvalidResponse)
}

func (p flushPipeline) witnessBatch(ctx context.Context, batch perasFlushBatch) error {
	c := p.runtime
	if c == nil || c.witness == nil {
		return nil
	}
	witnessStart := time.Now()
	if err := c.witness.signBatch(ctx, batch); err != nil {
		return c.recordErrorf("append peras segment witness batch: %w", err)
	}
	c.recordWitnessLatency(time.Since(witnessStart))
	return nil
}

func (p flushPipeline) installBatch(ctx context.Context, batch perasFlushBatch, started []time.Time) ([]perasFlushJob, error) {
	c := p.runtime
	jobs := make([]perasFlushJob, len(batch.jobs))
	copy(jobs, batch.jobs)
	if err := p.runJobs(ctx, jobs, func(ctx context.Context, idx int, job perasFlushJob) error {
		cursor, err := c.submitInstallJob(ctx, job)
		if err != nil {
			return c.recordErrorf("install peras segment: %w", err)
		}
		jobs[idx].cursor = cursor
		if idx >= 0 && idx < len(started) && !started[idx].IsZero() {
			c.recordFlushLatency(time.Since(started[idx]))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (p flushPipeline) sealBatch(ctx context.Context, batch perasFlushBatch) error {
	for _, job := range batch.jobs {
		if err := p.submitSeal(ctx, batch.holder, job); err != nil {
			return err
		}
	}
	return nil
}

func (p flushPipeline) commitBatch(ctx context.Context, batch perasFlushBatch) error {
	c := p.runtime
	if p.level.RequiresPublish() {
		if batch.publishErr != nil {
			c.metrics.flushTotal.Add(uint64(len(batch.jobs)))
			return batch.publishErr
		}
		switch batch.publishDecision {
		case publishDecisionNow:
			if err := p.sealBatch(ctx, batch); err != nil {
				return err
			}
		case publishDecisionOldEpochDrain:
			c.metrics.flushTotal.Add(uint64(len(batch.jobs)))
		default:
			c.metrics.flushTotal.Add(uint64(len(batch.jobs)))
			return c.recordError(ErrPublishRequired)
		}
	} else {
		c.metrics.flushTotal.Add(uint64(len(batch.jobs)))
	}
	for _, job := range batch.jobs {
		if err := c.finalizeSegment(ctx, job); err != nil {
			return err
		}
	}
	if err := batch.holder.MarkReplayPlanApplied(batch.plan); err != nil {
		return c.recordErrorf("mark peras plan applied: %w", err)
	}
	c.signalAdmissionCapacity()
	return c.markVisibleLogApplied(ctx, batch.holder, batch.plan)
}

func (c *Runtime) finalizeSegment(ctx context.Context, job perasFlushJob) error {
	if c == nil || c.finalizer == nil {
		return ErrRuntimeInvalid
	}
	return c.finalizer.FinalizeSegment(ctx, SegmentFinalizeRequest{
		Scope:           job.scope,
		Plan:            job.plan,
		Segment:         job.segment,
		InstallCursor:   job.cursor,
		MaterializeMVCC: job.materialize,
	})
}

func (p flushPipeline) runBatchJobs(ctx context.Context, batches []perasFlushBatch, run func(context.Context, int, perasFlushBatch) error) error {
	if len(batches) == 0 {
		return nil
	}
	workers := min(p.runtime.flushN, len(batches))
	if workers <= 0 {
		workers = 1
	}
	return runPerasConcurrent(ctx, workers, batches, run)
}

func (p flushPipeline) runJobs(ctx context.Context, jobs []perasFlushJob, run func(context.Context, int, perasFlushJob) error) error {
	if len(jobs) == 0 {
		return nil
	}
	workers := min(p.runtime.installN, len(jobs))
	if workers <= 0 {
		workers = 1
	}
	return runPerasConcurrent(ctx, workers, jobs, run)
}

func runPerasConcurrent[T any](ctx context.Context, workers int, items []T, run func(context.Context, int, T) error) error {
	if len(items) == 0 {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var errMu sync.Mutex
	var firstErr error
	setErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}
	throttle := utils.NewThrottle(workers)
	for idx, item := range items {
		if runCtx.Err() != nil {
			break
		}
		idx, item := idx, item
		if err := throttle.Go(func() error {
			if err := runCtx.Err(); err != nil {
				return err
			}
			if err := run(runCtx, idx, item); err != nil {
				setErr(err)
				return err
			}
			return nil
		}); err != nil {
			setErr(err)
			break
		}
	}
	finishErr := throttle.Finish()
	errMu.Lock()
	err := firstErr
	errMu.Unlock()
	if err != nil {
		return err
	}
	if finishErr != nil {
		return finishErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (p flushPipeline) submitSeal(ctx context.Context, holder *fsperas.Holder, job perasFlushJob) error {
	c := p.runtime
	if c == nil {
		return ErrRuntimeInvalid
	}
	if c.sealQ != nil {
		return c.sealQ.publish(ctx, holder, job)
	}
	return c.publishSegmentSeal(ctx, holder, job)
}

func (c *Runtime) publishSegmentSeal(ctx context.Context, holder *fsperas.Holder, job perasFlushJob) error {
	if publisher, ok := c.authority.(SealPublisher); ok {
		if !job.cursor.Valid() {
			return c.recordError(ErrRuntimeInvalid)
		}
		if err := verifySegmentInstallBeforeSeal(job); err != nil {
			return c.recordErrorf("verify peras segment install before seal: %w", err)
		}
		grant, found := c.grantForEpoch(holder.EpochID())
		if !found {
			return c.recordError(ErrNotHeld)
		}
		sealStart := time.Now()
		if err := publisher.PublishSegmentSeal(ctx, grant, job.segment, job.digest, job.cursor); err != nil {
			return c.recordErrorf("publish peras segment seal: %w", err)
		}
		c.recordSealLatency(time.Since(sealStart))
		c.metrics.sealTotal.Add(1)
	}
	c.metrics.flushTotal.Add(1)
	return nil
}

func verifySegmentInstallBeforeSeal(job perasFlushJob) error {
	if job.materialize {
		return nil
	}
	if !job.cursor.Valid() || job.segment.Root == ([32]byte{}) || job.digest == ([32]byte{}) || len(job.payload) == 0 {
		return ErrRuntimeInvalid
	}
	digest, err := fsperas.PerasSegmentPayloadDigest(job.payload)
	if err != nil {
		return err
	}
	if digest != job.digest {
		return fsperas.ErrInvalidPerasSegment
	}
	objectKey, err := fsperas.PerasSegmentObjectKey(job.segment)
	if err != nil {
		return err
	}
	if !bytes.Equal(job.install.CanonicalObjectKey, objectKey) {
		return fsperas.ErrInvalidPerasSegment
	}
	return nil
}

func (c *Runtime) grantForEpoch(epochID uint64) (rootproto.PerasAuthorityGrant, bool) {
	if c == nil || c.epochs == nil {
		return rootproto.PerasAuthorityGrant{}, false
	}
	return c.epochs.grant(epochID)
}
