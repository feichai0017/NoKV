// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/utils"
)

func (c *Runtime) flushLocked(ctx context.Context, scope *compile.AuthorityScope, level fsperas.SegmentPersistenceLevel) error {
	pipeline := flushPipeline{runtime: c, level: level}
	batches, err := pipeline.freeze(scope)
	if err != nil {
		return err
	}
	return pipeline.run(ctx, batches)
}

type flushPipeline struct {
	runtime *Runtime
	level   fsperas.SegmentPersistenceLevel
}

func (p flushPipeline) freeze(scope *compile.AuthorityScope) ([]perasFlushBatch, error) {
	c := p.runtime
	c.commitMu.Lock()
	plans, err := c.freezeReplayPlansLocked(scope, 0)
	c.commitMu.Unlock()
	if err != nil {
		return nil, err
	}
	return c.buildFlushBatches(plans, false)
}

func (p flushPipeline) run(ctx context.Context, batches []perasFlushBatch) error {
	c := p.runtime
	if len(batches) > 0 && c.installer == nil {
		return c.recordError(ErrRuntimeInvalid)
	}
	for _, batch := range batches {
		installed, err := p.runBatch(ctx, batch)
		if err != nil {
			return err
		}
		if err := p.commitBatch(installed); err != nil {
			return err
		}
	}
	return nil
}

func (p flushPipeline) runBatch(ctx context.Context, batch perasFlushBatch) (perasFlushBatch, error) {
	c := p.runtime
	c.recordFlushBatch(len(batch.jobs))
	started := make([]time.Time, len(batch.jobs))
	for idx := range started {
		started[idx] = time.Now()
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
		if err := p.sealBatch(ctx, batch); err != nil {
			return perasFlushBatch{}, err
		}
	} else {
		c.metrics.flushTotal.Add(uint64(len(batch.jobs)))
	}
	return batch, nil
}

func (p flushPipeline) witnessBatch(ctx context.Context, batch perasFlushBatch) error {
	c := p.runtime
	return p.runJobs(ctx, batch.jobs, func(ctx context.Context, _ int, job perasFlushJob) error {
		witnessStart := time.Now()
		if err := c.appendSegmentWitnessesWithRetry(ctx, job.scope, batch.holder, job.segment, job.payload, job.digest); err != nil {
			return c.recordErrorf("append peras segment witness: %w", err)
		}
		c.recordWitnessLatency(time.Since(witnessStart))
		return nil
	})
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

func (p flushPipeline) commitBatch(batch perasFlushBatch) error {
	c := p.runtime
	if err := batch.holder.MarkReplayPlanApplied(batch.plan); err != nil {
		return c.recordErrorf("mark peras plan applied: %w", err)
	}
	for _, job := range batch.jobs {
		if err := c.installSegment(job.plan, job.segment); err != nil {
			return err
		}
	}
	return nil
}

func (p flushPipeline) runJobs(ctx context.Context, jobs []perasFlushJob, run func(context.Context, int, perasFlushJob) error) error {
	if len(jobs) == 0 {
		return nil
	}
	workers := min(p.runtime.installN, len(jobs))
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
	for idx, job := range jobs {
		if runCtx.Err() != nil {
			break
		}
		idx, job := idx, job
		if err := throttle.Go(func() error {
			if err := runCtx.Err(); err != nil {
				return err
			}
			if err := run(runCtx, idx, job); err != nil {
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

func (c *Runtime) grantForEpoch(epochID uint64) (rootproto.PerasAuthorityGrant, bool) {
	if c == nil || c.epochs == nil {
		return rootproto.PerasAuthorityGrant{}, false
	}
	return c.epochs.grant(epochID)
}
