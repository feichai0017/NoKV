package peras

import (
	"context"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/feichai0017/NoKV/utils"
)

func (c *Runtime) flushLocked(ctx context.Context, scope *compile.AuthorityScope, level fsperas.SegmentPersistenceLevel) error {
	c.commitMu.Lock()
	plans, err := c.freezeReplayPlansLocked(scope, 0)
	c.commitMu.Unlock()
	if err != nil {
		return err
	}
	batches, err := c.buildFlushBatches(plans, false)
	if err != nil {
		return err
	}
	return c.installFlushBatches(ctx, batches, level)
}

func (c *Runtime) installFlushBatches(ctx context.Context, batches []perasFlushBatch, level fsperas.SegmentPersistenceLevel) error {
	if len(batches) > 0 && c.installer == nil {
		return c.recordError(ErrRuntimeInvalid)
	}
	for _, batch := range batches {
		if err := c.installFlushBatchJobs(ctx, batch, level); err != nil {
			return err
		}
		if err := batch.holder.MarkReplayPlanApplied(batch.plan); err != nil {
			return c.recordErrorf("mark peras plan applied: %w", err)
		}
		for _, job := range batch.jobs {
			if err := c.installSegment(job.plan, job.segment); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Runtime) installFlushBatchJobs(ctx context.Context, batch perasFlushBatch, level fsperas.SegmentPersistenceLevel) error {
	c.recordFlushBatch(len(batch.jobs))
	if len(batch.jobs) <= 1 || c.installN <= 1 {
		jobs := make([]perasFlushJob, 0, len(batch.jobs))
		for _, job := range batch.jobs {
			installed, err := c.installOneFlushJob(ctx, batch.holder, job)
			if err != nil {
				return err
			}
			jobs = append(jobs, installed)
		}
		batch.jobs = jobs
		if level.RequiresPublish() {
			return c.publishFlushJobSeals(ctx, batch)
		}
		c.metrics.flushTotal.Add(uint64(len(batch.jobs)))
		return nil
	}
	started := make([]time.Time, len(batch.jobs))
	for idx := range started {
		started[idx] = time.Now()
	}
	if err := c.appendFlushBatchWitnesses(ctx, batch); err != nil {
		return err
	}
	jobs, err := c.installFlushBatchSegments(ctx, batch, started)
	if err != nil {
		return err
	}
	batch.jobs = jobs
	if level.RequiresPublish() {
		return c.publishFlushJobSeals(ctx, batch)
	}
	c.metrics.flushTotal.Add(uint64(len(batch.jobs)))
	return nil
}

func (c *Runtime) appendFlushBatchWitnesses(ctx context.Context, batch perasFlushBatch) error {
	return c.runFlushBatchJobs(ctx, batch.jobs, func(ctx context.Context, _ int, job perasFlushJob) error {
		witnessStart := time.Now()
		if err := c.appendSegmentWitnessesWithRetry(ctx, job.scope, batch.holder, job.segment, job.payload, job.digest); err != nil {
			return c.recordErrorf("append peras segment witness: %w", err)
		}
		c.recordWitnessLatency(time.Since(witnessStart))
		return nil
	})
}

func (c *Runtime) installFlushBatchSegments(ctx context.Context, batch perasFlushBatch, started []time.Time) ([]perasFlushJob, error) {
	jobs := make([]perasFlushJob, len(batch.jobs))
	copy(jobs, batch.jobs)
	if err := c.runFlushBatchJobs(ctx, jobs, func(ctx context.Context, idx int, job perasFlushJob) error {
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

func (c *Runtime) runFlushBatchJobs(ctx context.Context, jobs []perasFlushJob, run func(context.Context, int, perasFlushJob) error) error {
	workers := c.installN
	if workers > len(jobs) {
		workers = len(jobs)
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

func (c *Runtime) installOneFlushJob(ctx context.Context, holder *fsperas.Holder, job perasFlushJob) (perasFlushJob, error) {
	flushStart := time.Now()
	defer func() {
		c.recordFlushLatency(time.Since(flushStart))
	}()
	witnessStart := time.Now()
	if err := c.appendSegmentWitnessesWithRetry(ctx, job.scope, holder, job.segment, job.payload, job.digest); err != nil {
		return job, c.recordErrorf("append peras segment witness: %w", err)
	}
	c.recordWitnessLatency(time.Since(witnessStart))
	cursor, err := c.submitInstallJob(ctx, job)
	if err != nil {
		return job, c.recordErrorf("install peras segment: %w", err)
	}
	job.cursor = cursor
	return job, nil
}

func (c *Runtime) publishFlushJobSeals(ctx context.Context, batch perasFlushBatch) error {
	for _, job := range batch.jobs {
		if err := c.submitSealJob(ctx, batch.holder, job); err != nil {
			return err
		}
	}
	return nil
}

func (c *Runtime) submitSealJob(ctx context.Context, holder *fsperas.Holder, job perasFlushJob) error {
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

func (c *Runtime) grantForEpoch(epochID uint64) (AuthorityGrant, bool) {
	if c == nil || c.epochs == nil {
		return AuthorityGrant{}, false
	}
	return c.epochs.grant(epochID)
}
