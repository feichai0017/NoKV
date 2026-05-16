// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"errors"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

type preparedCreate struct {
	req          fsmeta.CreateRequest
	mount        fsmeta.MountIdentity
	plan         fsmeta.OperationPlan
	materialized compile.MaterializedOp
	dentry       fsmeta.DentryRecord
	inode        fsmeta.InodeRecord
}

func (e *Executor) CreateBatch(ctx context.Context, req fsmeta.CreateBatchRequest) (fsmeta.CreateBatchResult, error) {
	if err := fsmeta.ValidateCreateBatchRequest(req); err != nil {
		return fsmeta.CreateBatchResult{}, err
	}
	prepared, ok, err := e.preparePerasCreateBatch(ctx, req)
	if err != nil {
		return fsmeta.CreateBatchResult{}, err
	}
	if ok {
		if committed, err := e.tryPerasVisibleCommitBatch(ctx, prepared); committed || err != nil {
			if err != nil {
				return fsmeta.CreateBatchResult{}, err
			}
			return e.finishPreparedCreateBatch(prepared), nil
		}
	}
	return e.createBatchSequential(ctx, req)
}

func (e *Executor) preparePerasCreateBatch(ctx context.Context, req fsmeta.CreateBatchRequest) ([]preparedCreate, bool, error) {
	if e == nil || e.inodes == nil || e.perasCommitter == nil || e.perasAuthority == nil {
		return nil, false, nil
	}
	batchCommitter, ok := e.perasCommitter.(PerasBatchCommitter)
	if !ok || batchCommitter == nil {
		return nil, false, nil
	}
	if len(req.Entries) == 0 {
		return nil, false, fsmeta.ErrInvalidRequest
	}
	mountID := req.Entries[0].Mount
	for _, entry := range req.Entries[1:] {
		if entry.Mount != mountID {
			return nil, false, nil
		}
	}
	mountRecord, err := e.resolveActiveMount(ctx, mountID)
	if err != nil {
		return nil, false, err
	}
	mount := mountRecord.Identity()
	out := make([]preparedCreate, 0, len(req.Entries))
	for _, entry := range req.Entries {
		inodeID, err := e.inodes.AllocateCreateInode(ctx, mount, entry.Parent, entry.Name)
		if err != nil {
			return nil, false, err
		}
		program, err := compile.CompileCreateProgram(entry, mount, inodeID, compile.WithQuotaMode(e.perasQuotaMode()))
		if err != nil {
			return nil, false, err
		}
		delta := program.Compiled.Delta
		if err := e.admitPerasAuthority(ctx, delta); err != nil {
			return nil, false, err
		}
		if delta.Eligibility != compile.EligibilityVisibleCommit {
			return nil, false, nil
		}
		inode := entry.Attrs.InodeRecord(inodeID)
		quotaOK, err := e.perasQuotaAllowsVisibleCommit(ctx, []QuotaChange{{
			Mount:      entry.Mount,
			MountKeyID: mount.MountKeyID,
			Scope:      entry.Parent,
			Bytes:      inodeSizeDelta(inode.Size),
			Inodes:     1,
		}})
		if err != nil {
			return nil, false, err
		}
		if !quotaOK {
			return nil, false, nil
		}
		materialized, err := compile.MaterializeCreate(program, compile.CreateValues{})
		if err != nil {
			return nil, false, err
		}
		dentry := fsmeta.DentryRecord{
			Parent: entry.Parent,
			Name:   entry.Name,
			Inode:  inodeID,
			Type:   inode.Type,
		}
		out = append(out, preparedCreate{
			req:          entry,
			mount:        mount,
			plan:         delta.Plan,
			materialized: materialized,
			dentry:       dentry,
			inode:        inode,
		})
	}
	return out, true, nil
}

func (e *Executor) tryPerasVisibleCommitBatch(ctx context.Context, creates []preparedCreate) (bool, error) {
	if len(creates) == 0 {
		return false, nil
	}
	batchCommitter, ok := e.perasCommitter.(PerasBatchCommitter)
	if !ok || batchCommitter == nil {
		return false, nil
	}
	submissions := make([]fsperas.VisibleSubmission, 0, len(creates))
	for _, create := range creates {
		op := create.materialized
		delta := op.Delta
		if delta.Eligibility != compile.EligibilityVisibleCommit {
			e.perasVisible.skipIneligibleTotal.Add(1)
			return false, nil
		}
		if op.Placement.RequiresMaterialize {
			e.perasVisible.skipNonConcreteTotal.Add(1)
			return false, nil
		}
		if !op.Placement.CanSegment {
			e.perasVisible.skipPlacementTotal.Add(1)
			return false, nil
		}
		submissions = append(submissions, fsperas.VisibleSubmission{
			ID: e.nextPerasOperationID(delta.Kind),
			Op: op,
		})
	}
	e.perasVisible.attemptTotal.Add(uint64(len(submissions)))
	start := time.Now()
	_, err := batchCommitter.SubmitVisibleBatch(ctx, submissions, e.perasPredicatesHold)
	latency := uint64(time.Since(start).Nanoseconds())
	perOpLatency := latency / uint64(len(submissions))
	e.perasVisible.latencyTotalNanosecond.Add(perOpLatency * uint64(len(submissions)))
	recordUint64Max(&e.perasVisible.latencyMaxNanosecond, perOpLatency)
	if err != nil {
		if errors.Is(err, fsperas.ErrAdmissionRejected) ||
			errors.Is(err, fsperas.ErrIneligibleOperation) ||
			errors.Is(err, errPerasAuthorityNotHeld) ||
			nokverrors.KindOf(err) == nokverrors.KindNotLeader {
			e.perasVisible.skipPredicateTotal.Add(uint64(len(submissions)))
			return false, nil
		}
		if isPerasAdmissionTerminalError(err) {
			e.perasVisible.skipPredicateTotal.Add(uint64(len(submissions)))
			return true, err
		}
		e.perasVisible.errorTotal.Add(uint64(len(submissions)))
		return true, err
	}
	e.perasVisible.successTotal.Add(uint64(len(submissions)))
	return true, nil
}

func (e *Executor) finishPreparedCreateBatch(creates []preparedCreate) fsmeta.CreateBatchResult {
	result := fsmeta.CreateBatchResult{Entries: make([]fsmeta.CreateResult, 0, len(creates))}
	for _, create := range creates {
		e.createTotal.Add(1)
		e.rememberPerasCreate(create.mount, create.plan, create.inode)
		e.forgetPerasEmptyDirectory(create.mount, create.req.Parent)
		e.invalidateNegative(create.plan.MutateKeys[0])
		e.invalidateDirPages(create.req.Mount, create.req.Parent)
		result.Entries = append(result.Entries, fsmeta.CreateResult{Dentry: create.dentry, Inode: create.inode})
	}
	return result
}

func (e *Executor) createBatchSequential(ctx context.Context, req fsmeta.CreateBatchRequest) (fsmeta.CreateBatchResult, error) {
	result := fsmeta.CreateBatchResult{Entries: make([]fsmeta.CreateResult, 0, len(req.Entries))}
	for _, entry := range req.Entries {
		created, err := e.Create(ctx, entry)
		if err != nil {
			return result, err
		}
		result.Entries = append(result.Entries, created)
	}
	return result, nil
}
