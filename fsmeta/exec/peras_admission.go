// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/proof"
)

func (e *Executor) admitPerasAuthority(ctx context.Context, delta compile.SemanticDelta) error {
	if e == nil || e.perasAuthority == nil {
		return nil
	}
	if delta.Eligibility != compile.EligibilityVisibleCommit {
		e.perasAdmission.recordSlow(delta.SlowReason)
		return nil
	}
	e.perasAdmission.eligibleTotal.Add(1)
	if e.perasCommitter != nil {
		return nil
	}
	e.perasAdmission.acquireTotal.Add(1)
	owned, err := e.perasAuthority.AcquirePerasAuthority(ctx, delta.Authority)
	if err != nil {
		e.perasAdmission.errorTotal.Add(1)
		return nil
	}
	if !owned {
		e.perasAdmission.heldTotal.Add(1)
		return nil
	}
	e.perasAdmission.ownedTotal.Add(1)
	return nil
}

func (e *Executor) tryPerasVisibleCommit(ctx context.Context, op compile.MaterializedOp) (bool, error) {
	if e == nil || e.perasCommitter == nil {
		return false, nil
	}
	if e.perasAuthority == nil {
		e.perasVisible.skipNoAuthorityTotal.Add(1)
		return false, nil
	}
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
	id := e.nextPerasOperationID(delta.Kind)
	e.perasVisible.attemptTotal.Add(1)
	start := time.Now()
	_, err := e.perasCommitter.SubmitVisible(ctx, id, op, e.perasPredicatesHold)
	latency := uint64(time.Since(start).Nanoseconds())
	e.perasVisible.latencyTotalNanosecond.Add(latency)
	recordUint64Max(&e.perasVisible.latencyMaxNanosecond, latency)
	if err != nil {
		if errors.Is(err, fsperas.ErrAdmissionRejected) ||
			errors.Is(err, fsperas.ErrIneligibleOperation) ||
			errors.Is(err, errPerasAuthorityNotHeld) ||
			nokverrors.KindOf(err) == nokverrors.KindNotLeader {
			e.perasVisible.skipPredicateTotal.Add(1)
			return false, nil
		}
		if isPerasAdmissionTerminalError(err) {
			e.perasVisible.skipPredicateTotal.Add(1)
			return true, err
		}
		e.perasVisible.errorTotal.Add(1)
		return true, err
	}
	e.perasVisible.successTotal.Add(1)
	return true, nil
}

func (e *Executor) tryPerasVisibleCommitAfterRead(ctx context.Context, view *perasReadView, op compile.MaterializedOp) (bool, error) {
	committed, err := e.tryPerasVisibleCommit(ctx, op)
	if err != nil || committed {
		return committed, err
	}
	if view.observedPerasOverlay() {
		return false, errPerasOverlayFallbackUnsafe
	}
	return false, nil
}

func (e *Executor) perasPredicatesHold(ctx context.Context, op compile.MaterializedOp, admissionCtx fsperas.AdmissionContext) (fsperas.AdmissionResult, bool, error) {
	delta := op.Delta
	if !perasPredicateProofsValid(op.PredicateProofs) {
		return fsperas.AdmissionResult{}, false, nil
	}
	frontier := admissionCtx.ProofFrontier
	proofs := make([]proof.PredicateProof, 0, len(delta.ReadPredicates))
	if len(delta.ReadPredicates) == 0 {
		return e.perasAdmissionResult(op, proofs)
	}
	index := e.perasPredicateIndex()
	var version uint64
	var haveVersion bool
	read := func(key []byte) ([]byte, bool, proof.ReadSource, uint64, error) {
		if value, deleted, ok := e.perasOverlayGet(key); ok {
			if deleted {
				return nil, false, proof.ReadSourceOverlay, 0, nil
			}
			return value, true, proof.ReadSourceOverlay, 0, nil
		}
		if !haveVersion {
			var err error
			version, err = e.reserveReadVersion(ctx)
			if err != nil {
				return nil, false, proof.ReadSourceUnknown, 0, err
			}
			haveVersion = true
		}
		value, ok, err := e.runner.Get(ctx, key, version)
		return value, ok, proof.ReadSourceBase, version, err
	}
	for _, predicate := range delta.ReadPredicates {
		switch predicate.Kind {
		case compile.PredicateExists:
			if index != nil {
				present, known := index.KeyState(predicate.Key)
				if known {
					if !present {
						return fsperas.AdmissionResult{}, false, fsmeta.ErrNotFound
					}
					proofs = append(proofs, proof.NewPredicateProof(predicate.Key, nil, true, 0, proof.ReadSourceOverlay, frontier))
					continue
				}
			}
			value, ok, source, proofVersion, err := read(predicate.Key)
			if err != nil {
				return fsperas.AdmissionResult{}, false, err
			}
			if !ok {
				return fsperas.AdmissionResult{}, false, fsmeta.ErrNotFound
			}
			proofs = append(proofs, proof.NewPredicateProof(predicate.Key, value, true, proofVersion, source, proofFrontierForSource(source, frontier)))
		case compile.PredicateNotExists:
			if index != nil {
				present, known := index.KeyState(predicate.Key)
				if known {
					if present {
						return fsperas.AdmissionResult{}, false, fsmeta.ErrExists
					}
					proofs = append(proofs, proof.NewPredicateProof(predicate.Key, nil, false, 0, proof.ReadSourceOverlay, frontier))
					continue
				}
				if e.perasNotExistsKnown(delta.Authority, predicate.Key, index) ||
					perasNotExistsDerivedFromDelta(delta, predicate, index) {
					proofs = append(proofs, proof.NewPredicateProof(predicate.Key, nil, false, 0, proof.ReadSourceOverlay, frontier))
					continue
				}
			}
			_, ok, source, proofVersion, err := read(predicate.Key)
			if err != nil {
				return fsperas.AdmissionResult{}, false, err
			}
			if ok {
				return fsperas.AdmissionResult{}, false, fsmeta.ErrExists
			}
			proofs = append(proofs, proof.NewPredicateProof(predicate.Key, nil, false, proofVersion, source, proofFrontierForSource(source, frontier)))
		case compile.PredicateObservedValue:
			if !predicate.HasExpectedValue {
				return fsperas.AdmissionResult{}, false, nil
			}
			if value, deleted, ok := e.perasOverlayGet(predicate.Key); ok {
				if deleted || !bytes.Equal(value, predicate.ExpectedValue) {
					return fsperas.AdmissionResult{}, false, nil
				}
				proofs = append(proofs, proof.NewPredicateProof(predicate.Key, value, true, 0, proof.ReadSourceOverlay, frontier))
				continue
			}
			value, ok, source, proofVersion, err := read(predicate.Key)
			if err != nil {
				return fsperas.AdmissionResult{}, false, err
			}
			if !ok || !bytes.Equal(value, predicate.ExpectedValue) {
				return fsperas.AdmissionResult{}, false, nil
			}
			proofs = append(proofs, proof.NewPredicateProof(predicate.Key, value, true, proofVersion, source, proofFrontierForSource(source, frontier)))
		case compile.PredicatePrefixScan:
			return fsperas.AdmissionResult{}, false, nil
		default:
			return fsperas.AdmissionResult{}, false, nil
		}
	}
	return e.perasAdmissionResult(op, proofs)
}

func (e *Executor) perasAdmissionResult(op compile.MaterializedOp, proofs []proof.PredicateProof) (fsperas.AdmissionResult, bool, error) {
	guardProofs, err := compile.GuardProofsFor(op.CompiledOp, proofs, op.Delta.RuntimeGuards)
	if err != nil {
		return fsperas.AdmissionResult{}, false, nil
	}
	return fsperas.AdmissionResult{
		PredicateProofs: proofs,
		GuardProofs:     guardProofs,
	}, true, nil
}

func proofFrontierForSource(source proof.ReadSource, frontier proof.ProofFrontier) proof.ProofFrontier {
	if source == proof.ReadSourceOverlay {
		return frontier
	}
	return proof.ProofFrontier{}
}

func perasPredicateProofsValid(proofs []proof.PredicateProof) bool {
	for _, predicateProof := range proofs {
		if err := proof.VerifyPredicateProof(predicateProof); err != nil {
			return false
		}
	}
	return true
}

func (e *Executor) perasPredicateIndex() fsperas.PredicateIndex {
	if e == nil || e.perasCommitter == nil {
		return nil
	}
	index, ok := e.perasCommitter.(fsperas.PredicateIndex)
	if !ok {
		return nil
	}
	return index
}

func (e *Executor) rememberPerasCreate(mount fsmeta.MountIdentity, plan fsmeta.OperationPlan, inode fsmeta.InodeRecord) {
	index := e.perasPredicateIndex()
	if index == nil {
		return
	}
	if len(plan.MutateKeys) > 0 {
		index.RememberKey(plan.MutateKeys[0], true)
	}
	if len(plan.MutateKeys) > 1 {
		index.RememberKey(plan.MutateKeys[1], true)
	}
	if inode.Type == fsmeta.InodeTypeDirectory {
		index.RememberEmptyDirectory(mount, inode.Inode)
		return
	}
	ownerKey, err := fsmeta.EncodeInodeSessionKey(mount, inode.Inode)
	if err == nil {
		index.RememberKey(ownerKey, false)
	}
	index.RememberEmptySessionNamespace(mount, inode.Inode)
}

type perasEmptyDirectoryForgetter interface {
	ForgetEmptyDirectory(mount fsmeta.MountIdentity, inode fsmeta.InodeID)
}

func (e *Executor) forgetPerasEmptyDirectory(mount fsmeta.MountIdentity, inode fsmeta.InodeID) {
	index := e.perasPredicateIndex()
	if index == nil {
		return
	}
	forgetter, ok := index.(perasEmptyDirectoryForgetter)
	if !ok {
		return
	}
	forgetter.ForgetEmptyDirectory(mount, inode)
}

func perasDeltaAllowsAbsentObservedValue(delta compile.SemanticDelta) bool {
	return slices.Contains(delta.RuntimeGuards, compile.GuardExpiredSessionOwner)
}

func perasNotExistsDerivedFromDelta(delta compile.SemanticDelta, predicate compile.Predicate, index fsperas.PredicateIndex) bool {
	if delta.Kind != fsmeta.OperationCreate || len(delta.Plan.MutateKeys) < 2 {
		return false
	}
	if bytes.Equal(predicate.Key, delta.Plan.MutateKeys[1]) {
		return true
	}
	if !bytes.Equal(predicate.Key, delta.Plan.MutateKeys[0]) || len(delta.Authority.Parents) != 1 {
		return false
	}
	return index.DirectoryBaseEmpty(fsmeta.MountIdentity{
		MountID:    delta.Authority.Mount,
		MountKeyID: delta.Authority.MountKeyID,
	}, delta.Authority.Parents[0])
}

func (e *Executor) perasNotExistsKnown(scope compile.AuthorityScope, key []byte, index fsperas.PredicateIndex) bool {
	if index == nil || len(key) == 0 || scope.Mount == "" || scope.MountKeyID == 0 {
		return false
	}
	present, known := index.KeyState(key)
	if known {
		return !present
	}
	parts, ok := fsmeta.InspectKey(key)
	if !ok || parts.MountKeyID != scope.MountKeyID {
		return false
	}
	if parts.Kind == fsmeta.KeyKindSession {
		return index.SessionNamespaceEmpty(fsmeta.MountIdentity{
			MountID:    scope.Mount,
			MountKeyID: scope.MountKeyID,
		}, parts.Inode)
	}
	if parts.Kind != fsmeta.KeyKindDentry {
		return false
	}
	return index.DirectoryEmpty(fsmeta.MountIdentity{
		MountID:    scope.Mount,
		MountKeyID: scope.MountKeyID,
	}, parts.Parent)
}

func isPerasAdmissionTerminalError(err error) bool {
	return errors.Is(err, fsmeta.ErrExists) ||
		errors.Is(err, fsmeta.ErrNotFound) ||
		errors.Is(err, fsmeta.ErrInvalidRequest) ||
		errors.Is(err, fsmeta.ErrInvalidValue)
}
