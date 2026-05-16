// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/proof"
)

type AdmissionResult struct {
	PredicateProofs []proof.PredicateProof
	GuardProofs     []proof.GuardProof
}

type AdmissionContext struct {
	ProofFrontier proof.ProofFrontier
}

type AdmissionFunc func(context.Context, compile.MaterializedOp, AdmissionContext) (AdmissionResult, bool, error)

type VisibleSubmission struct {
	ID OperationID
	Op compile.MaterializedOp
}

type AuthorityRetirer interface {
	RetirePerasAuthority(context.Context, ...compile.AuthorityScope) error
}

func Admit(ctx context.Context, op compile.MaterializedOp, fn AdmissionFunc, admissionCtx AdmissionContext) error {
	if err := op.ValidateForAdmissionIntent(); err != nil {
		return ErrAdmissionRejected
	}
	if fn == nil {
		if err := op.ValidateForAdmission(); err != nil {
			return ErrAdmissionRejected
		}
		return nil
	}
	result, ok, err := fn(ctx, op, admissionCtx)
	if err != nil {
		return err
	}
	if !ok {
		return ErrAdmissionRejected
	}
	if err := compile.WithAdmissionProofs(op, result.PredicateProofs, result.GuardProofs).ValidateForAdmission(); err != nil {
		return ErrAdmissionRejected
	}
	return nil
}

func AdmitAndSeal(ctx context.Context, op compile.MaterializedOp, fn AdmissionFunc, admissionCtx AdmissionContext) (compile.MaterializedOp, error) {
	if err := op.ValidateForAdmissionIntent(); err != nil {
		return compile.MaterializedOp{}, ErrAdmissionRejected
	}
	if fn == nil {
		if err := op.ValidateForAdmission(); err != nil {
			return compile.MaterializedOp{}, ErrAdmissionRejected
		}
		return op, nil
	}
	result, ok, err := fn(ctx, op, admissionCtx)
	if err != nil {
		return compile.MaterializedOp{}, err
	}
	if !ok {
		return compile.MaterializedOp{}, ErrAdmissionRejected
	}
	op = compile.WithAdmissionProofs(op, result.PredicateProofs, result.GuardProofs)
	if err := op.ValidateForAdmission(); err != nil {
		return compile.MaterializedOp{}, ErrAdmissionRejected
	}
	return op, nil
}
