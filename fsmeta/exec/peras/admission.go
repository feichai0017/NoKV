package peras

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

type AdmissionResult struct {
	PredicateProofs []compile.PredicateProof
	GuardProofs     []compile.GuardProof
}

type AdmissionFunc func(context.Context, compile.MaterializedOp) (AdmissionResult, bool, error)

type AuthorityRetirer interface {
	RetirePerasAuthority(context.Context, ...compile.AuthorityScope) error
}

func Admit(ctx context.Context, op compile.MaterializedOp, fn AdmissionFunc) error {
	if err := op.ValidateForAdmissionIntent(); err != nil {
		return ErrAdmissionRejected
	}
	if fn == nil {
		if err := op.ValidateForAdmission(); err != nil {
			return ErrAdmissionRejected
		}
		return nil
	}
	result, ok, err := fn(ctx, op)
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

func AdmitAndSeal(ctx context.Context, op compile.MaterializedOp, fn AdmissionFunc) (compile.MaterializedOp, error) {
	if err := op.ValidateForAdmissionIntent(); err != nil {
		return compile.MaterializedOp{}, ErrAdmissionRejected
	}
	if fn == nil {
		if err := op.ValidateForAdmission(); err != nil {
			return compile.MaterializedOp{}, ErrAdmissionRejected
		}
		return op, nil
	}
	result, ok, err := fn(ctx, op)
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
