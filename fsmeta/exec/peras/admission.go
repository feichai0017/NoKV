package peras

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

type AdmissionFunc func(context.Context, compile.MaterializedOp) (bool, error)

type AuthorityRetirer interface {
	RetirePerasAuthority(context.Context, ...compile.AuthorityScope) error
}

func Admit(ctx context.Context, op compile.MaterializedOp, fn AdmissionFunc) error {
	if err := op.ValidateForAdmission(); err != nil {
		return ErrAdmissionRejected
	}
	if fn == nil {
		return nil
	}
	ok, err := fn(ctx, op)
	if err != nil {
		return err
	}
	if !ok {
		return ErrAdmissionRejected
	}
	return nil
}
