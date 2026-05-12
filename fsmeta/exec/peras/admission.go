package peras

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

type AdmissionFunc func(context.Context, compile.SemanticDelta) (bool, error)

type AuthorityRetirer interface {
	RetirePerasAuthority(context.Context, ...compile.AuthorityScope) error
}

func Admit(ctx context.Context, delta compile.SemanticDelta, fn AdmissionFunc) error {
	if fn == nil {
		return nil
	}
	ok, err := fn(ctx, delta)
	if err != nil {
		return err
	}
	if !ok {
		return ErrAdmissionRejected
	}
	return nil
}
