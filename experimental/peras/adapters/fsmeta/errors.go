// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package fsmeta

import (
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

var (
	errStoreListerRequired             = nokverrors.New(nokverrors.KindInvalidArgument, "experimental/peras/adapters/fsmeta: store lister is required")
	errPerasInstallRouteRetryExhausted = nokverrors.New(nokverrors.KindRegionRouting, "experimental/peras/adapters/fsmeta: peras install route retry budget exhausted")
)

func runnerKeyError(op string, keyErr *kvrpcpb.KeyError) error {
	if keyErr == nil {
		return nil
	}
	return fmt.Errorf("experimental/peras/adapters/fsmeta: %s: %w", op, nokverrors.NewTxnKeyError(keyErr))
}

type perasInstallRouteRetryExhaustedError struct {
	cause error
}

func (e perasInstallRouteRetryExhaustedError) Error() string {
	if e.cause == nil {
		return errPerasInstallRouteRetryExhausted.Error()
	}
	return fmt.Sprintf("%s: %v", errPerasInstallRouteRetryExhausted, e.cause)
}

func (e perasInstallRouteRetryExhaustedError) Unwrap() error {
	return e.cause
}

func (e perasInstallRouteRetryExhaustedError) ErrorKind() nokverrors.Kind {
	return nokverrors.KindRegionRouting
}
