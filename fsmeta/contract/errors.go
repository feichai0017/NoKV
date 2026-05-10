package contract

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	errExecutorRequired = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/contract: executor is required")
	errModelRequired    = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/contract: model is required")
	errMappingRequired  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/contract: inode mapping executor requires a base executor")
)
