package client

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	errAddressRequired             = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/client: address is required")
	errDirectoryReaderRequired     = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/client: directory reader is required")
	errWatchClientRequired         = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/client: watch client is required")
	errStagedPublishClientRequired = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/client: staged publish client is required")
	errWatchStreamNotConfigured    = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/client: watch stream is not configured")
	errWatchSessionNotConfigured   = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/client: watch session is not configured")
	errRPCClientNotConfigured      = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/client: rpc client is not configured")
	errWatchEventBeforeReady       = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/client: watch stream delivered event before ready")
	errConnectionNotReady          = nokverrors.New(nokverrors.KindUnavailable, "fsmeta/client: connection did not become ready")
)
