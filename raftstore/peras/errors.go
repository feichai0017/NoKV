package peras

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	ErrWitnessNodeConfigInvalid = nokverrors.New(nokverrors.KindInvalidArgument, "raftstore/peras: invalid witness node config")
	ErrWitnessAuthorityMissing  = nokverrors.New(nokverrors.KindStaleEpoch, "raftstore/peras: missing active authority")
	ErrWitnessAuthorityMismatch = nokverrors.New(nokverrors.KindStaleEpoch, "raftstore/peras: authority mismatch")
	ErrInvalidInstallRequest    = nokverrors.New(nokverrors.KindProtocolViolation, "raftstore/peras: invalid install request")
)
