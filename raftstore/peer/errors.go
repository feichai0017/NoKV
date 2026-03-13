package peer

import "errors"

// ErrMissingLocalMetaOrWAL indicates WAL and local raft metadata must be provided together.
var ErrMissingLocalMetaOrWAL = errors.New("raftstore/peer: WAL and local metadata must both be provided")
