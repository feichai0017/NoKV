package peer

import "errors"

// ErrMissingManifestOrWAL indicates WAL and manifest must be provided together.
var ErrMissingManifestOrWAL = errors.New("raftstore/peer: WAL and manifest must both be provided")
