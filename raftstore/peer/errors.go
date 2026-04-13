package peer

import "errors"

var (
	// errPeerStopped indicates that the raft peer has already shut down.
	errPeerStopped = errors.New("raftstore: peer stopped")
	// errNilConfig indicates that peer construction received a nil config.
	errNilConfig = errors.New("raftstore: config is nil")
	// errNilTransport indicates that peer construction needs a transport implementation.
	errNilTransport = errors.New("raftstore: transport must be provided")
	// errNilApplyFunc indicates that peer construction needs an apply callback.
	errNilApplyFunc = errors.New("raftstore: apply function must be provided")
	// errZeroRaftID indicates that peer construction needs a non-zero raft id.
	errZeroRaftID = errors.New("raftstore: raft config must specify ID")
)
