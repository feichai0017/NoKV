package rootraft

import "errors"

var (
	ErrNotLeader       = errors.New("meta/root/raft: node is not leader")
	ErrNoTransport     = errors.New("meta/root/raft: transport is required for remote delivery")
	ErrUnsupportedType = errors.New("meta/root/raft: unsupported committed entry type")
)
