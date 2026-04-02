package rootraft

import myraft "github.com/feichai0017/NoKV/raft"

// Transport delivers raft messages between metadata-root nodes.
//
// The first implementation can stay minimal. Root replication only needs a
// tiny message path and should not depend on raftstore peer transport.
type Transport interface {
	Send(messages []myraft.Message) error
}

type nopTransport struct{}

func (nopTransport) Send(_ []myraft.Message) error { return nil }
