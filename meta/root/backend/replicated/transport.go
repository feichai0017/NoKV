package replicated

import myraft "github.com/feichai0017/NoKV/raft"

// MessageHandler consumes one incoming raft message for the replicated
// metadata root backend.
type MessageHandler func(myraft.Message) error

// Transport carries raft messages between replicated metadata root nodes.
// The first implementation is gRPC-backed; higher layers should depend on this
// narrow surface instead of in-process routing details.
type Transport interface {
	Addr() string
	SetHandler(MessageHandler)
	SetPeer(id uint64, addr string)
	SetPeers(peers map[uint64]string)
	Send(msgs ...myraft.Message) error
	Close() error
}
