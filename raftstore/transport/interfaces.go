package transport

import myraft "github.com/feichai0017/NoKV/raft"

// Transport abstracts the medium used to forward raft messages between peers.
type Transport interface {
	Send(msg myraft.Message)
}
