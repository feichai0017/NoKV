package rootraft

import (
	"context"
	"strings"
	"sync"

	metapb "github.com/feichai0017/NoKV/pb/meta"
	myraft "github.com/feichai0017/NoKV/raft"
	pbraft "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCTransport struct {
	localID uint64
	peers   map[uint64]string

	mu    sync.Mutex
	conns map[uint64]*grpc.ClientConn
}

func NewGRPCTransport(localID uint64, peers []Peer) *GRPCTransport {
	addrs := make(map[uint64]string, len(peers))
	for _, peer := range peers {
		addrs[peer.ID] = strings.TrimSpace(peer.Address)
	}
	return &GRPCTransport{
		localID: localID,
		peers:   addrs,
		conns:   make(map[uint64]*grpc.ClientConn),
	}
}

func (t *GRPCTransport) Send(messages []myraft.Message) error {
	for _, msg := range messages {
		if msg.To == 0 || msg.To == t.localID {
			continue
		}
		client, err := t.clientFor(msg.To)
		if err != nil {
			return err
		}
		pbMsg := pbraft.Message(msg)
		payload, err := (&pbMsg).Marshal()
		if err != nil {
			return err
		}
		if _, err := client.Step(context.Background(), &metapb.RootRaftWireMessage{Payload: payload}); err != nil {
			return err
		}
	}
	return nil
}

func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, conn := range t.conns {
		_ = conn.Close()
		delete(t.conns, id)
	}
	return nil
}

func (t *GRPCTransport) clientFor(id uint64) (metapb.MetadataRootServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if conn, ok := t.conns[id]; ok {
		return metapb.NewMetadataRootServiceClient(conn), nil
	}
	addr := t.peers[id]
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[id] = conn
	return metapb.NewMetadataRootServiceClient(conn), nil
}
