package raftstore

import (
	"github.com/feichai0017/NoKV/raftstore/engine"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/transport"
)

type Config = peer.Config
type Peer = peer.Peer
type ApplyFunc = peer.ApplyFunc
type Transport = transport.Transport
type RPCTransport = transport.RPCTransport

func NewPeer(cfg *Config) (*Peer, error) {
	return peer.NewPeer(cfg)
}

func ResolveStorage(cfg *Config) (engine.PeerStorage, error) {
	return peer.ResolveStorage(cfg)
}

func NewRPCTransport(localID uint64, listenAddr string) (*RPCTransport, error) {
	return transport.NewRPCTransport(localID, listenAddr)
}
