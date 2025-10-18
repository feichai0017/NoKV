package raftstore

import (
	"time"

	"github.com/feichai0017/NoKV/raftstore/engine"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/transport"
	"google.golang.org/grpc/credentials"
)

type Config = peer.Config
type Peer = peer.Peer
type ApplyFunc = peer.ApplyFunc
type Transport = transport.Transport
type GRPCTransport = transport.GRPCTransport
type GRPCOption = transport.GRPCOption
type Store = store.Store
type Router = store.Router

func NewPeer(cfg *Config) (*Peer, error) {
	return peer.NewPeer(cfg)
}

func NewRouter() *Router {
	return store.NewRouter()
}

func NewStore(router *Router) *Store {
	return store.NewStore(router)
}

func ResolveStorage(cfg *Config) (engine.PeerStorage, error) {
	return peer.ResolveStorage(cfg)
}

func NewGRPCTransport(localID uint64, listenAddr string, opts ...GRPCOption) (*GRPCTransport, error) {
	return transport.NewGRPCTransport(localID, listenAddr, opts...)
}

func WithGRPCServerCredentials(creds credentials.TransportCredentials) GRPCOption {
	return transport.WithServerCredentials(creds)
}

func WithGRPCClientCredentials(creds credentials.TransportCredentials) GRPCOption {
	return transport.WithClientCredentials(creds)
}

func WithGRPCDialTimeout(d time.Duration) GRPCOption {
	return transport.WithDialTimeout(d)
}

func WithGRPCSendTimeout(d time.Duration) GRPCOption {
	return transport.WithSendTimeout(d)
}

func WithGRPCRetry(maxRetries int, backoff time.Duration) GRPCOption {
	return transport.WithRetry(maxRetries, backoff)
}
