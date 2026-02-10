package raftstore

import (
	"time"

	"github.com/feichai0017/NoKV/raftstore/engine"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/server"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Config defines an exported API type.
type Config = peer.Config

// Peer defines an exported API type.
type Peer = peer.Peer

// ApplyFunc defines an exported API type.
type ApplyFunc = peer.ApplyFunc

// Transport defines an exported API type.
type Transport = transport.Transport

// GRPCTransport defines an exported API type.
type GRPCTransport = transport.GRPCTransport

// GRPCOption defines an exported API type.
type GRPCOption = transport.GRPCOption

// Store defines an exported API type.
type Store = store.Store

// Router defines an exported API type.
type Router = store.Router

// StoreConfig defines an exported API type.
type StoreConfig = store.Config

// StorePeerFactory defines an exported API type.
type StorePeerFactory = store.PeerFactory

// StoreLifecycleHooks defines an exported API type.
type StoreLifecycleHooks = store.LifecycleHooks

// StorePeerHandle defines an exported API type.
type StorePeerHandle = store.PeerHandle

// StoreRegionHooks defines an exported API type.
type StoreRegionHooks = store.RegionHooks

// ServerConfig defines an exported API type.
type ServerConfig = server.Config

// Server defines an exported API type.
type Server = server.Server

// NewPeer creates a new value for the API.
func NewPeer(cfg *Config) (*Peer, error) {
	return peer.NewPeer(cfg)
}

// NewRouter creates a new value for the API.
func NewRouter() *Router {
	return store.NewRouter()
}

// NewStore creates a new value for the API.
func NewStore(router *Router) *Store {
	return store.NewStore(router)
}

// NewStoreWithConfig creates a new value for the API.
func NewStoreWithConfig(cfg StoreConfig) *Store {
	st := store.NewStoreWithConfig(cfg)
	store.RegisterStore(st)
	return st
}

// ResolveStorage is part of the exported package API.
func ResolveStorage(cfg *Config) (engine.PeerStorage, error) {
	return peer.ResolveStorage(cfg)
}

// NewServer creates a new value for the API.
func NewServer(cfg ServerConfig) (*Server, error) {
	return server.New(cfg)
}

// NewGRPCTransport creates a new value for the API.
func NewGRPCTransport(localID uint64, listenAddr string, opts ...GRPCOption) (*GRPCTransport, error) {
	return transport.NewGRPCTransport(localID, listenAddr, opts...)
}

// WithGRPCServerCredentials is part of the exported package API.
func WithGRPCServerCredentials(creds credentials.TransportCredentials) GRPCOption {
	return transport.WithServerCredentials(creds)
}

// WithGRPCClientCredentials is part of the exported package API.
func WithGRPCClientCredentials(creds credentials.TransportCredentials) GRPCOption {
	return transport.WithClientCredentials(creds)
}

// WithGRPCDialTimeout is part of the exported package API.
func WithGRPCDialTimeout(d time.Duration) GRPCOption {
	return transport.WithDialTimeout(d)
}

// WithGRPCSendTimeout is part of the exported package API.
func WithGRPCSendTimeout(d time.Duration) GRPCOption {
	return transport.WithSendTimeout(d)
}

// WithGRPCRetry is part of the exported package API.
func WithGRPCRetry(maxRetries int, backoff time.Duration) GRPCOption {
	return transport.WithRetry(maxRetries, backoff)
}

// WithGRPCServerRegistrar is part of the exported package API.
func WithGRPCServerRegistrar(regs ...func(grpc.ServiceRegistrar)) GRPCOption {
	return transport.WithServerRegistrar(regs...)
}
