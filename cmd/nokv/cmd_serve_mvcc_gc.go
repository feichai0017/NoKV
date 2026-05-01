package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/config"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	rootclient "github.com/feichai0017/NoKV/meta/root/client"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

type serveTSOSource struct {
	coord   *coordclient.GRPCClient
	timeout time.Duration
	lag     uint64
	mu      sync.Mutex
}

func newServeTSOSource(coord *coordclient.GRPCClient, timeout time.Duration, lag uint64) *serveTSOSource {
	if coord == nil || lag == 0 {
		return nil
	}
	return &serveTSOSource{
		coord:   coord,
		timeout: timeout,
		lag:     lag,
	}
}

func (s *serveTSOSource) Current() uint64 {
	if s == nil || s.coord == nil {
		return 0
	}
	timeout := s.timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := s.coord.Tso(ctx, &coordpb.TsoRequest{Count: 1})
	if err != nil {
		return 0
	}
	return resp.GetTimestamp()
}

func (s *serveTSOSource) SafePoint() uint64 {
	current := s.Current()
	if current <= s.lag {
		return 0
	}
	return current - s.lag
}

type serveRootRetentionSource struct {
	client *rootclient.Client
	mu     sync.Mutex
}

func newServeRootRetentionSource(ctx context.Context, cfg *config.File, scope, explicitAddr string) (*serveRootRetentionSource, error) {
	targets := resolveServeMetaRootTargets(cfg, scope, explicitAddr)
	if len(targets) == 0 {
		return nil, fmt.Errorf("MVCC GC requires metadata-root addresses via --mvcc-gc-meta-root-addr or config meta_root")
	}
	client, err := rootclient.DialCluster(ctx, targets)
	if err != nil {
		return nil, err
	}
	return &serveRootRetentionSource{client: client}, nil
}

func (s *serveRootRetentionSource) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *serveRootRetentionSource) Retention() rootstate.SnapshotRetentionIndex {
	if s == nil || s.client == nil {
		return rootstate.SnapshotRetentionIndex{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot, err := s.client.Snapshot()
	if err != nil {
		return rootstate.SnapshotRetentionIndex{}
	}
	return snapshot.SnapshotRetentionIndex()
}

func resolveServeMetaRootTargets(cfg *config.File, scope, explicitAddr string) map[uint64]string {
	addr := strings.TrimSpace(explicitAddr)
	if addr != "" {
		return map[uint64]string{1: addr}
	}
	if cfg == nil {
		return nil
	}
	return cfg.MetaRootServicePeers(scope)
}
