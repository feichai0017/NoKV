package rootview

import (
	"context"
	"fmt"
	rootclient "github.com/feichai0017/NoKV/meta/root/client"
	"google.golang.org/grpc"
	"strings"
	"time"
)

const defaultRemoteRootDialTimeout = 5 * time.Second

// RemoteRootConfig describes one coordinator-side client connection set for a
// remote metadata-root deployment.
type RemoteRootConfig struct {
	Targets     map[uint64]string
	DialTimeout time.Duration
	DialOptions []grpc.DialOption
}

// OpenRootRemoteStore opens one Coordinator storage backend backed by a remote
// metadata-root RPC service.
func OpenRootRemoteStore(cfg RemoteRootConfig) (*RootStore, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	timeout := cfg.DialTimeout
	if timeout <= 0 {
		timeout = defaultRemoteRootDialTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := rootclient.DialCluster(ctx, cfg.Targets, cfg.DialOptions...)
	if err != nil {
		return nil, err
	}
	store, err := OpenRootStore(&remoteRootBackend{Client: client})
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return store, nil
}

func (cfg RemoteRootConfig) Validate() error {
	if len(cfg.Targets) == 0 {
		return fmt.Errorf("coordinator/rootview: remote root mode requires at least one target")
	}
	for id, addr := range cfg.Targets {
		if id == 0 {
			return fmt.Errorf("coordinator/rootview: remote root target ids must be > 0")
		}
		if strings.TrimSpace(addr) == "" {
			return fmt.Errorf("coordinator/rootview: missing remote root address for node %d", id)
		}
	}
	return nil
}

type remoteRootBackend struct {
	*rootclient.Client
}

func (b *remoteRootBackend) IsLeader() bool {
	// A remote coordinator is not co-located with one root raft peer. Writes are
	// routed by the remote client to the current metadata-root leader, so the
	// coordinator-side RootStorage should not reject writes based on whichever
	// endpoint happens to be preferred locally.
	return b.Client != nil
}
