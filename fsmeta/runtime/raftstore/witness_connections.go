package raftstore

import (
	"context"
	"fmt"
	"slices"
	"time"

	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftclient "github.com/feichai0017/NoKV/raftstore/client"
	"google.golang.org/grpc"
)

const (
	perasWitnessDiscoveryTimeout = 45 * time.Second
	perasWitnessDiscoveryBackoff = 100 * time.Millisecond
)

type witnessStoreLister interface {
	ListStores(context.Context, *coordpb.ListStoresRequest) (*coordpb.ListStoresResponse, error)
}

type witnessConnections struct {
	witnesses []fsperas.WitnessReplica
	conns     []*grpc.ClientConn
}

func buildWitnessConnections(ctx context.Context, lister witnessStoreLister, dialOpts []grpc.DialOption, storeIDs []uint64) (*witnessConnections, error) {
	if lister == nil {
		return nil, errStoreListerRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, perasWitnessDiscoveryTimeout)
	defer cancel()

	allowed := make(map[uint64]struct{}, len(storeIDs))
	for _, id := range storeIDs {
		if id != 0 {
			allowed[id] = struct{}{}
		}
	}
	for {
		out, complete, err := tryBuildWitnessConnections(ctx, lister, dialOpts, allowed)
		if err != nil {
			return nil, err
		}
		if complete {
			return out, nil
		}
		if out != nil {
			_ = out.Close()
		}
		timer := time.NewTimer(perasWitnessDiscoveryBackoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, runtimeperas.ErrRuntimeInvalid
		case <-timer.C:
		}
	}
}

func tryBuildWitnessConnections(ctx context.Context, lister witnessStoreLister, dialOpts []grpc.DialOption, allowed map[uint64]struct{}) (*witnessConnections, bool, error) {
	resp, err := lister.ListStores(ctx, &coordpb.ListStoresRequest{})
	if err != nil {
		return nil, false, err
	}
	out := &witnessConnections{}
	seen := make(map[uint64]struct{}, len(allowed))
	for _, store := range resp.GetStores() {
		if !witnessStoreSelected(store, allowed) {
			continue
		}
		if len(allowed) > 0 {
			seen[store.GetStoreId()] = struct{}{}
		}
		conn, err := grpc.NewClient(store.GetClientAddr(), dialOpts...)
		if err != nil {
			_ = out.Close()
			return nil, false, fmt.Errorf("dial peras witness store %d: %w", store.GetStoreId(), err)
		}
		witness, err := raftclient.NewRemotePerasWitness(
			fmt.Sprintf("store-%d", store.GetStoreId()),
			kvrpcpb.NewStoreKVClient(conn),
		)
		if err != nil {
			_ = conn.Close()
			_ = out.Close()
			return nil, false, err
		}
		out.conns = append(out.conns, conn)
		out.witnesses = append(out.witnesses, witness)
	}
	complete := len(out.witnesses) > 0
	if len(allowed) > 0 {
		complete = len(seen) == len(allowed)
	}
	if !complete {
		return out, false, nil
	}
	slices.SortFunc(out.witnesses, func(left, right fsperas.WitnessReplica) int {
		if left.ID() < right.ID() {
			return -1
		}
		if left.ID() > right.ID() {
			return 1
		}
		return 0
	})
	return out, true, nil
}

func witnessStoreSelected(store *coordpb.StoreInfo, allowed map[uint64]struct{}) bool {
	if store == nil || store.GetState() != coordpb.StoreState_STORE_STATE_UP || store.GetClientAddr() == "" {
		return false
	}
	if len(allowed) == 0 {
		return true
	}
	_, ok := allowed[store.GetStoreId()]
	return ok
}

func (c *witnessConnections) Close() error {
	if c == nil {
		return nil
	}
	var first error
	for _, conn := range c.conns {
		if conn == nil {
			continue
		}
		if err := conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	c.conns = nil
	c.witnesses = nil
	return first
}
