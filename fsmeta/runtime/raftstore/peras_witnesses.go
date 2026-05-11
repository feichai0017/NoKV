package raftstore

import (
	"context"
	"fmt"
	"slices"

	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftclient "github.com/feichai0017/NoKV/raftstore/client"
	"google.golang.org/grpc"
)

type perasStoreLister interface {
	ListStores(context.Context, *coordpb.ListStoresRequest) (*coordpb.ListStoresResponse, error)
}

type perasWitnessConnections struct {
	witnesses []fsperas.WitnessReplica
	conns     []*grpc.ClientConn
}

func buildRemotePerasWitnesses(ctx context.Context, lister perasStoreLister, dialOpts []grpc.DialOption, storeIDs []uint64) (*perasWitnessConnections, error) {
	if lister == nil {
		return nil, errStoreListerRequired
	}
	resp, err := lister.ListStores(ctx, &coordpb.ListStoresRequest{})
	if err != nil {
		return nil, err
	}
	allowed := make(map[uint64]struct{}, len(storeIDs))
	for _, id := range storeIDs {
		if id != 0 {
			allowed[id] = struct{}{}
		}
	}
	out := &perasWitnessConnections{}
	for _, store := range resp.GetStores() {
		if store.GetState() != coordpb.StoreState_STORE_STATE_UP || store.GetClientAddr() == "" {
			continue
		}
		if len(allowed) > 0 {
			if _, ok := allowed[store.GetStoreId()]; !ok {
				continue
			}
		}
		conn, err := grpc.NewClient(store.GetClientAddr(), dialOpts...)
		if err != nil {
			_ = out.Close()
			return nil, fmt.Errorf("dial peras witness store %d: %w", store.GetStoreId(), err)
		}
		witness, err := raftclient.NewRemotePerasWitness(
			fmt.Sprintf("store-%d", store.GetStoreId()),
			kvrpcpb.NewStoreKVClient(conn),
		)
		if err != nil {
			_ = conn.Close()
			_ = out.Close()
			return nil, err
		}
		out.conns = append(out.conns, conn)
		out.witnesses = append(out.witnesses, witness)
	}
	if len(out.witnesses) == 0 {
		_ = out.Close()
		return nil, errPerasCommitterInvalid
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
	return out, nil
}

func (c *perasWitnessConnections) Close() error {
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
