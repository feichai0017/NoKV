package exec

import (
	"context"
	"errors"
	"fmt"

	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

// KVClient is the raftstore client surface required by RaftstoreRunner.
type KVClient interface {
	Get(ctx context.Context, key []byte, version uint64) (*kvrpcpb.GetResponse, error)
	BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string]*kvrpcpb.GetResponse, error)
	Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]*kvrpcpb.KV, error)
	Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error
}

// TSOClient is the coordinator timestamp surface required by RaftstoreRunner.
type TSOClient interface {
	Tso(ctx context.Context, req *coordpb.TsoRequest) (*coordpb.TsoResponse, error)
}

// RaftstoreRunner adapts NoKV's real raftstore client and coordinator TSO into
// the fsmeta TxnRunner contract. It intentionally contains no filesystem
// semantics; fsmeta/exec remains the only interpreter of OperationPlan.
type RaftstoreRunner struct {
	kv  KVClient
	tso TSOClient
}

// NewRaftstoreRunner constructs a TxnRunner backed by raftstore KV RPCs and
// coordinator TSO. Both dependencies are explicit because timestamps do not
// belong to the KV data path.
func NewRaftstoreRunner(kv KVClient, tso TSOClient) (*RaftstoreRunner, error) {
	if kv == nil {
		return nil, errors.New("fsmeta/exec: raftstore kv client required")
	}
	if tso == nil {
		return nil, errors.New("fsmeta/exec: tso client required")
	}
	return &RaftstoreRunner{kv: kv, tso: tso}, nil
}

// ReserveTimestamp reserves count consecutive timestamps from coordinator TSO.
func (r *RaftstoreRunner) ReserveTimestamp(ctx context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errors.New("fsmeta/exec: timestamp count must be > 0")
	}
	resp, err := r.tso.Tso(ctx, &coordpb.TsoRequest{Count: count})
	if err != nil {
		return 0, err
	}
	if resp == nil {
		return 0, errors.New("fsmeta/exec: nil tso response")
	}
	if resp.GetCount() != count {
		return 0, fmt.Errorf("fsmeta/exec: tso count=%d requested=%d", resp.GetCount(), count)
	}
	if resp.GetTimestamp() == 0 {
		return 0, errors.New("fsmeta/exec: zero tso timestamp")
	}
	return resp.GetTimestamp(), nil
}

// Get returns the value visible at version.
func (r *RaftstoreRunner) Get(ctx context.Context, key []byte, version uint64) ([]byte, bool, error) {
	resp, err := r.kv.Get(ctx, key, version)
	if err != nil {
		return nil, false, err
	}
	if resp == nil || resp.GetNotFound() {
		return nil, false, nil
	}
	if keyErr := resp.GetError(); keyErr != nil {
		return nil, false, fmt.Errorf("fsmeta/exec: kv get key error: %v", keyErr)
	}
	return append([]byte(nil), resp.GetValue()...), true, nil
}

// BatchGet returns found values visible at version, keyed by string(key).
func (r *RaftstoreRunner) BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string][]byte, error) {
	responses, err := r.kv.BatchGet(ctx, keys, version)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(responses))
	for keyID, resp := range responses {
		if resp == nil || resp.GetNotFound() {
			continue
		}
		if keyErr := resp.GetError(); keyErr != nil {
			return nil, fmt.Errorf("fsmeta/exec: kv batch get key error: %v", keyErr)
		}
		out[keyID] = append([]byte(nil), resp.GetValue()...)
	}
	return out, nil
}

// Scan returns up to limit key/value pairs starting at startKey.
func (r *RaftstoreRunner) Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]KV, error) {
	kvs, err := r.kv.Scan(ctx, startKey, limit, version)
	if err != nil {
		return nil, err
	}
	out := make([]KV, 0, len(kvs))
	for _, kv := range kvs {
		if kv == nil {
			continue
		}
		out = append(out, KV{
			Key:   append([]byte(nil), kv.GetKey()...),
			Value: append([]byte(nil), kv.GetValue()...),
		})
	}
	return out, nil
}

// Mutate delegates to raftstore's two-phase commit path.
func (r *RaftstoreRunner) Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error {
	return r.kv.Mutate(ctx, primary, mutations, startVersion, commitVersion, lockTTL)
}
