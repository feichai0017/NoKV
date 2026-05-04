package raftstore

import (
	"context"

	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

// KVClient is the raftstore client surface required by Runner.
type KVClient interface {
	Get(ctx context.Context, key []byte, version uint64) (*kvrpcpb.GetResponse, error)
	BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string]*kvrpcpb.GetResponse, error)
	Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]*kvrpcpb.KV, error)
	Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error
}

type fsmetaCreateFastPath interface {
	FSMetaCreate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (bool, error)
}

// TSOClient is the coordinator timestamp surface required by Runner.
type TSOClient interface {
	Tso(ctx context.Context, req *coordpb.TsoRequest) (*coordpb.TsoResponse, error)
}

// Runner adapts NoKV's real raftstore client and coordinator TSO into the
// fsmeta TxnRunner contract. It intentionally contains no filesystem
// semantics; fsmeta/exec remains the only interpreter of OperationPlan.
type Runner struct {
	kv  KVClient
	tso TSOClient
}

// NewRunner constructs a TxnRunner backed by raftstore KV RPCs and coordinator
// TSO. Both dependencies are explicit because timestamps do not belong to the
// KV data path.
func NewRunner(kv KVClient, tso TSOClient) (*Runner, error) {
	if kv == nil {
		return nil, errKVClientRequired
	}
	if tso == nil {
		return nil, errTSOClientRequired
	}
	return &Runner{kv: kv, tso: tso}, nil
}

// ReserveTimestamp reserves count consecutive timestamps from coordinator TSO.
func (r *Runner) ReserveTimestamp(ctx context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errTimestampCountRequired
	}
	resp, err := r.tso.Tso(ctx, &coordpb.TsoRequest{Count: count})
	if err != nil {
		return 0, err
	}
	if resp == nil {
		return 0, errNilTSOResponse
	}
	if resp.GetCount() != count {
		return 0, errTSOCountMismatch(resp.GetCount(), count)
	}
	if resp.GetTimestamp() == 0 {
		return 0, errZeroTSOTimestamp
	}
	return resp.GetTimestamp(), nil
}

// Get returns the value visible at version.
func (r *Runner) Get(ctx context.Context, key []byte, version uint64) ([]byte, bool, error) {
	resp, err := r.kv.Get(ctx, key, version)
	if err != nil {
		return nil, false, err
	}
	if resp == nil || resp.GetNotFound() {
		return nil, false, nil
	}
	if keyErr := resp.GetError(); keyErr != nil {
		return nil, false, runnerKeyError("kv get", keyErr)
	}
	return append([]byte(nil), resp.GetValue()...), true, nil
}

// BatchGet returns found values visible at version, keyed by string(key).
func (r *Runner) BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string][]byte, error) {
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
			return nil, runnerKeyError("kv batch get", keyErr)
		}
		out[keyID] = append([]byte(nil), resp.GetValue()...)
	}
	return out, nil
}

// Scan returns up to limit key/value pairs starting at startKey.
func (r *Runner) Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]fsmetaexec.KV, error) {
	kvs, err := r.kv.Scan(ctx, startKey, limit, version)
	if err != nil {
		return nil, err
	}
	out := make([]fsmetaexec.KV, 0, len(kvs))
	for _, kv := range kvs {
		if kv == nil {
			continue
		}
		out = append(out, fsmetaexec.KV{
			Key:   append([]byte(nil), kv.GetKey()...),
			Value: append([]byte(nil), kv.GetValue()...),
		})
	}
	return out, nil
}

// Mutate delegates to raftstore's two-phase commit path.
func (r *Runner) Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error {
	return r.kv.Mutate(ctx, primary, mutations, startVersion, commitVersion, lockTTL)
}

// FSMetaCreate delegates to the region-local fsmeta create fast path when the
// underlying KV client supports it. handled=false means callers should keep the
// regular Percolator 2PC path.
func (r *Runner) FSMetaCreate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (bool, error) {
	fast, ok := r.kv.(fsmetaCreateFastPath)
	if !ok {
		return false, nil
	}
	return fast.FSMetaCreate(ctx, primary, mutations, startVersion, commitVersion)
}
