package raftstore

import (
	"context"
	"sync/atomic"

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

type atomicMutateFastPath interface {
	TryAtomicMutate(ctx context.Context, primary []byte, predicates []*kvrpcpb.AtomicPredicate, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (bool, error)
}

type commitTimestampMutator interface {
	MutateWithCommitTimestamp(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, lockTTL uint64, allocateCommitVersion func(context.Context) (uint64, error)) (uint64, error)
}

type statsProvider interface {
	Stats() map[string]any
}

// TSOClient is the coordinator timestamp surface required by Runner.
type TSOClient interface {
	Tso(ctx context.Context, req *coordpb.TsoRequest) (*coordpb.TsoResponse, error)
}

// Runner adapts NoKV's real raftstore client and coordinator TSO into the
// fsmeta TxnRunner contract. It intentionally contains no filesystem
// semantics; fsmeta/exec remains the only interpreter of OperationPlan.
type Runner struct {
	kv                           KVClient
	tso                          TSOClient
	atomicRunnerUnsupportedTotal atomic.Uint64
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

// Mutate delegates to raftstore's two-phase commit path and returns the commit
// timestamp that actually published the mutation. Real raftstore clients may
// allocate commit_ts after prewrite; callers that publish root frontiers must
// use the returned timestamp instead of the speculative one they passed in.
func (r *Runner) Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) (uint64, error) {
	if kv, ok := r.kv.(commitTimestampMutator); ok {
		// The executor still passes a preallocated commitVersion for in-memory
		// test runners. Real raftstore clients can allocate commit_ts after
		// prewrite, which avoids repeated CommitTsExpired under mixed reads.
		return kv.MutateWithCommitTimestamp(ctx, primary, mutations, startVersion, lockTTL, func(ctx context.Context) (uint64, error) {
			return r.ReserveTimestamp(ctx, 1)
		})
	}
	if err := r.kv.Mutate(ctx, primary, mutations, startVersion, commitVersion, lockTTL); err != nil {
		return 0, err
	}
	return commitVersion, nil
}

// MutateAtCommit uses the caller-provided commitVersion exactly. fsmeta uses
// this for root-authority protocols that publish the commit frontier outside the
// KV data path before the transaction can safely allocate a later timestamp.
func (r *Runner) MutateAtCommit(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) (uint64, error) {
	if err := r.kv.Mutate(ctx, primary, mutations, startVersion, commitVersion, lockTTL); err != nil {
		return commitVersion, err
	}
	return commitVersion, nil
}

// Stats returns runtime-adapter counters. Nested KV stats come from the real
// raftstore client when available, keeping fsmeta expvar useful without making
// optional observability part of KVClient.
func (r *Runner) Stats() map[string]any {
	if r == nil {
		return map[string]any{
			"atomic_runner_unsupported_total": uint64(0),
		}
	}
	out := map[string]any{
		"atomic_runner_unsupported_total": r.atomicRunnerUnsupportedTotal.Load(),
	}
	if stats, ok := r.kv.(statsProvider); ok {
		out["kv"] = stats.Stats()
	}
	return out
}

// TryAtomicMutate delegates to the region-local 1PC fast path when the
// underlying KV client supports it. handled=false means callers should keep
// the regular Percolator 2PC path.
func (r *Runner) TryAtomicMutate(ctx context.Context, primary []byte, predicates []*kvrpcpb.AtomicPredicate, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (bool, error) {
	fast, ok := r.kv.(atomicMutateFastPath)
	if !ok {
		r.atomicRunnerUnsupportedTotal.Add(1)
		return false, nil
	}
	return fast.TryAtomicMutate(ctx, primary, predicates, mutations, startVersion, commitVersion)
}
