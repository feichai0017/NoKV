package contract

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestFSMetaExecutorModelContract(t *testing.T) {
	seeds := envInt("NOKV_CONTRACT_SEEDS", 16)
	steps := envInt("NOKV_CONTRACT_STEPS", 80)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%03d", seed), func(t *testing.T) {
			model := NewModel("vol")
			runner := newVersionedRunner()
			executor, err := fsmetaexec.New(runner, fsmetaexec.WithClock(func() time.Time {
				return time.Unix(0, model.NowUnixNs)
			}))
			require.NoError(t, err)

			ops := GenerateScript(seed, steps)
			err = Run(context.Background(), executor, model, ops)
			require.NoError(t, err, "seed=%d steps=%d", seed, steps)
		})
	}
}

func envInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

type versionedRunner struct {
	mu     sync.Mutex
	nextTS uint64
	data   map[string][]versionedValue
}

type versionedValue struct {
	version uint64
	value   []byte
	deleted bool
}

func newVersionedRunner() *versionedRunner {
	return &versionedRunner{
		nextTS: 1,
		data:   make(map[string][]versionedValue),
	}
}

func (r *versionedRunner) ReserveTimestamp(_ context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errors.New("zero timestamp reservation")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	first := r.nextTS
	r.nextTS += count
	return first, nil
}

func (r *versionedRunner) Get(_ context.Context, key []byte, version uint64) ([]byte, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.visibleLocked(key, version)
	return value, ok, nil
}

func (r *versionedRunner) BatchGet(_ context.Context, keys [][]byte, version uint64) (map[string][]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if value, ok := r.visibleLocked(key, version); ok {
			out[string(key)] = value
		}
	}
	return out, nil
}

func (r *versionedRunner) Scan(_ context.Context, startKey []byte, limit uint32, version uint64) ([]fsmetaexec.KV, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([][]byte, 0, len(r.data))
	for key := range r.data {
		raw := []byte(key)
		if bytes.Compare(raw, startKey) < 0 {
			continue
		}
		if _, ok := r.visibleLocked(raw, version); ok {
			keys = append(keys, append([]byte(nil), raw...))
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	out := make([]fsmetaexec.KV, 0, limit)
	for _, key := range keys {
		if uint32(len(out)) >= limit {
			break
		}
		value, _ := r.visibleLocked(key, version)
		out = append(out, fsmetaexec.KV{
			Key:   append([]byte(nil), key...),
			Value: value,
		})
	}
	return out, nil
}

func (r *versionedRunner) Mutate(_ context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, _ uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, mut := range mutations {
		if mut.GetAssertionNotExist() {
			if _, ok := r.visibleLocked(mut.GetKey(), startVersion); ok {
				return fsmeta.ErrExists
			}
			if _, ok := r.visibleLatestLocked(mut.GetKey()); ok {
				return fsmeta.ErrExists
			}
		}
		if bytes.Equal(mut.GetKey(), primary) && mut.GetOp() == kvrpcpb.Mutation_Delete {
			if _, ok := r.visibleLatestLocked(mut.GetKey()); !ok {
				return fsmeta.ErrNotFound
			}
		}
	}
	for _, mut := range mutations {
		key := string(mut.GetKey())
		switch mut.GetOp() {
		case kvrpcpb.Mutation_Put:
			r.data[key] = append(r.data[key], versionedValue{
				version: commitVersion,
				value:   append([]byte(nil), mut.GetValue()...),
			})
		case kvrpcpb.Mutation_Delete:
			r.data[key] = append(r.data[key], versionedValue{
				version: commitVersion,
				deleted: true,
			})
		default:
			return fsmeta.ErrInvalidRequest
		}
	}
	return nil
}

func (r *versionedRunner) visibleLatestLocked(key []byte) ([]byte, bool) {
	return r.visibleLocked(key, ^uint64(0))
}

func (r *versionedRunner) visibleLocked(key []byte, version uint64) ([]byte, bool) {
	versions := r.data[string(key)]
	var (
		best      versionedValue
		bestFound bool
	)
	for _, candidate := range versions {
		if candidate.version > version {
			continue
		}
		if !bestFound || candidate.version > best.version {
			best = candidate
			bestFound = true
		}
	}
	if !bestFound || best.deleted {
		return nil, false
	}
	return append([]byte(nil), best.value...), true
}
