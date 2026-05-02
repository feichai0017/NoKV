package contract

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
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
	first := r.nextTS
	r.nextTS += count
	return first, nil
}

func (r *versionedRunner) Get(_ context.Context, key []byte, version uint64) ([]byte, bool, error) {
	value, ok := r.visible(key, version)
	return value, ok, nil
}

func (r *versionedRunner) BatchGet(_ context.Context, keys [][]byte, version uint64) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if value, ok := r.visible(key, version); ok {
			out[string(key)] = value
		}
	}
	return out, nil
}

func (r *versionedRunner) Scan(_ context.Context, startKey []byte, limit uint32, version uint64) ([]fsmetaexec.KV, error) {
	keys := make([][]byte, 0, len(r.data))
	for key := range r.data {
		raw := []byte(key)
		if bytes.Compare(raw, startKey) < 0 {
			continue
		}
		if _, ok := r.visible(raw, version); ok {
			keys = append(keys, append([]byte(nil), raw...))
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	out := make([]fsmetaexec.KV, 0, limit)
	for _, key := range keys {
		if uint32(len(out)) >= limit {
			break
		}
		value, _ := r.visible(key, version)
		out = append(out, fsmetaexec.KV{
			Key:   append([]byte(nil), key...),
			Value: value,
		})
	}
	return out, nil
}

func (r *versionedRunner) Mutate(_ context.Context, _ []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, _ uint64) error {
	for _, mut := range mutations {
		if mut.GetAssertionNotExist() {
			if _, ok := r.visible(mut.GetKey(), startVersion); ok {
				return fsmeta.ErrExists
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

func (r *versionedRunner) visible(key []byte, version uint64) ([]byte, bool) {
	versions := r.data[string(key)]
	for i := len(versions) - 1; i >= 0; i-- {
		candidate := versions[i]
		if candidate.version > version {
			continue
		}
		if candidate.deleted {
			return nil, false
		}
		return append([]byte(nil), candidate.value...), true
	}
	return nil, false
}
