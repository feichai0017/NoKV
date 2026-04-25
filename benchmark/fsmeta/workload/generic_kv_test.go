package workload

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestGenericKVDriverCreateUsesPlainMutations(t *testing.T) {
	runner := newFakeTxnRunner()
	driver, err := NewGenericKVDriver(runner)
	require.NoError(t, err)

	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file-a",
		Inode:  42,
	}
	require.NoError(t, driver.Create(context.Background(), req, fsmeta.InodeRecord{
		Type:      fsmeta.InodeTypeFile,
		Mode:      0o644,
		LinkCount: 1,
	}))
	require.Len(t, runner.mutateCalls, 1)
	require.Len(t, runner.mutateCalls[0], 2)
	for _, mut := range runner.mutateCalls[0] {
		require.False(t, mut.GetAssertionNotExist(), "generic-KV baseline must not use native assertion")
	}

	err = driver.Create(context.Background(), req, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.ErrorIs(t, err, fsmeta.ErrExists)
	require.Len(t, runner.mutateCalls, 1, "duplicate create should stop at client-side existence check")
}

func TestGenericKVDriverReadDirPlusUsesPointGets(t *testing.T) {
	runner := newFakeTxnRunner()
	driver, err := NewGenericKVDriver(runner)
	require.NoError(t, err)

	ctx := context.Background()
	for i, name := range []string{"file-a", "file-b"} {
		require.NoError(t, driver.Create(ctx, fsmeta.CreateRequest{
			Mount:  "vol",
			Parent: fsmeta.RootInode,
			Name:   name,
			Inode:  fsmeta.InodeID(100 + i),
		}, fsmeta.InodeRecord{
			Type:      fsmeta.InodeTypeFile,
			Size:      uint64(4096 + i),
			Mode:      0o644,
			LinkCount: 1,
		}))
	}
	runner.resetCallCounters()

	pairs, err := driver.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Len(t, pairs, 2)
	require.Equal(t, "file-a", pairs[0].Dentry.Name)
	require.Equal(t, "file-b", pairs[1].Dentry.Name)
	require.Len(t, runner.getCalls, 2, "generic ReadDirPlus should issue one point Get per dentry")
	require.Zero(t, runner.batchGetCalls, "generic ReadDirPlus should not use native BatchGet fusion")
}

func TestGenericKVDriverReadDirPlusReturnsNotFoundForDanglingDentry(t *testing.T) {
	runner := newFakeTxnRunner()
	driver, err := NewGenericKVDriver(runner)
	require.NoError(t, err)

	dentryKey, err := fsmeta.EncodeDentryKey("vol", fsmeta.RootInode, "dangling")
	require.NoError(t, err)
	dentryValue, err := fsmeta.EncodeDentryValue(fsmeta.DentryRecord{
		Parent: fsmeta.RootInode,
		Name:   "dangling",
		Inode:  99,
		Type:   fsmeta.InodeTypeFile,
	})
	require.NoError(t, err)
	runner.data[string(dentryKey)] = dentryValue

	_, err = driver.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Limit:  8,
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
}

type fakeTxnRunner struct {
	ts            uint64
	data          map[string][]byte
	getCalls      [][]byte
	batchGetCalls int
	mutateCalls   [][]*kvrpcpb.Mutation
}

func newFakeTxnRunner() *fakeTxnRunner {
	return &fakeTxnRunner{
		ts:   100,
		data: make(map[string][]byte),
	}
}

func (r *fakeTxnRunner) ReserveTimestamp(_ context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errors.New("count required")
	}
	start := r.ts + 1
	r.ts += count
	return start, nil
}

func (r *fakeTxnRunner) Get(_ context.Context, key []byte, _ uint64) ([]byte, bool, error) {
	r.getCalls = append(r.getCalls, cloneBytes(key))
	value, ok := r.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return cloneBytes(value), true, nil
}

func (r *fakeTxnRunner) BatchGet(_ context.Context, keys [][]byte, _ uint64) (map[string][]byte, error) {
	r.batchGetCalls++
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if value, ok := r.data[string(key)]; ok {
			out[string(key)] = cloneBytes(value)
		}
	}
	return out, nil
}

func (r *fakeTxnRunner) Scan(_ context.Context, startKey []byte, limit uint32, _ uint64) ([]fsmetaexec.KV, error) {
	keys := make([]string, 0, len(r.data))
	for key := range r.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]fsmetaexec.KV, 0)
	for _, key := range keys {
		if bytes.Compare([]byte(key), startKey) < 0 {
			continue
		}
		out = append(out, fsmetaexec.KV{
			Key:   []byte(key),
			Value: cloneBytes(r.data[key]),
		})
		if limit > 0 && uint32(len(out)) >= limit {
			break
		}
	}
	return out, nil
}

func (r *fakeTxnRunner) Mutate(_ context.Context, _ []byte, mutations []*kvrpcpb.Mutation, _, _, _ uint64) error {
	call := make([]*kvrpcpb.Mutation, 0, len(mutations))
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		copyMut := &kvrpcpb.Mutation{
			Op:                mut.GetOp(),
			Key:               cloneBytes(mut.GetKey()),
			Value:             cloneBytes(mut.GetValue()),
			AssertionNotExist: mut.GetAssertionNotExist(),
		}
		call = append(call, copyMut)
		switch mut.GetOp() {
		case kvrpcpb.Mutation_Put:
			r.data[string(mut.GetKey())] = cloneBytes(mut.GetValue())
		case kvrpcpb.Mutation_Delete:
			delete(r.data, string(mut.GetKey()))
		}
	}
	r.mutateCalls = append(r.mutateCalls, call)
	return nil
}

func (r *fakeTxnRunner) resetCallCounters() {
	r.getCalls = nil
	r.batchGetCalls = 0
}
