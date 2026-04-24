package exec

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

type fakeRunner struct {
	nextTS    uint64
	data      map[string][]byte
	mutations [][]*kvrpcpb.Mutation
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{
		nextTS: 1,
		data:   make(map[string][]byte),
	}
}

func (r *fakeRunner) ReserveTimestamp(_ context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errors.New("zero timestamp reservation")
	}
	first := r.nextTS
	r.nextTS += count
	return first, nil
}

func (r *fakeRunner) Get(_ context.Context, key []byte, _ uint64) ([]byte, bool, error) {
	value, ok := r.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (r *fakeRunner) Scan(_ context.Context, startKey []byte, limit uint32, _ uint64) ([]KV, error) {
	keys := make([][]byte, 0, len(r.data))
	for key := range r.data {
		if bytes.Compare([]byte(key), startKey) >= 0 {
			keys = append(keys, []byte(key))
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	out := make([]KV, 0, limit)
	for _, key := range keys {
		if uint32(len(out)) >= limit {
			break
		}
		out = append(out, KV{
			Key:   append([]byte(nil), key...),
			Value: append([]byte(nil), r.data[string(key)]...),
		})
	}
	return out, nil
}

func (r *fakeRunner) Mutate(_ context.Context, _ []byte, mutations []*kvrpcpb.Mutation, _, _, _ uint64) error {
	cloned := make([]*kvrpcpb.Mutation, 0, len(mutations))
	for _, mut := range mutations {
		if mut.GetAssertionNotExist() {
			if _, ok := r.data[string(mut.GetKey())]; ok {
				return fsmeta.ErrExists
			}
		}
		cloned = append(cloned, cloneMutation(mut))
	}
	for _, mut := range cloned {
		switch mut.GetOp() {
		case kvrpcpb.Mutation_Put:
			r.data[string(mut.GetKey())] = append([]byte(nil), mut.GetValue()...)
		case kvrpcpb.Mutation_Delete:
			delete(r.data, string(mut.GetKey()))
		}
	}
	r.mutations = append(r.mutations, cloned)
	return nil
}

func TestExecutorCreateAndLookup(t *testing.T) {
	runner := newFakeRunner()
	executor, err := New(runner)
	require.NoError(t, err)

	err = executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Inode:  22,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.NoError(t, err)

	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{
		Parent: fsmeta.RootInode,
		Name:   "file",
		Inode:  22,
		Type:   fsmeta.InodeTypeFile,
	}, record)

	require.Len(t, runner.mutations, 1)
	require.Len(t, runner.mutations[0], 2)
	require.True(t, runner.mutations[0][0].GetAssertionNotExist())
	require.True(t, runner.mutations[0][1].GetAssertionNotExist())
}

func TestExecutorCreateRejectsExistingDentry(t *testing.T) {
	runner := newFakeRunner()
	executor, err := New(runner)
	require.NoError(t, err)

	req := fsmeta.CreateRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "file", Inode: 22}
	err = executor.Create(context.Background(), req, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.NoError(t, err)

	err = executor.Create(context.Background(), req, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.ErrorIs(t, err, fsmeta.ErrExists)
	require.Len(t, runner.mutations, 1)
}

func TestExecutorLookupReturnsNotFound(t *testing.T) {
	executor, err := New(newFakeRunner())
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "missing",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
}

func TestExecutorReadDirConsumesPlanCursorAndLimit(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedDentry(t, runner, "vol", 7, "b", 22)
	seedDentry(t, runner, "vol", 7, "c", 23)
	seedDentry(t, runner, "vol", 8, "outside", 99)

	executor, err := New(runner)
	require.NoError(t, err)

	records, err := executor.ReadDir(context.Background(), fsmeta.ReadDirRequest{
		Mount:      "vol",
		Parent:     7,
		StartAfter: "a",
		Limit:      1,
	})
	require.NoError(t, err)
	require.Equal(t, []fsmeta.DentryRecord{{
		Parent: 7,
		Name:   "b",
		Inode:  22,
		Type:   fsmeta.InodeTypeFile,
	}}, records)
}

func seedDentry(t *testing.T, runner *fakeRunner, mount fsmeta.MountID, parent fsmeta.InodeID, name string, inode fsmeta.InodeID) {
	t.Helper()
	key, err := fsmeta.EncodeDentryKey(mount, parent, name)
	require.NoError(t, err)
	value, err := fsmeta.EncodeDentryValue(fsmeta.DentryRecord{
		Parent: parent,
		Name:   name,
		Inode:  inode,
		Type:   fsmeta.InodeTypeFile,
	})
	require.NoError(t, err)
	runner.data[string(key)] = value
}

func cloneMutation(mut *kvrpcpb.Mutation) *kvrpcpb.Mutation {
	if mut == nil {
		return nil
	}
	return &kvrpcpb.Mutation{
		Op:                mut.GetOp(),
		Key:               append([]byte(nil), mut.GetKey()...),
		Value:             append([]byte(nil), mut.GetValue()...),
		AssertionNotExist: mut.GetAssertionNotExist(),
		ExpiresAt:         mut.GetExpiresAt(),
	}
}
