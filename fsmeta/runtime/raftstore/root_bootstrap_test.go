// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

type fakeRootStore struct {
	nextTS      uint64
	data        map[string][]byte
	getCalls    int
	mutateCalls int
}

func newFakeRootStore() *fakeRootStore {
	return &fakeRootStore{data: make(map[string][]byte)}
}

func (s *fakeRootStore) ReserveTimestamp(_ context.Context, count uint64) (uint64, error) {
	first := s.nextTS + 1
	s.nextTS += count
	return first, nil
}

func (s *fakeRootStore) Get(_ context.Context, key []byte, _ uint64) ([]byte, bool, error) {
	s.getCalls++
	value, ok := s.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (s *fakeRootStore) BatchGet(context.Context, [][]byte, uint64) (map[string][]byte, error) {
	return nil, nil
}

func (s *fakeRootStore) Scan(context.Context, []byte, uint32, uint64) ([]backend.KV, error) {
	return nil, nil
}

func (s *fakeRootStore) Mutate(_ context.Context, _ []byte, mutations []*backend.Mutation, _, _ uint64, _ uint64) (uint64, error) {
	s.mutateCalls++
	for _, mutation := range mutations {
		if mutation == nil {
			continue
		}
		if mutation.Op == backend.MutationPut {
			s.data[string(mutation.Key)] = append([]byte(nil), mutation.Value...)
		}
	}
	return s.nextTS, nil
}

func (s *fakeRootStore) MutateAtCommit(ctx context.Context, primary []byte, mutations []*backend.Mutation, startVersion, commitVersion, lockTTL uint64) (uint64, error) {
	return s.Mutate(ctx, primary, mutations, startVersion, commitVersion, lockTTL)
}

func TestRootBootstrapperWritesRootInodeOnce(t *testing.T) {
	store := newFakeRootStore()
	bootstrap := newRootBootstrapper(store, func() time.Time { return time.Unix(123, 0) })
	mount := fsmetaexec.MountAdmission{
		MountID:       "vol",
		MountKeyID:    7,
		RootInode:     model.RootInode,
		SchemaVersion: 1,
	}

	require.NoError(t, bootstrap.EnsureMountRoot(context.Background(), mount))
	require.NoError(t, bootstrap.EnsureMountRoot(context.Background(), mount))

	key, err := layout.EncodeInodeKey(mount.Identity(), model.RootInode)
	require.NoError(t, err)
	value, ok := store.data[string(key)]
	require.True(t, ok)
	inode, err := layout.DecodeInodeValue(value)
	require.NoError(t, err)
	require.Equal(t, model.RootInode, inode.Inode)
	require.Equal(t, model.InodeTypeDirectory, inode.Type)
	require.Equal(t, uint32(1), inode.LinkCount)
	require.Equal(t, int64(123*time.Second), inode.CreatedUnixNs)
	require.Equal(t, 1, store.getCalls)
	require.Equal(t, 1, store.mutateCalls)
}
