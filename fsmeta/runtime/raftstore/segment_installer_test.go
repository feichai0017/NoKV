// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	runtimeperas "github.com/feichai0017/NoKV/fsmeta/runtime/peras"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/stretchr/testify/require"
)

func TestValidatePerasSegmentInstallResponseChecksRootAndCounts(t *testing.T) {
	segment := testRaftstoreInstallSegment(t, [][]byte{[]byte("dentry/a"), []byte("inode/a")})
	stats := segment.Stats()
	resp := &kvrpcpb.PerasInstallSegmentResponse{
		SegmentRoot:    append([]byte(nil), segment.Root[:]...),
		OperationCount: stats.OperationCount,
		EntryCount:     stats.EntryCount,
		AppliedEntries: 1,
	}
	require.NoError(t, validatePerasSegmentInstallResponse(segment, resp))

	resp.SegmentRoot[0] ^= 0xff
	require.ErrorIs(t, validatePerasSegmentInstallResponse(segment, resp), runtimeperas.ErrRuntimeInvalid)

	resp.SegmentRoot = append([]byte(nil), segment.Root[:]...)
	resp.EntryCount++
	require.ErrorIs(t, validatePerasSegmentInstallResponse(segment, resp), runtimeperas.ErrRuntimeInvalid)
}

func TestRaftstoreSegmentInstallerUsesLocalInstallVersion(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "a")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 10)
	require.NoError(t, err)
	segment := testRaftstoreInstallSegment(t, [][]byte{dentryKey, inodeKey})
	payload, digest := encodeRaftstoreInstallSegment(t, segment)
	stats := segment.Stats()
	kv := &fakeRaftstorePerasInstallKV{resp: &kvrpcpb.PerasInstallSegmentResponse{
		SegmentRoot:    append([]byte(nil), segment.Root[:]...),
		OperationCount: stats.OperationCount,
		EntryCount:     stats.EntryCount,
		AppliedEntries: 1,
		RegionId:       7,
		Term:           3,
		Index:          99,
		CommitVersion:  1,
	}}
	runner, err := NewRunner(kv, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 77, Count: 1}})
	require.NoError(t, err)

	installer := newRaftstoreSegmentInstaller(runner, nil)
	cursor, err := installer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:         segment,
		Payload:         payload,
		PayloadDigest:   digest,
		MaterializeMVCC: true,
	})
	require.NoError(t, err)
	require.Equal(t, runtimeperas.InstallCursor{RegionID: 7, Term: 3, Index: 99, InstallVersion: 1}, cursor)

	req := kv.lastRequest()
	require.NotNil(t, req)
	require.Equal(t, uint64(1), req.GetInstallVersion())
	require.True(t, req.GetMaterializeMvcc())
	require.Len(t, req.GetRoutingKeys(), 1)
	require.NotEmpty(t, req.GetDependencyKeys())
	require.NotEmpty(t, req.GetCatalogKeys())
	require.Len(t, req.GetMaterializedKeys(), int(stats.EntryCount))
	readHeader := segment.ReadHeaderView()
	require.Equal(t, readHeader.FirstKey, req.GetReadFirstKey())
	require.Equal(t, readHeader.LastKey, req.GetReadLastKey())
	require.Equal(t, readHeader.DentryCount, req.GetReadDentryCount())
	require.Equal(t, readHeader.InodeCount, req.GetReadInodeCount())
	require.Equal(t, readHeader.SessionCount, req.GetReadSessionCount())
	require.Equal(t, readHeader.TombstoneCount, req.GetReadTombstoneCount())
	require.Equal(t, readHeader.DirectoryCount, req.GetReadDirectoryCount())
}

func TestRaftstoreSegmentInstallerPublishesInstalledDentries(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "a")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 10)
	require.NoError(t, err)
	segment := testRaftstoreInstallSegment(t, [][]byte{dentryKey, inodeKey})
	payload, digest := encodeRaftstoreInstallSegment(t, segment)
	stats := segment.Stats()
	kv := &fakeRaftstorePerasInstallKV{resp: &kvrpcpb.PerasInstallSegmentResponse{
		SegmentRoot:    append([]byte(nil), segment.Root[:]...),
		OperationCount: stats.OperationCount,
		EntryCount:     stats.EntryCount,
		AppliedEntries: 1,
		RegionId:       7,
		Term:           3,
		Index:          99,
		CommitVersion:  1,
	}}
	runner, err := NewRunner(kv, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 77, Count: 1}})
	require.NoError(t, err)
	router := fsmetawatch.NewRouter()
	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix:          dentryKey,
		BackPressureWindow: 4,
	})
	require.NoError(t, err)
	defer sub.Close()

	installer := newRaftstoreSegmentInstaller(runner, router)
	cursor, err := installer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:       segment,
		Payload:       payload,
		PayloadDigest: digest,
	})
	require.NoError(t, err)
	require.Equal(t, runtimeperas.InstallCursor{RegionID: 7, Term: 3, Index: 99, InstallVersion: 1}, cursor)

	select {
	case evt := <-sub.Events():
		require.Equal(t, dentryKey, evt.Key)
		require.Equal(t, fsmeta.WatchCursor{RegionID: 7, Term: 3, Index: 99}, evt.Cursor)
		require.Equal(t, uint64(1), evt.CommitVersion)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for installed segment watch event")
	}
}

func TestRaftstoreSegmentInstallerInstallsCatalogRoutesInParallel(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)

	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	rootKey, err := fsmeta.EncodeInodeKey(mount, fsmeta.RootInode)
	require.NoError(t, err)
	var otherKey []byte
	for inode := fsmeta.InodeID(2); inode < 100_000; inode++ {
		if fsmeta.BucketForInodeID(inode) == fsmeta.BucketForInodeID(fsmeta.RootInode) {
			continue
		}
		otherKey, err = fsmeta.EncodeInodeKey(mount, inode)
		require.NoError(t, err)
		break
	}
	require.NotEmpty(t, otherKey)

	segment := testRaftstoreInstallSegment(t, [][]byte{rootKey, otherKey})
	routingKeys, err := runtimeperas.SegmentInstallRoutingKeys(segment, false)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(routingKeys), 2)
	payload, digest := encodeRaftstoreInstallSegment(t, segment)

	kv := &parallelRaftstorePerasInstallKV{
		stats: segment.Stats(),
		root:  segment.Root,
		delay: 20 * time.Millisecond,
	}
	runner, err := NewRunner(kv, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 77, Count: 1}})
	require.NoError(t, err)

	installer := newRaftstoreSegmentInstaller(runner, nil)
	cursor, err := installer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:       segment,
		Payload:       payload,
		PayloadDigest: digest,
	})
	require.NoError(t, err)
	require.True(t, cursor.Valid())
	require.Equal(t, uint64(1000), cursor.RegionID)
	require.Equal(t, len(routingKeys), kv.callCount())
	require.Greater(t, kv.maxInFlight(), int32(1))
	require.Equal(t, int32(1), kv.payloadRouteCount())
	require.Equal(t, int32(len(routingKeys)-1), kv.indexOnlyRouteCount())
	require.Equal(t, int32(len(routingKeys)), kv.headerRouteCount())
}

func TestRaftstoreSegmentInstallerReturnsWhenRouteErrorsExceedWorkers(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)

	segment := testRaftstoreInstallSegment(t, testRaftstoreInstallKeysAcrossBuckets(t, 4))
	routingKeys, err := runtimeperas.SegmentInstallRoutingKeys(segment, false)
	require.NoError(t, err)
	require.Greater(t, len(routingKeys), 2)
	payload, digest := encodeRaftstoreInstallSegment(t, segment)

	kv := &cancelingRaftstorePerasInstallKV{}
	runner, err := NewRunner(kv, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 77, Count: 1}})
	require.NoError(t, err)
	installer := newRaftstoreSegmentInstaller(runner, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := installer.InstallSegment(ctx, runtimeperas.SegmentInstallRequest{
			Segment:       segment,
			Payload:       payload,
			PayloadDigest: digest,
		})
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("InstallSegment blocked after more route errors than install workers")
	}
	require.Equal(t, int32(len(routingKeys)), kv.callCount())
}

func TestRaftstoreSegmentInstallerMarksInstallRetryExhaustedRoutingRetryable(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "a")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 10)
	require.NoError(t, err)
	segment := testRaftstoreInstallSegment(t, [][]byte{dentryKey, inodeKey})
	payload, digest := encodeRaftstoreInstallSegment(t, segment)
	kv := &fakeRaftstorePerasInstallKV{
		err: &client.RetryExhaustedError{
			Operation: "peras install segment",
			Key:       dentryKey,
			Detail:    "region 7 returned not_leader",
		},
	}
	runner, err := NewRunner(kv, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 77, Count: 1}})
	require.NoError(t, err)
	installer := newRaftstoreSegmentInstaller(runner, nil)

	_, err = installer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:         segment,
		Payload:         payload,
		PayloadDigest:   digest,
		MaterializeMVCC: true,
	})
	require.Error(t, err)
	require.True(t, client.IsRetryExhausted(err), "%T %v", err, err)
	require.Equal(t, nokverrors.KindRegionRouting, nokverrors.KindOf(err))
	require.True(t, nokverrors.Retryable(err), "%T %v", err, err)
}

func testRaftstoreInstallSegment(t *testing.T, keys [][]byte) fsperas.PerasSegment {
	t.Helper()
	mutations := make([]fsperas.ReplayMutation, 0, len(keys))
	for _, key := range keys {
		mutations = append(mutations, fsperas.ReplayMutation{Key: key, Value: []byte("value")})
	}
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID:      fsperas.OperationID{ClientID: "client", Seq: 1},
			Kind:      fsmeta.OperationCreate,
			Mutations: mutations,
		}},
	})
	require.NoError(t, err)
	return segment
}

func testRaftstoreInstallKeysAcrossBuckets(t *testing.T, count int) [][]byte {
	t.Helper()
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	seen := make(map[fsmeta.AffinityBucket]struct{}, count)
	keys := make([][]byte, 0, count)
	for inode := fsmeta.InodeID(1); len(keys) < count && inode < 1_000_000; inode++ {
		bucket := fsmeta.BucketForInodeID(inode)
		if _, ok := seen[bucket]; ok {
			continue
		}
		key, err := fsmeta.EncodeInodeKey(mount, inode)
		require.NoError(t, err)
		seen[bucket] = struct{}{}
		keys = append(keys, key)
	}
	require.Len(t, keys, count)
	return keys
}

func encodeRaftstoreInstallSegment(t *testing.T, segment fsperas.PerasSegment) ([]byte, [32]byte) {
	t.Helper()
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	return payload, digest
}

type fakeRaftstorePerasInstallKV struct {
	fakeRunnerKV
	mu   sync.Mutex
	req  *kvrpcpb.PerasInstallSegmentRequest
	resp *kvrpcpb.PerasInstallSegmentResponse
	err  error
}

func (f *fakeRaftstorePerasInstallKV) InstallPerasSegment(_ context.Context, _ []byte, req *kvrpcpb.PerasInstallSegmentRequest) (*kvrpcpb.PerasInstallSegmentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.req = req
	return f.resp, f.err
}

func (f *fakeRaftstorePerasInstallKV) lastRequest() *kvrpcpb.PerasInstallSegmentRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.req
}

type parallelRaftstorePerasInstallKV struct {
	fakeRunnerKV
	root            [32]byte
	stats           fsperas.SegmentStats
	delay           time.Duration
	active          atomic.Int32
	max             atomic.Int32
	calls           atomic.Int32
	payloadRoutes   atomic.Int32
	indexOnlyRoutes atomic.Int32
	headerRoutes    atomic.Int32
}

func (f *parallelRaftstorePerasInstallKV) InstallPerasSegment(ctx context.Context, _ []byte, req *kvrpcpb.PerasInstallSegmentRequest) (*kvrpcpb.PerasInstallSegmentResponse, error) {
	if len(req.GetSegmentPayload()) == 0 {
		f.indexOnlyRoutes.Add(1)
	} else {
		f.payloadRoutes.Add(1)
	}
	if len(req.GetRoutingKeys()) > 0 && len(req.GetDependencyKeys()) > 0 {
		f.headerRoutes.Add(1)
	}
	active := f.active.Add(1)
	for {
		current := f.max.Load()
		if active <= current || f.max.CompareAndSwap(current, active) {
			break
		}
	}
	defer f.active.Add(-1)
	if f.delay > 0 {
		timer := time.NewTimer(f.delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
	call := f.calls.Add(1)
	regionID := uint64(call)
	if len(req.GetSegmentPayload()) > 0 {
		regionID = 1000
	}
	return &kvrpcpb.PerasInstallSegmentResponse{
		SegmentRoot:    append([]byte(nil), f.root[:]...),
		OperationCount: f.stats.OperationCount,
		EntryCount:     f.stats.EntryCount,
		AppliedEntries: 1,
		RegionId:       regionID,
		Term:           1,
		Index:          uint64(call),
		CommitVersion:  1,
	}, nil
}

func (f *parallelRaftstorePerasInstallKV) callCount() int {
	return int(f.calls.Load())
}

func (f *parallelRaftstorePerasInstallKV) maxInFlight() int32 {
	return f.max.Load()
}

func (f *parallelRaftstorePerasInstallKV) payloadRouteCount() int32 {
	return f.payloadRoutes.Load()
}

func (f *parallelRaftstorePerasInstallKV) indexOnlyRouteCount() int32 {
	return f.indexOnlyRoutes.Load()
}

func (f *parallelRaftstorePerasInstallKV) headerRouteCount() int32 {
	return f.headerRoutes.Load()
}

type cancelingRaftstorePerasInstallKV struct {
	fakeRunnerKV
	calls atomic.Int32
}

func (f *cancelingRaftstorePerasInstallKV) InstallPerasSegment(ctx context.Context, _ []byte, _ *kvrpcpb.PerasInstallSegmentRequest) (*kvrpcpb.PerasInstallSegmentResponse, error) {
	f.calls.Add(1)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *cancelingRaftstorePerasInstallKV) callCount() int32 {
	return f.calls.Load()
}
