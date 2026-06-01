// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

type fakeRunner struct {
	nextTS              uint64
	data                map[string][]byte
	mutations           [][]*backend.Mutation
	getCalls            int
	getVersions         []uint64
	scanVersions        []uint64
	batchVersions       []uint64
	scanErrs            []error
	batchErrs           []error
	timestampErrs       []error
	beforeBatchGet      func()
	mutateErr           error
	mutateErrs          []error
	actualCommitVersion uint64
}

type metadataPredicateCommitCall struct {
	primary       []byte
	predicates    []*backend.Predicate
	mutations     []*backend.Mutation
	startVersion  uint64
	commitVersion uint64
}

type fakePredicateRunner struct {
	*fakeRunner
	err            error
	predicateCalls []metadataPredicateCommitCall
}

var testMountIdentity = model.MountIdentity{MountID: "vol", MountKeyID: 1}

func testMountIdentityFor(mount model.MountID) model.MountIdentity {
	if mount == testMountIdentity.MountID {
		return testMountIdentity
	}
	return model.MountIdentity{MountID: mount, MountKeyID: 2}
}

func testMountAdmission() MountAdmission {
	return MountAdmission{
		MountID:       testMountIdentity.MountID,
		MountKeyID:    testMountIdentity.MountKeyID,
		RootInode:     model.RootInode,
		SchemaVersion: 1,
	}
}

func newTestExecutor(runner backend.Store, opts ...Option) (*Executor, error) {
	defaults := []Option{WithMountResolver(&fakeMountResolver{records: map[model.MountID]MountAdmission{
		testMountIdentity.MountID: testMountAdmission(),
	}})}
	return New(runner, append(defaults, opts...)...)
}

func requireStatUint(t *testing.T, stats map[string]any, key string, want uint64) {
	t.Helper()
	raw, ok := stats[key]
	require.Truef(t, ok, "missing stat %s", key)
	got, ok := raw.(uint64)
	require.Truef(t, ok, "stat %s has type %T", key, raw)
	require.Equal(t, want, got)
}

func requireMetadataPredicateStatUint(t *testing.T, stats map[string]any, kind model.OperationKind, key string, want uint64) {
	t.Helper()
	raw, ok := stats["metadata_predicate_commit"]
	require.True(t, ok, "missing metadata_predicate_commit stats")
	byOp, ok := raw.(map[string]any)
	require.Truef(t, ok, "metadata_predicate_commit has type %T", raw)
	rawOp, ok := byOp[string(kind)]
	require.Truef(t, ok, "missing metadata_predicate_commit stats for %s", kind)
	opStats, ok := rawOp.(map[string]uint64)
	require.Truef(t, ok, "metadata_predicate_commit[%s] has type %T", kind, rawOp)
	require.Equal(t, want, opStats[key])
}

func metadataLockedError(mount model.MountID, parent model.InodeID, name string) error {
	key, err := layout.EncodeDentryKey(testMountIdentityFor(mount), parent, name)
	if err != nil {
		panic(err)
	}
	return nokverrors.NewMetadataKeyError(nokverrors.MetadataKeyIssue{
		Kind:        nokverrors.KindLockConflict,
		Primary:     key,
		Key:         key,
		LockVersion: 10,
		LockTTL:     defaultLockTTL,
	})
}

type fakeMountResolver struct {
	records map[model.MountID]MountAdmission
	err     error
	calls   int
}

type fakeAuthorityResolver struct {
	same  bool
	err   error
	calls int
}

type fakeSubtreePublisher struct {
	starts      []subtreePublishCall
	completes   []subtreePublishCall
	err         error
	startErr    error
	completeErr error
}

type fakeQuotaResolver struct {
	err      error
	changes  [][]QuotaChange
	mutation *backend.Mutation
}

type fakeInodeAllocator struct {
	next  model.InodeID
	ids   []model.InodeID
	err   error
	calls int
}

func testInodeForParentBucket(t *testing.T, parent model.InodeID, exclude ...model.InodeID) model.InodeID {
	t.Helper()
	target := layout.BucketForInodeID(parent)
	excluded := make(map[model.InodeID]struct{}, len(exclude))
	for _, id := range exclude {
		excluded[id] = struct{}{}
	}
	for id := model.InodeID(2); id < 1_000_000; id++ {
		if _, ok := excluded[id]; ok {
			continue
		}
		if layout.BucketForInodeID(id) == target {
			return id
		}
	}
	t.Fatalf("no inode found for parent bucket %d", target)
	return 0
}

func (a *fakeInodeAllocator) AllocateCreateInode(context.Context, model.MountIdentity, model.InodeID, string) (model.InodeID, error) {
	a.calls++
	if a.err != nil {
		return 0, a.err
	}
	if len(a.ids) > 0 {
		id := a.ids[0]
		a.ids = a.ids[1:]
		return id, nil
	}
	if a.next == 0 {
		a.next = 22
	}
	id := a.next
	a.next++
	return id, nil
}

type subtreePublishCall struct {
	mount    model.MountID
	root     model.InodeID
	frontier uint64
}

func (q *fakeQuotaResolver) ReserveQuota(_ context.Context, _ backend.Store, changes []QuotaChange, _ uint64) ([]*backend.Mutation, error) {
	q.changes = append(q.changes, append([]QuotaChange(nil), changes...))
	if q.err != nil {
		return nil, q.err
	}
	if q.mutation != nil {
		return []*backend.Mutation{cloneMutation(q.mutation)}, nil
	}
	return nil, nil
}

func (p *fakeSubtreePublisher) StartSubtreeHandoff(_ context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	if p.startErr != nil {
		return p.startErr
	}
	if p.err != nil {
		return p.err
	}
	p.starts = append(p.starts, subtreePublishCall{mount: mount, root: root, frontier: frontier})
	return nil
}

func (p *fakeSubtreePublisher) CompleteSubtreeHandoff(_ context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	if p.completeErr != nil {
		return p.completeErr
	}
	if p.err != nil {
		return p.err
	}
	p.completes = append(p.completes, subtreePublishCall{mount: mount, root: root, frontier: frontier})
	return nil
}

func (r *fakeMountResolver) ResolveMount(_ context.Context, mount model.MountID) (MountAdmission, error) {
	r.calls++
	if r.err != nil {
		return MountAdmission{}, r.err
	}
	record, ok := r.records[mount]
	if !ok {
		return MountAdmission{}, model.ErrMountNotRegistered
	}
	return record, nil
}

func (r *fakeAuthorityResolver) SameAuthority(context.Context, model.MountID, model.InodeID, model.InodeID) (bool, error) {
	r.calls++
	if r.err != nil {
		return false, r.err
	}
	return r.same, nil
}

func newFakeRunner() *fakeRunner {
	runner := &fakeRunner{
		nextTS: 1,
		data:   make(map[string][]byte),
	}
	seedInodeValue(runner, testMountIdentity.MountID, model.InodeRecord{
		Inode:     model.RootInode,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	})
	return runner
}

func (r *fakeRunner) ReserveTimestamp(_ context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errors.New("zero timestamp reservation")
	}
	if len(r.timestampErrs) > 0 {
		err := r.timestampErrs[0]
		r.timestampErrs = r.timestampErrs[1:]
		if err != nil {
			return 0, err
		}
	}
	first := r.nextTS
	r.nextTS += count
	return first, nil
}

func (r *fakeRunner) Get(_ context.Context, key []byte, version uint64) ([]byte, bool, error) {
	r.getCalls++
	r.getVersions = append(r.getVersions, version)
	value, ok := r.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (r *fakeRunner) BatchGet(_ context.Context, keys [][]byte, version uint64) (map[string][]byte, error) {
	r.batchVersions = append(r.batchVersions, version)
	if r.beforeBatchGet != nil {
		r.beforeBatchGet()
	}
	if len(r.batchErrs) > 0 {
		err := r.batchErrs[0]
		r.batchErrs = r.batchErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if value, ok := r.data[string(key)]; ok {
			out[string(key)] = append([]byte(nil), value...)
		}
	}
	return out, nil
}

func (r *fakeRunner) Scan(_ context.Context, startKey, prefix []byte, limit uint32, version uint64) ([]backend.KV, error) {
	r.scanVersions = append(r.scanVersions, version)
	if len(r.scanErrs) > 0 {
		err := r.scanErrs[0]
		r.scanErrs = r.scanErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	keys := make([][]byte, 0, len(r.data))
	for key := range r.data {
		if bytes.Compare([]byte(key), startKey) >= 0 && (len(prefix) == 0 || bytes.HasPrefix([]byte(key), prefix)) {
			keys = append(keys, []byte(key))
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	out := make([]backend.KV, 0, limit)
	for _, key := range keys {
		if uint32(len(out)) >= limit {
			break
		}
		out = append(out, backend.KV{
			Key:   append([]byte(nil), key...),
			Value: append([]byte(nil), r.data[string(key)]...),
		})
	}
	return out, nil
}

func (r *fakeRunner) CommitMetadata(_ context.Context, command backend.MetadataCommand) (backend.MetadataCommitResult, error) {
	for _, pred := range command.Predicates {
		if pred == nil {
			continue
		}
		value, exists := r.data[string(pred.Key)]
		switch pred.Kind {
		case backend.PredicateNotExists:
			if exists {
				return backend.MetadataCommitResult{}, model.ErrExists
			}
		case backend.PredicateExists:
			if !exists {
				return backend.MetadataCommitResult{}, model.ErrNotFound
			}
		case backend.PredicateValueEquals:
			if !exists || !bytes.Equal(value, pred.ExpectedValue) {
				return backend.MetadataCommitResult{}, model.ErrInvalidValue
			}
		case backend.PredicatePrefixEmpty:
			if !fakePrefixEmpty(r.data, pred.Key) {
				return backend.MetadataCommitResult{}, model.ErrInvalidRequest
			}
		default:
			return backend.MetadataCommitResult{}, model.ErrInvalidRequest
		}
	}
	commitVersion := command.CommitVersion
	if commitVersion == 0 {
		commitVersion = command.ReadVersion + 1
		if r.actualCommitVersion != 0 {
			commitVersion = r.actualCommitVersion
		}
	}
	primary := command.PrimaryKey
	if len(primary) == 0 {
		primary = metadataCommandPrimary(command.Mutations)
	}
	if _, err := r.applyMutations(primary, command.Mutations, commitVersion, 0); err != nil {
		return backend.MetadataCommitResult{}, err
	}
	return backend.MetadataCommitResult{
		CommitVersion:    commitVersion,
		Index:            commitVersion,
		AppliedMutations: uint64(len(command.Mutations)),
	}, nil
}

func (r *fakeRunner) applyMutations(primary []byte, mutations []*backend.Mutation, commitVersion, overrideCommitVersion uint64) (uint64, error) {
	if len(r.mutateErrs) > 0 {
		err := r.mutateErrs[0]
		r.mutateErrs = r.mutateErrs[1:]
		if err != nil {
			return 0, err
		}
	}
	if r.mutateErr != nil {
		return 0, r.mutateErr
	}
	cloned := make([]*backend.Mutation, 0, len(mutations))
	hasPrimary := len(mutations) == 0
	for _, mut := range mutations {
		if mut.AssertionNotExist {
			if _, ok := r.data[string(mut.Key)]; ok {
				return 0, model.ErrExists
			}
		}
		if bytes.Equal(mut.Key, primary) {
			hasPrimary = true
		}
		cloned = append(cloned, cloneMutation(mut))
	}
	if !hasPrimary {
		return 0, fmt.Errorf("primary key %q not present in mutations", primary)
	}
	for _, mut := range cloned {
		switch mut.Op {
		case backend.MutationPut:
			r.data[string(mut.Key)] = append([]byte(nil), mut.Value...)
		case backend.MutationDelete:
			delete(r.data, string(mut.Key))
		}
	}
	r.mutations = append(r.mutations, cloned)
	if overrideCommitVersion != 0 {
		return overrideCommitVersion, nil
	}
	return commitVersion, nil
}

func (r *fakePredicateRunner) TryMetadataPredicateCommit(_ context.Context, primary []byte, predicates []*backend.Predicate, mutations []*backend.Mutation, startVersion, commitVersion uint64) (bool, error) {
	r.predicateCalls = append(r.predicateCalls, metadataPredicateCommitCall{
		primary:       cloneBytes(primary),
		predicates:    cloneMetadataPredicates(predicates),
		mutations:     cloneMutations(mutations),
		startVersion:  startVersion,
		commitVersion: commitVersion,
	})
	if r.err != nil {
		return true, r.err
	}
	for _, pred := range predicates {
		if pred == nil {
			continue
		}
		_, exists := r.data[string(pred.Key)]
		switch pred.Kind {
		case backend.PredicateNotExists:
			if exists {
				return true, model.ErrExists
			}
		case backend.PredicateExists:
			if !exists {
				return true, model.ErrNotFound
			}
		case backend.PredicateValueEquals:
			if !exists || !bytes.Equal(r.data[string(pred.Key)], pred.ExpectedValue) {
				return true, model.ErrInvalidValue
			}
		case backend.PredicatePrefixEmpty:
			if !fakePrefixEmpty(r.data, pred.Key) {
				return true, model.ErrInvalidRequest
			}
		default:
			return true, model.ErrInvalidRequest
		}
	}
	cloned := make([]*backend.Mutation, 0, len(mutations))
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		if mut.AssertionNotExist {
			if _, exists := r.data[string(mut.Key)]; exists {
				return true, model.ErrExists
			}
		}
		cloned = append(cloned, cloneMutation(mut))
	}
	for _, mut := range cloned {
		switch mut.Op {
		case backend.MutationPut:
			r.data[string(mut.Key)] = append([]byte(nil), mut.Value...)
		case backend.MutationDelete:
			delete(r.data, string(mut.Key))
		}
	}
	return true, nil
}

func (r *fakePredicateRunner) CommitMetadata(_ context.Context, command backend.MetadataCommand) (backend.MetadataCommitResult, error) {
	commitVersion := command.CommitVersion
	if commitVersion == 0 {
		commitVersion = command.ReadVersion + 1
	}
	primary := command.PrimaryKey
	if len(primary) == 0 {
		primary = metadataCommandPrimary(command.Mutations)
	}
	if len(command.Predicates) == 0 {
		return r.fakeRunner.CommitMetadata(context.Background(), command)
	}
	_, err := r.TryMetadataPredicateCommit(context.Background(), primary, command.Predicates, command.Mutations, command.ReadVersion, commitVersion)
	if err != nil {
		return backend.MetadataCommitResult{}, err
	}
	return backend.MetadataCommitResult{
		CommitVersion:    commitVersion,
		Index:            commitVersion,
		AppliedMutations: uint64(len(command.Mutations)),
	}, nil
}

func fakePrefixEmpty(data map[string][]byte, prefix []byte) bool {
	for key := range data {
		if bytes.HasPrefix([]byte(key), prefix) {
			return false
		}
	}
	return true
}

func seedDentry(t *testing.T, runner *fakeRunner, mount model.MountID, parent model.InodeID, name string, inode model.InodeID) {
	t.Helper()
	seedDentryType(t, runner, mount, parent, name, inode, model.InodeTypeFile)
}

func seedDentryType(t *testing.T, runner *fakeRunner, mount model.MountID, parent model.InodeID, name string, inode model.InodeID, typ model.InodeType) {
	t.Helper()
	key, err := layout.EncodeDentryKey(testMountIdentityFor(mount), parent, name)
	require.NoError(t, err)
	if _, exists := runner.data[string(key)]; !exists {
		incrementSeedParentChildCount(t, runner, mount, parent)
	}
	value, err := layout.EncodeDentryValue(model.DentryRecord{
		Parent: parent,
		Name:   name,
		Inode:  inode,
		Type:   typ,
	})
	require.NoError(t, err)
	runner.data[string(key)] = value
}

func seedInode(t *testing.T, runner *fakeRunner, mount model.MountID, record model.InodeRecord) {
	t.Helper()
	seedInodeValue(runner, mount, record)
}

func seedDirectory(t *testing.T, runner *fakeRunner, mount model.MountID, inode model.InodeID) {
	t.Helper()
	seedInodeValue(runner, mount, model.InodeRecord{
		Inode:     inode,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	})
}

func seedInodeValue(runner *fakeRunner, mount model.MountID, record model.InodeRecord) {
	key, err := layout.EncodeInodeKey(testMountIdentityFor(mount), record.Inode)
	if err != nil {
		panic(err)
	}
	value, err := layout.EncodeInodeValue(record)
	if err != nil {
		panic(err)
	}
	runner.data[string(key)] = value
}

func incrementSeedParentChildCount(t *testing.T, runner *fakeRunner, mount model.MountID, parent model.InodeID) {
	t.Helper()
	key, err := layout.EncodeInodeKey(testMountIdentityFor(mount), parent)
	require.NoError(t, err)
	record := model.InodeRecord{
		Inode:     parent,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	}
	if value, ok := runner.data[string(key)]; ok {
		record, err = layout.DecodeInodeValue(value)
		require.NoError(t, err)
		require.Equal(t, model.InodeTypeDirectory, record.Type)
	}
	record.ChildCount++
	value, err := layout.EncodeInodeValue(record)
	require.NoError(t, err)
	runner.data[string(key)] = value
}

func seedSession(t *testing.T, runner *fakeRunner, mount model.MountID, record model.SessionRecord) {
	t.Helper()
	value, err := layout.EncodeSessionValue(record)
	require.NoError(t, err)
	sessionKey, err := layout.EncodeSessionKey(testMountIdentityFor(mount), record.Inode, record.Session)
	require.NoError(t, err)
	ownerKey, err := layout.EncodeInodeSessionKey(testMountIdentityFor(mount), record.Inode)
	require.NoError(t, err)
	runner.data[string(sessionKey)] = value
	runner.data[string(ownerKey)] = value
}

func cloneMutation(mut *backend.Mutation) *backend.Mutation {
	if mut == nil {
		return nil
	}
	return &backend.Mutation{
		Op:                mut.Op,
		Key:               append([]byte(nil), mut.Key...),
		Value:             append([]byte(nil), mut.Value...),
		AssertionNotExist: mut.AssertionNotExist,
		ExpiresAt:         mut.ExpiresAt,
	}
}

type fakeMetadataKeyError struct {
	errors []nokverrors.MetadataKeyIssue
}

func (e fakeMetadataKeyError) Error() string {
	return "fake metadata key error"
}

func (e fakeMetadataKeyError) KeyErrors() []nokverrors.MetadataKeyIssue {
	return e.errors
}

func benchmarkExecutorCreate(b *testing.B, executor *Executor) {
	ctx := context.Background()
	var seq uint64
	b.ReportAllocs()
	for b.Loop() {
		name := "file-" + strconv.FormatUint(seq, 10)
		seq++
		if _, err := executor.Create(ctx, model.CreateRequest{
			Mount:  "vol",
			Parent: model.RootInode,
			Name:   name,
			Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorCheckpointStorm(b *testing.B, executor *Executor, files int) {
	ctx := context.Background()
	var batch uint64
	var ops uint64
	b.ReportAllocs()
	for b.Loop() {
		prefix := "ckpt-" + strconv.FormatUint(batch, 10) + "-"
		batch++
		dir, err := executor.Create(ctx, model.CreateRequest{
			Mount:  "vol",
			Parent: model.RootInode,
			Name:   prefix + "dir",
			Attrs:  model.CreateAttrs{Type: model.InodeTypeDirectory},
		})
		if err != nil {
			b.Fatal(err)
		}
		for i := range files {
			if _, err := executor.Create(ctx, model.CreateRequest{
				Mount:  "vol",
				Parent: dir.Inode.Inode,
				Name:   prefix + strconv.Itoa(i),
				Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
			}); err != nil {
				b.Fatal(err)
			}
		}
		ops += uint64(files + 1)
	}
	if ops > 0 {
		elapsed := b.Elapsed()
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(ops), "ns/metadata_op")
		b.ReportMetric(float64(ops)/elapsed.Seconds(), "metadata_ops/s")
	}
}

func benchmarkExecutorUpdateInode(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := model.InodeID(i + 1000)
		name := "file-" + strconv.Itoa(i)
		benchmarkSeedDentry(b, runner, 7, name, inode)
		benchmarkSeedInode(b, runner, inode)
		if _, err := executor.UpdateInode(ctx, model.UpdateInodeRequest{
			Mount:            "vol",
			Parent:           7,
			Inode:            inode,
			Name:             name,
			SetSize:          true,
			Size:             uint64(i + 4096),
			SetMode:          true,
			Mode:             0o600,
			SetUpdatedUnixNs: true,
			UpdatedUnixNs:    int64(i + 1),
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorRename(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := model.InodeID(i + 1000)
		oldName := "old-" + strconv.Itoa(i)
		newName := "new-" + strconv.Itoa(i)
		benchmarkSeedDentry(b, runner, 7, oldName, inode)
		if err := executor.Rename(ctx, model.RenameRequest{
			Mount:      "vol",
			FromParent: 7,
			FromName:   oldName,
			ToParent:   7,
			ToName:     newName,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorOpenWriteSession(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := model.InodeID(i + 1000)
		benchmarkSeedInode(b, runner, inode)
		if _, err := executor.OpenWriteSession(ctx, model.OpenWriteSessionRequest{
			Mount:   "vol",
			Inode:   inode,
			Session: model.SessionID("writer-" + strconv.Itoa(i)),
			TTL:     100 * time.Nanosecond,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorLink(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	benchmarkSeedDirectory(b, runner, 8)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := model.InodeID(i + 1000)
		fromName := "file-" + strconv.Itoa(i)
		toName := "alias-" + strconv.Itoa(i)
		benchmarkSeedDentry(b, runner, 7, fromName, inode)
		benchmarkSeedInode(b, runner, inode)
		if err := executor.Link(ctx, model.LinkRequest{
			Mount:      "vol",
			FromParent: 7,
			FromName:   fromName,
			ToParent:   8,
			ToName:     toName,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkExecutorUnlink(b *testing.B, runner *fakeRunner, executor *Executor) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		inode := model.InodeID(i + 1000)
		name := "file-" + strconv.Itoa(i)
		benchmarkSeedDentry(b, runner, 7, name, inode)
		benchmarkSeedInodeRecord(b, runner, model.InodeRecord{Inode: inode, Type: model.InodeTypeFile, Size: 4096, LinkCount: 2})
		if err := executor.Unlink(ctx, model.UnlinkRequest{
			Mount:  "vol",
			Parent: 7,
			Name:   name,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSeedDentry(b *testing.B, runner *fakeRunner, parent model.InodeID, name string, inode model.InodeID) {
	b.Helper()
	benchmarkIncrementSeedParentChildCount(b, runner, parent)
	key, err := layout.EncodeDentryKey(testMountIdentity, parent, name)
	if err != nil {
		b.Fatal(err)
	}
	value, err := layout.EncodeDentryValue(model.DentryRecord{
		Parent: parent,
		Name:   name,
		Inode:  inode,
		Type:   model.InodeTypeFile,
	})
	if err != nil {
		b.Fatal(err)
	}
	runner.data[string(key)] = value
}

func benchmarkSeedDirectory(b *testing.B, runner *fakeRunner, inode model.InodeID) {
	b.Helper()
	benchmarkSeedInodeRecord(b, runner, model.InodeRecord{
		Inode:     inode,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	})
}

func benchmarkIncrementSeedParentChildCount(b *testing.B, runner *fakeRunner, parent model.InodeID) {
	b.Helper()
	key, err := layout.EncodeInodeKey(testMountIdentity, parent)
	if err != nil {
		b.Fatal(err)
	}
	record := model.InodeRecord{
		Inode:     parent,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	}
	if value, ok := runner.data[string(key)]; ok {
		record, err = layout.DecodeInodeValue(value)
		if err != nil {
			b.Fatal(err)
		}
		if record.Type != model.InodeTypeDirectory {
			b.Fatalf("seed parent %d is %s", parent, record.Type)
		}
	}
	record.ChildCount++
	value, err := layout.EncodeInodeValue(record)
	if err != nil {
		b.Fatal(err)
	}
	runner.data[string(key)] = value
}

func benchmarkSeedInode(b *testing.B, runner *fakeRunner, inode model.InodeID) {
	b.Helper()
	benchmarkSeedInodeRecord(b, runner, model.InodeRecord{Inode: inode, Type: model.InodeTypeFile, LinkCount: 1})
}

func benchmarkSeedInodeRecord(b *testing.B, runner *fakeRunner, record model.InodeRecord) {
	b.Helper()
	key, err := layout.EncodeInodeKey(testMountIdentity, record.Inode)
	if err != nil {
		b.Fatal(err)
	}
	value, err := layout.EncodeInodeValue(record)
	if err != nil {
		b.Fatal(err)
	}
	runner.data[string(key)] = value
}
