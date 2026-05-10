package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

type lookupCacheBase struct {
	lookupCount      int
	readDirPlusCount int
	entries          map[lookupCacheKey]fsmeta.DentryRecord
}

func newLookupCacheBase(entries ...fsmeta.DentryRecord) *lookupCacheBase {
	base := &lookupCacheBase{entries: make(map[lookupCacheKey]fsmeta.DentryRecord)}
	for _, entry := range entries {
		base.entries[lookupCacheKey{mount: "vol", parent: entry.Parent, name: entry.Name}] = entry
	}
	return base
}

func (b *lookupCacheBase) Create(_ context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	dentry := fsmeta.DentryRecord{Parent: req.Parent, Name: req.Name, Inode: 99, Type: req.Attrs.Type}
	b.entries[lookupCacheKey{mount: req.Mount, parent: req.Parent, name: req.Name}] = dentry
	return fsmeta.CreateResult{Dentry: dentry, Inode: req.Attrs.InodeRecord(99)}, nil
}

func (b *lookupCacheBase) UpdateInode(context.Context, fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error) {
	return fsmeta.InodeRecord{}, nil
}

func (b *lookupCacheBase) Lookup(_ context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	b.lookupCount++
	record, ok := b.entries[lookupCacheKey{mount: req.Mount, parent: req.Parent, name: req.Name}]
	if !ok {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	return record, nil
}

func (b *lookupCacheBase) ReadDir(context.Context, fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	return nil, nil
}

func (b *lookupCacheBase) ReadDirPlus(_ context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	b.readDirPlusCount++
	var out []fsmeta.DentryAttrPair
	for key, entry := range b.entries {
		if key.mount == req.Mount && key.parent == req.Parent {
			out = append(out, fsmeta.DentryAttrPair{Dentry: entry, Inode: fsmeta.InodeRecord{Inode: entry.Inode, Type: entry.Type}})
		}
	}
	return out, nil
}

func (b *lookupCacheBase) WatchSubtree(context.Context, fsmeta.WatchRequest) (WatchSubscription, error) {
	return nil, errors.New("not implemented")
}

func (b *lookupCacheBase) GetReadVersion(context.Context, fsmeta.ReadVersionRequest) (uint64, error) {
	return 0, nil
}

func (b *lookupCacheBase) SnapshotSubtree(context.Context, fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	return fsmeta.SnapshotSubtreeToken{}, nil
}

func (b *lookupCacheBase) RetireSnapshotSubtree(context.Context, fsmeta.SnapshotSubtreeToken) error {
	return nil
}

func (b *lookupCacheBase) GetQuotaUsage(context.Context, fsmeta.QuotaUsageRequest) (fsmeta.UsageRecord, error) {
	return fsmeta.UsageRecord{}, nil
}

func (b *lookupCacheBase) Rename(_ context.Context, req fsmeta.RenameRequest) error {
	from := lookupCacheKey{mount: req.Mount, parent: req.FromParent, name: req.FromName}
	entry, ok := b.entries[from]
	if !ok {
		return fsmeta.ErrNotFound
	}
	delete(b.entries, from)
	entry.Parent = req.ToParent
	entry.Name = req.ToName
	b.entries[lookupCacheKey{mount: req.Mount, parent: req.ToParent, name: req.ToName}] = entry
	return nil
}

func (b *lookupCacheBase) RenameSubtree(ctx context.Context, req fsmeta.RenameSubtreeRequest) error {
	return b.Rename(ctx, fsmeta.RenameRequest(req))
}

func (b *lookupCacheBase) Link(context.Context, fsmeta.LinkRequest) error { return nil }

func (b *lookupCacheBase) Unlink(_ context.Context, req fsmeta.UnlinkRequest) error {
	delete(b.entries, lookupCacheKey{mount: req.Mount, parent: req.Parent, name: req.Name})
	return nil
}

func (b *lookupCacheBase) OpenWriteSession(context.Context, fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	return fsmeta.SessionRecord{}, nil
}

func (b *lookupCacheBase) HeartbeatWriteSession(context.Context, fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error) {
	return fsmeta.SessionRecord{}, nil
}

func (b *lookupCacheBase) CloseWriteSession(context.Context, fsmeta.CloseWriteSessionRequest) error {
	return nil
}

func (b *lookupCacheBase) ExpireWriteSessions(context.Context, fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error) {
	return fsmeta.ExpireWriteSessionsResult{}, nil
}

func (b *lookupCacheBase) Close() error { return nil }

func TestCachedClientLookupHit(t *testing.T) {
	base := newLookupCacheBase(fsmeta.DentryRecord{Parent: 7, Name: "run", Inode: 42, Type: fsmeta.InodeTypeDirectory})
	cli, err := NewCachedClient(base, LookupCacheConfig{MaxEntries: 8, TTL: time.Minute})
	require.NoError(t, err)

	req := fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "run"}
	first, err := cli.Lookup(context.Background(), req)
	require.NoError(t, err)
	second, err := cli.Lookup(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, first, second)
	require.Equal(t, 1, base.lookupCount)
	require.Equal(t, LookupCacheStats{Hits: 1, Misses: 1, Inserts: 1}, cli.LookupCacheStats())
}

func TestCachedClientExpiresLookup(t *testing.T) {
	base := newLookupCacheBase(fsmeta.DentryRecord{Parent: 7, Name: "run", Inode: 42, Type: fsmeta.InodeTypeDirectory})
	cli, err := NewCachedClient(base, LookupCacheConfig{MaxEntries: 8, TTL: time.Nanosecond})
	require.NoError(t, err)

	req := fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "run"}
	_, err = cli.Lookup(context.Background(), req)
	require.NoError(t, err)
	time.Sleep(time.Millisecond)
	_, err = cli.Lookup(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, 2, base.lookupCount)
	require.Equal(t, uint64(1), cli.LookupCacheStats().Expired)
}

func TestCachedClientPopulatesFromReadDirPlus(t *testing.T) {
	base := newLookupCacheBase(fsmeta.DentryRecord{Parent: 7, Name: "run", Inode: 42, Type: fsmeta.InodeTypeDirectory})
	cli, err := NewCachedClient(base, LookupCacheConfig{MaxEntries: 8, TTL: time.Minute})
	require.NoError(t, err)

	_, err = cli.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{Mount: "vol", Parent: 7})
	require.NoError(t, err)
	record, err := cli.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "run"})
	require.NoError(t, err)

	require.Equal(t, fsmeta.InodeID(42), record.Inode)
	require.Equal(t, 0, base.lookupCount)
	require.Equal(t, 1, base.readDirPlusCount)
	require.Equal(t, uint64(1), cli.LookupCacheStats().Hits)
}

func TestCachedClientInvalidatesAfterMutation(t *testing.T) {
	base := newLookupCacheBase(fsmeta.DentryRecord{Parent: 7, Name: "run", Inode: 42, Type: fsmeta.InodeTypeDirectory})
	cli, err := NewCachedClient(base, LookupCacheConfig{MaxEntries: 8, TTL: time.Minute})
	require.NoError(t, err)

	req := fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "run"}
	_, err = cli.Lookup(context.Background(), req)
	require.NoError(t, err)
	require.NoError(t, cli.Unlink(context.Background(), fsmeta.UnlinkRequest{Mount: "vol", Parent: 7, Name: "run"}))
	_, err = cli.Lookup(context.Background(), req)
	require.ErrorIs(t, err, fsmeta.ErrNotFound)

	require.Equal(t, 2, base.lookupCount)
	require.Equal(t, uint64(1), cli.LookupCacheStats().Invalidations)
}

func TestCachedClientMovesRenameHit(t *testing.T) {
	base := newLookupCacheBase(fsmeta.DentryRecord{Parent: 7, Name: "run.tmp", Inode: 42, Type: fsmeta.InodeTypeDirectory})
	cli, err := NewCachedClient(base, LookupCacheConfig{MaxEntries: 8, TTL: time.Minute})
	require.NoError(t, err)

	_, err = cli.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "run.tmp"})
	require.NoError(t, err)
	require.NoError(t, cli.Rename(context.Background(), fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "run.tmp",
		ToParent:   8,
		ToName:     "run",
	}))
	record, err := cli.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 8, Name: "run"})
	require.NoError(t, err)

	require.Equal(t, fsmeta.InodeID(42), record.Inode)
	require.Equal(t, 1, base.lookupCount)
}
