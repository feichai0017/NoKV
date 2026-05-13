package exec

import (
	"bytes"
	"context"
	"fmt"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/feichai0017/NoKV/engine/slab/dirpage"
	"github.com/feichai0017/NoKV/fsmeta"
)

// Lookup returns the dentry record for parent/name. When a negative cache
// is wired (WithNegativeCache), Lookup short-circuits a previously-known
// missing key into ErrNotFound without round-tripping through the runner.
// Misses observed by the runner are recorded so the next Lookup hits the
// visible commit; subsequent Create/Link/Rename for the same key Invalidate the
// entry so the negative memo cannot mask a now-existing dentry.
func (e *Executor) Lookup(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	plan, err := fsmeta.PlanLookup(req, mountRecord.Identity())
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	if e.negCache != nil && e.negCache.Has(plan.PrimaryKey) {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	if value, deleted, ok := e.perasOverlayGet(plan.PrimaryKey); ok {
		if deleted {
			return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
		}
		return fsmeta.DecodeDentryValue(value)
	}
	version, err := e.reserveReadVersion(ctx)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	value, ok, err := e.runner.Get(ctx, plan.PrimaryKey, version)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	if !ok {
		if e.negCache != nil {
			e.negCache.Remember(plan.PrimaryKey)
		}
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	return fsmeta.DecodeDentryValue(value)
}

// invalidateNegative drops cached "missing" memos for every dentry key that
// was just mutated, so the next Lookup re-issues against the runner instead
// of returning a stale ErrNotFound. Safe with a nil cache.
func (e *Executor) invalidateNegative(keys ...[]byte) {
	if e == nil || e.negCache == nil {
		return
	}
	for _, k := range keys {
		if len(k) > 0 {
			e.negCache.Invalidate(k)
		}
	}
}

// invalidateDirPages bumps the dirpage cache's epoch for every parent
// directory the just-committed mutation touched. Safe with a nil cache.
// Caller passes (mount, parent) tuples — the helper folds duplicates so
// rename across a single parent doesn't double-bump.
func (e *Executor) invalidateDirPages(mount fsmeta.MountID, parents ...fsmeta.InodeID) {
	if e == nil || e.dirPages == nil {
		return
	}
	seen := make(map[fsmeta.InodeID]struct{}, len(parents))
	for _, p := range parents {
		if p == 0 {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		e.dirPages.Invalidate(dirPageDirectoryKey(mount, p))
	}
}

// ReadDir returns one directory page from a dentry prefix scan.
func (e *Executor) ReadDir(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return nil, err
	}
	plan, err := fsmeta.PlanReadDir(req, mountRecord.Identity())
	if err != nil {
		return nil, err
	}
	includeOverlay := req.SnapshotVersion == 0
	if includeOverlay && e.perasDirectoryBaseEmpty(mountRecord.Identity(), req.Parent) {
		return e.scanDentries(ctx, plan, 0, true, true)
	}
	var out []fsmeta.DentryRecord
	err = e.withReadRetry(ctx, req.SnapshotVersion, func(version uint64) error {
		var err error
		out, err = e.scanDentries(ctx, plan, version, includeOverlay, includeOverlay && e.perasDirectoryBaseEmpty(mountRecord.Identity(), req.Parent))
		return err
	})
	return out, err
}

// ReadDirPlus returns one directory page fused with inode attributes at the
// same snapshot version. This is the first native fsmeta operation that avoids
// client-side dentry scan plus N point reads.
//
// When a dirpage cache is wired and the request omits an explicit
// SnapshotVersion (i.e. the caller is asking for "latest"), Lookup checks
// the cache first against the parent's current invalidation epoch. On hit
// the runner-side dentry scan + N inode BatchGet are skipped; on miss the
// runner path runs as today and the assembled pairs are asynchronously
// materialized into the cache for the next caller.
//
// Snapshot-versioned reads bypass the cache: pages are tagged with the
// "latest" frontier and a stale snapshot-versioned read might disagree
// with the live cache, so we keep that path on the authoritative LSM
// route.
func (e *Executor) ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return nil, err
	}
	mount := mountRecord.Identity()
	plan, err := fsmeta.PlanReadDir(req, mount)
	if err != nil {
		return nil, err
	}

	useDirPage := e.dirPages != nil && req.SnapshotVersion == 0
	var pageKey dirpage.PageKey
	var frontier uint64
	if useDirPage {
		pageKey = dirPageKey(req.Mount, req.Parent, req.StartAfter, plan.Limit)
		frontier = e.dirPages.CurrentEpoch(pageKey.Directory())
		if entries, ok := e.dirPages.Lookup(pageKey, frontier); ok {
			if cached, err := decodeDirPageEntries(pageKey, entries); err == nil {
				return cached, nil
			}
		}
	}
	if req.SnapshotVersion == 0 && e.perasDirectoryBaseEmpty(mount, req.Parent) {
		dentries, err := e.scanDentries(ctx, plan, 0, true, true)
		if err != nil {
			return nil, err
		}
		if pairs, ok, err := e.readDirPlusFromPerasView(mount, dentries); err != nil {
			return nil, err
		} else if ok {
			return pairs, nil
		}
	}

	var out []fsmeta.DentryAttrPair
	err = e.withReadRetry(ctx, req.SnapshotVersion, func(version uint64) error {
		includeOverlay := req.SnapshotVersion == 0
		dentries, err := e.scanDentries(ctx, plan, version, includeOverlay, includeOverlay && e.perasDirectoryBaseEmpty(mount, req.Parent))
		if err != nil {
			return err
		}
		if len(dentries) == 0 {
			out = []fsmeta.DentryAttrPair{}
			return nil
		}
		inodeKeys := make([][]byte, 0, len(dentries))
		for _, dentry := range dentries {
			key, err := fsmeta.EncodeInodeKey(mount, dentry.Inode)
			if err != nil {
				return err
			}
			inodeKeys = append(inodeKeys, key)
		}
		inodeValues, err := e.batchGetMergedValues(ctx, inodeKeys, version, includeOverlay)
		if err != nil {
			return err
		}
		pairs := make([]fsmeta.DentryAttrPair, 0, len(dentries))
		for i, dentry := range dentries {
			value, ok := inodeValues[string(inodeKeys[i])]
			if !ok {
				return fmt.Errorf("%w: inode %d", fsmeta.ErrNotFound, dentry.Inode)
			}
			inode, err := fsmeta.DecodeInodeValue(value)
			if err != nil {
				return err
			}
			if inode.Inode != dentry.Inode {
				return fmt.Errorf("%w: dentry inode=%d value inode=%d", fsmeta.ErrInvalidValue, dentry.Inode, inode.Inode)
			}
			pairs = append(pairs, fsmeta.DentryAttrPair{
				Dentry: dentry,
				Inode:  inode,
			})
		}
		out = pairs
		return nil
	})
	if err != nil {
		return nil, err
	}
	if useDirPage {
		// Materialize is best-effort: if Invalidate fired since we read,
		// the cache drops the write and the next call re-fetches. Encoding must
		// be all-or-none: a partial cached page would be worse than a miss.
		if entries, err := encodeDirPageEntries(out); err == nil {
			_ = e.dirPages.MaterializeAsync(pageKey, frontier, entries)
		}
	}
	return out, nil
}

func (e *Executor) readDirPlusFromPerasView(mount fsmeta.MountIdentity, dentries []fsmeta.DentryRecord) ([]fsmeta.DentryAttrPair, bool, error) {
	if len(dentries) == 0 {
		return []fsmeta.DentryAttrPair{}, true, nil
	}
	pairs := make([]fsmeta.DentryAttrPair, 0, len(dentries))
	for _, dentry := range dentries {
		key, err := fsmeta.EncodeInodeKey(mount, dentry.Inode)
		if err != nil {
			return nil, false, err
		}
		value, deleted, ok := e.perasOverlayGet(key)
		if !ok || deleted {
			return nil, false, nil
		}
		inode, err := fsmeta.DecodeInodeValue(value)
		if err != nil {
			return nil, false, err
		}
		if inode.Inode != dentry.Inode {
			return nil, false, fmt.Errorf("%w: dentry inode=%d value inode=%d", fsmeta.ErrInvalidValue, dentry.Inode, inode.Inode)
		}
		pairs = append(pairs, fsmeta.DentryAttrPair{Dentry: dentry, Inode: inode})
	}
	return pairs, true, nil
}

func (e *Executor) scanDentries(ctx context.Context, plan fsmeta.OperationPlan, version uint64, includeOverlay, overlayOnly bool) ([]fsmeta.DentryRecord, error) {
	var kvs []KV
	if !overlayOnly {
		var err error
		kvs, err = e.runner.Scan(ctx, plan.StartKey, plan.Limit, version)
		if err != nil {
			return nil, err
		}
	}
	prefix := plan.ReadPrefixes[0]
	if includeOverlay {
		kvs = e.mergePerasOverlayScan(kvs, plan.StartKey, plan.Limit)
	}
	out := make([]fsmeta.DentryRecord, 0, len(kvs))
	for _, kv := range kvs {
		if !bytes.HasPrefix(kv.Key, prefix) {
			break
		}
		record, err := fsmeta.DecodeDentryValue(kv.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, nil
}

func (e *Executor) perasDirectoryBaseEmpty(mount fsmeta.MountIdentity, parent fsmeta.InodeID) bool {
	index := e.perasPredicateIndex()
	if index == nil {
		return false
	}
	return index.DirectoryEmpty(mount, parent)
}

func (e *Executor) readDentry(ctx context.Context, key []byte, version uint64) (fsmeta.DentryRecord, error) {
	value, ok, err := e.getMergedValue(ctx, key, version)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	if !ok {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	return fsmeta.DecodeDentryValue(value)
}

func (e *Executor) readInode(ctx context.Context, mount fsmeta.MountIdentity, inodeID fsmeta.InodeID, version uint64) (fsmeta.InodeRecord, bool, error) {
	key, err := fsmeta.EncodeInodeKey(mount, inodeID)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	value, ok, err := e.getMergedValue(ctx, key, version)
	if err != nil || !ok {
		return fsmeta.InodeRecord{}, ok, err
	}
	inode, err := fsmeta.DecodeInodeValue(value)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	return inode, true, nil
}

func (e *Executor) readSessionByKey(ctx context.Context, key []byte, version uint64) (fsmeta.SessionRecord, bool, error) {
	value, ok, err := e.getMergedValue(ctx, key, version)
	if err != nil || !ok {
		return fsmeta.SessionRecord{}, ok, err
	}
	record, err := fsmeta.DecodeSessionValue(value)
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	return record, true, nil
}

// dirPageDirectoryKey hashes (mount, parent) into the dirpage cache's
// directory invalidation key. fsmeta.MountID is a string; we use xxhash.Sum64
// to fold it into a uint64 mount slot. Collision probability across reasonable
// mount counts (<= 10K) is ~5e-12, well below "fallback re-warm" tolerance.
func dirPageDirectoryKey(mount fsmeta.MountID, parent fsmeta.InodeID) dirpage.DirectoryKey {
	return dirpage.DirectoryKey{
		Mount:  xxhash.Sum64String(string(mount)),
		Parent: uint64(parent),
	}
}

// dirPageKey includes the caller-visible page cursor. ReadDirPlus cache hits
// are only valid for the exact StartAfter/Limit shape that produced them.
func dirPageKey(mount fsmeta.MountID, parent fsmeta.InodeID, startAfter string, limit uint32) dirpage.PageKey {
	return dirpage.PageKey{
		Mount:      xxhash.Sum64String(string(mount)),
		Parent:     uint64(parent),
		StartAfter: startAfter,
		Limit:      limit,
	}
}

// encodeDirPageEntries converts assembled DentryAttrPairs into the
// generic dirpage Entry shape. AttrBlob is the encoded InodeRecord; if any
// entry cannot be encoded, the whole materialization is skipped so the cache
// never serves a truncated page as complete.
func encodeDirPageEntries(pairs []fsmeta.DentryAttrPair) ([]dirpage.Entry, error) {
	out := make([]dirpage.Entry, 0, len(pairs))
	for _, p := range pairs {
		blob, err := fsmeta.EncodeInodeValue(p.Inode)
		if err != nil {
			return nil, err
		}
		out = append(out, dirpage.Entry{
			Name:     []byte(p.Dentry.Name),
			Inode:    uint64(p.Dentry.Inode),
			AttrBlob: blob,
		})
	}
	return out, nil
}

// decodeDirPageEntries reverses encodeDirPageEntries. Decode failure on
// any entry treats the whole page set as corrupt and forces a fallback
// to the runner.
func decodeDirPageEntries(key dirpage.PageKey, entries []dirpage.Entry) ([]fsmeta.DentryAttrPair, error) {
	out := make([]fsmeta.DentryAttrPair, 0, len(entries))
	for _, e := range entries {
		inode, err := fsmeta.DecodeInodeValue(e.AttrBlob)
		if err != nil {
			return nil, err
		}
		out = append(out, fsmeta.DentryAttrPair{
			Dentry: fsmeta.DentryRecord{
				Parent: fsmeta.InodeID(key.Parent),
				Name:   string(e.Name),
				Inode:  fsmeta.InodeID(e.Inode),
				Type:   inode.Type,
			},
			Inode: inode,
		})
	}
	return out, nil
}
