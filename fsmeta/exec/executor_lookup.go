// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"fmt"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/feichai0017/NoKV/engine/slab/dirpage"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

// Lookup returns the dentry record for parent/name. Peras overlay is consulted
// before the negative cache so a visible or recovered record cannot be hidden
// by a stale miss memo. Misses observed by the runner are recorded so the next
// Lookup can skip the authoritative probe; mutating operations invalidate the
// affected dentry keys after a successful commit.
func (e *Executor) Lookup(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	program, err := compile.CompileLookupReadProgram(req, mountRecord.Identity())
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	plan := program.Plan
	if value, deleted, ok := e.readPerasProgram(program); ok {
		e.invalidateNegative(plan.PrimaryKey)
		if deleted {
			return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
		}
		return fsmeta.DecodeDentryValue(value)
	}
	if e.negCache != nil && e.negCache.Has(plan.PrimaryKey) {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
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

func (e *Executor) clearNegativeCache() {
	if e == nil || e.negCache == nil {
		return
	}
	clearer, ok := e.negCache.(negativeCacheClearer)
	if ok {
		clearer.Clear()
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
	mount := mountRecord.Identity()
	snapshotHasOverlay := req.SnapshotVersion != 0 && e.perasSnapshotDirectoryHasOverlay(req.SnapshotVersion, mount, req.Parent)
	overlayOnly := req.SnapshotVersion == 0 && e.perasDirectoryBaseEmpty(mount, req.Parent)
	hasVisibleOverlay := req.SnapshotVersion == 0 && e.perasDirectoryHasVisibleOverlay(mount, req.Parent)
	includeOverlay := snapshotHasOverlay || overlayOnly || hasVisibleOverlay || (req.SnapshotVersion == 0 && e.perasDirectoryHasOverlay(mount, req.Parent))
	plan, err := compile.CompileDirectoryReadPlan(req, mountRecord.Identity(), includeOverlay, overlayOnly)
	if err != nil {
		return nil, err
	}
	if overlayOnly {
		return e.scanDentries(ctx, plan, 0, false)
	}
	var out []fsmeta.DentryRecord
	snapshotRead := req.SnapshotVersion != 0
	err = e.withReadRetry(ctx, req.SnapshotVersion, func(version uint64) error {
		var err error
		out, err = e.scanDentries(ctx, plan, version, snapshotRead)
		return err
	})
	return out, err
}

// ReadDirPlus returns one directory page fused with inode attributes at the
// same snapshot version. This is the first native fsmeta operation that avoids
// client-side dentry scan plus N point reads.
//
// When a dirpage cache is wired and the request omits an explicit
// SnapshotVersion (i.e. the caller is asking for "latest"), ReadDirPlus checks
// the cache first against the parent's current invalidation epoch. Peras-backed
// reads bypass the persistent cache because visible overlay rows are not durable
// until they have flushed and installed.
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
	snapshotHasOverlay := req.SnapshotVersion != 0 && e.perasSnapshotDirectoryHasOverlay(req.SnapshotVersion, mount, req.Parent)
	overlayOnly := req.SnapshotVersion == 0 && e.perasDirectoryBaseEmpty(mount, req.Parent)
	hasVisibleOverlay := req.SnapshotVersion == 0 && e.perasDirectoryHasVisibleOverlay(mount, req.Parent)
	includeOverlay := snapshotHasOverlay || overlayOnly || hasVisibleOverlay || (req.SnapshotVersion == 0 && e.perasDirectoryHasOverlay(mount, req.Parent))
	plan, err := compile.CompileDirectoryReadPlan(req, mount, includeOverlay, overlayOnly)
	if err != nil {
		return nil, err
	}

	useDirPage := e.dirPages != nil && req.SnapshotVersion == 0 && !hasVisibleOverlay
	var pageKey dirpage.PageKey
	var frontier uint64
	if useDirPage {
		pageKey = dirPageKey(req.Mount, req.Parent, req.StartAfter, plan.Limit)
		frontier = e.dirPageReadFrontier(pageKey.Directory(), mount, req.Parent)
		if entries, ok := e.dirPages.Lookup(pageKey, frontier); ok {
			if cached, err := decodeDirPageEntries(pageKey, entries); err == nil {
				return cached, nil
			}
		}
	}
	if overlayOnly {
		dentries, err := e.scanDentries(ctx, plan, 0, false)
		if err != nil {
			return nil, err
		}
		if pairs, ok, err := e.readDirPlusFromPerasView(mount, dentries); err != nil {
			return nil, err
		} else if ok {
			if useDirPage {
				e.materializeDirPage(pageKey, frontier, pairs)
			}
			return pairs, nil
		}
	}

	var out []fsmeta.DentryAttrPair
	snapshotRead := req.SnapshotVersion != 0
	err = e.withReadRetry(ctx, req.SnapshotVersion, func(version uint64) error {
		dentries, err := e.scanDentries(ctx, plan, version, snapshotRead)
		if err != nil {
			return err
		}
		if len(dentries) == 0 {
			out = []fsmeta.DentryAttrPair{}
			return nil
		}
		inodeKeys, err := compile.CompileReadDirPlusInodeKeys(mount, dentries)
		if err != nil {
			return err
		}
		inodeValues, inodePresent, err := e.batchGetMergedValuesOrdered(ctx, inodeKeys, version, includeOverlay, snapshotRead)
		if err != nil {
			return err
		}
		pairs := make([]fsmeta.DentryAttrPair, 0, len(dentries))
		for i, dentry := range dentries {
			if !inodePresent[i] {
				return fmt.Errorf("%w: inode %d", fsmeta.ErrNotFound, dentry.Inode)
			}
			inode, err := fsmeta.DecodeInodeValue(inodeValues[i])
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
		e.materializeDirPage(pageKey, frontier, out)
	}
	return out, nil
}

func (e *Executor) materializeDirPage(pageKey dirpage.PageKey, frontier uint64, pairs []fsmeta.DentryAttrPair) {
	if e == nil || e.dirPages == nil {
		return
	}
	entries, err := encodeDirPageEntries(pairs)
	if err != nil {
		return
	}
	_ = e.dirPages.MaterializeAsync(pageKey, frontier, entries)
}

func (e *Executor) readDirPlusFromPerasView(mount fsmeta.MountIdentity, dentries []fsmeta.DentryRecord) ([]fsmeta.DentryAttrPair, bool, error) {
	if len(dentries) == 0 {
		return []fsmeta.DentryAttrPair{}, true, nil
	}
	inodeKeys, err := compile.CompileReadDirPlusInodeKeys(mount, dentries)
	if err != nil {
		return nil, false, err
	}
	overlay := e.perasOverlay()
	if overlay == nil {
		return nil, false, nil
	}
	pairs := make([]fsmeta.DentryAttrPair, 0, len(dentries))
	for i, dentry := range dentries {
		value, deleted, ok := overlay.GetPerasOverlayView(inodeKeys[i])
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

func (e *Executor) scanDentries(ctx context.Context, plan compile.DirectoryReadPlan, version uint64, snapshotRead bool) ([]fsmeta.DentryRecord, error) {
	var kvs []KV
	stats := compile.DirectoryReadStats{UsedPerasOnly: plan.PerasOnly}
	if !plan.PerasOnly {
		if plan.IncludePeras {
			var perasRows uint32
			var err error
			kvs, stats.BaseRows, perasRows, stats.UsedDirIndex, err = e.scanMergedDirectoryRows(ctx, plan, version, snapshotRead)
			if err != nil {
				return nil, err
			}
			stats.PerasRows = perasRows
		} else {
			var err error
			kvs, err = e.runner.Scan(ctx, plan.StartKey, plan.Limit, version)
			if err != nil {
				return nil, err
			}
			stats.BaseRows = uint32(len(kvs))
		}
	} else if plan.IncludePeras {
		var perasRows uint32
		kvs, perasRows, stats.UsedDirIndex = e.mergePerasDirectoryOverlayScan(kvs, plan.Prefix, plan.StartKey, plan.Limit)
		stats.PerasRows = perasRows
	}
	out := make([]fsmeta.DentryRecord, 0, len(kvs))
	for _, kv := range kvs {
		if !bytes.HasPrefix(kv.Key, plan.Prefix) {
			break
		}
		record, err := fsmeta.DecodeDentryValue(kv.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	stats.OutputRows = uint32(len(out))
	e.perasDirectoryRead.record(stats)
	return out, nil
}

func (e *Executor) perasDirectoryBaseEmpty(mount fsmeta.MountIdentity, parent fsmeta.InodeID) bool {
	index := e.perasPredicateIndex()
	if index == nil {
		return false
	}
	return index.DirectoryEmpty(mount, parent)
}

func (e *Executor) perasDirectoryHasOverlay(mount fsmeta.MountIdentity, parent fsmeta.InodeID) bool {
	overlay := e.perasOverlay()
	if overlay == nil {
		return false
	}
	presence, ok := overlay.(PerasDirectoryOverlayPresence)
	if !ok {
		return true
	}
	prefix, err := fsmeta.EncodeDentryPrefix(mount, parent)
	if err != nil {
		return true
	}
	return presence.HasPerasDirectory(prefix)
}

func (e *Executor) perasDirectoryHasVisibleOverlay(mount fsmeta.MountIdentity, parent fsmeta.InodeID) bool {
	overlay := e.perasOverlay()
	if overlay == nil {
		return false
	}
	presence, ok := overlay.(PerasVisibleDirectoryPresence)
	if !ok {
		return e.perasDirectoryHasOverlay(mount, parent)
	}
	prefix, err := fsmeta.EncodeDentryPrefix(mount, parent)
	if err != nil {
		return true
	}
	return presence.HasPerasVisibleDirectory(prefix)
}

func (e *Executor) perasSnapshotDirectoryHasOverlay(version uint64, mount fsmeta.MountIdentity, parent fsmeta.InodeID) bool {
	reader := e.perasSnapshotOverlay()
	if reader == nil {
		return false
	}
	prefix, err := fsmeta.EncodeDentryPrefix(mount, parent)
	if err != nil {
		return true
	}
	return reader.HasPerasSnapshotDirectory(version, prefix)
}

func (e *Executor) dirPageReadFrontier(directory dirpage.DirectoryKey, mount fsmeta.MountIdentity, parent fsmeta.InodeID) uint64 {
	e.syncDirPagePerasFrontier(directory, e.perasDirectoryCacheFrontier(mount, parent))
	return e.dirPages.CurrentEpoch(directory)
}

func (e *Executor) perasDirectoryCacheFrontier(mount fsmeta.MountIdentity, parent fsmeta.InodeID) uint64 {
	overlay := e.perasOverlay()
	if overlay == nil {
		return 0
	}
	reporter, ok := overlay.(PerasDirectoryCacheFrontier)
	if !ok {
		return 0
	}
	prefix, err := fsmeta.EncodeDentryPrefix(mount, parent)
	if err != nil {
		return 0
	}
	return reporter.PerasDirectoryCacheFrontier(prefix)
}

func (e *Executor) syncDirPagePerasFrontier(directory dirpage.DirectoryKey, frontier uint64) {
	if e == nil || e.dirPages == nil {
		return
	}
	e.dirPagePerasMu.Lock()
	if e.dirPagePerasFrontier == nil {
		e.dirPagePerasFrontier = make(map[dirpage.DirectoryKey]uint64)
	}
	previous, known := e.dirPagePerasFrontier[directory]
	if (!known && frontier == 0) || (known && previous == frontier) {
		e.dirPagePerasMu.Unlock()
		return
	}
	if frontier == 0 {
		delete(e.dirPagePerasFrontier, directory)
	} else {
		e.dirPagePerasFrontier[directory] = frontier
	}
	e.dirPagePerasMu.Unlock()
	e.dirPages.Invalidate(directory)
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
	program, err := compile.CompileGetAttrReadProgram(mount, inodeID)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	value, ok, err := e.getMergedProgramValue(ctx, program, version)
	if err != nil || !ok {
		return fsmeta.InodeRecord{}, ok, err
	}
	inode, err := fsmeta.DecodeInodeValue(value)
	if err != nil {
		return fsmeta.InodeRecord{}, false, err
	}
	return inode, true, nil
}

func (e *Executor) readSessionByKey(ctx context.Context, mount fsmeta.MountIdentity, key []byte, version uint64) (fsmeta.SessionRecord, bool, error) {
	parts, ok := fsmeta.InspectKey(key)
	if !ok || parts.Kind != fsmeta.KeyKindSession {
		return fsmeta.SessionRecord{}, false, fsmeta.ErrInvalidKey
	}
	if parts.MountKeyID != mount.MountKeyID {
		return fsmeta.SessionRecord{}, false, fsmeta.ErrInvalidRequest
	}
	program, err := compile.CompileReadSessionKeyProgram(mount, key)
	if err != nil {
		return fsmeta.SessionRecord{}, false, err
	}
	value, ok, err := e.getMergedProgramValue(ctx, program, version)
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
