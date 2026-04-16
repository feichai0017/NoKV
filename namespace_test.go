package NoKV

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/feichai0017/NoKV/engine/wal"
	ns "github.com/feichai0017/NoKV/namespace"
	"github.com/stretchr/testify/require"
)

func TestDBNamespaceFacadeCreateList(t *testing.T) {
	db := openNamespaceFacadeTestDB(t)
	defer func() { require.NoError(t, db.Close()) }()

	h := db.Namespace(NamespaceOptions{Shards: 4})
	require.NotNil(t, h)

	require.NoError(t, h.Create([]byte("/bucket/a/file1"), ns.EntryKindFile, []byte("m1")))
	require.NoError(t, h.Create([]byte("/bucket/a/file2"), ns.EntryKindFile, []byte("m2")))

	meta, err := h.Lookup([]byte("/bucket/a/file1"))
	require.NoError(t, err)
	require.Equal(t, []byte("m1"), meta)

	entries, cursor, stats, err := h.RepairAndList([]byte("/bucket/a"), ns.Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Empty(t, cursor.PageID)
	require.GreaterOrEqual(t, stats.PagesVisited, 1)
}

func TestDBNamespaceFacadePaginatedMembershipStableAcrossMaterialize(t *testing.T) {
	db := openNamespaceFacadeTestDB(t)
	defer func() { require.NoError(t, db.Close()) }()

	h := db.Namespace(NamespaceOptions{Shards: 2})
	require.NotNil(t, h)

	for _, name := range []string{"a0", "a1", "b0", "b1", "c0", "c1"} {
		require.NoError(t, h.Create([]byte("/bucket/hot/"+name), ns.EntryKindFile, []byte(name)))
	}

	before := collectNamespaceNames(t, h, []byte("/bucket/hot"), 2)
	require.Len(t, before, 6)

	_, err := h.Materialize([]byte("/bucket/hot"))
	require.NoError(t, err)

	after := collectNamespaceNames(t, h, []byte("/bucket/hot"), 2)
	require.Equal(t, before, after)
}

func TestDBNamespaceFacadeDeleteVisibleBeforeAndAfterMaterialize(t *testing.T) {
	db := openNamespaceFacadeTestDB(t)
	defer func() { require.NoError(t, db.Close()) }()

	h := db.Namespace(NamespaceOptions{Shards: 2})
	require.NotNil(t, h)

	for _, name := range []string{"file1", "file2", "file3"} {
		require.NoError(t, h.Create([]byte("/bucket/hot/"+name), ns.EntryKindFile, []byte(name)))
	}
	_, err := h.Materialize([]byte("/bucket/hot"))
	require.NoError(t, err)

	require.NoError(t, h.Delete([]byte("/bucket/hot/file2")))

	before := collectNamespaceNames(t, h, []byte("/bucket/hot"), 2)
	require.Equal(t, []string{"file1", "file3"}, before)

	_, err = h.Materialize([]byte("/bucket/hot"))
	require.NoError(t, err)

	after := collectNamespaceNames(t, h, []byte("/bucket/hot"), 2)
	require.Equal(t, before, after)
}

func TestDBNamespaceFacadeCertifiedRejectThenRepairRetry(t *testing.T) {
	db := openNamespaceFacadeTestDB(t)
	defer func() { require.NoError(t, db.Close()) }()

	h := db.Namespace(NamespaceOptions{Shards: 2})
	require.NotNil(t, h)

	for _, name := range []string{"file1", "file2"} {
		require.NoError(t, h.Create([]byte("/bucket/hot/"+name), ns.EntryKindFile, []byte(name)))
	}
	_, err := h.Materialize([]byte("/bucket/hot"))
	require.NoError(t, err)

	entries, _, _, err := h.List([]byte("/bucket/hot"), ns.Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 2)

	require.NoError(t, h.Create([]byte("/bucket/hot/file3"), ns.EntryKindFile, []byte("file3")))

	_, _, _, err = h.List([]byte("/bucket/hot"), ns.Cursor{}, 16)
	require.ErrorIs(t, err, ns.ErrCoverageIncomplete)

	entries, _, _, err = h.RepairAndList([]byte("/bucket/hot"), ns.Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 3)

	entries, _, _, err = h.List([]byte("/bucket/hot"), ns.Cursor{}, 16)
	require.NoError(t, err)
	require.Len(t, entries, 3)
}

func TestDBNamespaceFacadeEndToEndPersistedReadPlaneDeepDescendants(t *testing.T) {
	db := openNamespaceFacadeTestDB(t)
	defer func() { require.NoError(t, db.Close()) }()

	h := db.Namespace(NamespaceOptions{Shards: 4})
	require.NotNil(t, h)

	parent := []byte("/bucket/hot")
	expected := seedDeepNamespaceChildren(t, h, string(parent), 6, 3)

	_, pages, err := h.MaterializeReadPlane(parent, 2)
	require.NoError(t, err)
	require.Len(t, pages, 3)

	view, ok, err := h.LoadReadPlaneView(parent)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, view.Root.Pages, 3)

	fromFacade := collectNamespaceNames(t, h, parent, 2)
	require.Equal(t, expected, fromFacade)

	fromView := collectReadPlaneNames(t, view, 2)
	require.Equal(t, expected, fromView)
}

func TestDBNamespaceFacadeEndToEndPersistedReadPlaneDeltaMerge(t *testing.T) {
	db := openNamespaceFacadeTestDB(t)
	defer func() { require.NoError(t, db.Close()) }()

	h := db.Namespace(NamespaceOptions{Shards: 4})
	require.NotNil(t, h)

	parent := []byte("/bucket/hot")
	for _, name := range []string{"file1", "file2", "file3"} {
		require.NoError(t, h.Create(joinNamespacePath(parent, name), ns.EntryKindFile, []byte(name)))
	}

	_, err := h.Materialize(parent)
	require.NoError(t, err)

	require.NoError(t, h.Create(joinNamespacePath(parent, "file4"), ns.EntryKindFile, []byte("file4")))
	require.NoError(t, h.Delete(joinNamespacePath(parent, "file2")))

	before := collectNamespaceNames(t, h, parent, 2)
	require.Equal(t, []string{"file1", "file3", "file4"}, before)

	_, err = h.Materialize(parent)
	require.NoError(t, err)

	view, ok, err := h.LoadReadPlaneView(parent)
	require.NoError(t, err)
	require.True(t, ok)

	after := collectNamespaceNames(t, h, parent, 2)
	require.Equal(t, before, after)
	require.Equal(t, before, collectReadPlaneNames(t, view, 2))
}

func TestDBNamespaceFacadeVerifyAndRebuildPersistedReadPlane(t *testing.T) {
	db := openNamespaceFacadeTestDB(t)
	defer func() { require.NoError(t, db.Close()) }()

	h := db.Namespace(NamespaceOptions{Shards: 4})
	require.NotNil(t, h)

	parent := []byte("/bucket/hot")
	expected := seedDeepNamespaceChildren(t, h, string(parent), 4, 2)
	_, err := h.Materialize(parent)
	require.NoError(t, err)

	okStats, err := h.Verify(parent)
	require.NoError(t, err)
	require.True(t, okStats.Consistent)

	kv := ns.NewNoKVStore(db)
	require.NoError(t, deleteNamespaceReadPlane(kv, parent))

	h2 := db.Namespace(NamespaceOptions{Shards: 4})
	require.NotNil(t, h2)

	drifted, err := h2.Verify(parent)
	require.NoError(t, err)
	require.False(t, drifted.Consistent)
	require.Equal(t, expected, drifted.Membership.MissingNames)

	rebuildStats, err := h2.Rebuild(parent)
	require.NoError(t, err)
	require.Equal(t, len(expected), rebuildStats.TruthEntries)
	require.GreaterOrEqual(t, rebuildStats.PagesWritten, 1)

	after, err := h2.Verify(parent)
	require.NoError(t, err)
	require.Truef(t, after.Consistent, "verify after rebuild: %+v", after)
	require.Equal(t, expected, collectNamespaceNames(t, h2, parent, 2))
}

func TestDBNamespaceFacadeReopenPreservesTruthAndDeltaPlaneBeforeRepair(t *testing.T) {
	workdir := filepath.Join(t.TempDir(), "nokv")
	db := openNamespaceFacadeTestDBAt(t, workdir)

	h := db.Namespace(NamespaceOptions{Shards: 4})
	parent := []byte("/bucket/hot")
	require.NoError(t, h.Create(joinNamespacePath(parent, "file1"), ns.EntryKindFile, []byte("m1")))
	require.NoError(t, h.Create(joinNamespacePath(parent, "file2"), ns.EntryKindFile, []byte("m2")))

	statsBefore, err := h.Stats(parent)
	require.NoError(t, err)
	require.Equal(t, 2, statsBefore.DeltaRecords)
	require.NoError(t, db.Close())

	db = openNamespaceFacadeTestDBAt(t, workdir)
	defer func() { require.NoError(t, db.Close()) }()

	h2 := db.Namespace(NamespaceOptions{Shards: 4})
	meta, err := h2.Lookup(joinNamespacePath(parent, "file1"))
	require.NoError(t, err)
	require.Equal(t, []byte("m1"), meta)

	statsAfter, err := h2.Stats(parent)
	require.NoError(t, err)
	require.Equal(t, 2, statsAfter.DeltaRecords)

	_, _, _, err = h2.List(parent, ns.Cursor{}, 2)
	require.ErrorIs(t, err, ns.ErrCoverageIncomplete)
}

func TestDBNamespaceFacadeReopenConvenienceListRepairsDeltaPlane(t *testing.T) {
	workdir := filepath.Join(t.TempDir(), "nokv")
	db := openNamespaceFacadeTestDBAt(t, workdir)

	h := db.Namespace(NamespaceOptions{Shards: 4})
	parent := []byte("/bucket/hot")
	require.NoError(t, h.Create(joinNamespacePath(parent, "file1"), ns.EntryKindFile, []byte("m1")))
	require.NoError(t, h.Create(joinNamespacePath(parent, "file2"), ns.EntryKindFile, []byte("m2")))
	require.NoError(t, db.Close())

	db = openNamespaceFacadeTestDBAt(t, workdir)
	defer func() { require.NoError(t, db.Close()) }()

	h2 := db.Namespace(NamespaceOptions{Shards: 4})
	names := collectNamespaceNames(t, h2, parent, 2)
	require.Equal(t, []string{"file1", "file2"}, names)

	statsAfter, err := h2.Stats(parent)
	require.NoError(t, err)
	require.Equal(t, 0, statsAfter.DeltaRecords)

	verify, err := h2.Verify(parent)
	require.NoError(t, err)
	require.True(t, verify.Consistent)
}

func TestDBNamespaceFacadeReopenPreservesPersistedReadPlane(t *testing.T) {
	workdir := filepath.Join(t.TempDir(), "nokv")
	db := openNamespaceFacadeTestDBAt(t, workdir)

	h := db.Namespace(NamespaceOptions{Shards: 4})
	parent := []byte("/bucket/hot")
	expected := seedDeepNamespaceChildren(t, h, string(parent), 4, 2)
	_, err := h.Materialize(parent)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db = openNamespaceFacadeTestDBAt(t, workdir)
	defer func() { require.NoError(t, db.Close()) }()

	h2 := db.Namespace(NamespaceOptions{Shards: 4})
	view, ok, err := h2.LoadReadPlaneView(parent)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, view.Root.Pages)

	verify, err := h2.Verify(parent)
	require.NoError(t, err)
	require.True(t, verify.Consistent)
	require.Equal(t, expected, collectNamespaceNames(t, h2, parent, 2))
	require.Equal(t, expected, collectReadPlaneNames(t, view, 2))
}

func TestDBNamespaceFacadeCreateWritesTruthAndDeltaInOneWALEntryBatch(t *testing.T) {
	workdir := filepath.Join(t.TempDir(), "nokv")
	db := openNamespaceFacadeTestDBAt(t, workdir)
	defer func() { require.NoError(t, db.Close()) }()

	h := db.Namespace(NamespaceOptions{Shards: 4})
	parent := []byte("/bucket/hot")
	path := joinNamespacePath(parent, "file1")
	require.NoError(t, h.Create(path, ns.EntryKindFile, []byte("m1")))
	require.NoError(t, db.WAL().Sync())

	truthKey := []byte("M|/bucket/hot/file1")
	deltaPrefix := []byte("LD|/bucket/hot|")
	var matched bool
	err := db.WAL().Replay(func(info wal.EntryInfo, payload []byte) error {
		if info.Type != wal.RecordTypeEntryBatch {
			return nil
		}
		entries, err := wal.DecodeEntryBatch(payload)
		if err != nil {
			return err
		}
		defer func() {
			for _, entry := range entries {
				entry.DecrRef()
			}
		}()
		if len(entries) != 2 {
			return nil
		}
		var foundTruth, foundDelta bool
		for _, entry := range entries {
			_, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
			require.True(t, ok)
			switch {
			case string(userKey) == string(truthKey):
				foundTruth = true
			case len(userKey) >= len(deltaPrefix) && string(userKey[:len(deltaPrefix)]) == string(deltaPrefix):
				foundDelta = true
			}
		}
		if foundTruth && foundDelta {
			matched = true
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, matched)
}

func openNamespaceFacadeTestDB(t *testing.T) *DB {
	t.Helper()
	return openNamespaceFacadeTestDBAt(t, filepath.Join(t.TempDir(), "nokv"))
}

func openNamespaceFacadeTestDBAt(t *testing.T, workdir string) *DB {
	t.Helper()
	opt := NewDefaultOptions()
	opt.WorkDir = workdir
	opt.EnableWALWatchdog = false
	opt.ValueLogGCInterval = 0
	opt.HotRingEnabled = false
	db, err := Open(opt)
	require.NoError(t, err)
	h := db.Namespace(NamespaceOptions{Shards: 4})
	mustNamespaceEnsure(t, h, "/bucket", ns.EntryKindDirectory, []byte("bucket"))
	mustNamespaceEnsure(t, h, "/bucket/a", ns.EntryKindDirectory, []byte("a"))
	mustNamespaceEnsure(t, h, "/bucket/hot", ns.EntryKindDirectory, []byte("hot"))
	h.Close()
	return db
}

func collectNamespaceNames(t *testing.T, h *NamespaceHandle, parent []byte, limit int) []string {
	t.Helper()
	cursor := ns.Cursor{}
	out := make([]string, 0, 8)
	for {
		entries, next, _, err := h.RepairAndList(parent, cursor, limit)
		require.NoError(t, err)
		for _, entry := range entries {
			out = append(out, string(entry.Name))
		}
		if len(next.PageID) == 0 {
			break
		}
		cursor = next
	}
	return out
}

func collectReadPlaneNames(t *testing.T, view ns.ReadPlaneView, limit int) []string {
	t.Helper()
	cursor := ns.Cursor{}
	out := make([]string, 0, 8)
	for {
		entries, next, _, err := view.List(cursor, limit)
		require.NoError(t, err)
		for _, entry := range entries {
			out = append(out, string(entry.Name))
		}
		if len(next.PageID) == 0 {
			break
		}
		cursor = next
	}
	return out
}

func seedDeepNamespaceChildren(t *testing.T, h *NamespaceHandle, parent string, children, descendantsPerChild int) []string {
	t.Helper()
	names := make([]string, 0, children)
	for child := range children {
		dir := joinNamespaceName("dir", child)
		names = append(names, dir)
		mustNamespaceCreate(t, h, parent+"/"+dir, ns.EntryKindDirectory, []byte(dir))
		mustNamespaceCreate(t, h, parent+"/"+dir+"/nested-00", ns.EntryKindDirectory, []byte("nested-00"))
		for descendant := range descendantsPerChild {
			nestedParent := parent + "/" + dir + "/nested-00"
			leaf := joinNamespaceName("leaf", descendant)
			mustNamespaceCreate(t, h, nestedParent+"/"+leaf, ns.EntryKindFile, []byte(leaf))
		}
	}
	sort.Strings(names)
	return names
}

func joinNamespacePath(parent []byte, child string) []byte {
	return []byte(string(parent) + "/" + child)
}

func joinNamespaceName(prefix string, n int) string {
	return fmt.Sprintf("%s-%06d", prefix, n)
}

func deleteNamespaceReadPlane(kv ns.KV, parent []byte) error {
	rootPairs, err := kv.ScanPrefix([]byte("LR|"+string(parent)), nil, 0)
	if err != nil {
		return err
	}
	pagePairs, err := kv.ScanPrefix([]byte("LP|"+string(parent)+"|"), nil, 0)
	if err != nil {
		return err
	}
	batch := make([]ns.Mutation, 0, len(rootPairs)+len(pagePairs))
	for _, pair := range rootPairs {
		batch = append(batch, ns.Mutation{Kind: ns.MutationDelete, Key: pair.Key})
	}
	for _, pair := range pagePairs {
		batch = append(batch, ns.Mutation{Kind: ns.MutationDelete, Key: pair.Key})
	}
	return kv.Apply(batch)
}

func mustNamespaceCreate(t *testing.T, h *NamespaceHandle, path string, kind ns.EntryKind, meta []byte) {
	t.Helper()
	require.NoError(t, h.Create([]byte(path), kind, meta))
}

func mustNamespaceEnsure(t *testing.T, h *NamespaceHandle, path string, kind ns.EntryKind, meta []byte) {
	t.Helper()
	err := h.Create([]byte(path), kind, meta)
	if err != nil {
		require.ErrorIs(t, err, ns.ErrPathExists)
	}
}

func TestDBNamespaceFacadeCreateRejectsMissingParentAndFileParent(t *testing.T) {
	db := openNamespaceFacadeTestDB(t)
	defer func() { require.NoError(t, db.Close()) }()

	h := db.Namespace(NamespaceOptions{Shards: 4})
	require.ErrorIs(t, h.Create([]byte("/missing/a/file1"), ns.EntryKindFile, []byte("m1")), ns.ErrParentNotFound)

	mustNamespaceCreate(t, h, "/bucket/file-parent", ns.EntryKindFile, []byte("fp"))
	require.ErrorIs(t, h.Create([]byte("/bucket/file-parent/child"), ns.EntryKindFile, []byte("m1")), ns.ErrParentNotDir)
}

func TestDBNamespaceFacadeFaultInjectedWALWriteFailsWithoutPartialNamespaceCommit(t *testing.T) {
	workdir := filepath.Join(t.TempDir(), "nokv")
	walPath := filepath.Join(workdir, "00001.wal")
	injected := errors.New("namespace wal write injected")
	opt := NewDefaultOptions()
	opt.WorkDir = workdir
	opt.EnableWALWatchdog = false
	opt.ValueLogGCInterval = 0
	opt.HotRingEnabled = false
	db, err := Open(opt)
	require.NoError(t, err)

	h := db.Namespace(NamespaceOptions{Shards: 4})
	mustNamespaceEnsure(t, h, "/bucket", ns.EntryKindDirectory, []byte("bucket"))
	mustNamespaceEnsure(t, h, "/bucket/hot", ns.EntryKindDirectory, []byte("hot"))
	require.NoError(t, db.Close())

	opt.FS = vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpFileWrite, walPath, injected),
	))
	opt.WALBufferSize = 256 << 10
	opt.ValueThreshold = 1 << 20
	db, err = Open(opt)
	require.NoError(t, err)

	big := bytes.Repeat([]byte("w"), 512<<10)
	h = db.Namespace(NamespaceOptions{Shards: 4})
	err = h.Create([]byte("/bucket/hot/file1"), ns.EntryKindFile, big)
	require.ErrorIs(t, err, injected)
	err = db.Close()
	require.ErrorIs(t, err, injected)

	opt.FS = vfs.OSFS{}
	db, err = Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	h = db.Namespace(NamespaceOptions{Shards: 4})
	_, err = h.Lookup([]byte("/bucket/hot/file1"))
	require.ErrorIs(t, err, ns.ErrPathNotFound)

	entries, _, stats, err := h.RepairAndList([]byte("/bucket/hot"), ns.Cursor{}, 16)
	require.NoError(t, err)
	require.Empty(t, entries)
	require.Equal(t, 0, stats.DeltasRead)
}

func TestDBCloseClearsRuntimeModuleRegistry(t *testing.T) {
	db := openNamespaceFacadeTestDB(t)
	h1 := db.Namespace(NamespaceOptions{Shards: 2})
	h2 := db.Namespace(NamespaceOptions{Shards: 4})
	require.NotNil(t, h1)
	require.NotNil(t, h2)

	require.Equal(t, 2, db.runtimeModules.Count())

	require.NoError(t, db.Close())

	require.True(t, db.runtimeModules.Cleared())

	h1.Close()
	h2.Close()
}
