package namespacebench

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/engine/index"
	ns "github.com/feichai0017/NoKV/namespace"
)

const (
	namespaceDepth      = 4
	namespaceHotPrefix  = "/bucket/checkpoint/run-1"
	namespaceFlatRoot   = "/bucket"
	namespaceDeepParent = "/bucket/deep/a/b/c/d/e/f/g"
)

var (
	namespaceChildren            = envInt("NAMESPACE_CHILDREN", 16384)
	namespaceListLimit           = envInt("NAMESPACE_LIST_LIMIT", 512)
	namespacePagedShards         = envInt("NAMESPACE_PAGED_SHARDS", 16)
	namespaceMatChildren         = envInt("NAMESPACE_MAT_CHILDREN", 2048)
	namespacePageSize            = envInt("NAMESPACE_PAGE_SIZE", 64)
	namespaceMatChunk            = envInt("NAMESPACE_MAT_CHUNK", 4)
	namespaceDeepChildren        = envInt("NAMESPACE_DEEP_CHILDREN", 1024)
	namespaceDescendantsPerChild = envInt("NAMESPACE_DESCENDANTS_PER_CHILD", 32)
	namespaceReadPlanePageSize   = envInt("NAMESPACE_READPLANE_PAGE_SIZE", 128)
)

func envInt(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func BenchmarkNamespaceListSecondaryIndexPaginated(b *testing.B) {
	index := newNoKVSecondaryBaseline(b)
	defer index.Close(b)
	seedNoKVBaselineChildren(b, index, namespaceHotPrefix, namespaceChildren)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		total := 0
		cursor := ""
		for {
			out, next, err := index.ListPaginated(namespaceHotPrefix, cursor, namespacePageSize)
			if err != nil {
				b.Fatal(err)
			}
			total += len(out)
			if len(out) == 0 || next == "" {
				break
			}
			cursor = next
		}
		if total == 0 {
			b.Fatal("secondary index paginated returned no entries")
		}
	}
}

func BenchmarkNamespaceListFlatScanDeepDescendants(b *testing.B) {
	index := newNoKVFlatBaseline(b)
	defer index.Close(b)
	seedDeepDescendantBaseline(b, index, namespaceHotPrefix, namespaceDeepChildren, namespaceDescendantsPerChild)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		out, err := index.List(namespaceHotPrefix, namespaceListLimit)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("flat scan deep-descendant listing returned no entries")
		}
	}
}

func BenchmarkNamespaceListSecondaryIndexDeepDescendants(b *testing.B) {
	index := newNoKVSecondaryBaseline(b)
	defer index.Close(b)
	seedDeepDescendantBaseline(b, index, namespaceHotPrefix, namespaceDeepChildren, namespaceDescendantsPerChild)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		out, err := index.List(namespaceHotPrefix, namespaceListLimit)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("secondary index deep-descendant listing returned no entries")
		}
	}
}

func BenchmarkNamespaceListReadPlaneStorePath(b *testing.B) {
	store := newNoKVStoreIndex(b, namespacePagedShards)
	defer store.Close(b)
	for child := 0; child < namespaceChildren; child++ {
		if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
			b.Fatal(err)
		}
	}
	if _, _, err := store.store.MaterializeReadPlane([]byte(namespaceHotPrefix), namespaceReadPlanePageSize); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	var totalPages float64
	b.ResetTimer()
	for range b.N {
		out, _, stats, err := store.store.List([]byte(namespaceHotPrefix), ns.Cursor{}, namespaceListLimit)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("store-path read plane returned no entries")
		}
		totalPages += float64(stats.PagesVisited)
	}
	if b.N > 0 {
		b.ReportMetric(totalPages/float64(b.N), "pages/op")
	}
}

func BenchmarkNamespaceListReadPlaneStorePathPaginated(b *testing.B) {
	store := newNoKVStoreIndex(b, namespacePagedShards)
	defer store.Close(b)
	for child := 0; child < namespaceChildren; child++ {
		if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
			b.Fatal(err)
		}
	}
	if _, _, err := store.store.MaterializeReadPlane([]byte(namespaceHotPrefix), namespaceReadPlanePageSize); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	var totalPages float64
	b.ResetTimer()
	for range b.N {
		total := 0
		cursor := ns.Cursor{}
		for {
			out, next, stats, err := store.store.List([]byte(namespaceHotPrefix), cursor, namespacePageSize)
			if err != nil {
				b.Fatal(err)
			}
			total += len(out)
			totalPages += float64(stats.PagesVisited)
			if len(out) == 0 || len(next.PageID) == 0 {
				break
			}
			cursor = next
		}
		if total == 0 {
			b.Fatal("store-path read plane paginated returned no entries")
		}
	}
	if b.N > 0 {
		b.ReportMetric(totalPages/float64(b.N), "pages/op")
	}
}

func BenchmarkNamespaceListStrictReadPlaneStorePathPaginated(b *testing.B) {
	store := newNoKVStoreIndex(b, namespacePagedShards)
	defer store.Close(b)
	for child := 0; child < namespaceChildren; child++ {
		if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
			b.Fatal(err)
		}
	}
	if _, _, err := store.store.MaterializeReadPlane([]byte(namespaceHotPrefix), namespaceReadPlanePageSize); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	var totalPages float64
	b.ResetTimer()
	for range b.N {
		total := 0
		cursor := ns.Cursor{}
		for {
			out, next, stats, err := store.store.List([]byte(namespaceHotPrefix), cursor, namespacePageSize)
			if err != nil {
				b.Fatal(err)
			}
			total += len(out)
			totalPages += float64(stats.PagesVisited)
			if len(out) == 0 || len(next.PageID) == 0 {
				break
			}
			cursor = next
		}
		if total == 0 {
			b.Fatal("strict read plane paginated returned no entries")
		}
	}
	if b.N > 0 {
		b.ReportMetric(totalPages/float64(b.N), "pages/op")
	}
}

func BenchmarkNamespaceListReadPlaneViewPaginatedNoContract(b *testing.B) {
	store := newNoKVStoreIndex(b, namespacePagedShards)
	defer store.Close(b)
	for child := 0; child < namespaceChildren; child++ {
		if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
			b.Fatal(err)
		}
	}
	root, pages, err := store.store.MaterializeReadPlane([]byte(namespaceHotPrefix), namespaceReadPlanePageSize)
	if err != nil {
		b.Fatal(err)
	}
	view, err := ns.NewReadPlaneView(root, pages)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	var totalPages float64
	b.ResetTimer()
	for range b.N {
		total := 0
		cursor := ns.Cursor{}
		for {
			out, next, stats, err := view.List(cursor, namespacePageSize)
			if err != nil {
				b.Fatal(err)
			}
			total += len(out)
			totalPages += float64(stats.PagesVisited)
			if len(out) == 0 || len(next.PageID) == 0 {
				break
			}
			cursor = next
		}
		if total == 0 {
			b.Fatal("read-plane view paginated returned no entries")
		}
	}
	if b.N > 0 {
		b.ReportMetric(totalPages/float64(b.N), "pages/op")
	}
}

func BenchmarkNamespaceListReadPlaneStorePathDeepDescendants(b *testing.B) {
	store := newNoKVStoreIndex(b, namespacePagedShards)
	defer store.Close(b)
	seedDeepDescendantBaseline(b, store, namespaceHotPrefix, namespaceDeepChildren, namespaceDescendantsPerChild)
	if _, _, err := store.store.MaterializeReadPlane([]byte(namespaceHotPrefix), namespaceReadPlanePageSize); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	var totalPages float64
	b.ResetTimer()
	for range b.N {
		out, _, stats, err := store.store.List([]byte(namespaceHotPrefix), ns.Cursor{}, namespaceListLimit)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("store-path read plane deep-descendant listing returned no entries")
		}
		totalPages += float64(stats.PagesVisited)
	}
	if b.N > 0 {
		b.ReportMetric(totalPages/float64(b.N), "pages/op")
	}
}

func BenchmarkNamespaceListReadPlaneColdStartDeepDescendants(b *testing.B) {
	b.ReportAllocs()
	var totalPages float64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		workdir := filepath.Join(b.TempDir(), fmt.Sprintf("cold-start-%06d", i))
		db, kv := openNoKVBenchmarkDBAt(b, workdir)
		seedTruthOnlyDeepDescendants(b, kv, namespaceHotPrefix, namespaceDeepChildren, namespaceDescendantsPerChild)
		h := db.Namespace(NoKV.NamespaceOptions{Shards: namespacePagedShards})
		b.StartTimer()
		out, _, stats, err := h.RepairAndList([]byte(namespaceHotPrefix), ns.Cursor{}, namespaceListLimit)
		b.StopTimer()
		if err != nil {
			h.Close()
			if closeErr := db.Close(); closeErr != nil {
				b.Fatal(closeErr)
			}
			b.Fatal(err)
		}
		if len(out) == 0 {
			h.Close()
			if closeErr := db.Close(); closeErr != nil {
				b.Fatal(closeErr)
			}
			b.Fatal("cold-start read plane deep-descendant listing returned no entries")
		}
		totalPages += float64(stats.PagesVisited)
		h.Close()
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
	if b.N > 0 {
		b.ReportMetric(totalPages/float64(b.N), "pages/op")
	}
}

func BenchmarkNamespaceMixedCreateListSecondaryIndex(b *testing.B) {
	index := newNoKVSecondaryBaseline(b)
	defer index.Close(b)
	seedNoKVBaselineChildren(b, index, namespaceHotPrefix, namespaceChildren)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		child := fmt.Sprintf("mix-%08d", i)
		if err := index.Add(namespaceHotPrefix, child); err != nil {
			b.Fatal(err)
		}
		out, err := index.List(namespaceHotPrefix, namespacePageSize)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("secondary mixed create+list returned no entries")
		}
	}
}

func BenchmarkNamespaceMixedCreatePaginatedListSecondaryIndex(b *testing.B) {
	index := newNoKVSecondaryBaseline(b)
	defer index.Close(b)
	seedNoKVBaselineChildren(b, index, namespaceHotPrefix, namespaceChildren)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		child := fmt.Sprintf("mix-%08d", i)
		if err := index.Add(namespaceHotPrefix, child); err != nil {
			b.Fatal(err)
		}
		total := 0
		cursor := ""
		for {
			out, next, err := index.ListPaginated(namespaceHotPrefix, cursor, namespacePageSize)
			if err != nil {
				b.Fatal(err)
			}
			total += len(out)
			if len(out) == 0 || next == "" {
				break
			}
			cursor = next
		}
		if total == 0 {
			b.Fatal("secondary mixed create+paginated-list returned no entries")
		}
	}
}

func BenchmarkNamespaceMixedCreateListReadPlaneNoKV(b *testing.B) {
	store := newNoKVStoreIndex(b, namespacePagedShards)
	defer store.Close(b)
	for child := 0; child < namespaceChildren; child++ {
		if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	var totalPages, totalDeltas float64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		child := fmt.Sprintf("mix-%08d", i)
		if err := store.Add(namespaceHotPrefix, child); err != nil {
			b.Fatal(err)
		}
		out, _, stats, err := store.store.RepairAndList([]byte(namespaceHotPrefix), ns.Cursor{}, namespacePageSize)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("nokv-backed read plane mixed create+list returned no entries")
		}
		totalPages += float64(stats.PagesVisited)
		totalDeltas += float64(stats.DeltasRead)
	}
	if b.N > 0 {
		b.ReportMetric(totalPages/float64(b.N), "pages/op")
		b.ReportMetric(totalDeltas/float64(b.N), "deltas/op")
	}
}

func BenchmarkNamespaceMixedCreatePaginatedListReadPlaneNoKV(b *testing.B) {
	benchmarkNamespaceMixedCreatePaginatedListReadPlaneNoKV(b, namespacePagedShards)
}

func benchmarkNamespaceMixedCreatePaginatedListReadPlaneNoKV(b *testing.B, shards int) {
	store := newNoKVStoreIndex(b, shards)
	defer store.Close(b)
	for child := 0; child < namespaceChildren; child++ {
		if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	var totalPages, totalDeltas float64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		child := fmt.Sprintf("mix-%08d", i)
		if err := store.Add(namespaceHotPrefix, child); err != nil {
			b.Fatal(err)
		}
		total := 0
		cursor := ns.Cursor{}
		for {
			out, next, stats, err := store.store.RepairAndList([]byte(namespaceHotPrefix), cursor, namespacePageSize)
			if err != nil {
				b.Fatal(err)
			}
			total += len(out)
			totalPages += float64(stats.PagesVisited)
			totalDeltas += float64(stats.DeltasRead)
			if len(out) == 0 || len(next.PageID) == 0 {
				break
			}
			cursor = next
		}
		if total == 0 {
			b.Fatal("nokv-backed read plane mixed create+paginated-list returned no entries")
		}
	}
	if b.N > 0 {
		b.ReportMetric(totalPages/float64(b.N), "pages/op")
		b.ReportMetric(totalDeltas/float64(b.N), "deltas/op")
	}
}

func BenchmarkNamespaceSteadyStateMixedCreatePaginatedListReadPlaneNoKV(b *testing.B) {
	benchmarkNamespaceSteadyStateMixedCreatePaginatedListReadPlaneNoKV(b, namespacePagedShards, 1)
}

func BenchmarkNamespaceSteadyStateMixedCreatePaginatedListStrictRejectNoKV(b *testing.B) {
	store := newNoKVStoreIndex(b, namespacePagedShards)
	defer store.Close(b)
	for child := 0; child < namespaceChildren; child++ {
		if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
			b.Fatal(err)
		}
	}
	if _, _, err := store.store.MaterializeReadPlane([]byte(namespaceHotPrefix), namespaceReadPlanePageSize); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		child := fmt.Sprintf("mix-%08d", i)
		if err := store.Add(namespaceHotPrefix, child); err != nil {
			b.Fatal(err)
		}
		if _, err := firstStrictRejectCursor(store); err == nil {
			b.Fatal("expected strict listing to reject dirty page")
		} else if !ns.IsCoverageIncomplete(err) {
			b.Fatal(err)
		}
		if _, err := store.store.MaterializeDeltaPages([]byte(namespaceHotPrefix), 1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNamespaceSteadyStateMixedCreatePaginatedRepairThenCertifiedNoKV(b *testing.B) {
	store := newNoKVStoreIndex(b, namespacePagedShards)
	defer store.Close(b)
	for child := 0; child < namespaceChildren; child++ {
		if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
			b.Fatal(err)
		}
	}
	if _, _, err := store.store.MaterializeReadPlane([]byte(namespaceHotPrefix), namespaceReadPlanePageSize); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	var repairedPages float64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		child := fmt.Sprintf("mix-%08d", i)
		if err := store.Add(namespaceHotPrefix, child); err != nil {
			b.Fatal(err)
		}
		rejectCursor, err := firstStrictRejectCursor(store)
		if !ns.IsCoverageIncomplete(err) {
			if err == nil {
				b.Fatal("expected strict listing to reject dirty page")
			}
			b.Fatal(err)
		}
		out, _, stats, err := store.store.RepairAndList([]byte(namespaceHotPrefix), rejectCursor, namespacePageSize)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("repair path returned no entries")
		}
		repairedPages += float64(stats.PagesVisited)
		if _, err := strictWalkExpectSuccess(store); err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("strict path returned no entries after repair+materialize")
		}
	}
	if b.N > 0 {
		b.ReportMetric(repairedPages/float64(b.N), "repair-pages/op")
	}
}

func firstStrictRejectCursor(store *nokvStoreIndex) (ns.Cursor, error) {
	cursor := ns.Cursor{}
	for {
		out, next, _, err := store.store.List([]byte(namespaceHotPrefix), cursor, namespacePageSize)
		if err != nil {
			return cursor, err
		}
		if len(out) == 0 || len(next.PageID) == 0 {
			return ns.Cursor{}, nil
		}
		cursor = next
	}
}

func strictWalkExpectSuccess(store *nokvStoreIndex) (int, error) {
	total := 0
	cursor := ns.Cursor{}
	for {
		out, next, _, err := store.store.List([]byte(namespaceHotPrefix), cursor, namespacePageSize)
		if err != nil {
			return total, err
		}
		total += len(out)
		if len(out) == 0 || len(next.PageID) == 0 {
			return total, nil
		}
		cursor = next
	}
}

func benchmarkNamespaceSteadyStateMixedCreatePaginatedListReadPlaneNoKV(b *testing.B, shards, listsPerWrite int) {
	store := newNoKVStoreIndex(b, shards)
	defer store.Close(b)
	for child := 0; child < namespaceChildren; child++ {
		if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
			b.Fatal(err)
		}
	}
	if _, _, err := store.store.MaterializeReadPlane([]byte(namespaceHotPrefix), namespaceReadPlanePageSize); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	var totalPages, totalDeltas float64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		child := fmt.Sprintf("mix-%08d", i)
		if err := store.Add(namespaceHotPrefix, child); err != nil {
			b.Fatal(err)
		}
		for r := 0; r < listsPerWrite; r++ {
			total := 0
			cursor := ns.Cursor{}
			for {
				out, next, stats, err := store.store.RepairAndList([]byte(namespaceHotPrefix), cursor, namespacePageSize)
				if err != nil {
					b.Fatal(err)
				}
				total += len(out)
				totalPages += float64(stats.PagesVisited)
				totalDeltas += float64(stats.DeltasRead)
				if len(out) == 0 || len(next.PageID) == 0 {
					break
				}
				cursor = next
			}
			if total == 0 {
				b.Fatal("steady-state mixed create+paginated-list returned no entries")
			}
		}
	}
	if b.N > 0 {
		b.ReportMetric(totalPages/float64(b.N), "pages/op")
		b.ReportMetric(totalDeltas/float64(b.N), "deltas/op")
	}
}

func BenchmarkNamespaceMaterializeReadPlaneNoKV(b *testing.B) {
	b.ReportAllocs()
	var totalDeltas, totalPages float64
	for i := 0; i < b.N; i++ {
		store := newNoKVStoreIndex(b, namespacePagedShards)
		for child := 0; child < namespaceMatChildren; child++ {
			if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
				b.Fatal(err)
			}
		}
		stats, err := store.Materialize(namespaceHotPrefix)
		if err != nil {
			store.Close(b)
			b.Fatal(err)
		}
		totalDeltas += float64(stats.DeltasFolded)
		totalPages += float64(stats.PagesWritten)
		store.Close(b)
	}
	if b.N > 0 {
		b.ReportMetric(totalDeltas/float64(b.N), "deltas/op")
		b.ReportMetric(totalPages/float64(b.N), "pages/op")
	}
}

func BenchmarkNamespaceMaterializeReadPlaneNoKVHotPageFold(b *testing.B) {
	const pageEntries = 32
	b.ReportAllocs()
	var totalDeltas, totalDeltaPages, totalPages float64
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		store := newNoKVStoreIndex(b, namespacePagedShards)
		for child := 0; child < 255; child++ {
			if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
				store.Close(b)
				b.Fatal(err)
			}
		}
		if _, _, err := store.store.MaterializeReadPlane([]byte(namespaceHotPrefix), pageEntries); err != nil {
			store.Close(b)
			b.Fatal(err)
		}
		if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", 999999)); err != nil {
			store.Close(b)
			b.Fatal(err)
		}

		b.StartTimer()
		stats, err := store.store.MaterializeDeltaPages([]byte(namespaceHotPrefix), 1)
		b.StopTimer()
		if err != nil {
			store.Close(b)
			b.Fatal(err)
		}
		totalDeltas += float64(stats.DeltasFolded)
		totalDeltaPages += float64(stats.DeltaPagesFolded)
		totalPages += float64(stats.PagesWritten)
		store.Close(b)
	}
	if b.N > 0 {
		b.ReportMetric(totalDeltas/float64(b.N), "deltas/op")
		b.ReportMetric(totalDeltaPages/float64(b.N), "delta-pages/op")
		b.ReportMetric(totalPages/float64(b.N), "pages/op")
	}
}

func BenchmarkNamespaceMaterializeReadPlaneNoKVHotPageSplit(b *testing.B) {
	const pageEntries = 32
	b.ReportAllocs()
	var totalDeltas, totalDeltaPages, totalPages float64
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		store := newNoKVStoreIndex(b, namespacePagedShards)
		for child := 0; child < 256; child++ {
			if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
				store.Close(b)
				b.Fatal(err)
			}
		}
		if _, _, err := store.store.MaterializeReadPlane([]byte(namespaceHotPrefix), pageEntries); err != nil {
			store.Close(b)
			b.Fatal(err)
		}
		if err := store.Add(namespaceHotPrefix, "entry-000015a"); err != nil {
			store.Close(b)
			b.Fatal(err)
		}

		b.StartTimer()
		stats, err := store.store.MaterializeDeltaPages([]byte(namespaceHotPrefix), 1)
		b.StopTimer()
		if err != nil {
			store.Close(b)
			b.Fatal(err)
		}
		totalDeltas += float64(stats.DeltasFolded)
		totalDeltaPages += float64(stats.DeltaPagesFolded)
		totalPages += float64(stats.PagesWritten)
		store.Close(b)
	}
	if b.N > 0 {
		b.ReportMetric(totalDeltas/float64(b.N), "deltas/op")
		b.ReportMetric(totalDeltaPages/float64(b.N), "delta-pages/op")
		b.ReportMetric(totalPages/float64(b.N), "pages/op")
	}
}

func BenchmarkNamespaceVerifyLargeParent(b *testing.B) {
	store := newNoKVStoreIndex(b, namespacePagedShards)
	defer store.Close(b)
	for child := 0; child < namespaceMatChildren; child++ {
		if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := store.MaterializeAll(namespaceHotPrefix, namespaceMatChunk); err != nil {
		b.Fatal(err)
	}
	parent := []byte(namespaceHotPrefix)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		stats, err := store.store.Verify(parent)
		if err != nil {
			b.Fatal(err)
		}
		if !stats.Consistent {
			b.Fatal("verify reported unexpected drift")
		}
	}
}

func BenchmarkNamespaceRebuildLargeParent(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		store := newNoKVStoreIndex(b, namespacePagedShards)
		for child := 0; child < namespaceMatChildren; child++ {
			if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := store.MaterializeAll(namespaceHotPrefix, namespaceMatChunk); err != nil {
			store.Close(b)
			b.Fatal(err)
		}
		if err := deleteReadRootForBenchmark(store.storeKV(), []byte(namespaceHotPrefix)); err != nil {
			store.Close(b)
			b.Fatal(err)
		}
		store.store.Close()
		store.store = store.db.Namespace(NoKV.NamespaceOptions{Shards: namespacePagedShards})
		stats, err := store.store.Rebuild([]byte(namespaceHotPrefix))
		if err != nil {
			store.Close(b)
			b.Fatal(err)
		}
		if stats.PagesWritten == 0 {
			store.Close(b)
			b.Fatal("rebuild wrote no pages")
		}
		store.Close(b)
	}
}

func BenchmarkNamespaceVerifyDetectsDrift(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		store := newNoKVStoreIndex(b, namespacePagedShards)
		for child := 0; child < namespaceMatChildren; child++ {
			if err := store.Add(namespaceHotPrefix, fmt.Sprintf("entry-%06d", child)); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := store.MaterializeAll(namespaceHotPrefix, namespaceMatChunk); err != nil {
			store.Close(b)
			b.Fatal(err)
		}
		if err := deleteReadRootForBenchmark(store.storeKV(), []byte(namespaceHotPrefix)); err != nil {
			store.Close(b)
			b.Fatal(err)
		}
		store.store.Close()
		store.store = store.db.Namespace(NoKV.NamespaceOptions{Shards: namespacePagedShards})
		stats, err := store.store.Verify([]byte(namespaceHotPrefix))
		if err != nil {
			store.Close(b)
			b.Fatal(err)
		}
		if stats.Consistent {
			store.Close(b)
			b.Fatal("verify failed to detect drift")
		}
		store.Close(b)
	}
}

type nokvStoreIndex struct {
	db    *NoKV.DB
	store *NoKV.NamespaceHandle
	dirs  map[string]struct{}
}

type nokvFlatBaseline struct {
	db *NoKV.DB
	kv ns.KV
}

type nokvSecondaryBaseline struct {
	db *NoKV.DB
	kv ns.KV
}

func newNoKVFlatBaseline(b *testing.B) *nokvFlatBaseline {
	db, kv := openNoKVBenchmarkDB(b)
	return &nokvFlatBaseline{db: db, kv: kv}
}

func newNoKVSecondaryBaseline(b *testing.B) *nokvSecondaryBaseline {
	db, kv := openNoKVBenchmarkDB(b)
	return &nokvSecondaryBaseline{db: db, kv: kv}
}

func newNoKVStoreIndex(b *testing.B, shards int) *nokvStoreIndex {
	db, _ := openNoKVBenchmarkDB(b)
	idx := &nokvStoreIndex{
		db:    db,
		store: db.Namespace(NoKV.NamespaceOptions{Shards: shards}),
		dirs:  map[string]struct{}{"/": {}},
	}
	idx.ensureDir(b, "/bucket")
	idx.ensureDir(b, namespaceFlatRoot)
	idx.ensureDir(b, namespaceHotPrefix)
	return idx
}

func openNoKVBenchmarkDB(b *testing.B) (*NoKV.DB, ns.KV) {
	b.Helper()
	return openNoKVBenchmarkDBAt(b, filepath.Join(b.TempDir(), "nokv"))
}

func openNoKVBenchmarkDBAt(b *testing.B, workdir string) (*NoKV.DB, ns.KV) {
	b.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = workdir
	opt.EnableWALWatchdog = false
	opt.ValueLogGCInterval = 0
	opt.HotRingEnabled = false
	opt.WriteBatchMaxCount = 8192
	opt.MaxBatchCount = 8192
	opt.WriteBatchMaxSize = 64 << 20
	opt.MaxBatchSize = 64 << 20
	db, err := NoKV.Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	return db, ns.NewNoKVStore(db)
}

func seedDeepDescendantBaseline[T interface {
	Add(parent, child string) error
}](b *testing.B, index T, parent string, children, descendantsPerChild int) {
	b.Helper()
	for child := 0; child < children; child++ {
		dir := fmt.Sprintf("dir-%06d", child)
		if withDir, ok := any(index).(interface {
			AddDir(parent, child string) error
		}); ok {
			if err := withDir.AddDir(parent, dir); err != nil {
				b.Fatal(err)
			}
		} else {
			if err := index.Add(parent, dir); err != nil {
				b.Fatal(err)
			}
		}
		for descendant := 0; descendant < descendantsPerChild; descendant++ {
			nestedParent := parent + "/" + dir + "/nested-00"
			nestedChild := fmt.Sprintf("leaf-%06d", descendant)
			if err := index.Add(nestedParent, nestedChild); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func seedTruthOnlyDeepDescendants(b *testing.B, kv ns.KV, parent string, children, descendantsPerChild int) {
	b.Helper()
	for child := 0; child < children; child++ {
		dir := fmt.Sprintf("dir-%06d", child)
		if err := kv.Apply([]ns.Mutation{{
			Kind:  ns.MutationPut,
			Key:   []byte("M|" + parent + "/" + dir),
			Value: []byte(parent + "/" + dir),
		}}); err != nil {
			b.Fatal(err)
		}
		for descendant := 0; descendant < descendantsPerChild; descendant++ {
			nestedParent := parent + "/" + dir + "/nested-00"
			nestedChild := fmt.Sprintf("leaf-%06d", descendant)
			if err := kv.Apply([]ns.Mutation{{
				Kind:  ns.MutationPut,
				Key:   []byte("M|" + nestedParent + "/" + nestedChild),
				Value: []byte(nestedParent + "/" + nestedChild),
			}}); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func seedNoKVBaselineChildren[T interface {
	Add(parent, child string) error
}](b *testing.B, index T, parent string, children int) {
	b.Helper()
	for child := 0; child < children; child++ {
		if err := index.Add(parent, fmt.Sprintf("entry-%06d", child)); err != nil {
			b.Fatal(err)
		}
	}
}

func (s *nokvStoreIndex) Add(parent, child string) error {
	s.ensureDir(nil, parent)
	path := parent + "/" + child
	return s.store.Create([]byte(path), ns.EntryKindFile, []byte(path))
}

func (s *nokvStoreIndex) AddDir(parent, child string) error {
	s.ensureDir(nil, parent)
	path := parent + "/" + child
	err := s.store.Create([]byte(path), ns.EntryKindDirectory, []byte(path))
	if err != nil && !ns.IsPathExists(err) {
		return err
	}
	s.dirs[path] = struct{}{}
	return nil
}

func (s *nokvStoreIndex) Materialize(parent string) (ns.MaterializeStats, error) {
	return s.store.Materialize([]byte(parent))
}

func (s *nokvStoreIndex) MaterializeAll(parent string, deltaPagesPerRun int) (ns.MaterializeStats, error) {
	total := ns.MaterializeStats{}
	if _, ok, err := s.store.LoadReadPlaneView([]byte(parent)); err != nil {
		return ns.MaterializeStats{}, err
	} else if !ok {
		stats, err := s.store.Materialize([]byte(parent))
		if err != nil {
			return ns.MaterializeStats{}, err
		}
		total.DeltasFolded += stats.DeltasFolded
		total.DeltaPagesFolded += stats.DeltaPagesFolded
		total.PagesWritten += stats.PagesWritten
		total.PagesDeleted += stats.PagesDeleted
		total.EntriesMaterialized += stats.EntriesMaterialized
		return total, nil
	}
	for {
		stats, err := s.store.MaterializeDeltaPages([]byte(parent), deltaPagesPerRun)
		if err != nil {
			return ns.MaterializeStats{}, err
		}
		total.DeltasFolded += stats.DeltasFolded
		total.DeltaPagesFolded += stats.DeltaPagesFolded
		total.PagesWritten += stats.PagesWritten
		total.PagesDeleted += stats.PagesDeleted
		total.EntriesMaterialized += stats.EntriesMaterialized
		if stats.DeltasFolded == 0 {
			return total, nil
		}
	}
}

func (s *nokvStoreIndex) storeKV() ns.KV {
	return ns.NewNoKVStore(s.db)
}

func (s *nokvStoreIndex) ensureDir(tb testing.TB, path string) {
	if path == "" || path == "/" {
		return
	}
	if _, ok := s.dirs[path]; ok {
		return
	}
	parent := path[:strings.LastIndex(path, "/")]
	if parent == "" {
		parent = "/"
	}
	s.ensureDir(tb, parent)
	if err := s.store.Create([]byte(path), ns.EntryKindDirectory, []byte(path)); err != nil && !ns.IsPathExists(err) {
		if tb != nil {
			tb.Fatal(err)
		}
		panic(err)
	}
	s.dirs[path] = struct{}{}
}

func (s *nokvFlatBaseline) Add(parent, child string) error {
	path := parent + "/" + child
	return s.kv.Apply([]ns.Mutation{{
		Kind:  ns.MutationPut,
		Key:   []byte("M|" + path),
		Value: []byte(path),
	}})
}

func (s *nokvFlatBaseline) List(parent string, limit int) ([]string, error) {
	prefix := "M|" + parent
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	pairs, err := s.kv.ScanPrefix([]byte(prefix), nil, 0)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, limit)
	for _, pair := range pairs {
		rest := strings.TrimPrefix(string(pair.Key), prefix)
		if strings.Contains(rest, "/") {
			continue
		}
		out = append(out, rest)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (s *nokvFlatBaseline) Close(b *testing.B) {
	b.Helper()
	if s != nil && s.db != nil {
		if err := s.db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func (s *nokvSecondaryBaseline) Add(parent, child string) error {
	path := parent + "/" + child
	return s.kv.Apply([]ns.Mutation{
		{
			Kind:  ns.MutationPut,
			Key:   []byte("M|" + path),
			Value: []byte(path),
		},
		{
			Kind:  ns.MutationPut,
			Key:   []byte("S|" + parent + "|" + child),
			Value: []byte(path),
		},
	})
}

func (s *nokvSecondaryBaseline) List(parent string, limit int) ([]string, error) {
	prefix := []byte("S|" + parent + "|")
	pairs, err := s.kv.ScanPrefix(prefix, nil, 0)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, limit)
	for _, pair := range pairs {
		child := strings.TrimPrefix(string(pair.Key), string(prefix))
		out = append(out, child)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (s *nokvSecondaryBaseline) ListPaginated(parent, cursor string, limit int) ([]string, string, error) {
	if limit <= 0 {
		return nil, "", fmt.Errorf("secondary pagination limit must be positive: %d", limit)
	}
	prefix := []byte("S|" + parent + "|")
	iter := s.db.NewIterator(&index.Options{
		IsAsc:      true,
		LowerBound: prefix,
	})
	defer func() { _ = iter.Close() }()

	seekKey := prefix
	if cursor != "" {
		seekKey = []byte("S|" + parent + "|" + cursor)
	}
	iter.Seek(seekKey)

	out := make([]string, 0, limit)
	skipCursor := cursor != ""
	last := ""
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		if item == nil {
			break
		}
		entry := item.Entry()
		if entry == nil || !bytes.HasPrefix(entry.Key, prefix) {
			break
		}
		child := strings.TrimPrefix(string(entry.Key), string(prefix))
		if skipCursor && child == cursor {
			skipCursor = false
			continue
		}
		skipCursor = false
		out = append(out, child)
		last = child
		if len(out) == limit {
			iter.Next()
			if iter.Valid() {
				item := iter.Item()
				if item != nil {
					entry := item.Entry()
					if entry != nil && bytes.HasPrefix(entry.Key, prefix) {
						return out, last, nil
					}
				}
			}
			break
		}
	}
	return out, "", nil
}

func (s *nokvSecondaryBaseline) Close(b *testing.B) {
	b.Helper()
	if s != nil && s.db != nil {
		if err := s.db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func deleteReadRootForBenchmark(kv ns.KV, parent []byte) error {
	return kv.Apply([]ns.Mutation{{
		Kind: ns.MutationDelete,
		Key:  []byte("LR|" + string(parent)),
	}})
}

func (s *nokvStoreIndex) Close(b *testing.B) {
	b.Helper()
	if s != nil && s.db != nil {
		if err := s.db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
