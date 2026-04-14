package namespacebench

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	ns "github.com/feichai0017/NoKV/namespace"
)

const (
	namespaceDepth       = 4
	namespaceChildren    = 4096
	namespaceListLimit   = 256
	namespacePagedShards = 16
	namespaceHotPrefix   = "/bucket/checkpoint/run-1"
	namespaceFlatRoot    = "/bucket"
)

func BenchmarkNamespaceListFlatScan(b *testing.B) {
	ds := openSyntheticNamespaceDataset(namespaceChildren)
	parent := namespaceHotPrefix
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		out := ds.flat.List(parent, namespaceListLimit)
		if len(out) == 0 {
			b.Fatal("flat scan returned no entries")
		}
	}
}

func BenchmarkNamespaceListSecondaryIndex(b *testing.B) {
	ds := openSyntheticNamespaceDataset(namespaceChildren)
	parent := namespaceHotPrefix
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		out := ds.secondary.List(parent, namespaceListLimit)
		if len(out) == 0 {
			b.Fatal("secondary index returned no entries")
		}
	}
}

func BenchmarkNamespaceListPagedIndex(b *testing.B) {
	ds := openSyntheticNamespaceDataset(namespaceChildren)
	parent := namespaceHotPrefix
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		out := ds.paged.List(parent, namespaceListLimit)
		if len(out) == 0 {
			b.Fatal("paged index returned no entries")
		}
	}
}

func BenchmarkNamespaceListPagedIndexPaginated(b *testing.B) {
	ds := openSyntheticNamespaceDataset(namespaceChildren)
	parent := namespaceHotPrefix
	const pageSize = 64
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		total := 0
		cursor := ns.Cursor{}
		for {
			out, next := ds.paged.ListWithCursor(parent, cursor, pageSize)
			total += len(out)
			if len(out) == 0 || len(next.PageID) == 0 {
				break
			}
			cursor = next
		}
		if total == 0 {
			b.Fatal("paged pagination returned no entries")
		}
	}
}

func BenchmarkNamespaceHotCreateSecondaryIndex(b *testing.B) {
	ds := openSyntheticNamespaceDataset(namespaceChildren)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		child := fmt.Sprintf("hot-%08d", i)
		ds.secondary.Add(namespaceHotPrefix, child)
	}
}

func BenchmarkNamespaceHotCreatePagedIndex(b *testing.B) {
	ds := openSyntheticNamespaceDataset(namespaceChildren)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		child := fmt.Sprintf("hot-%08d", i)
		ds.paged.Add(namespaceHotPrefix, child)
	}
}

func BenchmarkNamespaceMixedCreateListSecondaryIndex(b *testing.B) {
	ds := openSyntheticNamespaceDataset(namespaceChildren)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		child := fmt.Sprintf("mix-%08d", i)
		ds.secondary.Add(namespaceHotPrefix, child)
		out := ds.secondary.List(namespaceHotPrefix, 64)
		if len(out) == 0 {
			b.Fatal("secondary mixed create+list returned no entries")
		}
	}
}

func BenchmarkNamespaceMixedCreateListPagedIndex(b *testing.B) {
	ds := openSyntheticNamespaceDataset(namespaceChildren)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		child := fmt.Sprintf("mix-%08d", i)
		ds.paged.Add(namespaceHotPrefix, child)
		out := ds.paged.List(namespaceHotPrefix, 64)
		if len(out) == 0 {
			b.Fatal("paged mixed create+list returned no entries")
		}
	}
}

type syntheticNamespaceDataset struct {
	flat      *flatScanIndex
	secondary *secondaryIndex
	paged     *pagedIndex
}

func openSyntheticNamespaceDataset(children int) syntheticNamespaceDataset {
	ds := syntheticNamespaceDataset{
		flat:      &flatScanIndex{},
		secondary: newSecondaryIndex(),
		paged:     newPagedIndex(namespacePagedShards),
	}
	for dir := 0; dir < namespaceDepth; dir++ {
		parent := syntheticParent(dir)
		for child := 0; child < children; child++ {
			name := fmt.Sprintf("entry-%06d", child)
			full := parent + "/" + name
			ds.flat.Add(full)
			ds.secondary.Add(parent, name)
			ds.paged.Add(parent, name)
		}
	}
	return ds
}

func syntheticParent(dir int) string {
	switch dir {
	case 0:
		return namespaceHotPrefix
	default:
		return fmt.Sprintf("%s/tenant-%d/partition", namespaceFlatRoot, dir)
	}
}

type flatScanIndex struct {
	keys []string
}

func (f *flatScanIndex) Add(fullPath string) {
	f.keys = append(f.keys, fullPath)
	sort.Strings(f.keys)
}

func (f *flatScanIndex) List(parent string, limit int) []string {
	prefix := parent
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	out := make([]string, 0, limit)
	for _, key := range f.keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		rest := strings.TrimPrefix(key, prefix)
		if strings.Contains(rest, "/") {
			continue
		}
		out = append(out, rest)
		if len(out) == limit {
			break
		}
	}
	return out
}

type secondaryIndex struct {
	children map[string][]string
}

func newSecondaryIndex() *secondaryIndex {
	return &secondaryIndex{children: make(map[string][]string)}
}

func (s *secondaryIndex) Add(parent, child string) {
	s.children[parent] = append(s.children[parent], child)
	sort.Strings(s.children[parent])
}

func (s *secondaryIndex) List(parent string, limit int) []string {
	children := s.children[parent]
	if len(children) > limit {
		children = children[:limit]
	}
	out := make([]string, len(children))
	copy(out, children)
	return out
}

type pagedIndex struct {
	index *ns.ListingMap
}

func newPagedIndex(shards int) *pagedIndex {
	return &pagedIndex{
		index: ns.NewListingMap(shards),
	}
}

func (p *pagedIndex) Add(parent, child string) {
	if err := p.index.AddChild([]byte(parent), ns.Entry{
		Name:    []byte(child),
		Kind:    ns.EntryKindFile,
		MetaKey: []byte(parent + "/" + child),
	}); err != nil {
		panic(err)
	}
}

func (p *pagedIndex) List(parent string, limit int) []string {
	entries, _, err := p.index.List([]byte(parent), ns.Cursor{}, limit)
	if err != nil {
		panic(err)
	}
	out := make([]string, 0, limit)
	for _, entry := range entries {
		out = append(out, string(entry.Name))
	}
	return out
}

func (p *pagedIndex) ListWithCursor(parent string, cursor ns.Cursor, limit int) ([]string, ns.Cursor) {
	entries, next, err := p.index.List([]byte(parent), cursor, limit)
	if err != nil {
		panic(err)
	}
	out := make([]string, 0, limit)
	for _, entry := range entries {
		out = append(out, string(entry.Name))
	}
	return out, next
}
