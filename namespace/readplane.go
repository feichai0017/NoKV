package namespace

import (
	"bytes"
	"fmt"
	"sort"
)

const defaultReadPlanePageEntries = 128

// ReadRoot is the compact root descriptor for one parent's ordered read plane.
// It carries only enough metadata to locate read-optimized micro-pages.
type ReadRoot struct {
	Parent         []byte
	RootGeneration uint64
	Pages          []ReadPageRef
}

// PageCoverageState captures the persisted coverage intent attached to one
// lexical interval in the read root. It describes whether the interval is
// directly answerable from the read plane or why it is still uncertified.
type PageCoverageState uint8

const (
	PageCoverageStateUnknown PageCoverageState = iota
	PageCoverageStateCovered
	PageCoverageStateUncovered
	PageCoverageStateCold
	PageCoverageStateDirty
)

func (s PageCoverageState) String() string {
	switch s {
	case PageCoverageStateCovered:
		return "covered"
	case PageCoverageStateUncovered:
		return "uncovered"
	case PageCoverageStateCold:
		return "cold"
	case PageCoverageStateDirty:
		return "dirty"
	default:
		return "unknown"
	}
}

func (s PageCoverageState) IsCovered() bool {
	return s == PageCoverageStateCovered
}

func (s PageCoverageState) IsAbsenceOfProof() bool {
	return !s.IsCovered()
}

func (s PageCoverageState) PersistedAnswerabilityState() PageAnswerabilityState {
	switch s {
	case PageCoverageStateCovered:
		return PageAnswerabilityStateAnswerable
	case PageCoverageStateCold:
		return PageAnswerabilityStateColdUnbootstrapped
	case PageCoverageStateDirty:
		return PageAnswerabilityStateDirtyPendingDelta
	case PageCoverageStateUncovered, PageCoverageStateUnknown:
		return PageAnswerabilityStateUncovered
	default:
		return PageAnswerabilityStateUncovered
	}
}

// PageAnswerabilityState captures the runtime reason why one interval is or is
// is not directly answerable on the strict listing path.
type PageAnswerabilityState uint8

const (
	PageAnswerabilityStateAnswerable PageAnswerabilityState = iota
	PageAnswerabilityStateUncovered
	PageAnswerabilityStateColdUnbootstrapped
	PageAnswerabilityStateDirtyPendingDelta
	PageAnswerabilityStateFrontierLag
	PageAnswerabilityStateCohortMismatch
	PageAnswerabilityStateGenerationRollback
	PageAnswerabilityStateRootPageMismatch
)

func (s PageAnswerabilityState) String() string {
	switch s {
	case PageAnswerabilityStateAnswerable:
		return "answerable"
	case PageAnswerabilityStateUncovered:
		return "uncovered"
	case PageAnswerabilityStateColdUnbootstrapped:
		return "cold-unbootstrapped"
	case PageAnswerabilityStateDirtyPendingDelta:
		return "dirty-pending-delta"
	case PageAnswerabilityStateFrontierLag:
		return "frontier-lag"
	case PageAnswerabilityStateCohortMismatch:
		return "cohort-mismatch"
	case PageAnswerabilityStateGenerationRollback:
		return "generation-rollback"
	case PageAnswerabilityStateRootPageMismatch:
		return "root-page-mismatch"
	default:
		return "unknown"
	}
}

func (s PageAnswerabilityState) IsAnswerable() bool {
	return s == PageAnswerabilityStateAnswerable
}

func (s PageAnswerabilityState) IsAbsenceOfProof() bool {
	switch s {
	case PageAnswerabilityStateUncovered,
		PageAnswerabilityStateColdUnbootstrapped,
		PageAnswerabilityStateDirtyPendingDelta,
		PageAnswerabilityStateFrontierLag:
		return true
	default:
		return false
	}
}

func (s PageAnswerabilityState) IsStructuralFailure() bool {
	switch s {
	case PageAnswerabilityStateCohortMismatch,
		PageAnswerabilityStateGenerationRollback,
		PageAnswerabilityStateRootPageMismatch:
		return true
	default:
		return false
	}
}

// PageCertificate is the smallest explicit answerability object attached to
// one lexical interval in the read plane. It carries the interval, publication
// metadata, and current coverage state that strict listing relies on.
type PageCertificate struct {
	LowFence          []byte
	HighFence         []byte
	CoverageState     PageCoverageState
	PublishedFrontier uint64
	Generation        uint64
}

// ReadPageRef is one fence-key entry in the read root.
type ReadPageRef struct {
	FenceKey          []byte
	HighFence         []byte
	PageID            []byte
	Count             uint32
	CoverageState     PageCoverageState
	PublishedFrontier uint64
	Generation        uint64
}

// ReadPage is one ordered micro-page in the persistent read plane.
// Entries remain authority-minimal: the truth key is derived from parent+name.
type ReadPage struct {
	Parent            []byte
	PageID            []byte
	LowFence          []byte
	HighFence         []byte
	NextPageID        []byte
	PublishedFrontier uint64
	Generation        uint64
	Entries           []Entry
}

func (r ReadPageRef) Certificate() PageCertificate {
	return PageCertificate{
		LowFence:          cloneBytes(r.FenceKey),
		HighFence:         cloneBytes(r.HighFence),
		CoverageState:     r.CoverageState,
		PublishedFrontier: r.PublishedFrontier,
		Generation:        r.Generation,
	}
}

func (p ReadPage) PublicationCertificate() PageCertificate {
	return PageCertificate{
		LowFence:          cloneBytes(p.LowFence),
		HighFence:         cloneBytes(p.HighFence),
		PublishedFrontier: p.PublishedFrontier,
		Generation:        p.Generation,
	}
}

func (c PageCertificate) PublicationEquivalent(other PageCertificate) bool {
	return bytes.Equal(c.LowFence, other.LowFence) &&
		bytes.Equal(c.HighFence, other.HighFence) &&
		c.PublishedFrontier == other.PublishedFrontier &&
		c.Generation == other.Generation
}

func (c PageCertificate) IsCovered() bool {
	return c.CoverageState.IsCovered()
}

// ReadPlaneView is an immutable, lookup-ready view of one parent's ordered read
// plane. It amortizes page lookup metadata across many list calls.
type ReadPlaneView struct {
	Root     ReadRoot
	Pages    []ReadPage
	pageByID map[string]int
}

// NewReadPlaneView validates one root/page set and builds the page lookup
// state needed by repeated ordered-list traversals.
func NewReadPlaneView(root ReadRoot, pages []ReadPage) (ReadPlaneView, error) {
	view := ReadPlaneView{
		Root:     cloneReadRoot(root),
		Pages:    make([]ReadPage, 0, len(pages)),
		pageByID: make(map[string]int, len(pages)),
	}
	for i, page := range pages {
		cloned := cloneReadPage(page)
		if len(cloned.PageID) == 0 {
			return ReadPlaneView{}, ErrCodecCorrupted
		}
		view.pageByID[string(cloned.PageID)] = i
		view.Pages = append(view.Pages, cloned)
	}
	for _, ref := range view.Root.Pages {
		if view.Root.RootGeneration == 0 || ref.Generation == 0 || ref.Generation > view.Root.RootGeneration {
			return ReadPlaneView{}, ErrCodecCorrupted
		}
		pageIdx, ok := view.pageByID[string(ref.PageID)]
		if !ok {
			return ReadPlaneView{}, ErrCodecCorrupted
		}
		page := view.Pages[pageIdx]
		if !ref.Certificate().PublicationEquivalent(page.PublicationCertificate()) {
			return ReadPlaneView{}, ErrCodecCorrupted
		}
	}
	for i := range view.Pages {
		for j := range view.Pages[i].Entries {
			view.Pages[i].Entries[j].MetaKey = encodeTruthKey(joinPath(view.Root.Parent, view.Pages[i].Entries[j].Name))
		}
	}
	return view, nil
}

// BuildReadPlane materializes one parent's ordered read plane from a direct-
// children membership set. The resulting root/page structures remain
// authority-minimal: page entries only keep child descriptors, and truth keys
// are reconstructed at read time.
func BuildReadPlane(parent []byte, entries []Entry, maxPageEntries int) (ReadRoot, []ReadPage, error) {
	if len(parent) == 0 {
		return ReadRoot{}, nil, ErrInvalidPath
	}
	if maxPageEntries <= 0 {
		maxPageEntries = defaultReadPlanePageEntries
	}
	ordered := cloneAndSortEntries(entries)
	return buildReadPlaneOrdered(parent, ordered, maxPageEntries)
}

func buildReadPlaneOrdered(parent []byte, ordered []Entry, maxPageEntries int) (ReadRoot, []ReadPage, error) {
	root := ReadRoot{Parent: cloneBytes(parent)}
	if len(ordered) == 0 {
		return root, nil, nil
	}
	pages := make([]ReadPage, 0, (len(ordered)+maxPageEntries-1)/maxPageEntries)
	for start := 0; start < len(ordered); start += maxPageEntries {
		end := min(start+maxPageEntries, len(ordered))
		pageID := encodeReadPlanePageID(len(pages))
		pageEntries := make([]Entry, 0, end-start)
		for _, entry := range ordered[start:end] {
			pageEntries = append(pageEntries, Entry{
				Name: cloneBytes(entry.Name),
				Kind: entry.Kind,
			})
		}
		page := ReadPage{
			Parent:   cloneBytes(parent),
			PageID:   pageID,
			LowFence: cloneBytes(pageEntries[0].Name),
			Entries:  pageEntries,
		}
		pages = append(pages, page)
		root.Pages = append(root.Pages, ReadPageRef{
			FenceKey:  cloneBytes(page.LowFence),
			HighFence: cloneBytes(page.HighFence),
			PageID:    cloneBytes(page.PageID),
			Count:     uint32(len(page.Entries)),
		})
	}
	for i := range pages {
		if i+1 < len(pages) {
			pages[i].HighFence = cloneBytes(pages[i+1].LowFence)
			pages[i].NextPageID = cloneBytes(pages[i+1].PageID)
			root.Pages[i].HighFence = cloneBytes(pages[i].HighFence)
		}
	}
	return root, pages, nil
}

func certifyReadRoot(root ReadRoot, frontier uint64) ReadRoot {
	out := cloneReadRoot(root)
	for i := range out.Pages {
		out.Pages[i].CoverageState = PageCoverageStateCovered
		out.Pages[i].PublishedFrontier = frontier
	}
	return out
}

func uncertifyReadRoot(root ReadRoot, state PageCoverageState) ReadRoot {
	out := cloneReadRoot(root)
	for i := range out.Pages {
		out.Pages[i].CoverageState = state
	}
	return out
}

func uncertifyReadRootPage(root ReadRoot, pageID []byte, state PageCoverageState) ReadRoot {
	out := cloneReadRoot(root)
	for i := range out.Pages {
		if bytes.Equal(out.Pages[i].PageID, pageID) {
			out.Pages[i].CoverageState = state
			break
		}
	}
	return out
}

func (v ReadPlaneView) List(cursor Cursor, limit int) ([]Entry, Cursor, ListStats, error) {
	if limit <= 0 {
		return nil, Cursor{}, ListStats{}, ErrInvalidLimit
	}
	if len(v.Root.Parent) == 0 {
		return nil, Cursor{}, ListStats{}, ErrInvalidPath
	}
	if len(v.Root.Pages) == 0 {
		return nil, Cursor{}, ListStats{}, nil
	}
	currentPageID := cursor.PageID
	entryOffset := int(cursor.EntryOffset)
	lastName := cursor.LastName
	if len(currentPageID) == 0 {
		currentPageID = v.Root.Pages[0].PageID
	}

	out := make([]Entry, 0, limit)
	stats := ListStats{}
	next := Cursor{}

	for len(currentPageID) > 0 && len(out) < limit {
		stats.PagesVisited++
		pageIdx, ok := v.pageByID[string(currentPageID)]
		if !ok {
			return nil, Cursor{}, stats, ErrCursorCorrupted
		}
		page := v.Pages[pageIdx]
		stats.PagesLoaded++
		startIdx := max(entryOffset, 0)
		if startIdx == 0 && len(lastName) > 0 {
			startIdx = sort.Search(len(page.Entries), func(i int) bool {
				return bytes.Compare(page.Entries[i].Name, lastName) > 0
			})
		}
		if startIdx > len(page.Entries) {
			return nil, Cursor{}, stats, ErrCursorCorrupted
		}
		if len(out) == 0 {
			remaining := len(page.Entries) - startIdx
			if remaining <= 0 {
				if len(page.NextPageID) == 0 {
					break
				}
				entryOffset = 0
				lastName = nil
				currentPageID = page.NextPageID
				continue
			}
			if remaining >= limit {
				endIdx := startIdx + limit
				stats.EntriesScanned += limit
				out = page.Entries[startIdx:endIdx]
				if endIdx >= len(page.Entries) {
					if len(page.NextPageID) > 0 {
						next = Cursor{PageID: page.NextPageID}
					}
				} else {
					last := page.Entries[endIdx-1]
					next = Cursor{
						PageID:      page.PageID,
						LastName:    last.Name,
						EntryOffset: uint32(endIdx),
					}
				}
				return out, next, stats, nil
			}
			if len(page.NextPageID) == 0 {
				stats.EntriesScanned += remaining
				return page.Entries[startIdx:], Cursor{}, stats, nil
			}
		}
		stats.EntriesScanned += len(page.Entries) - startIdx
		for i := startIdx; i < len(page.Entries) && len(out) < limit; i++ {
			entry := page.Entries[i]
			out = append(out, entry)
			next.PageID = page.PageID
			next.LastName = entry.Name
			next.EntryOffset = uint32(i + 1)
		}
		if len(out) == limit {
			if int(next.EntryOffset) >= len(page.Entries) {
				if len(page.NextPageID) > 0 {
					next = Cursor{PageID: page.NextPageID}
				} else {
					next = Cursor{}
				}
			}
			break
		}
		entryOffset = 0
		lastName = nil
		if len(page.NextPageID) == 0 {
			break
		}
		currentPageID = page.NextPageID
	}
	if len(out) < limit {
		next = Cursor{}
	}
	return out, next, stats, nil
}

func encodeReadPlanePageID(index int) []byte {
	return fmt.Appendf(nil, "rp%08d", index)
}

func cloneAndSortEntries(entries []Entry) []Entry {
	cloned := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		cloned = append(cloned, Entry{
			Name: entry.Name,
			Kind: entry.Kind,
		})
	}
	sort.Slice(cloned, func(i, j int) bool {
		return bytes.Compare(cloned[i].Name, cloned[j].Name) < 0
	})
	return cloned
}

func cloneReadPage(in ReadPage) ReadPage {
	out := ReadPage{
		Parent:            cloneBytes(in.Parent),
		PageID:            cloneBytes(in.PageID),
		LowFence:          cloneBytes(in.LowFence),
		HighFence:         cloneBytes(in.HighFence),
		NextPageID:        cloneBytes(in.NextPageID),
		PublishedFrontier: in.PublishedFrontier,
		Generation:        in.Generation,
		Entries:           make([]Entry, 0, len(in.Entries)),
	}
	for _, entry := range in.Entries {
		out.Entries = append(out.Entries, cloneEntry(entry))
	}
	return out
}

func cloneReadPages(in []ReadPage) []ReadPage {
	out := make([]ReadPage, 0, len(in))
	for _, page := range in {
		out = append(out, cloneReadPage(page))
	}
	return out
}

func cloneReadRoot(in ReadRoot) ReadRoot {
	out := ReadRoot{
		Parent:         cloneBytes(in.Parent),
		RootGeneration: in.RootGeneration,
		Pages:          make([]ReadPageRef, 0, len(in.Pages)),
	}
	for _, ref := range in.Pages {
		out.Pages = append(out.Pages, ReadPageRef{
			FenceKey:          cloneBytes(ref.FenceKey),
			HighFence:         cloneBytes(ref.HighFence),
			PageID:            cloneBytes(ref.PageID),
			Count:             ref.Count,
			CoverageState:     ref.CoverageState,
			PublishedFrontier: ref.PublishedFrontier,
			Generation:        ref.Generation,
		})
	}
	return out
}
