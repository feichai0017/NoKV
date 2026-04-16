package namespace

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
)

// Store is the executable namespace companion-plane prototype.
//
// Current structure:
//   - M|full_path is the authoritative truth plane.
//   - LD|parent|shard|child is the bootstrap delta plane for cold parents.
//   - LR|parent and LP|parent|fence are the persisted ordered read plane.
//   - LDP|parent|page|#seq is the steady-state append-style page-local delta log.
//
// The L|parent|page path is retired from the main execution path.
type Store struct {
	kv              KV
	shards          int
	readPageEntries int

	cacheMu             sync.RWMutex
	readRootCache       map[string]ReadRoot
	readPlaneViewCache  map[string]ReadPlaneView
	pageDeltaStateCache map[string]pageDeltaState
	pageDeltaSeqCache   map[string]uint64
	mutationSeq         atomic.Uint64
}

func NewStore(kv KV, shards int) *Store {
	if shards <= 0 {
		shards = 1
	}
	return &Store{
		kv:                  kv,
		shards:              shards,
		readPageEntries:     defaultReadPlanePageEntries,
		readRootCache:       make(map[string]ReadRoot),
		readPlaneViewCache:  make(map[string]ReadPlaneView),
		pageDeltaStateCache: make(map[string]pageDeltaState),
		pageDeltaSeqCache:   make(map[string]uint64),
	}
}

func (s *Store) shardFor(name []byte) uint32 {
	return shardFor(name, s.shards)
}

func (s *Store) nextMutationSeq() uint64 {
	return s.mutationSeq.Add(1)
}
func (s *Store) Create(path []byte, kind EntryKind, meta []byte) error {
	if s == nil || s.kv == nil {
		return ErrInvalidPath
	}
	parent, name, err := splitPath(path)
	if err != nil {
		return err
	}
	if err := s.requireParentDirectory(parent); err != nil {
		return err
	}
	truthKey := encodeTruthKey(path)
	existing, err := s.kv.Get(truthKey)
	if err != nil {
		return err
	}
	if existing != nil {
		return ErrPathExists
	}
	deltaRaw, err := encodeListingDelta(ListingDelta{
		Parent: cloneBytes(parent),
		Name:   cloneBytes(name),
		Kind:   kind,
		Op:     DeltaOpAdd,
		Seq:    s.nextMutationSeq(),
	})
	if err != nil {
		return err
	}
	deltaMut, rootMut, stateMut, updatedRoot, err := s.buildDeltaMutation(parent, name, deltaRaw)
	if err != nil {
		return err
	}
	batch := []Mutation{
		{Kind: MutationPut, Key: truthKey, Value: encodeTruthValue(kind, meta)},
		deltaMut,
	}
	if rootMut != nil {
		batch = append(batch, *rootMut)
	}
	if stateMut != nil {
		batch = append(batch, *stateMut)
	}
	err = s.kv.Apply(batch)
	if err == nil {
		if rootMut != nil {
			s.invalidateReadPlaneView(parent)
			if updatedRoot != nil {
				s.cacheReadRoot(parent, *updatedRoot)
			} else {
				s.invalidateReadRoot(parent)
			}
		}
		if stateMut != nil {
			if pageID, decErr := decodePageDeltaStatePageIDFromKey(parent, stateMut.Key); decErr == nil {
				if state, stateErr := decodePageDeltaStateValue(stateMut.Value); stateErr == nil {
					s.cachePageDeltaState(parent, pageID, state)
				}
			}
		}
	}
	return err
}

func (s *Store) Delete(path []byte) error {
	if s == nil || s.kv == nil {
		return ErrInvalidPath
	}
	parent, name, err := splitPath(path)
	if err != nil {
		return err
	}
	truthKey := encodeTruthKey(path)
	existing, err := s.kv.Get(truthKey)
	if err != nil {
		return err
	}
	if existing == nil {
		return ErrPathNotFound
	}
	deltaRaw, err := encodeListingDelta(ListingDelta{
		Parent: cloneBytes(parent),
		Name:   cloneBytes(name),
		Op:     DeltaOpRemove,
		Seq:    s.nextMutationSeq(),
	})
	if err != nil {
		return err
	}
	deltaMut, rootMut, stateMut, updatedRoot, err := s.buildDeltaMutation(parent, name, deltaRaw)
	if err != nil {
		return err
	}
	batch := []Mutation{
		{Kind: MutationDelete, Key: truthKey},
		deltaMut,
	}
	if rootMut != nil {
		batch = append(batch, *rootMut)
	}
	if stateMut != nil {
		batch = append(batch, *stateMut)
	}
	err = s.kv.Apply(batch)
	if err == nil {
		if rootMut != nil {
			s.invalidateReadPlaneView(parent)
			if updatedRoot != nil {
				s.cacheReadRoot(parent, *updatedRoot)
			} else {
				s.invalidateReadRoot(parent)
			}
		}
		if stateMut != nil {
			if pageID, decErr := decodePageDeltaStatePageIDFromKey(parent, stateMut.Key); decErr == nil {
				if state, stateErr := decodePageDeltaStateValue(stateMut.Value); stateErr == nil {
					s.cachePageDeltaState(parent, pageID, state)
				}
			}
		}
	}
	return err
}

func (s *Store) Lookup(path []byte) ([]byte, error) {
	if s == nil || s.kv == nil {
		return nil, ErrInvalidPath
	}
	_, _, err := splitPath(path)
	if err != nil {
		return nil, err
	}
	val, err := s.kv.Get(encodeTruthKey(path))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, ErrPathNotFound
	}
	_, meta, err := decodeTruthValue(val)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *Store) requireParentDirectory(parent []byte) error {
	if bytes.Equal(parent, []byte("/")) {
		return nil
	}
	raw, err := s.kv.Get(encodeTruthKey(parent))
	if err != nil {
		return err
	}
	if raw == nil {
		return ErrParentNotFound
	}
	kind, _, err := decodeTruthValue(raw)
	if err != nil {
		return err
	}
	if kind != EntryKindDirectory {
		return ErrParentNotDir
	}
	return nil
}

func (s *Store) buildDeltaMutation(parent, name, deltaRaw []byte) (Mutation, *Mutation, *Mutation, *ReadRoot, error) {
	if root, ok, err := s.loadReadRoot(parent); err != nil {
		return Mutation{}, nil, nil, nil, err
	} else if ok {
		pageID := findReadPageForName(root, name)
		delta, err := decodeListingDelta(deltaRaw)
		if err != nil {
			return Mutation{}, nil, nil, nil, err
		}
		delta.Parent = cloneBytes(parent)
		delta.PageID = cloneBytes(pageID)
		delta.Name = cloneBytes(name)
		pageRaw, err := encodePageDeltaRecord(delta)
		if err != nil {
			return Mutation{}, nil, nil, nil, err
		}
		seq, err := s.nextPageDeltaSeq(parent, pageID)
		if err != nil {
			return Mutation{}, nil, nil, nil, err
		}
		updatedRoot := uncertifyReadRootPage(root, pageID, PageCoverageStateDirty)
		rootRaw, err := encodeReadRoot(updatedRoot)
		if err != nil {
			return Mutation{}, nil, nil, nil, err
		}
		rootMut := Mutation{Kind: MutationPut, Key: encodeReadRootKey(parent), Value: rootRaw}
		stateMut := Mutation{
			Kind:  MutationPut,
			Key:   encodePageDeltaStateKey(parent, pageID),
			Value: encodePageDeltaStateValue(pageDeltaState{Pending: true, MaxSeq: seq}),
		}
		return Mutation{Kind: MutationPut, Key: encodePageDeltaLogKey(parent, pageID, seq), Value: pageRaw}, &rootMut, &stateMut, &updatedRoot, nil
	}
	pageID := encodePageID(s.shardFor(name))
	return Mutation{Kind: MutationPut, Key: encodeListingDeltaKey(parent, pageID, name), Value: deltaRaw}, nil, nil, nil, nil
}

func (s *Store) List(parent []byte, cursor Cursor, limit int) ([]Entry, Cursor, ListStats, error) {
	if err := s.validateListRequest(limit); err != nil {
		return nil, Cursor{}, ListStats{}, err
	}
	root, ok, err := s.loadReadRoot(parent)
	if err != nil {
		return nil, Cursor{}, ListStats{}, err
	}
	if !ok {
		return nil, Cursor{}, ListStats{}, ErrCoverageIncomplete
	}
	return s.listStrictReadPlane(parent, root, cursor, limit)
}

func (s *Store) RepairAndList(parent []byte, cursor Cursor, limit int) ([]Entry, Cursor, ListStats, error) {
	if err := s.validateListRequest(limit); err != nil {
		return nil, Cursor{}, ListStats{}, err
	}
	if root, ok, err := s.loadReadRoot(parent); err != nil {
		return nil, Cursor{}, ListStats{}, err
	} else if ok && cursorIsStart(cursor) && hasUncoveredIntervals(root) {
		if err := s.repairCoverage(parent, cursor, limit); err != nil {
			return nil, Cursor{}, ListStats{}, err
		}
		root, ok, err = s.loadReadRoot(parent)
		if err != nil {
			return nil, Cursor{}, ListStats{}, err
		}
		if !ok {
			return nil, Cursor{}, ListStats{}, nil
		}
		return s.listStrictReadPlane(parent, root, cursor, limit)
	}
	entries, next, preStats, _, ok, err := s.tryStrictList(parent, cursor, limit)
	if err != nil {
		return nil, Cursor{}, preStats, err
	}
	if ok {
		return entries, next, preStats, nil
	}
	if err := s.repairCoverage(parent, cursor, limit); err != nil {
		return nil, Cursor{}, preStats, err
	}
	root, ok, err := s.loadReadRoot(parent)
	if err != nil {
		return nil, Cursor{}, preStats, err
	}
	if !ok {
		return nil, Cursor{}, preStats, nil
	}
	entries, next, stats, err := s.listStrictReadPlane(parent, root, cursor, limit)
	return entries, next, mergeListStats(preStats, stats), err
}

func cursorIsStart(cursor Cursor) bool {
	return len(cursor.PageID) == 0 && len(cursor.LastName) == 0 && cursor.EntryOffset == 0
}

func (s *Store) repairCoverage(parent []byte, cursor Cursor, limit int) error {
	root, ok, err := s.loadReadRoot(parent)
	if err != nil {
		return err
	}
	deltas, err := s.loadDeltaSnapshot(parent)
	if err != nil {
		return err
	}
	return s.repairCoverageFromState(parent, root, ok, deltas, cursor, limit)
}

func (s *Store) LoadReadPlane(parent []byte) (ReadRoot, []ReadPage, bool, error) {
	if s == nil || s.kv == nil {
		return ReadRoot{}, nil, false, ErrInvalidPath
	}
	if view, ok := s.cachedReadPlaneView(parent); ok {
		return cloneReadRoot(view.Root), cloneReadPages(view.Pages), true, nil
	}
	root, ok, err := s.loadReadRoot(parent)
	if err != nil {
		return ReadRoot{}, nil, false, err
	}
	if !ok {
		return ReadRoot{}, nil, false, nil
	}
	pages, err := s.loadReadPlanePages(parent, root)
	if err != nil {
		return ReadRoot{}, nil, false, err
	}
	return root, pages, true, nil
}

func (s *Store) loadReadPlaneForRepair(parent []byte) (ReadRoot, []ReadPage, bool, error) {
	if s == nil || s.kv == nil {
		return ReadRoot{}, nil, false, ErrInvalidPath
	}
	if view, ok := s.cachedReadPlaneView(parent); ok {
		return view.Root, view.Pages, true, nil
	}
	root, ok, err := s.loadReadRoot(parent)
	if err != nil {
		return ReadRoot{}, nil, false, err
	}
	if !ok {
		return ReadRoot{}, nil, false, nil
	}
	pages, err := s.loadReadPlanePages(parent, root)
	if err != nil {
		return ReadRoot{}, nil, false, err
	}
	return root, pages, true, nil
}

func rootFullyCovered(root ReadRoot) bool {
	if len(root.Pages) == 0 || root.RootGeneration == 0 {
		return false
	}
	for _, ref := range root.Pages {
		if !ref.Certificate().IsCovered() {
			return false
		}
		if ref.Generation == 0 || ref.Generation > root.RootGeneration {
			return false
		}
	}
	return true
}

func (s *Store) loadStrictReadPlaneView(parent []byte, root ReadRoot) (ReadPlaneView, bool, error) {
	if !rootFullyCovered(root) {
		return ReadPlaneView{}, false, nil
	}
	if view, ok := s.cachedReadPlaneView(parent); ok {
		return view, true, nil
	}
	pages, err := s.loadReadPlanePages(parent, root)
	if err != nil {
		return ReadPlaneView{}, false, err
	}
	for _, ref := range root.Pages {
		state, err := s.loadPageDeltaState(parent, ref.PageID)
		if err != nil {
			return ReadPlaneView{}, false, err
		}
		if state.Pending || ref.PublishedFrontier < state.MaxSeq {
			return ReadPlaneView{}, false, nil
		}
	}
	view, err := NewReadPlaneView(root, pages)
	if err != nil {
		return ReadPlaneView{}, false, err
	}
	s.cacheReadPlaneView(parent, view)
	return view, true, nil
}

func (s *Store) validateListRequest(limit int) error {
	if s == nil || s.kv == nil {
		return ErrInvalidPath
	}
	if limit <= 0 {
		return ErrInvalidLimit
	}
	return nil
}

func (s *Store) tryStrictList(parent []byte, cursor Cursor, limit int) ([]Entry, Cursor, ListStats, ReadRoot, bool, error) {
	root, ok, err := s.loadReadRoot(parent)
	if err != nil {
		return nil, Cursor{}, ListStats{}, ReadRoot{}, false, err
	}
	if !ok {
		return nil, Cursor{}, ListStats{}, ReadRoot{}, false, nil
	}
	entries, next, stats, err := s.listStrictReadPlane(parent, root, cursor, limit)
	if err == nil {
		return entries, next, stats, root, true, nil
	}
	if errors.Is(err, ErrCoverageIncomplete) {
		return nil, Cursor{}, stats, root, false, nil
	}
	return nil, Cursor{}, stats, ReadRoot{}, false, err
}

func mergeListStats(base, extra ListStats) ListStats {
	base.PagesVisited += extra.PagesVisited
	base.PagesLoaded += extra.PagesLoaded
	base.DeltasRead += extra.DeltasRead
	base.EntriesScanned += extra.EntriesScanned
	return base
}

func (s *Store) repairCoverageFromState(parent []byte, root ReadRoot, ok bool, deltas deltaSnapshot, cursor Cursor, limit int) error {
	switch {
	case !ok:
		return s.bootstrapColdInterval(parent, deltas, cursor, limit)
	case len(deltas.pageLocal) > 0:
		return s.repairDirtyReadPlane(parent)
	case hasUncoveredIntervals(root):
		return s.bootstrapUncoveredInterval(parent, root, deltas, cursor, limit)
	case deltas.hasBootstrap():
		_, err := s.Rebuild(parent)
		return err
	default:
		return nil
	}
}

func (s *Store) repairDirtyReadPlane(parent []byte) error {
	_, err := s.MaterializeDeltaPages(parent, 0)
	if err == nil || !errors.Is(err, ErrRebuildRequired) {
		return err
	}
	_, err = s.Rebuild(parent)
	return err
}

func (s *Store) bootstrapColdInterval(parent []byte, deltas deltaSnapshot, cursor Cursor, limit int) error {
	if len(cursor.PageID) > 0 || len(cursor.LastName) > 0 || cursor.EntryOffset > 0 {
		_, _, err := s.MaterializeReadPlane(parent, s.readPageEntries)
		return err
	}
	windowEntries := max(limit, s.readPageEntries)
	mergedEntries, nextFence, err := s.bootstrapWindowEntries(parent, nil, deltas.bootstrap, windowEntries)
	if err != nil {
		return err
	}
	root, pages, err := buildBootstrapReadPlane(parent, nil, mergedEntries, nextFence, s.readPageEntries, 1, deltas.maxSeq())
	if err != nil {
		return err
	}
	consumed := []KVPair(nil)
	if !hasUncoveredIntervals(root) {
		consumed = deltas.bootstrap
	}
	return s.publishReadPlaneFresh(parent, root, pages, consumed)
}

func (s *Store) bootstrapUncoveredInterval(parent []byte, root ReadRoot, deltas deltaSnapshot, cursor Cursor, limit int) error {
	targetIdx := firstUncoveredRefIndex(root)
	if targetIdx < 0 {
		return nil
	}
	pages, err := s.loadReadPlanePages(parent, root)
	if err != nil {
		return err
	}
	prefixPages := cloneReadPages(pages[:targetIdx])
	targetRef := root.Pages[targetIdx]
	generation := nextPublicationGeneration(root)
	startFence := targetRef.FenceKey
	if targetIdx == 0 {
		startFence = nil
	}
	windowEntries := max(limit, s.readPageEntries)
	extensionEntries, nextFence, err := s.bootstrapWindowEntries(parent, startFence, deltas.bootstrap, windowEntries)
	if err != nil {
		return err
	}
	extensionRoot, extensionPages, err := buildBootstrapReadPlane(parent, targetRef.PageID, extensionEntries, nextFence, s.readPageEntries, generation, deltas.maxSeq())
	if err != nil {
		return err
	}
	combinedPages := append(prefixPages, extensionPages...)
	newRoot, combinedPages := rebuildReadPlaneMetadata(parent, combinedPages)
	oldRefsByID := make(map[string]ReadPageRef, len(root.Pages))
	for _, ref := range root.Pages {
		oldRefsByID[string(ref.PageID)] = ref
	}
	updatedRefsByID := make(map[string]ReadPageRef, len(extensionRoot.Pages))
	for _, ref := range extensionRoot.Pages {
		updatedRefsByID[string(ref.PageID)] = ref
	}
	for i := range newRoot.Pages {
		if updated, ok := updatedRefsByID[string(newRoot.Pages[i].PageID)]; ok {
			newRoot.Pages[i].CoverageState = updated.CoverageState
			newRoot.Pages[i].PublishedFrontier = updated.PublishedFrontier
			newRoot.Pages[i].Generation = updated.Generation
			continue
		}
		if oldRef, ok := oldRefsByID[string(newRoot.Pages[i].PageID)]; ok {
			newRoot.Pages[i].CoverageState = oldRef.CoverageState
			newRoot.Pages[i].PublishedFrontier = oldRef.PublishedFrontier
			newRoot.Pages[i].Generation = oldRef.Generation
		}
	}
	consumed := []KVPair(nil)
	if !hasUncoveredIntervals(newRoot) {
		consumed = deltas.bootstrap
	}
	return s.replaceReadPlane(parent, newRoot, combinedPages, consumed)
}

func hasUncoveredIntervals(root ReadRoot) bool {
	for _, ref := range root.Pages {
		if !ref.Certificate().IsCovered() {
			return true
		}
	}
	return false
}

func maxRootPageGeneration(root ReadRoot) uint64 {
	maxGen := root.RootGeneration
	for _, ref := range root.Pages {
		if ref.Generation > maxGen {
			maxGen = ref.Generation
		}
	}
	return maxGen
}

func firstUncoveredRefIndex(root ReadRoot) int {
	for i, ref := range root.Pages {
		if !ref.Certificate().IsCovered() {
			return i
		}
	}
	return -1
}

func (s *Store) loadReadPlanePages(parent []byte, root ReadRoot) ([]ReadPage, error) {
	pages := make([]ReadPage, 0, len(root.Pages))
	for _, ref := range root.Pages {
		page, err := s.loadReadPageByRef(parent, ref)
		if err != nil {
			return nil, err
		}
		pages = append(pages, page)
	}
	return pages, nil
}

func (s *Store) collectReadableReadPlanePages(parent []byte, root ReadRoot) ([]ReadPage, bool, error) {
	pages := make([]ReadPage, 0, len(root.Pages))
	rootPageMismatch := false
	for _, ref := range root.Pages {
		page, err := s.loadReadPageIgnoringGeneration(parent, ref)
		if err != nil {
			rootPageMismatch = true
			continue
		}
		pages = append(pages, page)
	}
	return pages, rootPageMismatch, nil
}

func (s *Store) loadReadRoot(parent []byte) (ReadRoot, bool, error) {
	if root, ok := s.cachedReadRoot(parent); ok {
		return root, true, nil
	}
	rootRaw, err := s.kv.Get(encodeReadRootKey(parent))
	if err != nil {
		return ReadRoot{}, false, err
	}
	if rootRaw == nil {
		return ReadRoot{}, false, nil
	}
	root, err := decodeReadRoot(rootRaw)
	if err != nil {
		return ReadRoot{}, false, err
	}
	s.cacheReadRoot(parent, root)
	return root, true, nil
}

func (s *Store) loadReadPageByRef(parent []byte, ref ReadPageRef) (ReadPage, error) {
	page, err := s.loadReadPageIgnoringGeneration(parent, ref)
	if err != nil {
		return ReadPage{}, err
	}
	if ref.Generation == 0 {
		return ReadPage{}, ErrCodecCorrupted
	}
	if !ref.Certificate().PublicationEquivalent(page.PublicationCertificate()) {
		return ReadPage{}, ErrCodecCorrupted
	}
	return page, nil
}

func answerabilityErr(state PageAnswerabilityState) error {
	if state.IsAnswerable() {
		return nil
	}
	if state.IsAbsenceOfProof() {
		return ErrCoverageIncomplete
	}
	if state.IsStructuralFailure() {
		return ErrCodecCorrupted
	}
	return ErrCodecCorrupted
}

func (s *Store) evaluatePageAnswerability(parent []byte, root ReadRoot, ref ReadPageRef) (PageAnswerabilityState, ReadPage, error) {
	cert := ref.Certificate()
	if persisted := cert.CoverageState.PersistedAnswerabilityState(); !persisted.IsAnswerable() {
		return persisted, ReadPage{}, nil
	}
	if root.RootGeneration == 0 || cert.Generation == 0 || cert.Generation > root.RootGeneration {
		return PageAnswerabilityStateGenerationRollback, ReadPage{}, nil
	}
	state, err := s.loadPageDeltaState(parent, ref.PageID)
	if err != nil {
		return PageAnswerabilityStateAnswerable, ReadPage{}, err
	}
	if state.Pending {
		return PageAnswerabilityStateDirtyPendingDelta, ReadPage{}, nil
	}
	if cert.PublishedFrontier < state.MaxSeq {
		return PageAnswerabilityStateFrontierLag, ReadPage{}, nil
	}
	page, err := s.loadReadPageIgnoringGeneration(parent, ref)
	if err != nil {
		return PageAnswerabilityStateRootPageMismatch, ReadPage{}, nil
	}
	if page.Generation > root.RootGeneration {
		return PageAnswerabilityStateGenerationRollback, ReadPage{}, nil
	}
	if !cert.PublicationEquivalent(page.PublicationCertificate()) {
		return PageAnswerabilityStateCohortMismatch, ReadPage{}, nil
	}
	return PageAnswerabilityStateAnswerable, page, nil
}

func noteCoverageState(stats *VerifyCertificateStats, state PageCoverageState) {
	switch state {
	case PageCoverageStateCovered:
		stats.CoveredPages++
	case PageCoverageStateCold:
		stats.ColdPages++
		stats.UncoveredPages++
	case PageCoverageStateDirty:
		stats.DirtyPages++
		stats.UncoveredPages++
	case PageCoverageStateUncovered, PageCoverageStateUnknown:
		stats.UncoveredPages++
	default:
		stats.UncoveredPages++
	}
}

func noteAnswerabilityState(stats *VerifyStats, pageID string, cert PageCertificate, state PageAnswerabilityState) {
	switch state {
	case PageAnswerabilityStateDirtyPendingDelta:
		if cert.IsCovered() {
			stats.Certificate.CoveredPendingDeltaIDs = append(stats.Certificate.CoveredPendingDeltaIDs, pageID)
		}
	case PageAnswerabilityStateFrontierLag:
		stats.Publication.FrontierLagIDs = append(stats.Publication.FrontierLagIDs, pageID)
	case PageAnswerabilityStateCohortMismatch:
		stats.Publication.CohortMismatchIDs = append(stats.Publication.CohortMismatchIDs, pageID)
	case PageAnswerabilityStateGenerationRollback:
		stats.Certificate.RootGenerationMismatch = true
		stats.Publication.GenerationRollbackIDs = append(stats.Publication.GenerationRollbackIDs, pageID)
	case PageAnswerabilityStateRootPageMismatch:
		stats.Certificate.RootPageMismatch = true
	}
}

func (s *Store) loadReadPageIgnoringGeneration(parent []byte, ref ReadPageRef) (ReadPage, error) {
	raw, err := s.kv.Get(encodeReadPageKey(parent, ref.FenceKey))
	if err != nil {
		return ReadPage{}, err
	}
	if raw == nil {
		return ReadPage{}, ErrCodecCorrupted
	}
	page, err := decodeReadPage(raw)
	if err != nil {
		return ReadPage{}, err
	}
	page.Parent = cloneBytes(parent)
	if !bytes.Equal(page.PageID, ref.PageID) || !bytes.Equal(page.LowFence, ref.FenceKey) || !bytes.Equal(page.HighFence, ref.HighFence) {
		return ReadPage{}, ErrCodecCorrupted
	}
	return page, nil
}

func buildBootstrapReadPlane(parent, originalPageID []byte, coveredEntries []Entry, nextFence []byte, maxPageEntries int, generation, frontier uint64) (ReadRoot, []ReadPage, error) {
	if len(coveredEntries) == 0 {
		return ReadRoot{Parent: cloneBytes(parent)}, nil, nil
	}
	root, pages, err := buildReadPlaneOrdered(parent, coveredEntries, maxPageEntries)
	if err != nil {
		return ReadRoot{}, nil, err
	}
	if len(originalPageID) > 0 && len(pages) > 0 {
		pages[0].PageID = cloneBytes(originalPageID)
		root.Pages[0].PageID = cloneBytes(originalPageID)
	}
	root, pages = assignReadPlanePublication(root, pages, generation, frontier)
	root = certifyReadRoot(root, frontier)
	nextPageID := nextReadPlanePageID(root)
	allocPageID := func() []byte {
		pageID := encodeReadPlanePageID(nextPageID)
		nextPageID++
		return pageID
	}
	if len(nextFence) > 0 {
		boundary := cloneBytes(nextFence)
		placeholderID := allocPageID()
		pages[len(pages)-1].HighFence = cloneBytes(boundary)
		pages[len(pages)-1].NextPageID = cloneBytes(placeholderID)
		root.Pages[len(root.Pages)-1].HighFence = cloneBytes(boundary)
		placeholder := ReadPage{
			Parent:            cloneBytes(parent),
			PageID:            placeholderID,
			LowFence:          cloneBytes(boundary),
			PublishedFrontier: frontier,
			Generation:        generation,
		}
		pages = append(pages, placeholder)
		root.Pages = append(root.Pages, ReadPageRef{
			FenceKey:          cloneBytes(boundary),
			PageID:            cloneBytes(placeholderID),
			CoverageState:     PageCoverageStateCold,
			PublishedFrontier: frontier,
			Generation:        generation,
		})
	}
	return root, pages, nil
}

func (s *Store) bootstrapWindowEntries(parent, lowFence []byte, bootstrapPairs []KVPair, windowEntries int) ([]Entry, []byte, error) {
	if windowEntries <= 0 {
		windowEntries = s.readPageEntries
	}
	filteredPairs, err := filterBootstrapDeltaPairs(parent, lowFence, bootstrapPairs)
	if err != nil {
		return nil, nil, err
	}
	truthLimit := windowEntries + len(filteredPairs) + 1
	truthEntries, _, err := s.scanTruthWindow(parent, lowFence, truthLimit)
	if err != nil {
		return nil, nil, err
	}
	mergedEntries, err := s.applyDeltaPairs(parent, truthEntries, filteredPairs)
	if err != nil {
		return nil, nil, err
	}
	if len(mergedEntries) <= windowEntries {
		return mergedEntries, nil, nil
	}
	return mergedEntries[:windowEntries], cloneBytes(mergedEntries[windowEntries].Name), nil
}

func filterBootstrapDeltaPairs(parent, lowFence []byte, pairs []KVPair) ([]KVPair, error) {
	if len(lowFence) == 0 {
		return pairs, nil
	}
	out := make([]KVPair, 0, len(pairs))
	for _, pair := range pairs {
		delta, err := decodeListingDeltaFromKV(parent, pair.Key, pair.Value)
		if err != nil {
			return nil, err
		}
		if bytes.Compare(delta.Name, lowFence) < 0 {
			continue
		}
		out = append(out, pair)
	}
	return out, nil
}

func (s *Store) listStrictReadPlane(parent []byte, root ReadRoot, cursor Cursor, limit int) ([]Entry, Cursor, ListStats, error) {
	if view, ok, err := s.loadStrictReadPlaneView(parent, root); err != nil {
		return nil, Cursor{}, ListStats{}, err
	} else if ok {
		return view.List(cursor, limit)
	}

	currentPageID := cursor.PageID
	entryOffset := int(cursor.EntryOffset)
	lastName := cursor.LastName
	pageIndex := 0
	if len(currentPageID) == 0 {
		if len(root.Pages) == 0 {
			return nil, Cursor{}, ListStats{}, nil
		}
		if !root.Pages[0].Certificate().IsCovered() {
			return nil, Cursor{}, ListStats{}, ErrCoverageIncomplete
		}
		currentPageID = root.Pages[0].PageID
	} else {
		var ok bool
		pageIndex, ok = findReadPageRefIndex(root, currentPageID)
		if !ok {
			return nil, Cursor{}, ListStats{}, ErrCursorCorrupted
		}
	}

	out := make([]Entry, 0, limit)
	stats := ListStats{}
	next := Cursor{}

	for len(currentPageID) > 0 && len(out) < limit {
		stats.PagesVisited++
		ref := root.Pages[pageIndex]
		state, page, err := s.evaluatePageAnswerability(parent, root, ref)
		if err != nil {
			return nil, Cursor{}, stats, err
		}
		if !state.IsAnswerable() {
			return nil, Cursor{}, stats, answerabilityErr(state)
		}
		stats.PagesLoaded++
		// Strict pages additionally verify that no newer page-local delta
		// remains staged for this interval. This prevents stale-but-valid root
		// metadata from silently serving uncovered data.
		startIdx := max(entryOffset, 0)
		if startIdx == 0 && len(lastName) > 0 {
			startIdx = sort.Search(len(page.Entries), func(i int) bool {
				return bytes.Compare(page.Entries[i].Name, lastName) > 0
			})
		}
		if startIdx > len(page.Entries) {
			return nil, Cursor{}, stats, ErrCursorCorrupted
		}
		stats.EntriesScanned += len(page.Entries) - startIdx
		for i := startIdx; i < len(page.Entries) && len(out) < limit; i++ {
			entry := page.Entries[i]
			entry.MetaKey = encodeTruthKey(joinPath(parent, entry.Name))
			out = append(out, entry)
			next.PageID = page.PageID
			next.LastName = entry.Name
			next.EntryOffset = uint32(i + 1)
		}
		if len(out) == limit {
			if int(next.EntryOffset) >= len(page.Entries) {
				if len(page.NextPageID) > 0 {
					if pageIndex+1 >= len(root.Pages) || !bytes.Equal(root.Pages[pageIndex+1].PageID, page.NextPageID) {
						return nil, Cursor{}, stats, ErrCoverageIncomplete
					}
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
		pageIndex++
		if pageIndex >= len(root.Pages) || !bytes.Equal(root.Pages[pageIndex].PageID, page.NextPageID) || !root.Pages[pageIndex].Certificate().IsCovered() {
			return nil, Cursor{}, stats, ErrCoverageIncomplete
		}
		currentPageID = page.NextPageID
	}
	if len(out) < limit {
		next = Cursor{}
	}
	return out, next, stats, nil
}

func (s *Store) scanTruthEntries(parent []byte) ([]Entry, error) {
	prefix := encodeTruthKey(parent)
	if len(prefix) == 0 || prefix[len(prefix)-1] != '/' {
		prefix = append(cloneBytes(prefix), '/')
	}
	pairs, err := s.kv.ScanPrefix(prefix, nil, 0)
	if err != nil {
		return nil, err
	}
	entriesByName := make(map[string]Entry, len(pairs))
	for _, pair := range pairs {
		if len(pair.Key) <= len(prefix) || !bytes.HasPrefix(pair.Key, prefix) {
			continue
		}
		rest := pair.Key[len(prefix):]
		if len(rest) == 0 {
			continue
		}
		name := rest
		var kind EntryKind
		if before, _, ok := bytes.Cut(rest, []byte{'/'}); ok {
			name = before
			kind = EntryKindDirectory
		} else {
			decodedKind, _, err := decodeTruthValue(pair.Value)
			if err != nil {
				return nil, err
			}
			kind = decodedKind
		}
		key := string(name)
		entry, ok := entriesByName[key]
		if !ok || kind == EntryKindDirectory {
			entriesByName[key] = Entry{
				Name:    cloneBytes(name),
				Kind:    kind,
				MetaKey: encodeTruthKey(joinPath(parent, name)),
			}
			if ok && kind == EntryKindDirectory && entry.Kind == EntryKindFile {
				entriesByName[key] = Entry{
					Name:    cloneBytes(name),
					Kind:    kind,
					MetaKey: encodeTruthKey(joinPath(parent, name)),
				}
			}
		}
	}
	names := make([]string, 0, len(entriesByName))
	for name := range entriesByName {
		names = append(names, name)
	}
	sort.Strings(names)
	entries := make([]Entry, 0, len(names))
	for _, name := range names {
		entries = append(entries, entriesByName[name])
	}
	return entries, nil
}

func (s *Store) scanTruthWindow(parent, lowFence []byte, limit int) ([]Entry, []byte, error) {
	if limit <= 0 {
		return nil, nil, nil
	}
	prefix := encodeTruthKey(parent)
	if len(prefix) == 0 || prefix[len(prefix)-1] != '/' {
		prefix = append(cloneBytes(prefix), '/')
	}
	start := prefix
	if len(lowFence) > 0 {
		start = append(cloneBytes(prefix), lowFence...)
	}
	rawLimit := max(limit*4, 32)
	entries := make([]Entry, 0, limit)
	nextStart := cloneBytes(start)
	for {
		pairs, err := s.kv.ScanPrefix(prefix, nextStart, rawLimit)
		if err != nil {
			return nil, nil, err
		}
		if len(pairs) == 0 {
			return entries, nil, nil
		}
		for _, pair := range pairs {
			entry, ok, err := directChildEntryFromTruthPair(parent, prefix, pair)
			if err != nil {
				return nil, nil, err
			}
			nextStart = append(cloneBytes(pair.Key), 0)
			if !ok {
				continue
			}
			if len(lowFence) > 0 && bytes.Compare(entry.Name, lowFence) < 0 {
				continue
			}
			if len(entries) > 0 && bytes.Equal(entries[len(entries)-1].Name, entry.Name) {
				if entry.Kind == EntryKindDirectory && entries[len(entries)-1].Kind != EntryKindDirectory {
					entries[len(entries)-1].Kind = EntryKindDirectory
				}
				continue
			}
			if len(entries) == limit {
				return entries, cloneBytes(entry.Name), nil
			}
			entries = append(entries, entry)
		}
		if len(pairs) < rawLimit {
			return entries, nil, nil
		}
	}
}

func directChildEntryFromTruthPair(parent, prefix []byte, pair KVPair) (Entry, bool, error) {
	if len(pair.Key) <= len(prefix) || !bytes.HasPrefix(pair.Key, prefix) {
		return Entry{}, false, nil
	}
	rest := pair.Key[len(prefix):]
	if len(rest) == 0 {
		return Entry{}, false, nil
	}
	name := rest
	var kind EntryKind
	if before, _, ok := bytes.Cut(rest, []byte{'/'}); ok {
		name = before
		kind = EntryKindDirectory
	} else {
		decodedKind, _, err := decodeTruthValue(pair.Value)
		if err != nil {
			return Entry{}, false, err
		}
		kind = decodedKind
	}
	return Entry{
		Name:    cloneBytes(name),
		Kind:    kind,
		MetaKey: encodeTruthKey(joinPath(parent, name)),
	}, true, nil
}

func (s *Store) applyDeltaPairs(parent []byte, baseEntries []Entry, deltaPairs []KVPair) ([]Entry, error) {
	entriesByName := make(map[string]Entry, len(baseEntries)+len(deltaPairs))
	for _, entry := range baseEntries {
		entriesByName[string(entry.Name)] = cloneEntryNoMeta(entry)
	}
	for _, pair := range deltaPairs {
		delta, err := decodeAnyDeltaFromKV(parent, pair.Key, pair.Value)
		if err != nil {
			return nil, err
		}
		nameKey := string(delta.Name)
		switch delta.Op {
		case DeltaOpAdd:
			entriesByName[nameKey] = Entry{
				Name: cloneBytes(delta.Name),
				Kind: delta.Kind,
			}
		case DeltaOpRemove:
			delete(entriesByName, nameKey)
		default:
			return nil, fmt.Errorf("namespace: unsupported delta op %d", delta.Op)
		}
	}
	out := make([]Entry, 0, len(entriesByName))
	for _, entry := range entriesByName {
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Name, out[j].Name) < 0
	})
	return out, nil
}

func decodeAnyDeltaFromKV(parent, key, raw []byte) (ListingDelta, error) {
	switch {
	case bytes.HasPrefix(key, encodeListingDeltaParentPrefix(parent)):
		return decodeListingDeltaFromKV(parent, key, raw)
	case bytes.HasPrefix(key, encodePageDeltaParentPrefix(parent)):
		return decodePageDeltaFromKV(parent, key, raw)
	default:
		return ListingDelta{}, ErrParentMismatch
	}
}

func (s *Store) Materialize(parent []byte) (MaterializeStats, error) {
	_, _, stats, err := s.materializeReadPlane(parent, s.readPageEntries, 0)
	return stats, err
}

func (s *Store) MaterializeReadPlane(parent []byte, maxPageEntries int) (ReadRoot, []ReadPage, error) {
	root, pages, _, err := s.materializeReadPlane(parent, maxPageEntries, 0)
	return root, pages, err
}

func (s *Store) MaterializeDeltaPages(parent []byte, maxDeltaPages int) (MaterializeStats, error) {
	_, _, stats, err := s.materializeReadPlane(parent, s.readPageEntries, maxDeltaPages)
	return stats, err
}

func (s *Store) materializeReadPlane(parent []byte, maxPageEntries, maxDeltaPages int) (ReadRoot, []ReadPage, MaterializeStats, error) {
	if s == nil || s.kv == nil {
		return ReadRoot{}, nil, MaterializeStats{}, ErrInvalidPath
	}
	if maxPageEntries <= 0 {
		maxPageEntries = s.readPageEntries
	}
	root, pages, ok, err := s.loadReadPlaneForRepair(parent)
	if err != nil {
		return ReadRoot{}, nil, MaterializeStats{}, err
	}
	deltas, err := s.loadDeltaSnapshot(parent)
	if err != nil {
		return ReadRoot{}, nil, MaterializeStats{}, err
	}
	oldPageCount := 0
	var baseEntries []Entry
	if ok {
		oldPageCount = len(root.Pages)
		baseEntries = collectReadPlaneEntries(pages)
	} else if maxDeltaPages <= 0 {
		baseEntries, err = s.scanTruthEntries(parent)
		if err != nil {
			return ReadRoot{}, nil, MaterializeStats{}, err
		}
	}
	deltaPairs, deltaPageIDs, err := deltas.selectPairs(root, ok, maxDeltaPages, s.shards)
	if err != nil {
		return ReadRoot{}, nil, MaterializeStats{}, err
	}
	if len(deltaPairs) == 0 {
		if ok {
			return root, pages, MaterializeStats{}, nil
		}
		return s.publishColdReadPlane(parent, baseEntries, nil, maxPageEntries, false)
	}
	if ok {
		if deltas.hasBootstrap() {
			return ReadRoot{}, nil, MaterializeStats{}, ErrRebuildRequired
		}
		if localRoot, localPages, localStats, used, err := s.materializePageLocalDeltas(parent, root, pages, deltaPairs, maxPageEntries); err != nil {
			return ReadRoot{}, nil, MaterializeStats{}, err
		} else if used {
			return localRoot, localPages, localStats, nil
		}
		return ReadRoot{}, nil, MaterializeStats{}, ErrRebuildRequired
	}
	mergedEntries := baseEntries
	if maxDeltaPages > 0 {
		mergedEntries, err = s.applyDeltaPairs(parent, baseEntries, deltaPairs)
		if err != nil {
			return ReadRoot{}, nil, MaterializeStats{}, err
		}
	}
	buildRoot, buildPages, stats, err := s.publishColdReadPlane(parent, mergedEntries, deltaPairs, maxPageEntries, maxDeltaPages > 0)
	if err != nil {
		return ReadRoot{}, nil, MaterializeStats{}, err
	}
	stats.DeltasFolded = len(deltaPairs)
	stats.DeltaPagesFolded = len(deltaPageIDs)
	if maxDeltaPages <= 0 && oldPageCount > len(buildPages) {
		stats.PagesDeleted = oldPageCount - len(buildPages)
	}
	return buildRoot, buildPages, stats, nil
}

func (s *Store) publishColdReadPlane(parent []byte, entries []Entry, consumedDeltas []KVPair, maxPageEntries int, uncertified bool) (ReadRoot, []ReadPage, MaterializeStats, error) {
	root, pages, err := buildReadPlaneOrdered(parent, entries, maxPageEntries)
	if err != nil {
		return ReadRoot{}, nil, MaterializeStats{}, err
	}
	frontier := maxDeltaSeq(consumedDeltas)
	root, pages = assignReadPlanePublication(root, pages, 1, frontier)
	if uncertified {
		root = uncertifyReadRoot(root, PageCoverageStateCold)
	} else {
		root = certifyReadRoot(root, frontier)
	}
	if err := s.publishReadPlaneFresh(parent, root, pages, consumedDeltas); err != nil {
		return ReadRoot{}, nil, MaterializeStats{}, err
	}
	return root, pages, MaterializeStats{
		PagesWritten:        len(pages),
		EntriesMaterialized: len(entries),
	}, nil
}

func assignReadPlanePublication(root ReadRoot, pages []ReadPage, generation, frontier uint64) (ReadRoot, []ReadPage) {
	root = cloneReadRoot(root)
	pages = cloneReadPages(pages)
	root.RootGeneration = generation
	for i := range pages {
		pages[i].PublishedFrontier = frontier
		pages[i].Generation = generation
	}
	for i := range root.Pages {
		root.Pages[i].Generation = generation
		root.Pages[i].PublishedFrontier = frontier
	}
	return root, pages
}

func nextPublicationGeneration(root ReadRoot) uint64 {
	maxGen := root.RootGeneration
	for _, ref := range root.Pages {
		if ref.Generation > maxGen {
			maxGen = ref.Generation
		}
	}
	if maxGen == 0 {
		return 1
	}
	return maxGen + 1
}

func (s *Store) replaceReadPlane(parent []byte, root ReadRoot, pages []ReadPage, consumedDeltas []KVPair) error {
	s.invalidateReadRoot(parent)
	batch, err := s.buildFullReadPlaneBatch(parent, root, pages, consumedDeltas)
	if err != nil {
		return err
	}
	if len(batch) > 0 {
		if err := s.kv.Apply(batch); err != nil {
			return err
		}
	}
	if len(root.Pages) > 0 {
		s.cacheReadRoot(parent, root)
		for _, ref := range root.Pages {
			s.cachePageDeltaState(parent, ref.PageID, pageDeltaState{})
		}
	}
	return nil
}

func (s *Store) publishReadPlaneFresh(parent []byte, root ReadRoot, pages []ReadPage, consumedDeltas []KVPair) error {
	s.invalidateReadRoot(parent)
	batch, err := s.buildFreshReadPlaneBatch(parent, root, pages, consumedDeltas)
	if err != nil {
		return err
	}
	if len(batch) > 0 {
		if err := s.kv.Apply(batch); err != nil {
			return err
		}
	}
	if len(root.Pages) > 0 {
		s.cacheReadRoot(parent, root)
		for _, ref := range root.Pages {
			s.cachePageDeltaState(parent, ref.PageID, pageDeltaState{})
		}
	}
	return nil
}

func (s *Store) buildFullReadPlaneBatch(parent []byte, root ReadRoot, pages []ReadPage, consumedDeltas []KVPair) ([]Mutation, error) {
	batch := make([]Mutation, 0, 1+len(pages)+len(consumedDeltas))
	batch = append(batch, Mutation{Kind: MutationDelete, Key: encodeReadRootKey(parent)})
	pagePairs, err := s.kv.ScanPrefix(encodeReadPagePrefix(parent), nil, 0)
	if err != nil {
		return nil, err
	}
	for _, pair := range pagePairs {
		batch = append(batch, Mutation{Kind: MutationDelete, Key: cloneBytes(pair.Key)})
	}
	statePairs, err := s.kv.ScanPrefix(encodePageDeltaStateParentPrefix(parent), nil, 0)
	if err != nil {
		return nil, err
	}
	for _, pair := range statePairs {
		batch = append(batch, Mutation{Kind: MutationDelete, Key: cloneBytes(pair.Key)})
	}
	return s.appendReadPlanePuts(batch, parent, root, pages, consumedDeltas)
}

func (s *Store) buildFreshReadPlaneBatch(parent []byte, root ReadRoot, pages []ReadPage, consumedDeltas []KVPair) ([]Mutation, error) {
	batch := make([]Mutation, 0, 1+len(pages)+len(consumedDeltas))
	return s.appendReadPlanePuts(batch, parent, root, pages, consumedDeltas)
}

func (s *Store) appendReadPlanePuts(batch []Mutation, parent []byte, root ReadRoot, pages []ReadPage, consumedDeltas []KVPair) ([]Mutation, error) {
	if len(root.Pages) > 0 {
		if root.RootGeneration == 0 {
			return nil, ErrCodecCorrupted
		}
		rootRaw, err := encodeReadRoot(root)
		if err != nil {
			return nil, err
		}
		batch = append(batch, Mutation{Kind: MutationPut, Key: encodeReadRootKey(parent), Value: rootRaw})
		for _, page := range pages {
			pageRaw, err := encodeReadPage(page)
			if err != nil {
				return nil, err
			}
			batch = append(batch, Mutation{Kind: MutationPut, Key: encodeReadPageKey(parent, page.LowFence), Value: pageRaw})
		}
	}
	for _, pair := range consumedDeltas {
		batch = append(batch, Mutation{Kind: MutationDelete, Key: cloneBytes(pair.Key)})
	}
	return batch, nil
}

func (s *Store) persistPageLocalFold(parent []byte, root ReadRoot, oldPagesByID map[string]ReadPage, newPages []ReadPage, consumedDeltas []KVPair) (int, error) {
	s.invalidateReadRoot(parent)
	batch := make([]Mutation, 0, 1+len(newPages)+len(consumedDeltas))
	rootRaw, err := encodeReadRoot(root)
	if err != nil {
		return 0, err
	}
	batch = append(batch, Mutation{Kind: MutationPut, Key: encodeReadRootKey(parent), Value: rootRaw})
	newPagesByID := make(map[string]ReadPage, len(newPages))
	for _, page := range newPages {
		newPagesByID[string(page.PageID)] = page
	}
	for pageID, oldPage := range oldPagesByID {
		newPage, ok := newPagesByID[pageID]
		if !ok {
			batch = append(batch, Mutation{Kind: MutationDelete, Key: encodeReadPageKey(parent, oldPage.LowFence)})
			continue
		}
		if !bytes.Equal(oldPage.LowFence, newPage.LowFence) {
			batch = append(batch, Mutation{Kind: MutationDelete, Key: encodeReadPageKey(parent, oldPage.LowFence)})
		}
	}
	pagesWritten := 0
	for _, page := range newPages {
		oldPage, ok := oldPagesByID[string(page.PageID)]
		if ok && readPageEqual(oldPage, page) {
			continue
		}
		pageRaw, err := encodeReadPage(page)
		if err != nil {
			return 0, err
		}
		batch = append(batch, Mutation{Kind: MutationPut, Key: encodeReadPageKey(parent, page.LowFence), Value: pageRaw})
		pagesWritten++
	}
	for _, pair := range consumedDeltas {
		batch = append(batch, Mutation{Kind: MutationDelete, Key: cloneBytes(pair.Key)})
	}
	clearedStates := make(map[string]struct{})
	for _, pair := range consumedDeltas {
		pageID, err := decodePageDeltaPageIDFromKey(parent, pair.Key)
		if err != nil {
			return 0, err
		}
		key := string(pageID)
		if _, ok := clearedStates[key]; ok {
			continue
		}
		clearedStates[key] = struct{}{}
		batch = append(batch, Mutation{Kind: MutationDelete, Key: encodePageDeltaStateKey(parent, pageID)})
	}
	if len(batch) == 0 {
		return 0, nil
	}
	if err := s.kv.Apply(batch); err != nil {
		return 0, err
	}
	s.cacheReadRoot(parent, root)
	for _, ref := range root.Pages {
		s.cachePageDeltaState(parent, ref.PageID, pageDeltaState{})
	}
	return pagesWritten, nil
}

func (s *Store) materializePageLocalDeltas(parent []byte, root ReadRoot, pages []ReadPage, deltaPairs []KVPair, maxPageEntries int) (ReadRoot, []ReadPage, MaterializeStats, bool, error) {
	if len(deltaPairs) == 0 {
		return root, pages, MaterializeStats{}, true, nil
	}
	grouped := make(map[string][]KVPair)
	for _, pair := range deltaPairs {
		if !bytes.HasPrefix(pair.Key, encodePageDeltaParentPrefix(parent)) {
			return ReadRoot{}, nil, MaterializeStats{}, false, nil
		}
		pageID, err := decodePageDeltaPageIDFromKey(parent, pair.Key)
		if err != nil {
			return ReadRoot{}, nil, MaterializeStats{}, false, err
		}
		grouped[string(pageID)] = append(grouped[string(pageID)], pair)
	}

	newPages := append([]ReadPage(nil), pages...)
	oldPagesByID := readPagesByID(pages)
	oldRefsByID := make(map[string]ReadPageRef, len(root.Pages))
	for _, ref := range root.Pages {
		oldRefsByID[string(ref.PageID)] = ref
	}
	pageIndex := make(map[string]int, len(pages))
	for i, page := range newPages {
		pageIndex[string(page.PageID)] = i
	}
	usedIDs := make(map[string]struct{}, len(newPages))
	for _, page := range newPages {
		usedIDs[string(page.PageID)] = struct{}{}
	}
	nextPageID := nextReadPlanePageID(root)
	allocPageID := func() []byte {
		for {
			candidate := encodeReadPlanePageID(nextPageID)
			nextPageID++
			if _, ok := usedIDs[string(candidate)]; ok {
				continue
			}
			usedIDs[string(candidate)] = struct{}{}
			return candidate
		}
	}

	entriesMaterialized := 0
	updatedRefsByID := make(map[string]ReadPageRef)
	replacementGeneration := nextPublicationGeneration(root)
	for _, ref := range root.Pages {
		pairs, ok := grouped[string(ref.PageID)]
		if !ok {
			continue
		}
		pagePos, ok := pageIndex[string(ref.PageID)]
		if !ok {
			return ReadRoot{}, nil, MaterializeStats{}, false, nil
		}
		page := newPages[pagePos]
		mergedEntries, err := mergePageEntriesWithDeltaPairsNoMeta(parent, page.Entries, pairs)
		if err != nil {
			return ReadRoot{}, nil, MaterializeStats{}, false, err
		}
		replacements := buildReplacementPages(parent, page.PageID, mergedEntries, maxPageEntries, allocPageID)
		frontier := max(maxDeltaSeq(pairs), ref.PublishedFrontier)
		for i := range replacements {
			replacements[i].PublishedFrontier = frontier
			replacements[i].Generation = replacementGeneration
		}
		for _, replacement := range replacements {
			updatedRefsByID[string(replacement.PageID)] = ReadPageRef{
				FenceKey:          cloneBytes(replacement.LowFence),
				HighFence:         cloneBytes(replacement.HighFence),
				PageID:            cloneBytes(replacement.PageID),
				Count:             uint32(len(replacement.Entries)),
				CoverageState:     PageCoverageStateCovered,
				PublishedFrontier: frontier,
				Generation:        replacementGeneration,
			}
		}
		newPages = replaceReadPages(newPages, pagePos, replacements)
		pageIndex = indexReadPages(newPages)
		entriesMaterialized += len(mergedEntries)
	}
	newRoot, newPages := rebuildReadPlaneMetadata(parent, newPages)
	for i := range newRoot.Pages {
		if updated, ok := updatedRefsByID[string(newRoot.Pages[i].PageID)]; ok {
			newRoot.Pages[i].CoverageState = updated.CoverageState
			newRoot.Pages[i].PublishedFrontier = updated.PublishedFrontier
			newRoot.Pages[i].Generation = updated.Generation
			continue
		}
		if oldRef, ok := oldRefsByID[string(newRoot.Pages[i].PageID)]; ok {
			newRoot.Pages[i].CoverageState = oldRef.CoverageState
			newRoot.Pages[i].PublishedFrontier = oldRef.PublishedFrontier
			newRoot.Pages[i].Generation = oldRef.Generation
		}
	}
	pagesWritten, err := s.persistPageLocalFold(parent, newRoot, oldPagesByID, newPages, deltaPairs)
	if err != nil {
		return ReadRoot{}, nil, MaterializeStats{}, false, err
	}
	return newRoot, newPages, MaterializeStats{
		DeltasFolded:        len(deltaPairs),
		DeltaPagesFolded:    len(grouped),
		PagesWritten:        pagesWritten,
		EntriesMaterialized: entriesMaterialized,
	}, true, nil
}

func (s *Store) Stats(parent []byte) (ListingStats, error) {
	if s == nil || s.kv == nil {
		return ListingStats{}, ErrInvalidPath
	}
	_, pages, ok, err := s.LoadReadPlane(parent)
	if err != nil {
		return ListingStats{}, err
	}
	deltas, err := s.loadDeltaSnapshot(parent)
	if err != nil {
		return ListingStats{}, err
	}
	if !ok {
		pages = nil
	}
	return deltas.listingStats(pages), nil
}

func (s *Store) Verify(parent []byte) (VerifyStats, error) {
	if s == nil || s.kv == nil {
		return VerifyStats{}, ErrInvalidPath
	}
	truthEntries, err := s.scanTruthEntries(parent)
	if err != nil {
		return VerifyStats{}, err
	}
	root, ok, err := s.loadReadRoot(parent)
	if err != nil {
		return VerifyStats{}, err
	}
	var pages []ReadPage
	rootPageMismatch := false
	if ok {
		pages, rootPageMismatch, err = s.collectReadableReadPlanePages(parent, root)
		if err != nil {
			return VerifyStats{}, err
		}
	}
	var baseEntries []Entry
	if ok && !rootPageMismatch {
		baseEntries = collectReadPlaneEntries(pages)
	}
	deltas, err := s.loadDeltaSnapshot(parent)
	if err != nil {
		return VerifyStats{}, err
	}
	companionEntries, err := s.applyDeltaPairs(parent, baseEntries, deltas.allPairs())
	if err != nil {
		return VerifyStats{}, err
	}
	stats := VerifyStats{
		PageCount: len(root.Pages),
		Membership: VerifyMembershipStats{
			TruthEntries:     len(truthEntries),
			CompanionEntries: len(companionEntries),
		},
		Certificate: VerifyCertificateStats{
			RootPageMismatch: rootPageMismatch,
		},
	}
	if ok {
		if len(root.Pages) > 0 && root.RootGeneration == 0 {
			stats.Certificate.RootGenerationMismatch = true
			stats.Publication.GenerationRollbackIDs = append(stats.Publication.GenerationRollbackIDs, "(root)")
		}
		if root.RootGeneration < maxRootPageGeneration(root) {
			stats.Certificate.RootGenerationMismatch = true
			stats.Publication.GenerationRollbackIDs = append(stats.Publication.GenerationRollbackIDs, "(root)")
		}
		for _, ref := range root.Pages {
			cert := ref.Certificate()
			pageID := string(ref.PageID)
			noteCoverageState(&stats.Certificate, cert.CoverageState)
			state, _, err := s.evaluatePageAnswerability(parent, root, ref)
			if err != nil {
				return VerifyStats{}, err
			}
			noteAnswerabilityState(&stats, pageID, cert, state)
			page, pageErr := s.loadReadPageIgnoringGeneration(parent, ref)
			if pageErr != nil {
				stats.Certificate.RootPageMismatch = true
				continue
			}
			if page.Generation != cert.Generation {
				stats.Certificate.GenerationMismatchIDs = append(stats.Certificate.GenerationMismatchIDs, pageID)
			}
			if root.RootGeneration > 0 && page.Generation > root.RootGeneration {
				stats.Certificate.RootGenerationMismatch = true
				stats.Publication.GenerationRollbackIDs = append(stats.Publication.GenerationRollbackIDs, pageID)
			}
			if !cert.PublicationEquivalent(page.PublicationCertificate()) {
				stats.Publication.CohortMismatchIDs = append(stats.Publication.CohortMismatchIDs, pageID)
			}
			if cert.Generation == 0 {
				stats.Certificate.GenerationMismatchIDs = append(stats.Certificate.GenerationMismatchIDs, pageID)
			}
		}
		for i := range root.Pages {
			ref := root.Pages[i]
			if len(ref.HighFence) > 0 && bytes.Compare(ref.FenceKey, ref.HighFence) > 0 {
				stats.Certificate.IntervalDisorder = append(stats.Certificate.IntervalDisorder, string(ref.PageID))
			}
			if i == 0 {
				continue
			}
			prev := root.Pages[i-1]
			if len(prev.HighFence) > 0 && bytes.Compare(ref.FenceKey, prev.HighFence) < 0 {
				stats.Certificate.IntervalDisorder = append(stats.Certificate.IntervalDisorder, string(ref.PageID))
			}
		}
	}
	truthByName := make(map[string]Entry, len(truthEntries))
	for _, entry := range truthEntries {
		truthByName[string(entry.Name)] = entry
	}
	companionByName := make(map[string]Entry, len(companionEntries))
	for _, entry := range companionEntries {
		companionByName[string(entry.Name)] = entry
	}
	for name, truthEntry := range truthByName {
		companionEntry, ok := companionByName[name]
		if !ok {
			stats.Membership.MissingNames = append(stats.Membership.MissingNames, name)
			continue
		}
		if companionEntry.Kind != truthEntry.Kind {
			stats.Membership.KindMismatch = append(stats.Membership.KindMismatch, name)
		}
	}
	for name := range companionByName {
		if _, ok := truthByName[name]; !ok {
			stats.Membership.ExtraNames = append(stats.Membership.ExtraNames, name)
		}
	}
	sort.Strings(stats.Membership.MissingNames)
	sort.Strings(stats.Membership.ExtraNames)
	sort.Strings(stats.Membership.KindMismatch)
	sort.Strings(stats.Certificate.CoveredPendingDeltaIDs)
	sort.Strings(stats.Certificate.IntervalDisorder)
	sort.Strings(stats.Certificate.GenerationMismatchIDs)
	sort.Strings(stats.Publication.FrontierLagIDs)
	sort.Strings(stats.Publication.CohortMismatchIDs)
	sort.Strings(stats.Publication.GenerationRollbackIDs)
	stats.Membership.Consistent =
		len(stats.Membership.MissingNames) == 0 &&
			len(stats.Membership.ExtraNames) == 0 &&
			len(stats.Membership.KindMismatch) == 0
	stats.Certificate.Consistent =
		!stats.Certificate.RootPageMismatch &&
			!stats.Certificate.RootGenerationMismatch &&
			len(stats.Certificate.CoveredPendingDeltaIDs) == 0 &&
			len(stats.Certificate.IntervalDisorder) == 0 &&
			len(stats.Certificate.GenerationMismatchIDs) == 0
	stats.Publication.Consistent =
		len(stats.Publication.FrontierLagIDs) == 0 &&
			len(stats.Publication.CohortMismatchIDs) == 0 &&
			len(stats.Publication.GenerationRollbackIDs) == 0
	stats.Consistent = stats.Membership.Consistent && stats.Certificate.Consistent && stats.Publication.Consistent
	return stats, nil
}

func (s *Store) Rebuild(parent []byte) (RebuildStats, error) {
	if s == nil || s.kv == nil {
		return RebuildStats{}, ErrInvalidPath
	}
	oldRoot, ok, err := s.loadReadRoot(parent)
	if err != nil && !errors.Is(err, ErrCodecCorrupted) {
		return RebuildStats{}, err
	}
	if errors.Is(err, ErrCodecCorrupted) {
		oldRoot = ReadRoot{}
		ok = false
	}
	truthEntries, err := s.scanTruthEntries(parent)
	if err != nil {
		return RebuildStats{}, err
	}
	root, pages, err := buildReadPlaneOrdered(parent, truthEntries, s.readPageEntries)
	if err != nil {
		return RebuildStats{}, err
	}
	generation := uint64(1)
	if ok {
		generation = nextPublicationGeneration(oldRoot)
	}
	root, pages = assignReadPlanePublication(root, pages, generation, 0)
	root = certifyReadRoot(root, 0)
	deltas, err := s.loadDeltaSnapshot(parent)
	if err != nil {
		return RebuildStats{}, err
	}
	allDeltas := deltas.allPairs()
	if err := s.replaceReadPlane(parent, root, pages, allDeltas); err != nil {
		return RebuildStats{}, err
	}
	return RebuildStats{
		TruthEntries:        len(truthEntries),
		PagesWritten:        len(pages),
		DeltaRecordsCleared: len(allDeltas),
	}, nil
}

func (s *Store) cachedReadRoot(parent []byte) (ReadRoot, bool) {
	s.cacheMu.RLock()
	root, ok := s.readRootCache[string(parent)]
	s.cacheMu.RUnlock()
	return cloneReadRoot(root), ok
}

func (s *Store) cacheReadRoot(parent []byte, root ReadRoot) {
	s.cacheMu.Lock()
	s.readRootCache[string(parent)] = cloneReadRoot(root)
	s.cacheMu.Unlock()
}

func (s *Store) cachedReadPlaneView(parent []byte) (ReadPlaneView, bool) {
	s.cacheMu.RLock()
	view, ok := s.readPlaneViewCache[string(parent)]
	s.cacheMu.RUnlock()
	return view, ok
}

func (s *Store) cacheReadPlaneView(parent []byte, view ReadPlaneView) {
	s.cacheMu.Lock()
	s.readPlaneViewCache[string(parent)] = view
	s.cacheMu.Unlock()
}

func (s *Store) invalidateReadRoot(parent []byte) {
	s.cacheMu.Lock()
	delete(s.readRootCache, string(parent))
	delete(s.readPlaneViewCache, string(parent))
	s.cacheMu.Unlock()
}

func (s *Store) invalidateReadPlaneView(parent []byte) {
	s.cacheMu.Lock()
	delete(s.readPlaneViewCache, string(parent))
	s.cacheMu.Unlock()
}

func (s *Store) cachedPageDeltaState(parent, pageID []byte) (pageDeltaState, bool) {
	s.cacheMu.RLock()
	state, ok := s.pageDeltaStateCache[pageDeltaSeqCacheKey(parent, pageID)]
	s.cacheMu.RUnlock()
	return state, ok
}

func (s *Store) cachePageDeltaState(parent, pageID []byte, state pageDeltaState) {
	s.cacheMu.Lock()
	s.pageDeltaStateCache[pageDeltaSeqCacheKey(parent, pageID)] = state
	s.cacheMu.Unlock()
}

func pageDeltaSeqCacheKey(parent, pageID []byte) string {
	return string(parent) + "\x00" + string(pageID)
}

func (s *Store) nextPageDeltaSeq(parent, pageID []byte) (uint64, error) {
	cacheKey := pageDeltaSeqCacheKey(parent, pageID)
	s.cacheMu.Lock()
	if next, ok := s.pageDeltaSeqCache[cacheKey]; ok {
		s.pageDeltaSeqCache[cacheKey] = next + 1
		s.cacheMu.Unlock()
		return next, nil
	}
	s.cacheMu.Unlock()

	pairs, err := s.kv.ScanPrefix(encodePageDeltaPrefix(parent, pageID), nil, 0)
	if err != nil {
		return 0, err
	}
	var maxSeq uint64
	for _, pair := range pairs {
		seq, err := decodePageDeltaSeqFromKey(parent, pair.Key)
		if err != nil {
			continue
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	next := maxSeq + 1

	s.cacheMu.Lock()
	if cached, ok := s.pageDeltaSeqCache[cacheKey]; ok && cached > next {
		next = cached
	}
	s.pageDeltaSeqCache[cacheKey] = next + 1
	s.cacheMu.Unlock()
	return next, nil
}

func (s *Store) loadPageDeltaState(parent, pageID []byte) (pageDeltaState, error) {
	if state, ok := s.cachedPageDeltaState(parent, pageID); ok {
		return state, nil
	}
	raw, err := s.kv.Get(encodePageDeltaStateKey(parent, pageID))
	if err != nil {
		return pageDeltaState{}, err
	}
	state, err := decodePageDeltaStateValue(raw)
	if err != nil {
		return pageDeltaState{}, err
	}
	s.cachePageDeltaState(parent, pageID, state)
	return state, nil
}

func collectReadPlaneEntries(pages []ReadPage) []Entry {
	total := 0
	for _, page := range pages {
		total += len(page.Entries)
	}
	entries := make([]Entry, 0, total)
	for _, page := range pages {
		for _, entry := range page.Entries {
			entries = append(entries, cloneEntry(entry))
		}
	}
	return entries
}

func findReadPageForName(root ReadRoot, name []byte) []byte {
	ref, ok := findReadPageRefForName(root, name)
	if !ok {
		return nil
	}
	return cloneBytes(ref.PageID)
}

func findReadPageRefIndex(root ReadRoot, pageID []byte) (int, bool) {
	for i, ref := range root.Pages {
		if bytes.Equal(ref.PageID, pageID) {
			return i, true
		}
	}
	return -1, false
}

func findReadPageRefForName(root ReadRoot, name []byte) (ReadPageRef, bool) {
	if len(root.Pages) == 0 {
		return ReadPageRef{}, false
	}
	for _, ref := range root.Pages {
		if bytes.Compare(name, ref.FenceKey) < 0 {
			continue
		}
		if len(ref.HighFence) == 0 || bytes.Compare(name, ref.HighFence) < 0 {
			return ref, true
		}
	}
	return root.Pages[len(root.Pages)-1], true
}

func readPagesByID(pages []ReadPage) map[string]ReadPage {
	out := make(map[string]ReadPage, len(pages))
	for _, page := range pages {
		out[string(page.PageID)] = page
	}
	return out
}

func replaceReadPages(pages []ReadPage, index int, replacements []ReadPage) []ReadPage {
	out := make([]ReadPage, 0, len(pages)-1+len(replacements))
	out = append(out, pages[:index]...)
	out = append(out, replacements...)
	out = append(out, pages[index+1:]...)
	return out
}

func indexReadPages(pages []ReadPage) map[string]int {
	out := make(map[string]int, len(pages))
	for i, page := range pages {
		out[string(page.PageID)] = i
	}
	return out
}

func buildReplacementPages(parent, originalPageID []byte, mergedEntries []Entry, maxPageEntries int, allocPageID func() []byte) []ReadPage {
	if len(mergedEntries) == 0 {
		return nil
	}
	if maxPageEntries <= 0 {
		maxPageEntries = defaultReadPlanePageEntries
	}
	replacements := make([]ReadPage, 0, (len(mergedEntries)+maxPageEntries-1)/maxPageEntries)
	for start := 0; start < len(mergedEntries); start += maxPageEntries {
		end := min(start+maxPageEntries, len(mergedEntries))
		pageID := cloneBytes(originalPageID)
		if len(replacements) > 0 {
			pageID = allocPageID()
		}
		page := ReadPage{
			Parent:   cloneBytes(parent),
			PageID:   pageID,
			LowFence: cloneBytes(mergedEntries[start].Name),
			Entries:  mergedEntries[start:end],
		}
		replacements = append(replacements, page)
	}
	return replacements
}

func rebuildReadPlaneMetadata(parent []byte, pages []ReadPage) (ReadRoot, []ReadPage) {
	root := ReadRoot{Parent: cloneBytes(parent), Pages: make([]ReadPageRef, 0, len(pages))}
	for i := range pages {
		page := &pages[i]
		page.Parent = cloneBytes(parent)
		page.HighFence = nil
		page.NextPageID = nil
		if i+1 < len(pages) {
			page.HighFence = cloneBytes(pages[i+1].LowFence)
			page.NextPageID = cloneBytes(pages[i+1].PageID)
		}
		root.Pages = append(root.Pages, ReadPageRef{
			FenceKey:          cloneBytes(page.LowFence),
			HighFence:         cloneBytes(page.HighFence),
			PageID:            cloneBytes(page.PageID),
			Count:             uint32(len(page.Entries)),
			PublishedFrontier: page.PublishedFrontier,
			Generation:        page.Generation,
		})
		if page.Generation > root.RootGeneration {
			root.RootGeneration = page.Generation
		}
	}
	return root, pages
}

func nextReadPlanePageID(root ReadRoot) int {
	maxID := len(root.Pages)
	for _, ref := range root.Pages {
		if len(ref.PageID) != len("rp00000000") || !bytes.HasPrefix(ref.PageID, []byte("rp")) {
			continue
		}
		n := 0
		for _, ch := range ref.PageID[2:] {
			if ch < '0' || ch > '9' {
				n = -1
				break
			}
			n = n*10 + int(ch-'0')
		}
		if n >= maxID {
			maxID = n + 1
		}
	}
	return maxID
}

func readPageEqual(a, b ReadPage) bool {
	if !bytes.Equal(a.PageID, b.PageID) ||
		!bytes.Equal(a.LowFence, b.LowFence) ||
		!bytes.Equal(a.HighFence, b.HighFence) ||
		!bytes.Equal(a.NextPageID, b.NextPageID) ||
		a.Generation != b.Generation ||
		len(a.Entries) != len(b.Entries) {
		return false
	}
	for i := range a.Entries {
		if !bytes.Equal(a.Entries[i].Name, b.Entries[i].Name) || a.Entries[i].Kind != b.Entries[i].Kind {
			return false
		}
	}
	return true
}

func encodePageID(shardID uint32) []byte {
	return []byte{byte(shardID >> 24), byte(shardID >> 16), byte(shardID >> 8), byte(shardID)}
}

func shardFor(name []byte, shards int) uint32 {
	h := fnv.New32a()
	_, _ = h.Write(name)
	return h.Sum32() % uint32(shards)
}
