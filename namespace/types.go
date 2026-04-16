package namespace

func cloneEntry(in Entry) Entry {
	return Entry{
		Name:    cloneBytes(in.Name),
		Kind:    in.Kind,
		MetaKey: cloneBytes(in.MetaKey),
	}
}

func cloneEntryNoMeta(in Entry) Entry {
	return Entry{
		Name: cloneBytes(in.Name),
		Kind: in.Kind,
	}
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

// EntryKind describes the logical namespace type carried by a listing entry.
type EntryKind uint8

const (
	EntryKindFile EntryKind = iota
	EntryKindDirectory
)

// Entry is the smallest namespace-visible child descriptor used by the listing
// layer. It intentionally stays separate from the authoritative metadata value:
// listing pages enumerate children; truth remains in the underlying KV record.
type Entry struct {
	Name    []byte
	Kind    EntryKind
	MetaKey []byte
}

// Cursor is the opaque page-local resume position returned by List.
//
// The current prototype uses page-local resume information:
//   - PageID identifies the current materialized page
//   - LastName keeps a stable name-based fallback
//   - EntryOffset is the fast-path resume position within the current page
//
// This keeps continuation explicit without adding hidden store-side session
// state.
type Cursor struct {
	PageID      []byte
	LastName    []byte
	EntryOffset uint32
}

// ListingStats summarizes the current read-plane and delta state for one parent
// prefix. It is used by benchmarks and design validation, not yet as a
// production metrics contract.
type ListingStats struct {
	MaterializedPages   int
	MaterializedEntries int
	DeltaRecords        int
	DistinctDeltaPages  int
}

// MaterializeStats captures the work performed when folding mutable deltas into
// persistent listing pages.
type MaterializeStats struct {
	DeltasFolded        int
	DeltaPagesFolded    int
	PagesWritten        int
	PagesDeleted        int
	EntriesMaterialized int
}

// ListStats captures the work performed by one List call.
type ListStats struct {
	PagesVisited   int
	PagesLoaded    int
	DeltasRead     int
	EntriesScanned int
}

// VerifyMembershipStats summarizes truth-vs-view set equality for one parent.
type VerifyMembershipStats struct {
	Consistent bool

	TruthEntries     int
	CompanionEntries int

	MissingNames []string
	ExtraNames   []string
	KindMismatch []string
}

// VerifyCertificateStats summarizes page-local answerability certificate
// invariants that do not depend on publication frontier ordering.
type VerifyCertificateStats struct {
	Consistent bool

	CoveredPages   int
	UncoveredPages int
	ColdPages      int
	DirtyPages     int

	CoveredPendingDeltaIDs []string
	IntervalDisorder       []string
	GenerationMismatchIDs  []string
	RootPageMismatch       bool
	RootGenerationMismatch bool
}

// VerifyPublicationStats summarizes publication metadata consistency across
// root/page refs and page-local delta frontiers.
type VerifyPublicationStats struct {
	Consistent bool

	FrontierLagIDs        []string
	CohortMismatchIDs     []string
	GenerationRollbackIDs []string
}

// VerifyStats summarizes whether the current read plane both matches the
// authoritative truth plane and still satisfies certificate-oriented
// answerability invariants for one parent prefix.
type VerifyStats struct {
	Consistent bool
	PageCount  int

	Membership  VerifyMembershipStats
	Certificate VerifyCertificateStats
	Publication VerifyPublicationStats
}

// RebuildStats captures work performed when rebuilding the persisted read
// plane directly from the truth plane.
type RebuildStats struct {
	TruthEntries        int
	PagesWritten        int
	DeltaRecordsCleared int
}
