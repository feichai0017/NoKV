package namespace

// DeltaOp describes the mutable-tier namespace change staged before
// materialization into a persistent listing page.
type DeltaOp uint8

const (
	DeltaOpAdd DeltaOp = iota
	DeltaOpRemove
)

// ListingDelta is the smallest mutable-tier record for recent namespace
// updates. The long shared prefix in its companion key is what makes the
// existing ART-backed memtable a natural staging area.
type ListingDelta struct {
	Parent  []byte
	PageID  []byte
	Name    []byte
	Kind    EntryKind
	MetaKey []byte
	Op      DeltaOp
}
