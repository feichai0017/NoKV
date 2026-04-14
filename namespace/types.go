package namespace

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
// The first prototype keeps it simple: the cursor identifies the page and the
// last returned child name within that page. Production encodings can evolve
// without changing the ListingIndex interface.
type Cursor struct {
	PageID   []byte
	LastName []byte
}
