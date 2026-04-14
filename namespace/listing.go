package namespace

// ListingIndex defines the minimum surface of the namespace-aware listing
// component under study.
//
// The boundary is intentionally narrow:
// - truth remains in the underlying metadata KV records
// - the listing layer only maintains child enumeration state
// - pagination is a first-class concern from the start
//
// This interface is a research boundary, not a production contract yet.
type ListingIndex interface {
	AddChild(parent []byte, child Entry) error
	RemoveChild(parent []byte, name []byte) error
	List(parent []byte, cursor Cursor, limit int) ([]Entry, Cursor, error)
}
