package namespace

// ListingPage is the logical persistence unit of the namespace-aware listing
// layer. A page owns a subset of children for one parent prefix.
//
// The proposal deliberately keeps this type minimal. It is sufficient for a
// prototype and benchmark harness while leaving room for future fields such as
// split metadata, checksums, and page statistics.
type ListingPage struct {
	Prefix     []byte
	PageID     []byte
	Entries    []Entry
	NextPageID []byte
}
