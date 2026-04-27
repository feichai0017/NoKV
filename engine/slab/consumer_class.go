package slab

// ConsumerClass tags how a slab consumer relates to truth: this is the
// machine-checkable form of the "Authoritative / Lifecycle-bound /
// Derived" taxonomy from docs/notes/2026-04-27-slab-substrate.md §3.
//
// The class controls what kind of corruption / truncation / fault is
// recoverable. Authoritative loss is data loss; Derived loss is a cold
// cache. Tests use the class to assert that fault injection on a
// Derived consumer never propagates back into the truth path.
type ConsumerClass uint8

const (
	// ConsumerClassUnspecified means the consumer has not declared its
	// class. Treated as Authoritative for safety; new consumers should
	// pick a concrete class.
	ConsumerClassUnspecified ConsumerClass = iota

	// ConsumerClassAuthoritative consumers own truth: their on-disk
	// segments must be recoverable byte-for-byte and corruption is a
	// data-loss incident. Example: engine/vlog (KV-separated values).
	ConsumerClassAuthoritative

	// ConsumerClassLifecycleBound consumers own state whose lifetime is
	// pinned to a higher-level transaction or coordination boundary
	// (snapshot era, replica install, txn). Loss before commit is
	// recoverable by replaying the parent operation.
	ConsumerClassLifecycleBound

	// ConsumerClassDerived consumers hold a cache or index built from
	// some other authoritative source. Total loss must NOT corrupt the
	// truth plane — a Derived consumer can always be discarded and
	// rebuilt cold. Examples: engine/slab/negativecache (LSM-derived
	// missing-key cache), engine/slab/dirpage (LSM-derived directory
	// page index).
	ConsumerClassDerived
)

// String returns a human-readable consumer-class label suitable for
// stats output and log lines.
func (c ConsumerClass) String() string {
	switch c {
	case ConsumerClassAuthoritative:
		return "authoritative"
	case ConsumerClassLifecycleBound:
		return "lifecycle-bound"
	case ConsumerClassDerived:
		return "derived"
	default:
		return "unspecified"
	}
}
