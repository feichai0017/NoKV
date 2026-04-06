package state

// AllocatorKind identifies one globally fenced allocator domain inside rooted
// metadata state.
type AllocatorKind uint8

const (
	AllocatorKindUnknown AllocatorKind = iota
	AllocatorKindID
	AllocatorKindTSO
)
