package root

// AllocatorKind identifies one globally fenced allocator domain.
type AllocatorKind uint8

const (
	AllocatorKindUnknown AllocatorKind = iota
	AllocatorKindID
	AllocatorKindTSO
)

// Cursor identifies one committed position in the metadata-root log.
type Cursor struct {
	Term  uint64
	Index uint64
}

// CommitInfo reports one successful root append together with the resulting
// compact root state.
type CommitInfo struct {
	Cursor Cursor
	State  State
}
