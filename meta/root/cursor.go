package root

import rootstate "github.com/feichai0017/NoKV/meta/root/state"

// AllocatorKind identifies one globally fenced allocator domain.
type AllocatorKind uint8

const (
	AllocatorKindUnknown AllocatorKind = iota
	AllocatorKindID
	AllocatorKindTSO
)

type Cursor = rootstate.Cursor
type CommitInfo = rootstate.CommitInfo
