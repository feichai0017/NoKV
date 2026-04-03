package replicated

import (
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"time"
)

// DriverState is one detached view of one replicated metadata driver state.
type DriverState struct {
	Checkpoint rootstorage.Checkpoint
	Records    []rootstorage.CommittedEvent
}

// Driver exposes the minimal committed-log, checkpoint, and bootstrap-install
// capabilities required by the replicated metadata-root backend.
type Driver interface {
	rootstorage.Substrate
	WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error)
	IsLeader() bool
	LeaderID() uint64
}
