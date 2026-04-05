package replicated

import (
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"time"
)

// Driver exposes the minimal committed-log, checkpoint, and bootstrap-install
// capabilities required by the replicated metadata-root backend.
type Driver interface {
	rootstorage.Substrate
	ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error)
	WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error)
	TailNotify() <-chan struct{}
	IsLeader() bool
	LeaderID() uint64
}
