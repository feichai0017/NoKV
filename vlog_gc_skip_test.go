package NoKV

import (
	"errors"
	"testing"

	kvpkg "github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

// GC should return ErrNoRewrite (not a hard error) when writes are blocked.
func TestValueGCSkipWhenBlocked(t *testing.T) {
	clearDir()
	opt := NewDefaultOptions()
	opt.ValueLogFileSize = 1 << 20
	opt.NumCompactors = 0 // avoid compaction interference
	db := Open(opt)
	defer db.Close()

	// Block writes to trigger ErrBlockedWrites path during GC.
	db.applyThrottle(true)
	defer db.applyThrottle(false)

	e := kvpkg.NewEntry([]byte("gc-skip"), []byte("v"))
	if err := db.Set(e); err != nil {
		t.Fatalf("set: %v", err)
	}

	if err := db.RunValueLogGC(0.5); err != nil && !errors.Is(err, utils.ErrNoRewrite) {
		t.Fatalf("expected ErrNoRewrite when writes blocked, got %v", err)
	}
}
