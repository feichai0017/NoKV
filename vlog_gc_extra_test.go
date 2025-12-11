package NoKV

import (
	"bytes"
	"errors"
	"testing"

	kvpkg "github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

// Expect GC to skip (ErrNoRewrite) when discard is below threshold.
func TestValueGCNoRewriteLowDiscard(t *testing.T) {
	clearDir()
	opt := NewDefaultOptions()
	opt.ValueLogFileSize = 1 << 20
	opt.ValueLogGCSampleSizeRatio = 1.0
	opt.ValueLogGCSampleCountRatio = 1.0
	opt.NumCompactors = 0 // avoid interference
	db := Open(opt)
	defer db.Close()

	e := kvpkg.NewEntry([]byte("gc-no-rewrite"), bytes.Repeat([]byte("x"), 512))
	require.NoError(t, db.Set(e))

	err := db.RunValueLogGC(0.99)
	if err != nil && !errors.Is(err, utils.ErrNoRewrite) {
		t.Fatalf("expected ErrNoRewrite for low discard, got %v", err)
	}
}

// reconcileManifest should ignore missing value log files gracefully.
func TestValueLogReconcileMissingFID(t *testing.T) {
	clearDir()
	opt := NewDefaultOptions()
	opt.ValueLogFileSize = 1 << 20
	opt.NumCompactors = 0
	db := Open(opt)
	defer db.Close()

	head := db.vlog.manager.Head()
	missingFID := uint32(12345)
	metas := map[uint32]manifest.ValueLogMeta{
		missingFID: {FileID: missingFID, Valid: true},
	}
	db.vlog.reconcileManifest(metas)

	newHead := db.vlog.manager.Head()
	if newHead != head {
		t.Fatalf("expected head unchanged after reconcile missing fid: before=%+v after=%+v", head, newHead)
	}
}
