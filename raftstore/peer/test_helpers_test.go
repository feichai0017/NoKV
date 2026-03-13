package peer_test

import (
	"os"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/utils"
)

func openDBAt(t *testing.T, dir string) (*NoKV.DB, *raftmeta.Store) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	localMeta, err := raftmeta.OpenLocalStore(dir, nil)
	if err != nil {
		t.Fatalf("open local metadata %s: %v", dir, err)
	}
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.MemTableSize = 1 << 12
	opt.SSTableMaxSz = 1 << 20
	opt.ValueLogFileSize = 1 << 20
	opt.ValueThreshold = utils.DefaultValueThreshold
	opt.RaftLagWarnSegments = 1
	opt.RaftPointerSnapshot = localMeta.RaftPointerSnapshot
	db := NoKV.Open(opt)
	t.Cleanup(func() { _ = localMeta.Close() })
	return db, localMeta
}
