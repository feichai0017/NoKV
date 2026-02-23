package peer_test

import (
	"os"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/utils"
)

func openDBAt(t *testing.T, dir string) *NoKV.DB {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.MemTableSize = 1 << 12
	opt.SSTableMaxSz = 1 << 20
	opt.ValueLogFileSize = 1 << 20
	opt.ValueThreshold = utils.DefaultValueThreshold
	opt.RaftLagWarnSegments = 1
	return NoKV.Open(opt)
}
