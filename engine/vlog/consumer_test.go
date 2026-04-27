package vlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestReconcileManifestRemovesInvalid(t *testing.T) {
	tmp := t.TempDir()
	mgr, err := Open(Config{Dir: tmp, FileMode: utils.DefaultFileMode, MaxSize: 1 << 20, Bucket: 0})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	require.NoError(t, mgr.Rotate())

	c := &Consumer{
		bucketCount: 1,
		managers:    []*Manager{mgr},
	}

	c.ReconcileManifest(map[manifest.ValueLogID]manifest.ValueLogMeta{
		{Bucket: 0, FileID: 0}: {Bucket: 0, FileID: 0, Valid: false},
	})

	_, err = os.Stat(filepath.Join(tmp, "00000.vlog"))
	require.Error(t, err)
}
