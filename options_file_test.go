package NoKV

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestApplyOptionsFileTOML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "opts.toml")
	raw := []byte(`
work_dir = "./data-toml"
write_hot_key_limit = 77
hot_ring_enabled = true
value_log_gc_interval = "45s"
value_log_gc_parallelism = 3
mem_table_engine = "art"
`)
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	opt := NewDefaultOptions()
	require.NoError(t, ApplyOptionsFile(opt, path))
	require.Equal(t, "./data-toml", opt.WorkDir)
	require.Equal(t, int32(77), opt.WriteHotKeyLimit)
	require.True(t, opt.HotRingEnabled)
	require.Equal(t, 45*time.Second, opt.ValueLogGCInterval)
	require.Equal(t, 3, opt.ValueLogGCParallelism)
	require.Equal(t, MemTableEngineART, opt.MemTableEngine)
}

func TestApplyOptionsFileUnknownField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "opts.toml")
	require.NoError(t, os.WriteFile(path, []byte(`unknown_option = 1`), 0o644))

	opt := NewDefaultOptions()
	require.Error(t, ApplyOptionsFile(opt, path))
}

func TestLoadOptionsFileRejectsJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "opts.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"work_dir":"./data"}`), 0o644))

	_, err := LoadOptionsFile(path)
	require.Error(t, err)
}
