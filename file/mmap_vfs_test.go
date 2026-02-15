package file

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/vfs"
	"github.com/stretchr/testify/require"
)

func TestMmapFileDeleteUsesInjectedFS(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mmap-vfs-delete.dat")
	injected := errors.New("remove injected")
	fs := vfs.NewFaultFS(vfs.OSFS{}, func(op vfs.Op, p string) error {
		if op == vfs.OpRemove && p == path {
			return injected
		}
		return nil
	})

	mf, err := OpenMmapFileWithFS(fs, path, os.O_CREATE|os.O_RDWR, 64)
	require.NoError(t, err)
	require.NotNil(t, mf)

	err = mf.Delete()
	require.ErrorIs(t, err, injected)
}

func TestSyncDirWithFSInjectsOpenFailure(t *testing.T) {
	dir := t.TempDir()
	injected := errors.New("open dir injected")
	fs := vfs.NewFaultFS(vfs.OSFS{}, func(op vfs.Op, p string) error {
		if op == vfs.OpOpen && p == dir {
			return injected
		}
		return nil
	})

	err := SyncDirWithFS(fs, dir)
	require.ErrorIs(t, err, injected)
}
