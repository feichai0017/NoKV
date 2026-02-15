package vfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFaultFSInjectOpenFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.data")
	injected := errors.New("openfile injected")
	fs := NewFaultFS(OSFS{}, func(op Op, p string) error {
		if op == OpOpenFile && p == path {
			return injected
		}
		return nil
	})

	_, err := fs.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	require.ErrorIs(t, err, injected)
}

func TestFaultFSPassThrough(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.data")
	fs := NewFaultFS(OSFS{}, nil)

	require.NoError(t, fs.MkdirAll(dir, 0o755))
	require.NoError(t, fs.WriteFile(path, []byte("ok"), 0o644))
	got, err := fs.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "ok", string(got))
}
