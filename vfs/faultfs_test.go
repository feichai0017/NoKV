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
	policy := NewFaultPolicy(FailOnceRule(OpOpenFile, path, injected))
	fs := NewFaultFSWithPolicy(OSFS{}, policy)

	_, err := fs.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	require.ErrorIs(t, err, injected)
}

func TestFaultPolicyFailOnNth(t *testing.T) {
	injected := errors.New("stat injected")
	policy := NewFaultPolicy(FailOnNthRule(OpStat, "", 3, injected))
	fs := NewFaultFSWithPolicy(OSFS{}, policy)
	path := filepath.Join(t.TempDir(), "x.data")
	require.NoError(t, fs.WriteFile(path, []byte("ok"), 0o644))

	_, err := fs.Stat(path)
	require.NoError(t, err)
	_, err = fs.Stat(path)
	require.NoError(t, err)
	_, err = fs.Stat(path)
	require.ErrorIs(t, err, injected)
	_, err = fs.Stat(path)
	require.NoError(t, err)
}

func TestFaultPolicyFailAfterNth(t *testing.T) {
	injected := errors.New("read injected")
	policy := NewFaultPolicy(FailAfterNthRule(OpReadFile, "", 2, injected))
	fs := NewFaultFSWithPolicy(OSFS{}, policy)
	path := filepath.Join(t.TempDir(), "x.data")
	require.NoError(t, fs.WriteFile(path, []byte("ok"), 0o644))

	_, err := fs.ReadFile(path)
	require.NoError(t, err)
	_, err = fs.ReadFile(path)
	require.ErrorIs(t, err, injected)
	_, err = fs.ReadFile(path)
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
