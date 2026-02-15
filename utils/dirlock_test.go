package utils

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/vfs"
)

func TestDirLockExclusive(t *testing.T) {
	dir := t.TempDir()

	lock, err := AcquireDirLock(dir)
	if err != nil {
		t.Fatalf("acquire dir lock: %v", err)
	}
	defer func() {
		if err := lock.Release(); err != nil {
			t.Fatalf("release dir lock: %v", err)
		}
	}()

	if _, err := os.Stat(filepath.Join(dir, "LOCK")); err != nil {
		t.Fatalf("lock file missing: %v", err)
	}

	other, err := AcquireDirLock(dir)
	if err == nil {
		_ = other.Release()
		t.Fatalf("expected second lock acquisition to fail")
	}
}

func TestDirLockReleaseRemovesFile(t *testing.T) {
	dir := t.TempDir()
	lock, err := AcquireDirLock(dir)
	if err != nil {
		t.Fatalf("acquire dir lock: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("release dir lock: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "LOCK")); !os.IsNotExist(err) {
		t.Fatalf("lock file should be removed after release, err=%v", err)
	}
}

func TestAcquireDirLockWithFSInjectedFailure(t *testing.T) {
	dir := t.TempDir()
	injected := errors.New("mkdir injected")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpMkdirAll, "", injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	_, err := AcquireDirLockWithFS(dir, fs)
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}
}
