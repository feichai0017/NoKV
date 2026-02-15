package vfs

import (
	"errors"
	"io"
	"os"
	"testing"
)

type noFDFile struct {
	file *os.File
}

func (f *noFDFile) Read(p []byte) (int, error) {
	return f.file.Read(p)
}

func (f *noFDFile) ReadAt(p []byte, off int64) (int, error) {
	return f.file.ReadAt(p, off)
}

func (f *noFDFile) Write(p []byte) (int, error) {
	return f.file.Write(p)
}

func (f *noFDFile) WriteAt(p []byte, off int64) (int, error) {
	return f.file.WriteAt(p, off)
}

func (f *noFDFile) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(offset, whence)
}

func (f *noFDFile) Close() error {
	return f.file.Close()
}

func (f *noFDFile) Stat() (os.FileInfo, error) {
	return f.file.Stat()
}

func (f *noFDFile) Sync() error {
	return f.file.Sync()
}

func (f *noFDFile) Truncate(size int64) error {
	return f.file.Truncate(size)
}

func (f *noFDFile) Name() string {
	return f.file.Name()
}

func TestFileFDExtractsFromOSFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "fd-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Fatalf("close temp file: %v", err)
		}
	})

	fd, ok := FileFD(f)
	if !ok {
		t.Fatalf("expected descriptor from os file")
	}
	if fd == 0 {
		t.Fatalf("expected non-zero descriptor")
	}
}

func TestFileFDReturnsFalseWhenDescriptorUnsupported(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "nofd-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Fatalf("close temp file: %v", err)
		}
	})

	noFD := &noFDFile{file: f}
	fd, ok := FileFD(noFD)
	if ok {
		t.Fatalf("expected descriptor to be unavailable, got %d", fd)
	}
	if _, err := noFD.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek nofd file: %v", err)
	}
}
