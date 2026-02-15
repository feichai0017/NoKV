// Package vfs provides a tiny filesystem abstraction and fault-injection wrapper.
package vfs

import (
	"io"
	"os"
	"path/filepath"
)

// File describes the file operations storage components rely on.
// *os.File satisfies this interface.
type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.WriterAt
	io.Seeker
	io.Closer
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
	Name() string
	Fd() uintptr
}

// FS defines filesystem operations used by storage/runtime components.
type FS interface {
	OpenHandle(name string) (File, error)
	OpenFileHandle(name string, flag int, perm os.FileMode) (File, error)
	MkdirAll(path string, perm os.FileMode) error
	RemoveAll(path string) error
	Remove(name string) error
	Rename(oldPath, newPath string) error
	Stat(name string) (os.FileInfo, error)
	ReadDir(name string) ([]os.DirEntry, error)
	ReadFile(name string) ([]byte, error)
	WriteFile(name string, data []byte, perm os.FileMode) error
	Truncate(name string, size int64) error
	Glob(pattern string) ([]string, error)
	Hostname() (string, error)
}

// OSFS is the production filesystem implementation backed by the os package.
type OSFS struct{}

// OpenHandle opens an existing file and returns a vfs.File.
func (OSFS) OpenHandle(name string) (File, error) {
	return os.Open(name)
}

// OpenFileHandle opens or creates a file and returns a vfs.File.
func (OSFS) OpenFileHandle(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(name, flag, perm)
}

// MkdirAll creates a directory hierarchy.
func (OSFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

// RemoveAll removes a path and all children.
func (OSFS) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

// Remove removes a file or empty directory.
func (OSFS) Remove(name string) error {
	return os.Remove(name)
}

// Rename renames (moves) a file or directory.
func (OSFS) Rename(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath)
}

// Stat returns file metadata.
func (OSFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

// ReadDir lists directory entries.
func (OSFS) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}

// ReadFile reads an entire file.
func (OSFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

// WriteFile writes an entire file with the provided permissions.
func (OSFS) WriteFile(name string, data []byte, perm os.FileMode) error {
	return os.WriteFile(name, data, perm)
}

// Truncate resizes a file.
func (OSFS) Truncate(name string, size int64) error {
	return os.Truncate(name, size)
}

// Glob expands filesystem patterns.
func (OSFS) Glob(pattern string) ([]string, error) {
	return filepath.Glob(pattern)
}

// Hostname returns the local hostname.
func (OSFS) Hostname() (string, error) {
	return os.Hostname()
}

// Ensure returns fs when non-nil; otherwise it returns OSFS.
func Ensure(fs FS) FS {
	if fs == nil {
		return OSFS{}
	}
	return fs
}

// UnwrapOSFile extracts the underlying *os.File from a File implementation when available.
func UnwrapOSFile(f File) (*os.File, bool) {
	if f == nil {
		return nil, false
	}
	if of, ok := f.(*os.File); ok {
		return of, true
	}
	if wrapped, ok := f.(interface{ OSFile() *os.File }); ok {
		if of := wrapped.OSFile(); of != nil {
			return of, true
		}
	}
	return nil, false
}
