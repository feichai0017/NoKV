// Package vfs provides a tiny filesystem abstraction and fault-injection wrapper.
package vfs

import (
	"os"
	"path/filepath"
)

// FS defines filesystem operations used by storage/runtime components.
type FS interface {
	Open(name string) (*os.File, error)
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
	MkdirAll(path string, perm os.FileMode) error
	Remove(name string) error
	Rename(oldPath, newPath string) error
	Stat(name string) (os.FileInfo, error)
	ReadFile(name string) ([]byte, error)
	WriteFile(name string, data []byte, perm os.FileMode) error
	Truncate(name string, size int64) error
	Glob(pattern string) ([]string, error)
	Hostname() (string, error)
}

// OSFS is the production filesystem implementation backed by the os package.
type OSFS struct{}

// Open opens an existing file for reading.
func (OSFS) Open(name string) (*os.File, error) {
	return os.Open(name)
}

// OpenFile opens or creates a file.
func (OSFS) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

// MkdirAll creates a directory hierarchy.
func (OSFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
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

// ReadFile reads an entire file.
func (OSFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

// WriteFile writes an entire file atomically with the provided permissions.
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
