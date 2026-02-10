//go:build linux
// +build linux

package mmap

import (
	"os"

	"golang.org/x/sys/unix"
)

// Mmap is part of the exported package API.
func Mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

// Munmap unmaps a previously mapped slice.
func Munmap(b []byte) error {
	return munmap(b)
}

// Madvise uses the madvise system call to give advise about the use of memory
// when using a slice that is memory-mapped to a file. Prefer MadvisePattern
// for explicit patterns.
func Madvise(b []byte, readahead bool) error {
	return MadvisePattern(b, adviseFromBool(readahead))
}

// MadvisePattern exposes richer access-pattern hints to the OS.
func MadvisePattern(b []byte, advice Advice) error {
	return madvisePattern(b, advice)
}

// Msync would call sync on the mmapped data.
func Msync(b []byte) error {
	return msync(b)
}

// MsyncAsync flushes dirty pages asynchronously.
func MsyncAsync(b []byte) error {
	return msyncAsync(b)
}

// MsyncAsyncRange flushes a range [off, off+len) asynchronously.
func MsyncAsyncRange(b []byte, off, n int64) error {
	if off < 0 || n <= 0 || off+n > int64(len(b)) {
		return unix.EINVAL
	}
	return unix.Msync(b[off:off+n], unix.MS_ASYNC)
}

// Mremap unmmap and mmap
func Mremap(data []byte, size int) ([]byte, error) {
	return mremap(data, size)
}
