//go:build linux
// +build linux

package mmap

import (
	"os"
)

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

// Mremap unmmap and mmap
func Mremap(data []byte, size int) ([]byte, error) {
	return mremap(data, size)
}
