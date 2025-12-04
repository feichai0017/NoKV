//go:build linux
// +build linux

package mmap

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// mmap uses the mmap system call to memory-map a file. If writable is true,
// memory protection of the pages is set so that they may be written to as well.
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

// mremap is a Linux-specific system call to remap pages in memory. This can be used in place of munmap + mmap.
func mremap(data []byte, size int) ([]byte, error) {
	return unix.Mremap(data, size, unix.MREMAP_MAYMOVE)
}

// munmap unmaps a previously mapped slice.
func munmap(data []byte) error {
	// Keep checks minimal; unix.Munmap already validates length/cap internally.
	if len(data) == 0 {
		return nil
	}
	return unix.Munmap(data)
}

// adviseFromBool maps the legacy bool readahead flag to Advice.
func adviseFromBool(readahead bool) Advice {
	if readahead {
		return AdviceNormal
	}
	return AdviceRandom
}

// madvisePattern uses the madvise system call to give advise about the use of
// memory when using a slice that is memory-mapped to a file.
func madvisePattern(b []byte, advice Advice) error {
	var flags int
	switch advice {
	case AdviceNormal:
		flags = unix.MADV_NORMAL
	case AdviceSequential:
		flags = unix.MADV_SEQUENTIAL
	case AdviceRandom:
		flags = unix.MADV_RANDOM
	case AdviceWillNeed:
		flags = unix.MADV_WILLNEED
	case AdviceDontNeed:
		flags = unix.MADV_DONTNEED
	default:
		flags = unix.MADV_NORMAL
	}
	return unix.Madvise(b, flags)
}

// msync writes any modified data to persistent storage.
func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
