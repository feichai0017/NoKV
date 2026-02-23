//go:build darwin

package mmap

import (
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Mmap uses the mmap system call to memory-map a file. If writable is true,
// memory protection of the pages is set so that they may be written to as well.
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

// Munmap unmaps a previously mapped slice.
func munmap(b []byte) error {
	return unix.Munmap(b)
}

func adviseFromBool(readahead bool) Advice {
	if readahead {
		return AdviceNormal
	}
	return AdviceRandom
}

// This is required because the unix package does not support the madvise system call on OS X.
func madvisePattern(b []byte, advice Advice) error {
	var flag int
	switch advice {
	case AdviceNormal:
		flag = unix.MADV_NORMAL
	case AdviceSequential:
		flag = unix.MADV_SEQUENTIAL
	case AdviceRandom:
		flag = unix.MADV_RANDOM
	case AdviceWillNeed:
		flag = unix.MADV_WILLNEED
	case AdviceDontNeed:
		flag = unix.MADV_DONTNEED
	default:
		flag = unix.MADV_NORMAL
	}

	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)), uintptr(flag))
	if e1 != 0 {
		return e1
	}
	return nil
}

func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}

// msyncAsync flushes dirty pages asynchronously.
func msyncAsync(b []byte) error {
	return unix.Msync(b, unix.MS_ASYNC)
}
