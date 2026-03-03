//go:build linux

package vfs

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func renameNoReplaceOS(oldPath, newPath string) error {
	err := unix.Renameat2(unix.AT_FDCWD, oldPath, unix.AT_FDCWD, newPath, unix.RENAME_NOREPLACE)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EEXIST) {
		return os.ErrExist
	}
	// Fallback for kernels/filesystems without RENAME_NOREPLACE support.
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) {
		return renameNoReplaceFallback(OSFS{}, oldPath, newPath)
	}
	return err
}
