//go:build !linux

package vfs

func renameNoReplaceOS(oldPath, newPath string) error {
	return renameNoReplaceFallback(OSFS{}, oldPath, newPath)
}
