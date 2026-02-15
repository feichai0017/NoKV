package vfs

import "os"

// Op identifies a filesystem operation for failure injection.
type Op string

const (
	OpOpen      Op = "open"
	OpOpenFile  Op = "open_file"
	OpMkdirAll  Op = "mkdir_all"
	OpRemove    Op = "remove"
	OpRename    Op = "rename"
	OpStat      Op = "stat"
	OpReadFile  Op = "read_file"
	OpWriteFile Op = "write_file"
	OpTruncate  Op = "truncate"
	OpGlob      Op = "glob"
	OpHostname  Op = "hostname"
)

// Hook is invoked before each filesystem operation.
// Returning a non-nil error simulates an operation failure.
type Hook func(op Op, path string) error

// FaultFS decorates an FS and injects failures via Hook.
type FaultFS struct {
	base FS
	hook Hook
}

// NewFaultFS returns an FS wrapper that can inject operation failures.
func NewFaultFS(base FS, hook Hook) *FaultFS {
	return &FaultFS{
		base: Ensure(base),
		hook: hook,
	}
}

func (f *FaultFS) before(op Op, path string) error {
	if f == nil || f.hook == nil {
		return nil
	}
	return f.hook(op, path)
}

// Open opens an existing file for reading.
func (f *FaultFS) Open(name string) (*os.File, error) {
	if err := f.before(OpOpen, name); err != nil {
		return nil, err
	}
	return f.base.Open(name)
}

// OpenFile opens or creates a file.
func (f *FaultFS) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if err := f.before(OpOpenFile, name); err != nil {
		return nil, err
	}
	return f.base.OpenFile(name, flag, perm)
}

// MkdirAll creates a directory hierarchy.
func (f *FaultFS) MkdirAll(path string, perm os.FileMode) error {
	if err := f.before(OpMkdirAll, path); err != nil {
		return err
	}
	return f.base.MkdirAll(path, perm)
}

// Remove removes a file or empty directory.
func (f *FaultFS) Remove(name string) error {
	if err := f.before(OpRemove, name); err != nil {
		return err
	}
	return f.base.Remove(name)
}

// Rename renames (moves) a file or directory.
func (f *FaultFS) Rename(oldPath, newPath string) error {
	if err := f.before(OpRename, oldPath+"->"+newPath); err != nil {
		return err
	}
	return f.base.Rename(oldPath, newPath)
}

// Stat returns file metadata.
func (f *FaultFS) Stat(name string) (os.FileInfo, error) {
	if err := f.before(OpStat, name); err != nil {
		return nil, err
	}
	return f.base.Stat(name)
}

// ReadFile reads an entire file.
func (f *FaultFS) ReadFile(name string) ([]byte, error) {
	if err := f.before(OpReadFile, name); err != nil {
		return nil, err
	}
	return f.base.ReadFile(name)
}

// WriteFile writes an entire file.
func (f *FaultFS) WriteFile(name string, data []byte, perm os.FileMode) error {
	if err := f.before(OpWriteFile, name); err != nil {
		return err
	}
	return f.base.WriteFile(name, data, perm)
}

// Truncate resizes a file.
func (f *FaultFS) Truncate(name string, size int64) error {
	if err := f.before(OpTruncate, name); err != nil {
		return err
	}
	return f.base.Truncate(name, size)
}

// Glob expands filesystem patterns.
func (f *FaultFS) Glob(pattern string) ([]string, error) {
	if err := f.before(OpGlob, pattern); err != nil {
		return nil, err
	}
	return f.base.Glob(pattern)
}

// Hostname returns local hostname.
func (f *FaultFS) Hostname() (string, error) {
	if err := f.before(OpHostname, ""); err != nil {
		return "", err
	}
	return f.base.Hostname()
}
