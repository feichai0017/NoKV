package namespace

import "errors"

var (
	ErrInvalidLimit       = errors.New("namespace: invalid list limit")
	ErrInvalidPath        = errors.New("namespace: invalid path")
	ErrPathExists         = errors.New("namespace: path already exists")
	ErrPathNotFound       = errors.New("namespace: path not found")
	ErrParentNotFound     = errors.New("namespace: parent path not found")
	ErrParentNotDir       = errors.New("namespace: parent path is not a directory")
	ErrParentMismatch     = errors.New("namespace: listing parent mismatch")
	ErrCodecCorrupted     = errors.New("namespace: codec payload corrupted")
	ErrCursorCorrupted    = errors.New("namespace: cursor corrupted")
	ErrRebuildRequired    = errors.New("namespace: rebuild required")
	ErrCoverageIncomplete = errors.New("namespace: coverage incomplete")
)

func IsInvalidLimit(err error) bool {
	return errors.Is(err, ErrInvalidLimit)
}

func IsInvalidPath(err error) bool {
	return errors.Is(err, ErrInvalidPath)
}

func IsPathExists(err error) bool {
	return errors.Is(err, ErrPathExists)
}

func IsPathNotFound(err error) bool {
	return errors.Is(err, ErrPathNotFound)
}

func IsParentNotFound(err error) bool {
	return errors.Is(err, ErrParentNotFound)
}

func IsParentNotDir(err error) bool {
	return errors.Is(err, ErrParentNotDir)
}

func IsParentMismatch(err error) bool {
	return errors.Is(err, ErrParentMismatch)
}

func IsCodecCorrupted(err error) bool {
	return errors.Is(err, ErrCodecCorrupted)
}

func IsCursorCorrupted(err error) bool {
	return errors.Is(err, ErrCursorCorrupted)
}

func IsRebuildRequired(err error) bool {
	return errors.Is(err, ErrRebuildRequired)
}

func IsCoverageIncomplete(err error) bool {
	return errors.Is(err, ErrCoverageIncomplete)
}
