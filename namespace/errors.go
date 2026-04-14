package namespace

import "errors"

var (
	ErrInvalidLimit    = errors.New("namespace: invalid list limit")
	ErrInvalidPath     = errors.New("namespace: invalid path")
	ErrPathExists      = errors.New("namespace: path already exists")
	ErrPathNotFound    = errors.New("namespace: path not found")
	ErrChildExists     = errors.New("namespace: child already exists")
	ErrChildNotFound   = errors.New("namespace: child not found")
	ErrPageNotFound    = errors.New("namespace: listing page not found")
	ErrParentMismatch  = errors.New("namespace: listing parent mismatch")
	ErrCursorCorrupted = errors.New("namespace: cursor corrupted")
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

func IsChildExists(err error) bool {
	return errors.Is(err, ErrChildExists)
}

func IsChildNotFound(err error) bool {
	return errors.Is(err, ErrChildNotFound)
}

func IsPageNotFound(err error) bool {
	return errors.Is(err, ErrPageNotFound)
}

func IsParentMismatch(err error) bool {
	return errors.Is(err, ErrParentMismatch)
}

func IsCursorCorrupted(err error) bool {
	return errors.Is(err, ErrCursorCorrupted)
}
