package idalloc

import "errors"

// ErrInvalidBatch indicates a requested allocation batch is invalid.
var ErrInvalidBatch = errors.New("coordinator/idalloc: invalid batch")
