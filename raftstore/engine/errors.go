package engine

import "errors"

var (
	// errStopPointerValidation stops WAL pointer validation once the current pointer was confirmed.
	errStopPointerValidation = errors.New("raftstore: stop pointer validation")
)
