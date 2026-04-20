package storage

import "errors"

var (
	errCoordinatorLeaseCommandUnsupported   = errors.New("coordinator/storage: coordinator lease command unsupported")
	errCoordinatorClosureCommandUnsupported = errors.New("coordinator/storage: coordinator closure command unsupported")
)
