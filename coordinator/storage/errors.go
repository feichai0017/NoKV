package storage

import "errors"

var (
	// errCoordinatorLeaseUnsupported indicates that the underlying rooted backend
	// does not implement coordinator lease campaign.
	errCoordinatorLeaseUnsupported = errors.New("coordinator/storage: coordinator lease campaign unsupported")
	// errCoordinatorLeaseReleaseUnsupported indicates that the underlying rooted
	// backend does not implement explicit coordinator lease release.
	errCoordinatorLeaseReleaseUnsupported = errors.New("coordinator/storage: coordinator lease release unsupported")
)
