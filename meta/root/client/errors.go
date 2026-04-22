package client

import "errors"

var (
	// errEmptyTarget indicates that a single remote root target was blank.
	errEmptyTarget = errors.New("meta/root/client: empty target")
	// errEmptyTargetSet indicates that no remote root endpoints were configured.
	errEmptyTargetSet = errors.New("meta/root/client: empty target set")
	// errNilClient indicates that a metadata-root client was nil.
	errNilClient = errors.New("meta/root/client: nil client")
	// errNoEndpoints indicates that the metadata-root client has no dialed endpoints.
	errNoEndpoints = errors.New("meta/root/client: no endpoints")
	// errNoReachableEndpoint indicates that no configured metadata-root endpoint responded.
	errNoReachableEndpoint = errors.New("meta/root/client: no reachable endpoint")
)
