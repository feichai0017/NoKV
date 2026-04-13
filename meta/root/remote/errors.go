package remote

import "errors"

var (
	// errEmptyTarget indicates that a single remote root target was blank.
	errEmptyTarget = errors.New("meta/root/remote: empty target")
	// errEmptyTargetSet indicates that no remote root endpoints were configured.
	errEmptyTargetSet = errors.New("meta/root/remote: empty target set")
	// errNilClient indicates that a remote metadata-root client was nil.
	errNilClient = errors.New("meta/root/remote: nil client")
	// errNoEndpoints indicates that the remote metadata-root client has no dialed endpoints.
	errNoEndpoints = errors.New("meta/root/remote: no endpoints")
	// errNoReachableEndpoint indicates that no configured metadata-root endpoint responded.
	errNoReachableEndpoint = errors.New("meta/root/remote: no reachable endpoint")
)
