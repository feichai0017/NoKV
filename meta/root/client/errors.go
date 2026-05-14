// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	// errEmptyTarget indicates that a single remote root target was blank.
	errEmptyTarget = nokverrors.New(nokverrors.KindInvalidArgument, "meta/root/client: empty target")
	// errEmptyTargetSet indicates that no remote root endpoints were configured.
	errEmptyTargetSet = nokverrors.New(nokverrors.KindInvalidArgument, "meta/root/client: empty target set")
	// errNilClient indicates that a metadata-root client was nil.
	errNilClient = nokverrors.New(nokverrors.KindInvalidArgument, "meta/root/client: nil client")
	// errNoEndpoints indicates that the metadata-root client has no dialed endpoints.
	errNoEndpoints = nokverrors.New(nokverrors.KindInvalidArgument, "meta/root/client: no endpoints")
	// errNoReachableEndpoint indicates that no configured metadata-root endpoint responded.
	errNoReachableEndpoint = nokverrors.New(nokverrors.KindUnavailable, "meta/root/client: no reachable endpoint")
)
