package raftstore

import "github.com/feichai0017/NoKV/raftstore/failpoints"

// ReadyFailpointMode is kept for backwards compatibility with legacy tests.
type ReadyFailpointMode = failpoints.Mode

const (
	ReadyFailpointNone          = failpoints.None
	ReadyFailpointBeforeStorage = failpoints.BeforeStorage
	ReadyFailpointSkipManifest  = failpoints.SkipManifest
)

// SetReadyFailpoint is part of the exported package API.
func SetReadyFailpoint(mode ReadyFailpointMode) {
	failpoints.Set(mode)
}

// ShouldInjectFailure is preserved for older call sites.
func ShouldInjectFailure() bool {
	return failpoints.ShouldFailBeforeStorage()
}
