// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"

	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	"google.golang.org/grpc"
)

// Extension attaches an optional runtime system to the raftstore-backed fsmeta
// assembly. Stable fsmeta and raftstore code only see executor options,
// diagnostics, and shutdown hooks; extension-specific protocols stay outside
// this package.
type Extension interface {
	Attach(context.Context, ExtensionContext) (*ExtensionAttachment, error)
}

// ExtensionContext exposes the narrow runtime assembly surfaces an extension
// may need. The coordinator and KV client are intentionally opaque beyond the
// stable interfaces owned by their packages; an extension may type-assert
// additional experimental capabilities in its own package.
type ExtensionContext struct {
	Coordinator any
	KV          KVClient
	Runner      *Runner
	WatchRouter *fsmetawatch.Router
	DialOptions []grpc.DialOption
}

// ExtensionAttachment is returned by an attached extension.
type ExtensionAttachment struct {
	ExecutorOptions []fsmetaexec.Option
	Stats           []ExtensionStats
	Close           func() error
}

// ExtensionStats describes one expvar-compatible diagnostic surface.
type ExtensionStats struct {
	Name     string
	Snapshot func() map[string]any
}

func closeExtensionAttachments(attachments []*ExtensionAttachment) error {
	var first error
	for i := len(attachments) - 1; i >= 0; i-- {
		attachment := attachments[i]
		if attachment == nil || attachment.Close == nil {
			continue
		}
		if err := attachment.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
