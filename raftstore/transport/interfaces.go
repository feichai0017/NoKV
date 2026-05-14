// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package transport

import "context"
import myraft "github.com/feichai0017/NoKV/raft"

// Transport abstracts the medium used to forward raft messages between peers.
type Transport interface {
	Send(ctx context.Context, msg myraft.Message)
}
