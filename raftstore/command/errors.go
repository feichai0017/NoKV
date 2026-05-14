// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"errors"
	"fmt"
)

var (
	errNilRaftCommand          = errors.New("raftstore: nil raft command")
	errRaftCommandSizeOverflow = errors.New("raftstore: raft command size causes overflow")
)

func errRaftCommandTooLarge(size int) error {
	return fmt.Errorf("raftstore: raft command too large (%d bytes)", size)
}
