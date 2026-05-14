// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"os"
)

// ensureCleanDir removes the supplied directory (if present) and recreates it.
// The helper keeps engine implementations simple when they need a clean workdir.
func ensureCleanDir(dir string) error {
	_ = os.RemoveAll(dir)
	return os.MkdirAll(dir, 0o755)
}
