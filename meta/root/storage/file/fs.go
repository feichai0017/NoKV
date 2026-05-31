// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"fmt"
	"os"
)

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir %s: %w", dir, err)
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return fmt.Errorf("sync dir %s: %w", dir, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close dir %s: %w", dir, closeErr)
	}
	return nil
}
