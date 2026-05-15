// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package failpoints

import "sync/atomic"

// Mode configures coordinator service-layer failure injection hooks. Modes can
// be ORed together to simulate multiple cut points.
type Mode uint32

const (
	None Mode = 0

	// AfterSealGrantBeforeReload simulates a coordinator crash
	// after rooted grant retirement truth has already committed but before the local
	// service has reloaded/fenced its in-memory view.
	AfterSealGrantBeforeReload Mode = 1 << iota
)

var currentMode atomic.Uint32

// Set installs the active coordinator failpoint mask.
func Set(mode Mode) {
	currentMode.Store(uint32(mode))
}

// Current returns the currently active coordinator failpoint mask.
func Current() Mode {
	return Mode(currentMode.Load())
}

func enabled(mode Mode) bool {
	return Current()&mode != 0
}

// InjectAfterSealGrantBeforeReload returns the configured
// injected failure for the rooted-commit-before-local-reload cut.
func InjectAfterSealGrantBeforeReload() error {
	if enabled(AfterSealGrantBeforeReload) {
		return ErrAfterSealGrantBeforeReload
	}
	return nil
}
