package failpoints

import (
	"errors"
	"sync/atomic"
)

// Mode configures coordinator service-layer failure injection hooks. Modes can
// be ORed together to simulate multiple cut points.
type Mode uint32

const (
	None Mode = 0

	// AfterApplyTransitBeforeReload simulates a coordinator crash
	// after rooted closure truth has already committed but before the local
	// service has reloaded/fenced its in-memory view.
	AfterApplyTransitBeforeReload Mode = 1 << iota
)

var (
	currentMode atomic.Uint32

	ErrAfterApplyTransitBeforeReload = errors.New("coordinator failpoint: after apply coordinator closure before reload")
)

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

// InjectAfterApplyTransitBeforeReload returns the configured
// injected failure for the rooted-commit-before-local-reload cut.
func InjectAfterApplyTransitBeforeReload() error {
	if enabled(AfterApplyTransitBeforeReload) {
		return ErrAfterApplyTransitBeforeReload
	}
	return nil
}
