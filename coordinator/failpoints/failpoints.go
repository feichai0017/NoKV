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

	// AfterApplyHandoverBeforeReload simulates a coordinator crash
	// after rooted handover truth has already committed but before the local
	// service has reloaded/fenced its in-memory view.
	AfterApplyHandoverBeforeReload Mode = 1 << iota
)

var (
	currentMode atomic.Uint32

	ErrAfterApplyHandoverBeforeReload = errors.New("coordinator failpoint: after apply coordinator handover before reload")
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

// InjectAfterApplyHandoverBeforeReload returns the configured
// injected failure for the rooted-commit-before-local-reload cut.
func InjectAfterApplyHandoverBeforeReload() error {
	if enabled(AfterApplyHandoverBeforeReload) {
		return ErrAfterApplyHandoverBeforeReload
	}
	return nil
}
