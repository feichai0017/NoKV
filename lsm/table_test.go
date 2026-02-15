package lsm

import (
	"fmt"
	"testing"
)

func TestTableDecrRefUnderflow(t *testing.T) {
	// Initialize a table with refcount 0 to simulate an edge case.
	// We use fid 1 for identification in the panic message.
	tbl := &table{
		fid: 1,
		ref: 0,
	}

	// Set up a defer function to catch the expected panic.
	// In database kernels, an underflow is a critical logical error
	// that must trigger a panic to prevent data corruption.
	defer func() {
		if r := recover(); r == nil {
			// If no panic occurs, the underflow guard is failing.
			t.Errorf("The code did not panic on refcount underflow")
		} else {
			// Log the captured panic message for visual verification.
			fmt.Printf("Successfully captured expected panic: %v\n", r)
		}
	}()

	// This call should trigger the underflow guard (0 -> -1).
	// The panic will prevent the execution of t.Delete(),
	// thus avoiding potential nil pointer dereferences in this test.
	_ = tbl.DecrRef()
}
