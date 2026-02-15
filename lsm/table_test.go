package lsm

import (
	"fmt"
	"strings"
	"testing"
)

// TestTableDecrRefUnderflow verifies that DecrRef panics when the reference count drops below zero.
func TestTableDecrRefUnderflow(t *testing.T) {
	// Initialize a table with refcount 0 to simulate an edge case.
	tbl := &table{
		fid: 1,
		ref: 0,
	}

	// Set up a defer function to catch and validate the expected panic.
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("The code did not panic on refcount underflow")
			return
		}

		// Convert the recovered value to string to check the message.
		msg := fmt.Sprint(r)
		t.Logf("Captured expected panic: %v", msg)

		// Assert that the panic is actually our underflow guard and not something else.
		if !strings.Contains(msg, "underflow") {
			t.Errorf("Unexpected panic message: %v", msg)
		}
	}()

	// Trigger the underflow.
	_ = tbl.DecrRef()
}
