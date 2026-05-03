package failpoints

import (
	"errors"
	"testing"
)

func TestCoordinatorFailpointMask(t *testing.T) {
	t.Cleanup(func() { Set(None) })

	Set(None)
	if Current() != None {
		t.Fatalf("Current()=%v, want None", Current())
	}
	if err := InjectAfterApplyHandoverBeforeReload(); err != nil {
		t.Fatalf("disabled failpoint returned error: %v", err)
	}

	Set(AfterApplyHandoverBeforeReload)
	if Current() != AfterApplyHandoverBeforeReload {
		t.Fatalf("Current()=%v, want AfterApplyHandoverBeforeReload", Current())
	}
	if err := InjectAfterApplyHandoverBeforeReload(); !errors.Is(err, ErrAfterApplyHandoverBeforeReload) {
		t.Fatalf("expected handover failpoint error, got %v", err)
	}
}
