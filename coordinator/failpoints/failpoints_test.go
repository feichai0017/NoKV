// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

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
	if err := InjectAfterSealGrantBeforeReload(); err != nil {
		t.Fatalf("disabled failpoint returned error: %v", err)
	}

	Set(AfterSealGrantBeforeReload)
	if Current() != AfterSealGrantBeforeReload {
		t.Fatalf("Current()=%v, want AfterSealGrantBeforeReload", Current())
	}
	if err := InjectAfterSealGrantBeforeReload(); !errors.Is(err, ErrAfterSealGrantBeforeReload) {
		t.Fatalf("expected grant failpoint error, got %v", err)
	}
}
