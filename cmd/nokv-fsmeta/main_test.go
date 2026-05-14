// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"expvar"
	"fmt"
	"testing"
)

func TestPublishExpvarOnceDoesNotOverwriteExistingMetric(t *testing.T) {
	name := fmt.Sprintf("nokv_fsmeta_test_%s", t.Name())
	first := expvar.Func(func() any { return "first" })
	second := expvar.Func(func() any { return "second" })

	publishExpvarOnce(name, first)
	publishExpvarOnce(name, second)

	got := expvar.Get(name)
	if got == nil {
		t.Fatal("expected expvar metric to be published")
	}
	if got.String() != `"first"` {
		t.Fatalf("metric was overwritten, got %s", got.String())
	}
}
