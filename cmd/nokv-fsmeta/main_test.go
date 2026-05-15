// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"expvar"
	"fmt"
	"testing"

	"github.com/feichai0017/NoKV/engine/wal"
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

func TestParsePerasVisibleLogPolicy(t *testing.T) {
	cases := map[string]wal.DurabilityPolicy{
		"":              wal.DurabilityFlushed,
		"flushed":       wal.DurabilityFlushed,
		"fsync-batched": wal.DurabilityFsyncBatched,
		"fsync_batched": wal.DurabilityFsyncBatched,
		"batched":       wal.DurabilityFsyncBatched,
		"fsync":         wal.DurabilityFsync,
		"buffered":      wal.DurabilityBuffered,
	}
	for input, want := range cases {
		got, err := parsePerasVisibleLogPolicy(input)
		if err != nil {
			t.Fatalf("parse %q: %v", input, err)
		}
		if got != want {
			t.Fatalf("parse %q got %v want %v", input, got, want)
		}
	}
	if _, err := parsePerasVisibleLogPolicy("bad"); err == nil {
		t.Fatal("expected invalid policy to fail")
	}
}
