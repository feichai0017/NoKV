// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestStartExpvarServerExposesPprof(t *testing.T) {
	ln, err := StartExpvarServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("StartExpvarServer: %v", err)
	}
	defer func() { _ = ln.Close() }()

	baseURL := "http://" + ln.Addr().String()
	resp, err := http.Get(baseURL + "/debug/pprof/goroutine?debug=1")
	if err != nil {
		t.Fatalf("GET pprof goroutine: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pprof goroutine status = %s", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read pprof goroutine: %v", err)
	}
	if !strings.Contains(string(body), "goroutine profile:") {
		t.Fatalf("pprof goroutine response missing profile header: %q", string(body))
	}
}
