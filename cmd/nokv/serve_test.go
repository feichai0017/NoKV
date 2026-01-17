package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/feichai0017/NoKV/manifest"
)

func TestRunServeCmdMissingFlags(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"missing workdir", []string{"--store-id", "1"}},
		{"missing store", []string{"--workdir", t.TempDir()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := runServeCmd(&buf, tc.args)
			if err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestRunServeCmdInvalidPeer(t *testing.T) {
	dir := t.TempDir()
	var buf bytes.Buffer
	err := runServeCmd(&buf, []string{"--workdir", dir, "--store-id", "1", "--peer", "bad"})
	if err == nil || !strings.Contains(err.Error(), "storeID") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestServeHelpers(t *testing.T) {
	if got := formatKey(nil, true); got != "-inf" {
		t.Fatalf("expected -inf, got %q", got)
	}
	if got := formatKey(nil, false); got != "+inf" {
		t.Fatalf("expected +inf, got %q", got)
	}
	if got := formatKey([]byte("a"), true); got != "\"a\"" {
		t.Fatalf("expected quoted key, got %q", got)
	}

	meta := manifest.RegionMeta{
		Peers: []manifest.PeerMeta{
			{StoreID: 1, PeerID: 10},
			{StoreID: 2, PeerID: 20},
		},
	}
	if got := peerIDForStore(meta, 2); got != 20 {
		t.Fatalf("expected peer id 20, got %d", got)
	}
	if got := peerIDForStore(meta, 3); got != 0 {
		t.Fatalf("expected peer id 0, got %d", got)
	}

	if got, err := parseUint(" 42 "); err != nil || got != 42 {
		t.Fatalf("expected parseUint to return 42, got %d err=%v", got, err)
	}
	if _, err := parseUint("bad"); err == nil {
		t.Fatalf("expected parseUint error for bad input")
	}
}

func TestStartStorePeersNilArgs(t *testing.T) {
	if _, _, err := startStorePeers(nil, nil, 1, 1, 1, 1, 1); err == nil {
		t.Fatalf("expected error for nil server/db")
	}
}
