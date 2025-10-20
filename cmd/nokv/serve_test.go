package main

import (
    "bytes"
    "strings"
    "testing"
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
