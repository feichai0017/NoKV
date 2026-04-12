package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
)

func TestRunMetaRootCmdStartsAndStopsLocal(t *testing.T) {
	origNotify := metaRootNotifyContext
	metaRootNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { metaRootNotifyContext = origNotify })

	var buf bytes.Buffer
	if err := runMetaRootCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-mode", "local",
		"-workdir", t.TempDir(),
	}); err != nil {
		t.Fatalf("runMetaRootCmd(local): %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Metadata root service listening on") {
		t.Fatalf("expected listen line, got %q", out)
	}
	if !strings.Contains(out, "Metadata root mode: local") {
		t.Fatalf("expected local mode line, got %q", out)
	}
}

func TestRunMetaRootCmdStartsAndStopsReplicated(t *testing.T) {
	origNotify := metaRootNotifyContext
	metaRootNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { metaRootNotifyContext = origNotify })

	var buf bytes.Buffer
	if err := runMetaRootCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-mode", "replicated",
		"-workdir", t.TempDir(),
		"-node-id", "1",
		"-transport-addr", "127.0.0.1:0",
		"-peer", "1=127.0.0.1:7001",
		"-peer", "2=127.0.0.1:7002",
		"-peer", "3=127.0.0.1:7003",
	}); err != nil {
		t.Fatalf("runMetaRootCmd(replicated): %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Metadata root service listening on") {
		t.Fatalf("expected listen line, got %q", out)
	}
	if !strings.Contains(out, "Metadata root mode: replicated") {
		t.Fatalf("expected replicated mode line, got %q", out)
	}
}

func TestRunMetaRootCmdRejectsInvalidMode(t *testing.T) {
	var buf bytes.Buffer
	err := runMetaRootCmd(&buf, []string{"-mode", "bad"})
	if err == nil || !strings.Contains(err.Error(), "invalid meta-root mode") {
		t.Fatalf("expected invalid mode error, got %v", err)
	}
}
