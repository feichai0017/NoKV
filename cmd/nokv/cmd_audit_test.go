// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunAuditCmdRejectsMissingPeers(t *testing.T) {
	var buf bytes.Buffer
	err := runAuditCmd(&buf, []string{})
	if err == nil {
		t.Fatal("expected error when no --root-peer values are supplied")
	}
	if !strings.Contains(err.Error(), "exactly 3 --root-peer") {
		t.Fatalf("expected peer count error, got %v", err)
	}
}

func TestRunAuditCmdRejectsInvalidPeer(t *testing.T) {
	var buf bytes.Buffer
	err := runAuditCmd(&buf, []string{
		"--root-peer", "broken",
	})
	if err == nil {
		t.Fatal("expected error for malformed peer value")
	}
	if !strings.Contains(err.Error(), "invalid peer value") {
		t.Fatalf("expected parse error, got %v", err)
	}
}

func TestRunAuditCmdRejectsWrongPeerCount(t *testing.T) {
	var buf bytes.Buffer
	err := runAuditCmd(&buf, []string{
		"--root-peer", "1=127.0.0.1:1",
		"--root-peer", "2=127.0.0.1:2",
	})
	if err == nil {
		t.Fatal("expected error when fewer than 3 peers are supplied")
	}
	if !strings.Contains(err.Error(), "exactly 3 --root-peer") {
		t.Fatalf("expected peer count error, got %v", err)
	}
}
