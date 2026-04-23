package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunSuccessionAuditCmdParseError(t *testing.T) {
	var buf bytes.Buffer
	err := runSuccessionAuditCmd(&buf, []string{"-bad-flag"})
	require.Error(t, err)
}

func TestRunSuccessionAuditCmdRequiresThreeRootPeers(t *testing.T) {
	var buf bytes.Buffer
	err := runSuccessionAuditCmd(&buf, []string{
		"-root-peer", "1=127.0.0.1:2380",
		"-root-peer", "2=127.0.0.1:2381",
	})
	require.ErrorContains(t, err, "requires exactly 3 --root-peer")
}

func TestRunSuccessionAuditCmdInvalidReplyTraceFormat(t *testing.T) {
	targets, _, stop := newReplicatedMetaRoot(t)
	t.Cleanup(stop)

	var buf bytes.Buffer
	args := append([]string{
		"-reply-trace", "-",
		"-reply-trace-format", "bad-format",
	}, rootPeerArgsFromTargets(targets)...)
	err := runSuccessionAuditCmd(&buf, args)
	require.ErrorContains(t, err, "reply-trace-format")
}

func TestRunSuccessionAuditCmdBuildsReportAgainstLiveCluster(t *testing.T) {
	targets, _, stop := newReplicatedMetaRoot(t)
	t.Cleanup(stop)

	var buf bytes.Buffer
	args := append([]string{"-json"}, rootPeerArgsFromTargets(targets)...)
	require.NoError(t, runSuccessionAuditCmd(&buf, args))

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Contains(t, payload, "report")
}

func TestRunSuccessionAuditCmdTextOutput(t *testing.T) {
	targets, _, stop := newReplicatedMetaRoot(t)
	t.Cleanup(stop)

	var buf bytes.Buffer
	require.NoError(t, runSuccessionAuditCmd(&buf, rootPeerArgsFromTargets(targets)))
	out := buf.String()
	require.True(t, strings.Contains(out, "Succession audit report"), "output=%q", out)
	require.True(t, strings.Contains(out, "snapshot anomalies"), "output=%q", out)
}
