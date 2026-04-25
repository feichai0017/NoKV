package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunEunomiaAuditCmdParseError(t *testing.T) {
	var buf bytes.Buffer
	err := runEunomiaAuditCmd(&buf, []string{"-bad-flag"})
	require.Error(t, err)
}

func TestRunEunomiaAuditCmdRequiresThreeRootPeers(t *testing.T) {
	var buf bytes.Buffer
	err := runEunomiaAuditCmd(&buf, []string{
		"-root-peer", "1=127.0.0.1:2380",
		"-root-peer", "2=127.0.0.1:2381",
	})
	require.ErrorContains(t, err, "requires exactly 3 --root-peer")
}

func TestRunEunomiaAuditCmdInvalidReplyTraceFormat(t *testing.T) {
	targets, _, stop := newReplicatedMetaRoot(t)
	t.Cleanup(stop)

	var buf bytes.Buffer
	args := append([]string{
		"-reply-trace", "-",
		"-reply-trace-format", "bad-format",
	}, rootPeerArgsFromTargets(targets)...)
	err := runEunomiaAuditCmd(&buf, args)
	require.ErrorContains(t, err, "reply-trace-format")
}

func TestRunEunomiaAuditCmdBuildsReportAgainstLiveCluster(t *testing.T) {
	targets, _, stop := newReplicatedMetaRoot(t)
	t.Cleanup(stop)

	var buf bytes.Buffer
	args := append([]string{"-json"}, rootPeerArgsFromTargets(targets)...)
	require.NoError(t, runEunomiaAuditCmd(&buf, args))

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Contains(t, payload, "report")
}

func TestRunEunomiaAuditCmdTextOutput(t *testing.T) {
	targets, _, stop := newReplicatedMetaRoot(t)
	t.Cleanup(stop)

	var buf bytes.Buffer
	require.NoError(t, runEunomiaAuditCmd(&buf, rootPeerArgsFromTargets(targets)))
	out := buf.String()
	require.True(t, strings.Contains(out, "Eunomia audit report"), "output=%q", out)
	require.True(t, strings.Contains(out, "snapshot anomalies"), "output=%q", out)
}
