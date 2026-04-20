package main

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	pdstorage "github.com/feichai0017/NoKV/coordinator/storage"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestRunCCCAuditCmdJSON(t *testing.T) {
	dir := seedCCCAuditWorkdir(t, cccAuditSeedOptions{confirm: true, close: true, reattach: true})

	var buf bytes.Buffer
	err := runCCCAuditCmd(&buf, []string{
		"-workdir", dir,
		"-holder", "c1",
		"-now-unix-nano", "600",
		"-json",
	})
	require.NoError(t, err)

	var payload struct {
		Report coordaudit.Report `json:"report"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, "c1", payload.Report.HolderID)
	require.Equal(t, uint64(2), payload.Report.CurrentGeneration)
	require.True(t, payload.Report.ClosureWitness.ClosureSatisfied())
	require.Equal(t, rootstate.CoordinatorClosureStageReattached, payload.Report.Closure.Stage)
	require.False(t, payload.Report.Anomalies.ClosureIncomplete)
}

func TestRunCCCAuditCmdPlainShowsAnomalies(t *testing.T) {
	dir := seedCCCAuditWorkdir(t, cccAuditSeedOptions{})

	var buf bytes.Buffer
	err := runCCCAuditCmd(&buf, []string{
		"-workdir", dir,
		"-holder", "c1",
		"-now-unix-nano", "600",
	})
	require.NoError(t, err)

	out := buf.String()
	require.Contains(t, out, "ClosureSatisfied         true")
	require.Contains(t, out, "ClosureStage             pending_confirm")
	require.Contains(t, out, "Anomalies                missing_confirm")
}

func TestRunCCCAuditCmdReplyTraceJSON(t *testing.T) {
	dir := seedCCCAuditWorkdir(t, cccAuditSeedOptions{confirm: true, close: true, reattach: true})
	tracePath := writeCCCAuditReplyTrace(t, []coordaudit.ReplyTraceRecord{
		{Duty: "tso", CertGeneration: 1, Accepted: true},
		{Duty: "tso", CertGeneration: 2, Accepted: true},
		{Duty: "tso", CertGeneration: 2, Accepted: false},
	})

	var buf bytes.Buffer
	err := runCCCAuditCmd(&buf, []string{
		"-workdir", dir,
		"-holder", "c1",
		"-now-unix-nano", "600",
		"-reply-trace", tracePath,
		"-json",
	})
	require.NoError(t, err)

	var payload struct {
		ReplyTraceRecords   int                            `json:"reply_trace_records"`
		ReplyTraceAnomalies []coordaudit.ReplyTraceAnomaly `json:"reply_trace_anomalies"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, 3, payload.ReplyTraceRecords)
	require.Len(t, payload.ReplyTraceAnomalies, 1)
	require.Equal(t, "post_seal_accepted_reply", payload.ReplyTraceAnomalies[0].Kind)
	require.Equal(t, uint64(1), payload.ReplyTraceAnomalies[0].CertGeneration)
}

func TestRunCCCAuditCmdReplyTracePlain(t *testing.T) {
	dir := seedCCCAuditWorkdir(t, cccAuditSeedOptions{confirm: true, close: true, reattach: true})
	tracePath := writeCCCAuditReplyTraceEnvelope(t, []coordaudit.ReplyTraceRecord{
		{Duty: "allocid", CertGeneration: 1, Accepted: true},
	})

	var buf bytes.Buffer
	err := runCCCAuditCmd(&buf, []string{
		"-workdir", dir,
		"-holder", "c1",
		"-now-unix-nano", "600",
		"-reply-trace", tracePath,
	})
	require.NoError(t, err)

	out := buf.String()
	require.Contains(t, out, "ReplyTraceRecords        1")
	require.Contains(t, out, "ReplyTraceAnomalies      post_seal_accepted_reply[allocid]")
}

func TestRunCCCAuditCmdEtcdReplyTraceWithoutWorkdir(t *testing.T) {
	tracePath := writeCCCAuditRawJSON(t, []map[string]any{
		{
			"member_id":             "1",
			"read_state_generation": 1,
			"successor_generation":  2,
			"accepted":              true,
		},
	})

	var buf bytes.Buffer
	err := runCCCAuditCmd(&buf, []string{
		"-reply-trace", tracePath,
		"-reply-trace-format", "etcd-read-index",
		"-json",
	})
	require.NoError(t, err)

	var payload struct {
		ReplyTraceRecords   int                            `json:"reply_trace_records"`
		ReplyTraceAnomalies []coordaudit.ReplyTraceAnomaly `json:"reply_trace_anomalies"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, 1, payload.ReplyTraceRecords)
	require.Len(t, payload.ReplyTraceAnomalies, 1)
	require.Equal(t, "accepted_read_index_behind_successor", payload.ReplyTraceAnomalies[0].Kind)
	require.Equal(t, uint64(1), payload.ReplyTraceAnomalies[0].CertGeneration)
}

func TestRunCCCAuditCmdEtcdReplyTraceFromStdinWithoutWorkdir(t *testing.T) {
	data, err := json.Marshal([]map[string]any{
		{
			"member_id":             "1",
			"read_state_generation": 1,
			"successor_generation":  2,
			"accepted":              true,
		},
	})
	require.NoError(t, err)

	restore := swapStdin(t, data)
	defer restore()

	var buf bytes.Buffer
	err = runCCCAuditCmd(&buf, []string{
		"-reply-trace", "-",
		"-reply-trace-format", "etcd-read-index",
		"-json",
	})
	require.NoError(t, err)

	var payload struct {
		ReplyTraceRecords   int                            `json:"reply_trace_records"`
		ReplyTraceAnomalies []coordaudit.ReplyTraceAnomaly `json:"reply_trace_anomalies"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, 1, payload.ReplyTraceRecords)
	require.Len(t, payload.ReplyTraceAnomalies, 1)
	require.Equal(t, "accepted_read_index_behind_successor", payload.ReplyTraceAnomalies[0].Kind)
	require.Equal(t, uint64(1), payload.ReplyTraceAnomalies[0].CertGeneration)
}

func TestRunCCCAuditCmdEtcdLeaseRenewTraceWithoutWorkdir(t *testing.T) {
	tracePath := writeCCCAuditRawJSON(t, []map[string]any{
		{
			"member_id":         "1",
			"response_revision": 7,
			"revoke_revision":   8,
			"accepted":          true,
		},
	})

	var buf bytes.Buffer
	err := runCCCAuditCmd(&buf, []string{
		"-reply-trace", tracePath,
		"-reply-trace-format", "etcd-lease-renew",
		"-json",
	})
	require.NoError(t, err)

	var payload struct {
		ReplyTraceRecords   int                            `json:"reply_trace_records"`
		ReplyTraceAnomalies []coordaudit.ReplyTraceAnomaly `json:"reply_trace_anomalies"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, 1, payload.ReplyTraceRecords)
	require.Len(t, payload.ReplyTraceAnomalies, 1)
	require.Equal(t, "accepted_keepalive_success_after_revoke", payload.ReplyTraceAnomalies[0].Kind)
	require.Equal(t, "lease_renew", payload.ReplyTraceAnomalies[0].Duty)
	require.Equal(t, uint64(7), payload.ReplyTraceAnomalies[0].CertGeneration)
}

func TestRunCCCAuditCmdCRDBLeaseStartTraceWithoutWorkdir(t *testing.T) {
	tracePath := writeCCCAuditRawJSON(t, []map[string]any{
		{
			"key":                   "k",
			"successor_lease_start": 103,
			"served_timestamp":      105,
			"accepted":              true,
		},
	})

	var buf bytes.Buffer
	err := runCCCAuditCmd(&buf, []string{
		"-reply-trace", tracePath,
		"-reply-trace-format", "crdb-lease-start",
		"-json",
	})
	require.NoError(t, err)

	var payload struct {
		ReplyTraceRecords   int                            `json:"reply_trace_records"`
		ReplyTraceAnomalies []coordaudit.ReplyTraceAnomaly `json:"reply_trace_anomalies"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, 1, payload.ReplyTraceRecords)
	require.Len(t, payload.ReplyTraceAnomalies, 1)
	require.Equal(t, "lease_start_coverage_violation", payload.ReplyTraceAnomalies[0].Kind)
	require.Equal(t, "lease_start_coverage", payload.ReplyTraceAnomalies[0].Duty)
	require.Equal(t, uint64(103), payload.ReplyTraceAnomalies[0].CertGeneration)
}

type cccAuditSeedOptions struct {
	confirm  bool
	close    bool
	reattach bool
}

func applyCCCAuditLeaseIssue(t *testing.T, store *pdstorage.RootStore, holderID string, expiresUnixNano, nowUnixNano int64, handoffFrontiers rootstate.CoordinatorDutyFrontiers, predecessorDigest string) rootstate.CoordinatorLease {
	t.Helper()
	state, err := store.ApplyCoordinatorLease(rootstate.CoordinatorLeaseCommand{
		Kind:              rootstate.CoordinatorLeaseCommandIssue,
		HolderID:          holderID,
		ExpiresUnixNano:   expiresUnixNano,
		NowUnixNano:       nowUnixNano,
		PredecessorDigest: predecessorDigest,
		HandoffFrontiers:  handoffFrontiers,
	})
	require.NoError(t, err)
	return state.Lease
}

func applyCCCAuditClosure(t *testing.T, store *pdstorage.RootStore, kind rootstate.CoordinatorClosureCommandKind, holderID string, nowUnixNano int64, frontiers rootstate.CoordinatorDutyFrontiers) rootstate.CoordinatorClosure {
	t.Helper()
	state, err := store.ApplyCoordinatorClosure(rootstate.CoordinatorClosureCommand{
		Kind:        kind,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
		Frontiers:   frontiers,
	})
	require.NoError(t, err)
	return state.Closure
}

func applyCCCAuditSeal(t *testing.T, store *pdstorage.RootStore, holderID string, nowUnixNano int64, frontiers rootstate.CoordinatorDutyFrontiers) rootstate.CoordinatorSeal {
	t.Helper()
	state, err := store.ApplyCoordinatorClosure(rootstate.CoordinatorClosureCommand{
		Kind:        rootstate.CoordinatorClosureCommandSeal,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
		Frontiers:   frontiers,
	})
	require.NoError(t, err)
	return state.Seal
}

func seedCCCAuditWorkdir(t *testing.T, opts cccAuditSeedOptions) string {
	t.Helper()

	dir := t.TempDir()
	store, err := pdstorage.OpenRootLocalStore(dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	require.NoError(t, store.AppendRootEvent(rootevent.RegionBootstrapped(testDescriptor(10, []byte("a"), []byte("z"), metaregion.Epoch{
		Version:     1,
		ConfVersion: 1,
	}))))

	lease := applyCCCAuditLeaseIssue(t, store, "c1", 1_000, 100, controlplane.Frontiers(12, 34, 1), "")
	require.Equal(t, uint64(1), lease.CertGeneration)

	seal := applyCCCAuditSeal(t, store, "c1", 200, controlplane.Frontiers(12, 34, 1))
	sealDigest := rootstate.CoordinatorSealDigest(seal)

	lease = applyCCCAuditLeaseIssue(t, store, "c1", 1_300, 300, controlplane.Frontiers(12, 34, 1), sealDigest)
	require.Equal(t, uint64(2), lease.CertGeneration)

	if opts.confirm {
		applyCCCAuditClosure(t, store, rootstate.CoordinatorClosureCommandConfirm, "c1", 400, rootstate.CoordinatorDutyFrontiers{})
	}
	if opts.close {
		applyCCCAuditClosure(t, store, rootstate.CoordinatorClosureCommandClose, "c1", 450, rootstate.CoordinatorDutyFrontiers{})
	}
	if opts.reattach {
		applyCCCAuditClosure(t, store, rootstate.CoordinatorClosureCommandReattach, "c1", 500, rootstate.CoordinatorDutyFrontiers{})
	}
	return dir
}

func writeCCCAuditReplyTrace(t *testing.T, records []coordaudit.ReplyTraceRecord) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/reply-trace.json"
	data, err := json.Marshal(records)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	return path
}

func writeCCCAuditReplyTraceEnvelope(t *testing.T, records []coordaudit.ReplyTraceRecord) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/reply-trace-envelope.json"
	data, err := json.Marshal(struct {
		Records []coordaudit.ReplyTraceRecord `json:"records"`
	}{Records: records})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	return path
}

func writeCCCAuditRawJSON(t *testing.T, payload any) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/raw.json"
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	return path
}

func swapStdin(t *testing.T, data []byte) func() {
	t.Helper()
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	_, err = writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	original := os.Stdin
	os.Stdin = reader
	return func() {
		os.Stdin = original
		require.NoError(t, reader.Close())
	}
}
