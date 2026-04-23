package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	"github.com/feichai0017/NoKV/coordinator/rootview"
)

// runSuccessionAuditCmd runs one read-only succession audit pass
// against a live 3-peer meta-root cluster. It connects via the same remote
// gRPC client coordinators use (coordinator/rootview.OpenRootRemoteStore),
// loads the rooted snapshot, and projects it into coordinator/audit's
// SnapshotAnomalies / FinalityDefect vocabulary.
//
// Inputs the tool accepts:
//   - --root-peer nodeID=addr (repeatable, exactly 3): meta-root gRPC endpoints
//   - --holder: explicit holder id for the audit; defaults to snapshot's
//     current lease holder
//   - --now-unix-nano: audit timestamp; defaults to current time
//   - --reply-trace: optional path to reply-trace JSON; "-" reads stdin
//   - --reply-trace-format: projection vocabulary (nokv/etcd-read-index/
//     etcd-lease-renew/crdb-lease-start)
//   - --json: emit JSON instead of human-readable text
func runSuccessionAuditCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("succession-audit", flag.ContinueOnError)
	holder := fs.String("holder", "", "override holder id used for audit reattach checks")
	nowUnixNano := fs.Int64("now-unix-nano", 0, "override audit timestamp (unix nano); defaults to current time")
	replyTracePath := fs.String("reply-trace", "", "path to reply-trace JSON (\"-\" for stdin); optional")
	replyTraceFormat := fs.String("reply-trace-format", "nokv", "reply-trace projection: nokv|etcd-read-index|etcd-lease-renew|crdb-lease-start")
	asJSON := fs.Bool("json", false, "emit JSON output instead of human-readable text")
	var rootPeerFlags []string
	fs.Func("root-peer", "meta-root gRPC peer mapping in the form nodeID=address (repeatable, exactly 3)", func(value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("root-peer value cannot be empty")
		}
		rootPeerFlags = append(rootPeerFlags, value)
		return nil
	})
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	peers, err := parseReplicatedRootPeers(rootPeerFlags)
	if err != nil {
		return err
	}
	if len(peers) != 3 {
		return fmt.Errorf("succession-audit requires exactly 3 --root-peer values")
	}

	rootStore, err := rootview.OpenRootRemoteStore(rootview.RemoteRootConfig{
		Targets: peers,
	})
	if err != nil {
		return fmt.Errorf("succession-audit open remote metadata root: %w", err)
	}
	defer func() { _ = rootStore.Close() }()

	snapshot, err := rootStore.Load()
	if err != nil {
		return fmt.Errorf("succession-audit load rooted snapshot: %w", err)
	}

	holderID := strings.TrimSpace(*holder)
	if holderID == "" {
		holderID = snapshot.Tenure.HolderID
	}
	auditTime := *nowUnixNano
	if auditTime == 0 {
		auditTime = time.Now().UnixNano()
	}

	report := coordaudit.BuildReport(snapshot, holderID, auditTime)

	var traceAnomalies []coordaudit.ReplyTraceAnomaly
	if strings.TrimSpace(*replyTracePath) != "" {
		format, ferr := coordaudit.ParseReplyTraceFormat(*replyTraceFormat)
		if ferr != nil {
			return fmt.Errorf("succession-audit reply-trace-format: %w", ferr)
		}
		data, rerr := readReplyTrace(*replyTracePath)
		if rerr != nil {
			return fmt.Errorf("succession-audit read reply-trace: %w", rerr)
		}
		records, derr := coordaudit.DecodeReplyTrace(data, format)
		if derr != nil {
			return fmt.Errorf("succession-audit decode reply-trace: %w", derr)
		}
		traceAnomalies = coordaudit.EvaluateReplyTrace(report, records)
	}

	if *asJSON {
		return renderSuccessionAuditJSON(w, report, traceAnomalies)
	}
	return renderSuccessionAuditText(w, report, traceAnomalies)
}

func readReplyTrace(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

type successionAuditJSON struct {
	Report         coordaudit.Report              `json:"report"`
	TraceAnomalies []coordaudit.ReplyTraceAnomaly `json:"trace_anomalies,omitempty"`
}

func renderSuccessionAuditJSON(w io.Writer, report coordaudit.Report, anomalies []coordaudit.ReplyTraceAnomaly) error {
	out := successionAuditJSON{Report: report, TraceAnomalies: anomalies}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func renderSuccessionAuditText(w io.Writer, report coordaudit.Report, anomalies []coordaudit.ReplyTraceAnomaly) error {
	_, _ = fmt.Fprintln(w, "Succession audit report")
	_, _ = fmt.Fprintln(w, "----------------")
	_, _ = fmt.Fprintf(w, "holder             : %s\n", report.HolderID)
	_, _ = fmt.Fprintf(w, "now_unix_nano      : %d\n", report.NowUnixNano)
	_, _ = fmt.Fprintf(w, "root_desc_revision : %d\n", report.RootDescriptorRevision)
	_, _ = fmt.Fprintf(w, "catch_up_state     : %s\n", report.CatchUpState)
	_, _ = fmt.Fprintf(w, "current_holder     : %s\n", report.CurrentHolderID)
	_, _ = fmt.Fprintf(w, "current_generation : %d\n", report.CurrentGeneration)
	_, _ = fmt.Fprintf(w, "handover           : stage=%s\n", report.Handover.Stage)
	_, _ = fmt.Fprintf(w, "handover_witness   : stage=%s seal_gen=%d successor_present=%v inheritance=%v lineage_satisfied=%v sealed_gen_retired=%v\n",
		report.HandoverWitness.Stage,
		report.HandoverWitness.LegacyEpoch,
		report.HandoverWitness.SuccessorPresent,
		report.HandoverWitness.Inheritance,
		report.HandoverWitness.SuccessorLineageSatisfied,
		report.HandoverWitness.SealedGenerationRetired)
	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, "snapshot anomalies:")
	_, _ = fmt.Fprintf(w, "  successor_lineage_mismatch     : %v\n", report.Anomalies.SuccessorLineageMismatch)
	_, _ = fmt.Fprintf(w, "  uncovered_monotone_frontier    : %v\n", report.Anomalies.UncoveredMonotoneFrontier)
	_, _ = fmt.Fprintf(w, "  uncovered_descriptor_revision  : %v\n", report.Anomalies.UncoveredDescriptorRevision)
	_, _ = fmt.Fprintf(w, "  lease_start_coverage_violation : %v\n", report.Anomalies.LeaseStartCoverageViolation)
	_, _ = fmt.Fprintf(w, "  sealed_generation_still_live   : %v\n", report.Anomalies.SealedGenerationStillLive)
	_, _ = fmt.Fprintf(w, "  finality_defect                : %s\n", defectOrNone(report.Anomalies.FinalityDefect))

	if len(anomalies) > 0 {
		_, _ = fmt.Fprintln(w)
		_, _ = fmt.Fprintf(w, "reply-trace anomalies (%d):\n", len(anomalies))
		for _, a := range anomalies {
			_, _ = fmt.Fprintf(w, "  [%d] kind=%s duty=%s cert_gen=%d reason=%q\n",
				a.Index, a.Kind, a.Duty, a.Epoch, a.Reason)
		}
	}
	return nil
}

func defectOrNone(d coordaudit.FinalityDefect) string {
	if d == "" {
		return "none"
	}
	return string(d)
}
