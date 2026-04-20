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
	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

type cccAuditOutput struct {
	Report              coordaudit.Report              `json:"report"`
	Lease               cccAuditLeaseSummary           `json:"lease"`
	Seal                cccAuditSealSummary            `json:"seal"`
	ReplyTraceRecords   int                            `json:"reply_trace_records,omitempty"`
	ReplyTraceAnomalies []coordaudit.ReplyTraceAnomaly `json:"reply_trace_anomalies,omitempty"`
}

type cccAuditLeaseSummary struct {
	HolderID          string                    `json:"holder_id"`
	CertGeneration    uint64                    `json:"cert_generation"`
	Frontiers         []cccAuditFrontierSummary `json:"frontiers,omitempty"`
	PredecessorDigest string                    `json:"predecessor_digest,omitempty"`
}

type cccAuditSealSummary struct {
	HolderID       string                    `json:"holder_id"`
	CertGeneration uint64                    `json:"cert_generation"`
	Frontiers      []cccAuditFrontierSummary `json:"frontiers,omitempty"`
}

type cccAuditFrontierSummary struct {
	DutyMask uint32 `json:"duty_mask"`
	DutyName string `json:"duty_name"`
	Frontier uint64 `json:"frontier"`
}

func runCCCAuditCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("ccc-audit", flag.ContinueOnError)
	workdir := fs.String("workdir", "", "coordinator work directory containing rooted metadata")
	holderID := fs.String("holder", "", "holder id to evaluate for reattach checks (defaults to current rooted holder)")
	nowUnixNano := fs.Int64("now-unix-nano", 0, "override audit time in unix nanos")
	replyTracePath := fs.String("reply-trace", "", "optional JSON reply trace projected into ccc-audit anomaly vocabulary")
	replyTraceFormatRaw := fs.String("reply-trace-format", string(coordaudit.ReplyTraceFormatNoKV), "reply trace format: nokv, etcd-read-index, or etcd-lease-renew")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	replyTraceFormat, err := coordaudit.ParseReplyTraceFormat(*replyTraceFormatRaw)
	if err != nil {
		return err
	}
	if strings.TrimSpace(*workdir) == "" && strings.TrimSpace(*replyTracePath) == "" {
		return fmt.Errorf("ccc-audit requires --workdir or --reply-trace")
	}

	var snapshot coordstorage.Snapshot
	if strings.TrimSpace(*workdir) != "" {
		store, err := coordstorage.OpenRootLocalStore(strings.TrimSpace(*workdir))
		if err != nil {
			return err
		}
		defer func() { _ = store.Close() }()

		snapshot, err = store.Load()
		if err != nil {
			return err
		}
	}

	effectiveHolder := resolveCCCAuditHolder(snapshot, *holderID)
	effectiveNow := *nowUnixNano
	if effectiveNow == 0 {
		effectiveNow = time.Now().UnixNano()
	}
	report := coordaudit.BuildReport(snapshot, effectiveHolder, effectiveNow)
	replyTrace, err := loadCCCAuditReplyTrace(*replyTracePath, replyTraceFormat)
	if err != nil {
		return err
	}
	replyTraceAnomalies := coordaudit.EvaluateReplyTrace(report, replyTrace)
	return renderCCCAudit(w, snapshot, report, replyTrace, replyTraceAnomalies, *asJSON)
}

func resolveCCCAuditHolder(snapshot coordstorage.Snapshot, requested string) string {
	if holder := strings.TrimSpace(requested); holder != "" {
		return holder
	}
	if holder := strings.TrimSpace(snapshot.CoordinatorLease.HolderID); holder != "" {
		return holder
	}
	if holder := strings.TrimSpace(snapshot.CoordinatorClosure.HolderID); holder != "" {
		return holder
	}
	return ""
}

func renderCCCAudit(
	w io.Writer,
	snapshot coordstorage.Snapshot,
	report coordaudit.Report,
	replyTrace []coordaudit.ReplyTraceRecord,
	replyTraceAnomalies []coordaudit.ReplyTraceAnomaly,
	asJSON bool,
) error {
	if asJSON {
		leaseFrontiers := cccAuditFrontierSummaries(controlplane.FrontiersFromState(rootstate.State{
			IDFence:  snapshot.Allocator.IDCurrent,
			TSOFence: snapshot.Allocator.TSCurrent,
		}, report.RootDescriptorRevision))
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(cccAuditOutput{
			Report: report,
			Lease: cccAuditLeaseSummary{
				HolderID:          snapshot.CoordinatorLease.HolderID,
				CertGeneration:    snapshot.CoordinatorLease.CertGeneration,
				Frontiers:         leaseFrontiers,
				PredecessorDigest: snapshot.CoordinatorLease.PredecessorDigest,
			},
			Seal: cccAuditSealSummary{
				HolderID:       snapshot.CoordinatorSeal.HolderID,
				CertGeneration: snapshot.CoordinatorSeal.CertGeneration,
				Frontiers:      cccAuditFrontierSummaries(snapshot.CoordinatorSeal.Frontiers),
			},
			ReplyTraceRecords:   len(replyTrace),
			ReplyTraceAnomalies: replyTraceAnomalies,
		})
	}

	_, _ = fmt.Fprintf(w, "HolderID                 %s\n", emptyDash(report.HolderID))
	_, _ = fmt.Fprintf(w, "NowUnixNano              %d\n", report.NowUnixNano)
	_, _ = fmt.Fprintf(w, "CatchUpState             %s\n", report.CatchUpState)
	_, _ = fmt.Fprintf(w, "RootDescriptorRevision   %d\n", report.RootDescriptorRevision)
	_, _ = fmt.Fprintf(w, "CurrentHolder            %s\n", emptyDash(report.CurrentHolderID))
	_, _ = fmt.Fprintf(w, "CurrentGeneration        %d\n", report.CurrentGeneration)
	_, _ = fmt.Fprintf(w, "SealGeneration           %d\n", report.ClosureAudit.SealGeneration)
	_, _ = fmt.Fprintf(w, "ClosureSatisfied         %t\n", report.ClosureAudit.ClosureSatisfied())
	_, _ = fmt.Fprintf(w, "ClosureStage             %s\n", report.Closure.Stage)
	names := cccAuditAnomalyNames(report.Anomalies)
	if len(names) == 0 {
		_, _ = fmt.Fprintln(w, "Anomalies                none")
	} else {
		_, _ = fmt.Fprintf(w, "Anomalies                %s\n", strings.Join(names, ", "))
	}
	if len(replyTrace) == 0 {
		return nil
	}
	_, _ = fmt.Fprintf(w, "ReplyTraceRecords        %d\n", len(replyTrace))
	traceNames := cccAuditReplyTraceAnomalyNames(replyTraceAnomalies)
	if len(traceNames) == 0 {
		_, _ = fmt.Fprintln(w, "ReplyTraceAnomalies      none")
		return nil
	}
	_, _ = fmt.Fprintf(w, "ReplyTraceAnomalies      %s\n", strings.Join(traceNames, ", "))
	return nil
}

func cccAuditAnomalyNames(anomalies coordaudit.SnapshotAnomalies) []string {
	names := make([]string, 0, 8)
	if anomalies.SuccessorLineageMismatch {
		names = append(names, "successor_lineage_mismatch")
	}
	if anomalies.UncoveredMonotoneFrontier {
		names = append(names, "uncovered_monotone_frontier")
	}
	if anomalies.UncoveredDescriptorRevision {
		names = append(names, "uncovered_descriptor_revision")
	}
	if anomalies.LeaseStartCoverageViolation {
		names = append(names, "lease_start_coverage_violation")
	}
	if anomalies.SealedGenerationStillLive {
		names = append(names, "sealed_generation_still_live")
	}
	if anomalies.ClosureIncomplete {
		names = append(names, "closure_incomplete")
	}
	if anomalies.MissingConfirm {
		names = append(names, "missing_confirm")
	}
	if anomalies.MissingClose {
		names = append(names, "missing_close")
	}
	if anomalies.CloseWithoutConfirm {
		names = append(names, "close_without_confirm")
	}
	if anomalies.CloseLineageMismatch {
		names = append(names, "close_lineage_mismatch")
	}
	if anomalies.ReattachWithoutConfirm {
		names = append(names, "reattach_without_confirm")
	}
	if anomalies.ReattachWithoutClose {
		names = append(names, "reattach_without_close")
	}
	if anomalies.ReattachLineageMismatch {
		names = append(names, "reattach_lineage_mismatch")
	}
	if anomalies.ReattachIncomplete {
		names = append(names, "reattach_incomplete")
	}
	return names
}

func emptyDash(v string) string {
	if strings.TrimSpace(v) == "" {
		return "-"
	}
	return v
}

func loadCCCAuditReplyTrace(path string, format coordaudit.ReplyTraceFormat) ([]coordaudit.ReplyTraceRecord, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return nil, nil
	}
	var (
		data []byte
		err  error
	)
	if trimmed == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(trimmed)
	}
	if err != nil {
		return nil, fmt.Errorf("load reply trace: %w", err)
	}
	records, err := coordaudit.DecodeReplyTrace(data, format)
	if err != nil {
		return nil, fmt.Errorf("parse reply trace: %w", err)
	}
	return records, nil
}

func cccAuditReplyTraceAnomalyNames(anomalies []coordaudit.ReplyTraceAnomaly) []string {
	if len(anomalies) == 0 {
		return nil
	}
	names := make([]string, 0, len(anomalies))
	for _, anomaly := range anomalies {
		name := anomaly.Kind
		if strings.TrimSpace(anomaly.Duty) != "" {
			name = fmt.Sprintf("%s[%s]", name, anomaly.Duty)
		}
		names = append(names, name)
	}
	return names
}

func cccAuditFrontierSummaries(frontiers rootstate.CoordinatorDutyFrontiers) []cccAuditFrontierSummary {
	if frontiers.Len() == 0 {
		return nil
	}
	entries := frontiers.Entries()
	out := make([]cccAuditFrontierSummary, 0, len(entries))
	for _, entry := range entries {
		out = append(out, cccAuditFrontierSummary{
			DutyMask: entry.DutyMask,
			DutyName: entry.DutyName,
			Frontier: entry.Frontier,
		})
	}
	return out
}
