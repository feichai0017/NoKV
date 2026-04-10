package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	adminpb "github.com/feichai0017/NoKV/pb/admin"
	adminclient "github.com/feichai0017/NoKV/raftstore/admin"
)

var dialAdmin = adminclient.Dial

func runExecutionCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("execution", flag.ContinueOnError)
	addr := fs.String("addr", "", "raftstore admin address")
	regionID := fs.Uint64("region", 0, "optional region id filter for topology output")
	transitionID := fs.String("transition", "", "optional transition id filter for topology output")
	timeout := fs.Duration("timeout", 3*time.Second, "timeout for execution status query")
	asJSON := fs.Bool("json", false, "output JSON instead of plain text")
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*addr) == "" {
		return fmt.Errorf("--addr is required")
	}

	ctx := context.Background()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	client, closeFn, err := dialAdmin(ctx, strings.TrimSpace(*addr))
	if err != nil {
		return fmt.Errorf("dial raft admin %q: %w", strings.TrimSpace(*addr), err)
	}
	if closeFn != nil {
		defer func() { _ = closeFn() }()
	}

	resp, err := client.ExecutionStatus(ctx, &adminpb.ExecutionStatusRequest{})
	if err != nil {
		return fmt.Errorf("query execution status: %w", err)
	}
	filtered := filterExecutionStatus(resp, executionFilter{
		RegionID:     *regionID,
		TransitionID: strings.TrimSpace(*transitionID),
	})
	if *asJSON {
		return renderExecutionJSON(w, strings.TrimSpace(*addr), filtered)
	}
	return renderExecutionText(w, strings.TrimSpace(*addr), filtered)
}

type executionStatusView struct {
	Addr          string                  `json:"addr"`
	LastAdmission executionAdmissionView  `json:"last_admission"`
	Restart       executionRestartView    `json:"restart"`
	Topology      []executionTopologyView `json:"topology"`
}

type executionAdmissionView struct {
	Observed  bool   `json:"observed"`
	Class     string `json:"class"`
	Reason    string `json:"reason"`
	Accepted  bool   `json:"accepted"`
	RegionID  uint64 `json:"region_id,omitempty"`
	PeerID    uint64 `json:"peer_id,omitempty"`
	RequestID uint64 `json:"request_id,omitempty"`
	Detail    string `json:"detail,omitempty"`
	At        string `json:"at,omitempty"`
}

type executionRestartView struct {
	State              string   `json:"state"`
	RegionCount        uint64   `json:"region_count"`
	RaftGroupCount     uint64   `json:"raft_group_count"`
	MissingRaftPointer []uint64 `json:"missing_raft_pointer,omitempty"`
}

type executionTopologyView struct {
	TransitionID string `json:"transition_id"`
	RegionID     uint64 `json:"region_id,omitempty"`
	Action       string `json:"action,omitempty"`
	Outcome      string `json:"outcome"`
	Publish      string `json:"publish"`
	LastError    string `json:"last_error,omitempty"`
	UpdatedAt    string `json:"updated_at,omitempty"`
}

type executionFilter struct {
	RegionID     uint64
	TransitionID string
}

func renderExecutionJSON(w io.Writer, addr string, resp *adminpb.ExecutionStatusResponse) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(buildExecutionStatusView(addr, resp))
}

func renderExecutionText(w io.Writer, addr string, resp *adminpb.ExecutionStatusResponse) error {
	view := buildExecutionStatusView(addr, resp)
	_, _ = fmt.Fprintf(w, "Addr                  %s\n", view.Addr)
	_, _ = fmt.Fprintf(w, "Admission.Observed    %t\n", view.LastAdmission.Observed)
	_, _ = fmt.Fprintf(w, "Admission.Class       %s\n", view.LastAdmission.Class)
	_, _ = fmt.Fprintf(w, "Admission.Reason      %s\n", view.LastAdmission.Reason)
	_, _ = fmt.Fprintf(w, "Admission.Accepted    %t\n", view.LastAdmission.Accepted)
	if view.LastAdmission.RegionID != 0 {
		_, _ = fmt.Fprintf(w, "Admission.Region      %d\n", view.LastAdmission.RegionID)
	}
	if view.LastAdmission.PeerID != 0 {
		_, _ = fmt.Fprintf(w, "Admission.Peer        %d\n", view.LastAdmission.PeerID)
	}
	if view.LastAdmission.RequestID != 0 {
		_, _ = fmt.Fprintf(w, "Admission.Request     %d\n", view.LastAdmission.RequestID)
	}
	if view.LastAdmission.At != "" {
		_, _ = fmt.Fprintf(w, "Admission.At          %s\n", view.LastAdmission.At)
	}
	if view.LastAdmission.Detail != "" {
		_, _ = fmt.Fprintf(w, "Admission.Detail      %s\n", view.LastAdmission.Detail)
	}
	_, _ = fmt.Fprintf(w, "Restart.State         %s\n", view.Restart.State)
	_, _ = fmt.Fprintf(w, "Restart.Regions       %d\n", view.Restart.RegionCount)
	_, _ = fmt.Fprintf(w, "Restart.RaftGroups    %d\n", view.Restart.RaftGroupCount)
	if len(view.Restart.MissingRaftPointer) > 0 {
		_, _ = fmt.Fprintf(w, "Restart.Missing       %v\n", view.Restart.MissingRaftPointer)
	}
	_, _ = fmt.Fprintf(w, "Topology.Count        %d\n", len(view.Topology))
	if len(view.Topology) > 0 {
		_, _ = fmt.Fprintln(w, "Topology:")
		for _, entry := range view.Topology {
			_, _ = fmt.Fprintf(w, "  - transition=%s region=%d action=%s outcome=%s publish=%s",
				entry.TransitionID, entry.RegionID, entry.Action, entry.Outcome, entry.Publish)
			if entry.UpdatedAt != "" {
				_, _ = fmt.Fprintf(w, " updated=%s", entry.UpdatedAt)
			}
			if entry.LastError != "" {
				_, _ = fmt.Fprintf(w, " error=%s", entry.LastError)
			}
			_, _ = fmt.Fprintln(w)
		}
	}
	return nil
}

func buildExecutionStatusView(addr string, resp *adminpb.ExecutionStatusResponse) executionStatusView {
	view := executionStatusView{
		Addr:     addr,
		Topology: make([]executionTopologyView, 0),
	}
	if resp == nil {
		return view
	}
	admission := resp.GetLastAdmission()
	view.LastAdmission = executionAdmissionView{
		Observed:  admission.GetObserved(),
		Class:     formatExecutionAdmissionClass(admission.GetClass()),
		Reason:    formatExecutionAdmissionReason(admission.GetReason()),
		Accepted:  admission.GetAccepted(),
		RegionID:  admission.GetRegionId(),
		PeerID:    admission.GetPeerId(),
		RequestID: admission.GetRequestId(),
		Detail:    admission.GetDetail(),
		At:        formatUnixNano(admission.GetAtUnixNano()),
	}
	restart := resp.GetRestart()
	view.Restart = executionRestartView{
		State:              formatExecutionRestartState(restart.GetState()),
		RegionCount:        restart.GetRegionCount(),
		RaftGroupCount:     restart.GetRaftGroupCount(),
		MissingRaftPointer: append([]uint64(nil), restart.GetMissingRaftPointer()...),
	}
	for _, entry := range resp.GetTopology() {
		view.Topology = append(view.Topology, executionTopologyView{
			TransitionID: entry.GetTransitionId(),
			RegionID:     entry.GetRegionId(),
			Action:       entry.GetAction(),
			Outcome:      formatExecutionTopologyOutcome(entry.GetOutcome()),
			Publish:      formatExecutionPublishState(entry.GetPublish()),
			LastError:    entry.GetLastError(),
			UpdatedAt:    formatUnixNano(entry.GetUpdatedAtUnixNano()),
		})
	}
	return view
}

func filterExecutionStatus(resp *adminpb.ExecutionStatusResponse, filter executionFilter) *adminpb.ExecutionStatusResponse {
	if resp == nil {
		return nil
	}
	out := &adminpb.ExecutionStatusResponse{
		LastAdmission: cloneExecutionAdmissionStatus(resp.GetLastAdmission()),
		Restart:       cloneExecutionRestartStatus(resp.GetRestart()),
		Topology:      make([]*adminpb.ExecutionTopologyStatus, 0, len(resp.GetTopology())),
	}
	for _, entry := range resp.GetTopology() {
		if entry == nil {
			continue
		}
		if filter.RegionID != 0 && entry.GetRegionId() != filter.RegionID {
			continue
		}
		if filter.TransitionID != "" && entry.GetTransitionId() != filter.TransitionID {
			continue
		}
		out.Topology = append(out.Topology, cloneExecutionTopologyStatus(entry))
	}
	return out
}

func cloneExecutionAdmissionStatus(in *adminpb.ExecutionAdmissionStatus) *adminpb.ExecutionAdmissionStatus {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneExecutionRestartStatus(in *adminpb.ExecutionRestartStatus) *adminpb.ExecutionRestartStatus {
	if in == nil {
		return nil
	}
	out := *in
	out.MissingRaftPointer = append([]uint64(nil), in.GetMissingRaftPointer()...)
	return &out
}

func cloneExecutionTopologyStatus(in *adminpb.ExecutionTopologyStatus) *adminpb.ExecutionTopologyStatus {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func formatUnixNano(unixNano int64) string {
	if unixNano <= 0 {
		return ""
	}
	return time.Unix(0, unixNano).UTC().Format(time.RFC3339Nano)
}

func formatExecutionAdmissionClass(class adminpb.ExecutionAdmissionClass) string {
	switch class {
	case adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_READ:
		return "read"
	case adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_WRITE:
		return "write"
	case adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_TOPOLOGY:
		return "topology"
	default:
		return "unknown"
	}
}

func formatExecutionAdmissionReason(reason adminpb.ExecutionAdmissionReason) string {
	switch reason {
	case adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_ACCEPTED:
		return "accepted"
	case adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_INVALID:
		return "invalid"
	case adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_STORE_NOT_MATCH:
		return "store-not-match"
	case adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_NOT_HOSTED:
		return "not-hosted"
	case adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_EPOCH_MISMATCH:
		return "epoch-mismatch"
	case adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_KEY_NOT_IN_REGION:
		return "key-not-in-region"
	case adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_NOT_LEADER:
		return "not-leader"
	case adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_CANCELED:
		return "canceled"
	case adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_TIMED_OUT:
		return "timed-out"
	default:
		return "unknown"
	}
}

func formatExecutionTopologyOutcome(outcome adminpb.ExecutionTopologyOutcome) string {
	switch outcome {
	case adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_REJECTED:
		return "rejected"
	case adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_QUEUED:
		return "queued"
	case adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_PROPOSED:
		return "proposed"
	case adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_APPLIED:
		return "applied"
	case adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_FAILED:
		return "failed"
	default:
		return "unknown"
	}
}

func formatExecutionPublishState(state adminpb.ExecutionPublishState) string {
	switch state {
	case adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_NOT_REQUIRED:
		return "not-required"
	case adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_PLANNED_PUBLISHED:
		return "planned-published"
	case adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_PENDING:
		return "terminal-pending"
	case adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_PUBLISHED:
		return "terminal-published"
	case adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_FAILED:
		return "terminal-failed"
	default:
		return "unknown"
	}
}

func formatExecutionRestartState(state adminpb.ExecutionRestartState) string {
	switch state {
	case adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_READY:
		return "ready"
	case adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_DEGRADED:
		return "degraded"
	default:
		return "unknown"
	}
}
