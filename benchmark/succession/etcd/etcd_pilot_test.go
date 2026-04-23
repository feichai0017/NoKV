package etcd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	coordaudit "github.com/feichai0017/NoKV/coordinator/audit"
	"github.com/stretchr/testify/require"
)

type etcdPilotCase struct {
	name          string
	system        string
	format        coordaudit.ReplyTraceFormat
	fixturePath   string
	schedulePath  string
	wantAnomalies []string
}

type etcdReadIndexSchedule struct {
	Name  string                      `json:"name,omitempty"`
	Steps []etcdReadIndexScheduleStep `json:"steps"`
}

type etcdReadIndexScheduleStep struct {
	Op           string `json:"op"`
	MemberID     string `json:"member_id,omitempty"`
	Duty         string `json:"duty,omitempty"`
	Era          uint64 `json:"era,omitempty"`
	ReadStateEra uint64 `json:"read_state_era,omitempty"`
}

type etcdReadIndexTraceRecord struct {
	MemberID     string `json:"member_id,omitempty"`
	Duty         string `json:"duty,omitempty"`
	ReadStateEra uint64 `json:"read_state_era"`
	SuccessorEra uint64 `json:"successor_era"`
	Accepted     bool   `json:"accepted"`
}

func TestControlPlaneEtcdReadIndexPilot(t *testing.T) {
	cases := []etcdPilotCase{
		{
			name:          "accepted_read_index_behind_successor",
			system:        "etcd",
			format:        coordaudit.ReplyTraceFormatEtcdReadIndex,
			schedulePath:  filepath.Join("testdata", "etcd", "accepted_reply_behind_successor.schedule.json"),
			wantAnomalies: []string{"accepted_read_index_behind_successor"},
		},
		{
			name:          "process_pause_stale_reply_then_reject",
			system:        "etcd",
			format:        coordaudit.ReplyTraceFormatEtcdReadIndex,
			schedulePath:  filepath.Join("testdata", "etcd", "process_pause_stale_reply_then_reject.schedule.json"),
			wantAnomalies: []string{"accepted_read_index_behind_successor"},
		},
		{
			name:          "repeated_stale_reply_after_successor",
			system:        "etcd",
			format:        coordaudit.ReplyTraceFormatEtcdReadIndex,
			schedulePath:  filepath.Join("testdata", "etcd", "repeated_stale_reply_after_successor.schedule.json"),
			wantAnomalies: []string{"accepted_read_index_behind_successor", "accepted_read_index_behind_successor"},
		},
		{
			name:          "rejected_old_reply_after_successor",
			system:        "etcd",
			format:        coordaudit.ReplyTraceFormatEtcdReadIndex,
			schedulePath:  filepath.Join("testdata", "etcd", "rejected_old_reply_after_successor.schedule.json"),
			wantAnomalies: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			records := loadEtcdPilotReplyTrace(t, tc.fixturePath, tc.schedulePath, tc.format)
			anomalies := coordaudit.EvaluateReplyTrace(coordaudit.Report{}, records)
			require.Equal(t, tc.wantAnomalies, pilotAnomalyKinds(anomalies))
			t.Logf(
				"etcd_pilot system=%s scenario=%s records=%d anomalies=%d kinds=%s",
				tc.system,
				tc.name,
				len(records),
				len(anomalies),
				formatPilotAnomalyKinds(anomalies),
			)
		})
	}
}

func loadEtcdPilotReplyTrace(t *testing.T, fixturePath, schedulePath string, format coordaudit.ReplyTraceFormat) []coordaudit.ReplyTraceRecord {
	t.Helper()
	switch {
	case strings.TrimSpace(schedulePath) != "":
		require.Equal(t, coordaudit.ReplyTraceFormatEtcdReadIndex, format)
		records, err := loadEtcdReadIndexReplyTraceFromSchedule(schedulePath)
		require.NoError(t, err)
		return records
	case strings.TrimSpace(fixturePath) != "":
		data, err := os.ReadFile(fixturePath)
		require.NoError(t, err)
		records, err := coordaudit.DecodeReplyTrace(data, format)
		require.NoError(t, err)
		return records
	default:
		t.Fatalf("pilot case requires fixturePath or schedulePath")
		return nil
	}
}

func loadEtcdReadIndexReplyTraceFromSchedule(path string) ([]coordaudit.ReplyTraceRecord, error) {
	raw, err := projectEtcdReadIndexScheduleFile(path)
	if err != nil {
		return nil, err
	}
	records, err := coordaudit.DecodeReplyTrace(raw, coordaudit.ReplyTraceFormatEtcdReadIndex)
	if err != nil {
		return nil, fmt.Errorf("parse projected etcd read-index trace: %w", err)
	}
	return records, nil
}

func projectEtcdReadIndexScheduleFile(path string) ([]byte, error) {
	data, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return nil, fmt.Errorf("load etcd read-index schedule: %w", err)
	}
	var schedule etcdReadIndexSchedule
	if err := json.Unmarshal(data, &schedule); err != nil {
		return nil, fmt.Errorf("parse etcd read-index schedule: %w", err)
	}
	return projectEtcdReadIndexSchedule(schedule)
}

func projectEtcdReadIndexSchedule(schedule etcdReadIndexSchedule) ([]byte, error) {
	raw, err := projectEtcdReadIndexScheduleRecords(schedule)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("encode projected etcd read-index trace: %w", err)
	}
	return encoded, nil
}

func projectEtcdReadIndexScheduleRecords(schedule etcdReadIndexSchedule) ([]etcdReadIndexTraceRecord, error) {
	if len(schedule.Steps) == 0 {
		return nil, fmt.Errorf("schedule %q has no steps", schedule.Name)
	}

	var observedSuccessorEra uint64
	records := make([]etcdReadIndexTraceRecord, 0, len(schedule.Steps))
	for idx, step := range schedule.Steps {
		switch strings.TrimSpace(step.Op) {
		case "observe_successor":
			if step.Era == 0 {
				return nil, fmt.Errorf("schedule %q step %d observe_successor requires era", schedule.Name, idx)
			}
			observedSuccessorEra = step.Era
		case "accept_reply", "reject_reply":
			if step.ReadStateEra == 0 {
				return nil, fmt.Errorf("schedule %q step %d reply requires read_state_era", schedule.Name, idx)
			}
			if observedSuccessorEra == 0 {
				return nil, fmt.Errorf("schedule %q step %d reply requires prior observe_successor", schedule.Name, idx)
			}
			duty := strings.TrimSpace(step.Duty)
			if duty == "" {
				duty = "read_index"
			}
			records = append(records, etcdReadIndexTraceRecord{
				MemberID:     strings.TrimSpace(step.MemberID),
				Duty:         duty,
				ReadStateEra: step.ReadStateEra,
				SuccessorEra: observedSuccessorEra,
				Accepted:     step.Op == "accept_reply",
			})
		default:
			return nil, fmt.Errorf("schedule %q step %d has unsupported op %q", schedule.Name, idx, step.Op)
		}
	}
	return records, nil
}

func pilotAnomalyKinds(anomalies []coordaudit.ReplyTraceAnomaly) []string {
	if len(anomalies) == 0 {
		return nil
	}
	out := make([]string, 0, len(anomalies))
	for _, anomaly := range anomalies {
		out = append(out, anomaly.Kind)
	}
	slices.Sort(out)
	return out
}

func formatPilotAnomalyKinds(anomalies []coordaudit.ReplyTraceAnomaly) string {
	kinds := pilotAnomalyKinds(anomalies)
	if len(kinds) == 0 {
		return "none"
	}
	return strings.Join(kinds, ",")
}
