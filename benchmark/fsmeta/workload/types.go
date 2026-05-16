// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"context"
	"fmt"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
)

const (
	MDTestEasy           = "mdtest-easy"
	MDTestHard           = "mdtest-hard"
	FilebenchVarmail     = "filebench-varmail"
	MimesisNamespace     = "mimesis-namespace"
	AICheckpointAgent    = "ai-checkpoint-agent"
	DefaultWorkloadSuite = MDTestEasy + "," + MDTestHard + "," + FilebenchVarmail + "," + MimesisNamespace + "," + AICheckpointAgent

	DriverNativeFSMetadata = "native-fsmeta"
)

const (
	maxOperationAttempts = 4
	operationRetryBase   = 10 * time.Millisecond
	operationRetryMax    = 100 * time.Millisecond
	watchDeliveryPoll    = 10 * time.Millisecond
	watchTailTimeout     = 10 * time.Second
)

// MetadataClient is the native fsmeta service surface exercised by the
// official-aligned metadata workload suite.
type MetadataClient interface {
	Create(context.Context, fsmeta.CreateRequest) (fsmeta.CreateResult, error)
	UpdateInode(context.Context, fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error)
	Lookup(context.Context, fsmeta.LookupRequest) (fsmeta.DentryRecord, error)
	ReadDirPlus(context.Context, fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error)
	WatchSubtree(context.Context, fsmeta.WatchRequest) (fsmetaclient.WatchSubscription, error)
	SnapshotSubtree(context.Context, fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error)
	RetireSnapshotSubtree(context.Context, fsmeta.SnapshotSubtreeToken) error
	Rename(context.Context, fsmeta.RenameRequest) error
	Unlink(context.Context, fsmeta.UnlinkRequest) error
	OpenWriteSession(context.Context, fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error)
	HeartbeatWriteSession(context.Context, fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error)
	CloseWriteSession(context.Context, fsmeta.CloseWriteSessionRequest) error
}

type MDTestConfig struct {
	Mount             fsmeta.MountID
	RunID             string
	Clients           int
	Directories       int
	FilesPerDirectory int
	PageLimit         uint32
}

type FilebenchVarmailConfig struct {
	Mount           fsmeta.MountID
	RunID           string
	Clients         int
	Users           int
	MessagesPerUser int
	PageLimit       uint32
	SessionTTL      time.Duration
}

type MimesisNamespaceConfig struct {
	Mount             fsmeta.MountID
	RunID             string
	Clients           int
	Directories       int
	FilesPerDirectory int
	PageLimit         uint32
}

type AICheckpointAgentConfig struct {
	Mount                   fsmeta.MountID
	RunID                   string
	Clients                 int
	Workspaces              int
	CheckpointsPerWorkspace int
	FilesPerCheckpoint      int
	PageLimit               uint32
	WatchWindow             uint32
	SessionTTL              time.Duration
}

type Result struct {
	Name      string
	Driver    string
	RunID     string
	StartedAt time.Time
	Duration  time.Duration
	Ops       int
	Errors    int
	Samples   []Sample
}

type Sample struct {
	Operation string
	Duration  time.Duration
	Error     string
}

type SummaryRow struct {
	Workload             string
	Driver               string
	RunID                string
	Source               string
	SourceURL            string
	Projection           string
	Operation            string
	Count                int
	Errors               int
	Throughput           float64
	ActiveThroughput     float64
	ActiveDurationSecs   float64
	AverageUS            float64
	P50US                float64
	P95US                float64
	P99US                float64
	WorkloadDurationSecs float64
}

type OfficialProfile struct {
	Workload   string                   `yaml:"-"`
	Source     string                   `yaml:"source"`
	SourceURL  string                   `yaml:"source_url"`
	Shape      string                   `yaml:"shape"`
	Projection string                   `yaml:"projection"`
	Official   map[string]string        `yaml:"official"`
	Scale      map[string]OfficialScale `yaml:"scale"`
}

type OfficialScale struct {
	Clients                 int    `yaml:"clients"`
	Directories             int    `yaml:"directories"`
	FilesPerDirectory       int    `yaml:"files_per_directory"`
	Users                   int    `yaml:"users"`
	MessagesPerUser         int    `yaml:"messages_per_user"`
	Workspaces              int    `yaml:"workspaces"`
	CheckpointsPerWorkspace int    `yaml:"checkpoints_per_workspace"`
	FilesPerCheckpoint      int    `yaml:"files_per_checkpoint"`
	PageLimit               uint32 `yaml:"page_limit"`
	WatchWindow             uint32 `yaml:"watch_window"`
	SessionTTL              string `yaml:"session_ttl"`
}

func (s OfficialScale) SessionTTLDuration(fallback time.Duration) time.Duration {
	if s.SessionTTL == "" {
		return fallback
	}
	ttl, err := time.ParseDuration(s.SessionTTL)
	if err != nil {
		return fallback
	}
	return ttl
}

func (s OfficialScale) FormatLines(prefix string) []string {
	return []string{
		fmt.Sprintf("%sclients=%d", prefix, s.Clients),
		fmt.Sprintf("%sdirectories=%d", prefix, s.Directories),
		fmt.Sprintf("%sfiles_per_directory=%d", prefix, s.FilesPerDirectory),
		fmt.Sprintf("%susers=%d", prefix, s.Users),
		fmt.Sprintf("%smessages_per_user=%d", prefix, s.MessagesPerUser),
		fmt.Sprintf("%sworkspaces=%d", prefix, s.Workspaces),
		fmt.Sprintf("%scheckpoints_per_workspace=%d", prefix, s.CheckpointsPerWorkspace),
		fmt.Sprintf("%sfiles_per_checkpoint=%d", prefix, s.FilesPerCheckpoint),
		fmt.Sprintf("%spage_limit=%d", prefix, s.PageLimit),
		fmt.Sprintf("%swatch_window=%d", prefix, s.WatchWindow),
		fmt.Sprintf("%ssession_ttl=%s", prefix, s.SessionTTL),
	}
}
