// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/fsmeta"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	localdb "github.com/feichai0017/NoKV/local"
)

// PerasMode controls whether the embedded runtime uses its single-node Peras
// visible commit path. The zero value follows the package default and enables
// Peras.
type PerasMode uint8

const (
	PerasModeDefault PerasMode = iota
	PerasModeEnabled
	PerasModeDisabled
)

// Options configures the embedded fsmeta runtime.
type Options struct {
	// DB reuses an already-open embedded database. When nil, Open creates and
	// owns a DB from DBOptions/WorkDir.
	DB *localdb.DB

	// DBOptions configures a runtime-owned DB. Open copies these options before
	// filling fsmeta-specific storage settings.
	DBOptions *localdb.Options

	// WorkDir is required when DB is nil. It overrides DBOptions.WorkDir.
	WorkDir string

	// Mount is the single local fsmeta mount admitted by this runtime.
	Mount fsmeta.MountIdentity

	// RootInode is the mount root inode. Zero uses fsmeta.RootInode.
	RootInode fsmeta.InodeID

	// LockTTL overrides fsmeta/exec's lock TTL, in the same units expected by
	// fsmeta/exec.WithLockTTL. Local MVCC commits are one-phase, but keeping the
	// option lets callers share runtime configuration.
	LockTTL time.Duration

	// Clock overrides fsmeta/exec's wall clock for write-session expiry.
	Clock func() time.Time

	// PerasMode controls the single-node Peras visible commit path. The zero
	// value enables Peras; PerasModeDisabled keeps local fsmeta on direct
	// embedded MVCC for diagnostics and baseline comparisons.
	PerasMode PerasMode

	// PerasHolderID overrides the stable local holder id used when Peras is
	// enabled. Empty derives one from the local mount identity.
	PerasHolderID string

	// PerasVisibleLog is the holder-local visible WAL. PerasVisibleLogDir wires
	// the default WAL-backed implementation when no explicit log is supplied.
	PerasVisibleLog           fsperas.VisibleLog
	PerasVisibleLogDir        string
	PerasVisibleLogDurability wal.DurabilityPolicy
}

func (opts Options) rootInode() fsmeta.InodeID {
	if opts.RootInode != 0 {
		return opts.RootInode
	}
	return fsmeta.RootInode
}

func (opts Options) validate() error {
	if opts.Mount.MountID == "" || opts.Mount.MountKeyID == 0 {
		return errMountRequired
	}
	if opts.DB == nil && opts.WorkDir == "" && (opts.DBOptions == nil || opts.DBOptions.WorkDir == "") {
		return errWorkDirRequired
	}
	if !opts.validPerasMode() {
		return errInvalidPerasMode
	}
	if opts.perasEnabled() && opts.PerasVisibleLog == nil && localPerasVisibleLogDir(opts) == "" {
		return fsperas.ErrVisibleLogRequired
	}
	return nil
}

func (opts Options) validPerasMode() bool {
	switch opts.PerasMode {
	case PerasModeDefault, PerasModeEnabled, PerasModeDisabled:
		return true
	default:
		return false
	}
}

func (opts Options) perasEnabled() bool {
	return opts.PerasMode != PerasModeDisabled
}

func (opts Options) perasHolderID() string {
	if holderID := strings.TrimSpace(opts.PerasHolderID); holderID != "" {
		return holderID
	}
	mountID := strings.TrimSpace(string(opts.Mount.MountID))
	if mountID == "" {
		mountID = "default"
	}
	return "local/" + mountID
}

func localWorkDir(opts Options) string {
	if opts.WorkDir != "" {
		return opts.WorkDir
	}
	if opts.DBOptions != nil {
		return opts.DBOptions.WorkDir
	}
	return ""
}

func localDBOptions(opts Options) *localdb.Options {
	cfg := localdb.NewDefaultOptions()
	if opts.DBOptions != nil {
		copy := *opts.DBOptions
		cfg = &copy
	}
	if opts.WorkDir != "" {
		cfg.WorkDir = opts.WorkDir
	}
	cfg.UserKeyShapeExtractor = fsmeta.UserKeyShape
	return cfg
}

func localPerasVisibleLogDir(opts Options) string {
	if opts.PerasVisibleLogDir != "" {
		return opts.PerasVisibleLogDir
	}
	workDir := localWorkDir(opts)
	if workDir == "" {
		return ""
	}
	return filepath.Join(workDir, "peras-visible-log")
}
