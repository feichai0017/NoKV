package replicated

import rootstorage "github.com/feichai0017/NoKV/meta/root/storage"

// DriverState is one detached view of one replicated metadata driver state.
type DriverState struct {
	Checkpoint rootstorage.Checkpoint
	Records    []rootstorage.CommittedEvent
}

// Driver exposes the minimal committed-log, checkpoint, and bootstrap-install
// capabilities required by the replicated metadata-root backend.
type Driver interface {
	Log() rootstorage.EventLog
	CheckpointStore() rootstorage.CheckpointStore
	BootstrapInstaller() rootstorage.BootstrapInstaller
}

// LeaderAware reports whether a replicated driver is currently leader-backed.
type LeaderAware interface {
	IsLeader() bool
	LeaderID() uint64
}

// ConfigFromDriver wires one driver into the replicated backend config.
func ConfigFromDriver(driver Driver, maxRetainedRecords int) Config {
	if driver == nil {
		return Config{MaxRetainedRecords: maxRetainedRecords}
	}
	return Config{
		Log:                driver.Log(),
		Checkpoint:         driver.CheckpointStore(),
		Installer:          driver.BootstrapInstaller(),
		MaxRetainedRecords: maxRetainedRecords,
	}
}
