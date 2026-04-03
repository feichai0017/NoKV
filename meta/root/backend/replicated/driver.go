package replicated

import rootstorage "github.com/feichai0017/NoKV/meta/root/storage"

// Driver exposes the minimal committed-log, checkpoint, and bootstrap-install
// capabilities required by the replicated metadata-root backend.
type Driver interface {
	Log() rootstorage.EventLog
	CheckpointStore() rootstorage.CheckpointStore
	BootstrapInstaller() rootstorage.BootstrapInstaller
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
