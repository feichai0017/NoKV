package lsm

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBaseManifest validates manifest integrity across restarts.
func TestBaseManifest(t *testing.T) {
	clearDir()
	recovery := func() {
		// Each run simulates an unexpected restart.
		lsm := buildLSM()
		// Validate correctness after recovery.
		baseTest(t, lsm, 128)
		_ = lsm.Close()
	}
	// Run the closure multiple times to exercise recovery.
	runTest(5, recovery)
}

func TestManifestMagic(t *testing.T) {
	helpTestManifestFileCorruption(t, 3, "bad magic")
}

func TestManifestVersion(t *testing.T) {
	helpTestManifestFileCorruption(t, 4, "")
}

func TestManifestChecksum(t *testing.T) {
	helpTestManifestFileCorruption(t, 15, "")
}

func helpTestManifestFileCorruption(t *testing.T, off int64, errorContent string) {
	clearDir()
	// Create the LSM and close it to generate a manifest.
	{
		lsm := buildLSM()
		require.NoError(t, lsm.Close())
	}
	currentData, err := os.ReadFile(filepath.Join(opt.WorkDir, "CURRENT"))
	require.NoError(t, err)
	manifestName := strings.TrimSpace(string(currentData))
	fp, err := os.OpenFile(filepath.Join(opt.WorkDir, manifestName), os.O_RDWR, 0)
	require.NoError(t, err)
	// Inject a bad byte at the given offset.
	_, err = fp.WriteAt([]byte{'X'}, off)
	require.NoError(t, err)
	require.NoError(t, fp.Close())
	defer func() {
		if err := recover(); err != nil && errorContent != "" {
			require.Contains(t, err.(error).Error(), errorContent)
		}
	}()
	// Re-open LSM; it should panic on corruption.
	lsm := buildLSM()
	require.NoError(t, lsm.Close())
}
