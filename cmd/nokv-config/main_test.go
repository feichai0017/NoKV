package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/stretchr/testify/require"
)

func TestRunStoresSimpleFormat(t *testing.T) {
	cfgPath := writeSampleConfig(t)

	output, err := captureStdout(t, func() error {
		return runStores([]string{"--config", cfgPath, "--format", "simple"})
	})
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 2)
	require.Equal(t, "1 127.0.0.1:10170 127.0.0.1:10170 10.0.0.1:20160 store1-docker", lines[0])
	require.Equal(t, "2 127.0.0.1:10171 127.0.0.1:10171 127.0.0.1:10171 127.0.0.1:10171", lines[1])
}

func TestRunRegionsSimpleFormat(t *testing.T) {
	cfgPath := writeSampleConfig(t)

	output, err := captureStdout(t, func() error {
		return runRegions([]string{"--config", cfgPath, "--format", "simple"})
	})
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 2)
	require.Equal(t, "1 - m 1 1 1:101,2:201 1", lines[0])
	require.Equal(t, "2 m hex:0001 2 3 2:202 2", lines[1])
}

func TestRunTSOJsonFormat(t *testing.T) {
	cfgPath := writeSampleConfig(t)

	output, err := captureStdout(t, func() error {
		return runTSO([]string{"--config", cfgPath, "--format", "json"})
	})
	require.NoError(t, err)
	var tso tsoConfig
	require.NoError(t, json.Unmarshal([]byte(output), &tso))
	require.Equal(t, "0.0.0.0:9494", strings.TrimSpace(tso.ListenAddr))
	require.Equal(t, "http://127.0.0.1:9494", strings.TrimSpace(tso.AdvertiseURL))
}

func TestRunManifestWritesRegion(t *testing.T) {
	dir := t.TempDir()
	manifestDir := filepath.Join(dir, "manifest")
	args := []string{
		"--workdir", manifestDir,
		"--region-id", "99",
		"--start-key", "hex:6161",
		"--end-key", "zz",
		"--epoch-version", "7",
		"--epoch-conf-version", "5",
		"--peer", "1:1001",
		"--peer", "2:1002",
	}

	output, err := captureStdout(t, func() error {
		return runManifest(args)
	})
	require.NoError(t, err)
	require.Contains(t, output, "logged region 99")

	mgr, err := manifest.Open(manifestDir)
	require.NoError(t, err)
	defer func() {
		_ = mgr.Close()
	}()

	regions := mgr.RegionSnapshot()
	meta, ok := regions[99]
	require.True(t, ok)
	require.Equal(t, []byte("aa"), meta.StartKey)
	require.Equal(t, []byte("zz"), meta.EndKey)
	require.Equal(t, uint64(7), meta.Epoch.Version)
	require.Equal(t, uint64(5), meta.Epoch.ConfVersion)
	require.Len(t, meta.Peers, 2)
	require.Equal(t, manifest.PeerMeta{StoreID: 1, PeerID: 1001}, meta.Peers[0])
}

func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	runErr := fn()
	if closeErr := w.Close(); closeErr != nil && runErr == nil {
		runErr = closeErr
	}
	os.Stdout = orig
	out, readErr := io.ReadAll(r)
	require.NoError(t, r.Close())
	if runErr == nil && readErr != nil {
		runErr = readErr
	}
	return string(out), runErr
}

func writeSampleConfig(t *testing.T) string {
	t.Helper()
	cfg := configFile{
		MaxRetries: 3,
		TSO: &tsoConfig{
			ListenAddr:   "0.0.0.0:9494",
			AdvertiseURL: "http://127.0.0.1:9494",
		},
		Stores: []storeConfig{
			{
				StoreID:          1,
				ListenAddr:       "127.0.0.1:10170",
				Addr:             "127.0.0.1:10170",
				DockerListenAddr: "10.0.0.1:20160",
				DockerAddr:       "store1-docker",
			},
			{
				StoreID:    2,
				ListenAddr: "127.0.0.1:10171",
				Addr:       "127.0.0.1:10171",
			},
		},
		Regions: []regionConfig{
			{
				ID:       1,
				StartKey: "",
				EndKey:   "m",
				Epoch: regionEpoch{
					Version:     1,
					ConfVersion: 1,
				},
				Peers: []regionPeer{
					{StoreID: 1, PeerID: 101},
					{StoreID: 2, PeerID: 201},
				},
				LeaderStoreID: 1,
			},
			{
				ID:       2,
				StartKey: "m",
				EndKey:   string([]byte{0x00, 0x01}),
				Epoch: regionEpoch{
					Version:     2,
					ConfVersion: 3,
				},
				Peers: []regionPeer{
					{StoreID: 2, PeerID: 202},
				},
				LeaderStoreID: 2,
			},
		},
	}
	data, err := json.Marshal(cfg)
	require.NoError(t, err)

	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}
