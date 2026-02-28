package main

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/pd/core"

	"github.com/stretchr/testify/require"
)

func TestRunPDCmdParseError(t *testing.T) {
	var buf bytes.Buffer
	err := runPDCmd(&buf, []string{"-bad-flag"})
	require.Error(t, err)
}

func TestRunPDCmdStartsAndStops(t *testing.T) {
	origNotify := pdNotifyContext
	pdNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { pdNotifyContext = origNotify })

	var buf bytes.Buffer
	require.NoError(t, runPDCmd(&buf, []string{"-addr", "127.0.0.1:0"}))
	require.Contains(t, buf.String(), "PD-lite service listening on")
}

func TestMainPDCommand(t *testing.T) {
	origNotify := pdNotifyContext
	pdNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { pdNotifyContext = origNotify })

	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "pd", "-addr", "127.0.0.1:0"}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 0, code)
}

func TestRestorePDRegionsFromManifestSnapshot(t *testing.T) {
	dir := t.TempDir()
	mgr, err := manifest.Open(dir, nil)
	require.NoError(t, err)
	require.NoError(t, mgr.LogRegionUpdate(manifest.RegionMeta{
		ID:       10,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch: manifest.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
	}))
	require.NoError(t, mgr.LogRegionUpdate(manifest.RegionMeta{
		ID:       20,
		StartKey: []byte("m"),
		EndKey:   nil,
		Epoch: manifest.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
	}))
	snapshot := mgr.RegionSnapshot()
	require.NoError(t, mgr.Close())

	cluster := core.NewCluster()
	loaded, err := restorePDRegions(cluster, snapshot)
	require.NoError(t, err)
	require.Equal(t, 2, loaded)

	meta, ok := cluster.GetRegionByKey([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(10), meta.ID)
	meta, ok = cluster.GetRegionByKey([]byte("z"))
	require.True(t, ok)
	require.Equal(t, uint64(20), meta.ID)
}
