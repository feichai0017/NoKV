package main

import (
	"encoding/json"
	"expvar"
	"testing"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	"github.com/feichai0017/NoKV/coordinator/tso"
	"github.com/stretchr/testify/require"
)

func TestInstallCoordinatorExpvarPublishesDedicatedEunomiaSnapshot(t *testing.T) {
	svc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	installCoordinatorExpvar(svc)

	rootVar := expvar.Get("nokv_coordinator")
	require.NotNil(t, rootVar)
	cccVar := expvar.Get("nokv_coordinator_eunomia")
	require.NotNil(t, cccVar)

	var rootSnapshot map[string]any
	require.NoError(t, json.Unmarshal([]byte(rootVar.String()), &rootSnapshot))
	var cccSnapshot map[string]any
	require.NoError(t, json.Unmarshal([]byte(cccVar.String()), &cccSnapshot))

	state, ok := rootSnapshot["state"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, state["eunomia_metrics"], cccSnapshot)
}
