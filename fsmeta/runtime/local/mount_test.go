// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

func TestMountCatalogSameAuthorityRequiresRegisteredMount(t *testing.T) {
	ctx := context.Background()
	catalog := NewMountCatalog(MountConfig{Mount: testMount()})

	ok, err := catalog.SameAuthority(ctx, "vol", fsmeta.RootInode, fsmeta.RootInode)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = catalog.SameAuthority(ctx, "other", fsmeta.RootInode, fsmeta.RootInode)
	require.False(t, ok)
	require.True(t, errors.Is(err, fsmeta.ErrMountNotRegistered))

	var nilCatalog *MountCatalog
	ok, err = nilCatalog.SameAuthority(ctx, "vol", fsmeta.RootInode, fsmeta.RootInode)
	require.False(t, ok)
	require.True(t, errors.Is(err, fsmeta.ErrMountNotRegistered))
}

func TestMountCatalogHandoffRequiresRegisteredMount(t *testing.T) {
	ctx := context.Background()
	catalog := NewMountCatalog(MountConfig{Mount: testMount()})

	require.NoError(t, catalog.StartSubtreeHandoff(ctx, "vol", fsmeta.RootInode, 10))
	require.NoError(t, catalog.CompleteSubtreeHandoff(ctx, "vol", fsmeta.RootInode, 11))

	require.ErrorIs(t, catalog.StartSubtreeHandoff(ctx, "other", fsmeta.RootInode, 10), fsmeta.ErrMountNotRegistered)
	require.ErrorIs(t, catalog.CompleteSubtreeHandoff(ctx, "other", fsmeta.RootInode, 11), fsmeta.ErrMountNotRegistered)

	var nilCatalog *MountCatalog
	require.ErrorIs(t, nilCatalog.StartSubtreeHandoff(ctx, "vol", fsmeta.RootInode, 10), fsmeta.ErrMountNotRegistered)
	require.ErrorIs(t, nilCatalog.CompleteSubtreeHandoff(ctx, "vol", fsmeta.RootInode, 11), fsmeta.ErrMountNotRegistered)
}
