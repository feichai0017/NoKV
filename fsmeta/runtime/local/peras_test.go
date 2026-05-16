// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/stretchr/testify/require"
)

func TestLocalPerasAuthorityAcquireChecksMountID(t *testing.T) {
	catalog := NewMountCatalog(MountConfig{Mount: testMount()})
	authority := newLocalPerasAuthority("holder", catalog.Admission(), nil)

	_, owned, err := authority.Acquire(context.Background(), compile.AuthorityScope{
		Mount:      "other",
		MountKeyID: fsmeta.MountKeyID(1),
	})
	require.NoError(t, err)
	require.False(t, owned)

	_, owned, err = authority.Acquire(context.Background(), compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: fsmeta.MountKeyID(1),
	})
	require.NoError(t, err)
	require.True(t, owned)
}
