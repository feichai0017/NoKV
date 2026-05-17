// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

type AuthorityView interface {
	Find(compile.AuthorityScope, time.Time) (rootproto.PerasAuthorityGrant, bool, error)
}

type AuthorityFence interface {
	FencesKey([]byte, time.Time) (rootproto.PerasAuthorityGrant, bool, error)
}
