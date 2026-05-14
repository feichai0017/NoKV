// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	"github.com/stretchr/testify/require"
)

func TestCompletionMatchesOperationRejectsPredicateProofDigestMismatch(t *testing.T) {
	op := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	completion := fsperas.SegmentCompletion{
		Kind:                 op.Delta.Kind,
		MutationCount:        uint32(len(op.Effects)),
		DescriptorDigest:     op.DescriptorDigest,
		PredicateProofDigest: compile.AdmissionProofSetDigest(op.PredicateProofs, op.GuardProofs),
		ExecutionPlanDigest:  compile.ExecutionPlanDigest(op.Segment, op.Atomicity, op.Durability),
	}

	require.True(t, completionMatchesOperation(completion, op))
	completion.PredicateProofDigest[0] ^= 0xff
	require.False(t, completionMatchesOperation(completion, op))
}
