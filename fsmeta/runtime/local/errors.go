// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
)

var (
	errWorkDirRequired           = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: work dir is required")
	errDBRequired                = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: pebble DB is required")
	errMountRequired             = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: mount identity is required")
	errTimestampCount            = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: timestamp count must be > 0")
	errCommitVersion             = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: commit version must be greater than start version")
	errEmptyMutationKey          = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: empty mutation key")
	errUnsupportedMutation       = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: unsupported mutation op")
	errInvalidInternalEntry      = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/local: invalid local MVCC entry")
	errInvalidMetadataCommand    = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: invalid metadata command")
	errInvalidMetadataPredicate  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/local: invalid metadata predicate")
	errMetadataPredicateMismatch = nokverrors.New(nokverrors.KindRetryable, "fsmeta/runtime/local: metadata predicate mismatch")
	errKeyNotFound               = nokverrors.New(nokverrors.KindNotFound, "fsmeta/runtime/local: key not found")
)

func metadataKeyError(issues ...nokverrors.MetadataKeyIssue) error {
	return nokverrors.NewMetadataKeyError(issues...)
}

func metadataAbort(err error) error {
	if err == nil {
		return nil
	}
	return metadataKeyError(nokverrors.MetadataKeyIssue{Kind: nokverrors.KindAborted, Message: err.Error()})
}

func metadataRetryable(err error) error {
	if err == nil {
		return nil
	}
	return metadataKeyError(nokverrors.MetadataKeyIssue{Kind: nokverrors.KindRetryable, Message: err.Error()})
}

func metadataAlreadyExists(key []byte) error {
	return metadataKeyError(nokverrors.MetadataKeyIssue{Kind: nokverrors.KindAlreadyExists, Key: cloneBytes(key)})
}

func metadataCommitExpired(key []byte, commitVersion, minCommitVersion uint64) error {
	return metadataKeyError(nokverrors.MetadataKeyIssue{
		Kind:             nokverrors.KindCommitTsExpired,
		Key:              cloneBytes(key),
		CommitVersion:    commitVersion,
		MinCommitVersion: minCommitVersion,
	})
}

func metadataUnsupportedMutation(op backend.MutationOp) error {
	return metadataAbort(fmt.Errorf("%w: %d", errUnsupportedMutation, op))
}
