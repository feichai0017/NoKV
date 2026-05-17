// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"

	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
)

func (s *Service) ApplyVisibleAuthority(ctx context.Context, req *coordpb.ApplyVisibleAuthorityRequest) (*coordpb.ApplyVisibleAuthorityResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusContext(err)
	}
	if s == nil || s.storage == nil {
		return nil, statusProtocol("root storage is not configured", reasonRootStorageUnavailable)
	}
	if err := s.requireRootWriteAccess(); err != nil {
		return nil, err
	}
	cmd := metawire.RootVisibleAuthorityCommandFromProto(req.GetCommand())
	if !validCoordinatorVisibleAuthorityAct(cmd.Kind) {
		return nil, statusInvalidArgument("visible authority command is invalid")
	}

	state, grant, err := s.storage.ApplyVisibleAuthority(ctx, cmd)
	status := visibleAuthorityStatusFromCommand(cmd)
	if err != nil {
		if errors.Is(err, rootstate.ErrPrimacy) {
			status = metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_HELD
		} else {
			return nil, translateVisibleAuthorityError(err)
		}
	}
	if _, reloadErr := s.reloadRootedView(false); reloadErr != nil && err == nil {
		return nil, statusInternalf("reload rooted view after visible authority apply: %v", reloadErr)
	}
	return &coordpb.ApplyVisibleAuthorityResponse{
		Grant:        metawire.RootVisibleAuthorityGrantToProto(grant),
		Status:       status,
		ActiveGrants: metawire.RootVisibleAuthorityGrantsToProto(state.ActiveVisibleGrants),
	}, nil
}

func validCoordinatorVisibleAuthorityAct(kind rootproto.VisibleAuthorityAct) bool {
	return kind == rootproto.VisibleAuthorityActAcquire ||
		kind == rootproto.VisibleAuthorityActRetire ||
		kind == rootproto.VisibleAuthorityActSeal
}

func visibleAuthorityStatusFromCommand(cmd rootproto.VisibleAuthorityCommand) metapb.RootVisibleAuthorityApplyStatus {
	switch cmd.Kind {
	case rootproto.VisibleAuthorityActRetire:
		return metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_RETIRED
	case rootproto.VisibleAuthorityActSeal:
		return metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_SEALED
	default:
		return metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_GRANTED
	}
}

func translateVisibleAuthorityError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return statusContext(err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return statusContext(err)
	}
	if errors.Is(err, rootstate.ErrInvalidGrant) {
		return statusInvalidArgument(err.Error())
	}
	return statusInternalf("apply visible authority: %v", err)
}
