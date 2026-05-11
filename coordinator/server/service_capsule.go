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

func (s *Service) ApplyCapsuleAuthority(ctx context.Context, req *coordpb.ApplyCapsuleAuthorityRequest) (*coordpb.ApplyCapsuleAuthorityResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusContext(err)
	}
	if s == nil || s.storage == nil {
		return nil, statusProtocol("root storage is not configured", reasonRootStorageUnavailable)
	}
	if err := s.requireRootWriteAccess(); err != nil {
		return nil, err
	}
	cmd := metawire.RootCapsuleAuthorityCommandFromProto(req.GetCommand())
	if !validCoordinatorCapsuleAuthorityAct(cmd.Kind) {
		return nil, statusInvalidArgument("capsule authority command is invalid")
	}

	state, grant, err := s.storage.ApplyCapsuleAuthority(ctx, cmd)
	status := capsuleAuthorityStatusFromCommand(cmd)
	if err != nil {
		if errors.Is(err, rootstate.ErrPrimacy) {
			status = metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_HELD
		} else {
			return nil, translateCapsuleAuthorityError(err)
		}
	}
	if _, reloadErr := s.reloadRootedView(false); reloadErr != nil && err == nil {
		return nil, statusInternalf("reload rooted view after capsule authority apply: %v", reloadErr)
	}
	return &coordpb.ApplyCapsuleAuthorityResponse{
		Grant:        metawire.RootCapsuleAuthorityGrantToProto(grant),
		Status:       status,
		ActiveGrants: metawire.RootCapsuleAuthorityGrantsToProto(state.ActiveCapsuleGrants),
	}, nil
}

func validCoordinatorCapsuleAuthorityAct(kind rootproto.CapsuleAuthorityAct) bool {
	return kind == rootproto.CapsuleAuthorityActAcquire || kind == rootproto.CapsuleAuthorityActRetire
}

func capsuleAuthorityStatusFromCommand(cmd rootproto.CapsuleAuthorityCommand) metapb.RootCapsuleAuthorityApplyStatus {
	if cmd.Kind == rootproto.CapsuleAuthorityActRetire {
		return metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_RETIRED
	}
	return metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_GRANTED
}

func translateCapsuleAuthorityError(err error) error {
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
	return statusInternalf("apply capsule authority: %v", err)
}
