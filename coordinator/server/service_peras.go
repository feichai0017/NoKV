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

func (s *Service) ApplyPerasAuthority(ctx context.Context, req *coordpb.ApplyPerasAuthorityRequest) (*coordpb.ApplyPerasAuthorityResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusContext(err)
	}
	if s == nil || s.storage == nil {
		return nil, statusProtocol("root storage is not configured", reasonRootStorageUnavailable)
	}
	if err := s.requireRootWriteAccess(); err != nil {
		return nil, err
	}
	cmd := metawire.RootPerasAuthorityCommandFromProto(req.GetCommand())
	if !validCoordinatorPerasAuthorityAct(cmd.Kind) {
		return nil, statusInvalidArgument("peras authority command is invalid")
	}

	state, grant, err := s.storage.ApplyPerasAuthority(ctx, cmd)
	status := perasAuthorityStatusFromCommand(cmd)
	if err != nil {
		if errors.Is(err, rootstate.ErrPrimacy) {
			status = metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_HELD
		} else {
			return nil, translatePerasAuthorityError(err)
		}
	}
	if _, reloadErr := s.reloadRootedView(false); reloadErr != nil && err == nil {
		return nil, statusInternalf("reload rooted view after peras authority apply: %v", reloadErr)
	}
	return &coordpb.ApplyPerasAuthorityResponse{
		Grant:        metawire.RootPerasAuthorityGrantToProto(grant),
		Status:       status,
		ActiveGrants: metawire.RootPerasAuthorityGrantsToProto(state.ActivePerasGrants),
	}, nil
}

func validCoordinatorPerasAuthorityAct(kind rootproto.PerasAuthorityAct) bool {
	return kind == rootproto.PerasAuthorityActAcquire || kind == rootproto.PerasAuthorityActRetire
}

func perasAuthorityStatusFromCommand(cmd rootproto.PerasAuthorityCommand) metapb.RootPerasAuthorityApplyStatus {
	if cmd.Kind == rootproto.PerasAuthorityActRetire {
		return metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_RETIRED
	}
	return metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_GRANTED
}

func translatePerasAuthorityError(err error) error {
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
	return statusInternalf("apply peras authority: %v", err)
}
