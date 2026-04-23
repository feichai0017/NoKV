package server

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	errNotLeaderPrefix                 = "coordinator not leader"
	errRootUnavailable                 = "coordinator root unavailable"
	errTenurePrefix                    = "coordinator lease not held"
	errRootLagExceedsStrongFreshness   = "root lag exceeds strong freshness"
	errBootstrapRequiredBeforeBounded  = "bootstrap required before bounded freshness"
	errRequiredRootedTokenNotSatisfied = "required rooted token not satisfied"
	errRequiredDescriptorNotSatisfied  = "required descriptor revision not satisfied"
	errRootLagExceedsBound             = "root lag exceeds bound"
	errRangeChangePending              = "rooted range change still pending"
)

func statusNotLeader(leaderID uint64) error {
	if leaderID == 0 {
		return status.Error(codes.FailedPrecondition, errNotLeaderPrefix)
	}
	return status.Error(codes.FailedPrecondition, fmt.Sprintf("%s (leader_id=%d)", errNotLeaderPrefix, leaderID))
}

func statusTenure(err error) error {
	return status.Error(codes.FailedPrecondition, fmt.Sprintf("%s: %v", errTenurePrefix, err))
}
