package client

import (
	"errors"
	"strconv"

	nokverrors "github.com/feichai0017/NoKV/errors"
)

var (
	// errEmptyAddress indicates that no coordinator endpoint was provided.
	errEmptyAddress = nokverrors.New(nokverrors.KindInvalidArgument, "coordinator client: empty address")
	// errNoReachableAddress indicates that the client could not reach any configured endpoint.
	errNoReachableAddress = nokverrors.New(nokverrors.KindUnavailable, "coordinator client: no reachable address")
	// errConnectionShutdown indicates that the underlying gRPC connection shut down before becoming ready.
	errConnectionShutdown = nokverrors.New(nokverrors.KindUnavailable, "coordinator client: grpc connection shutdown")
	// errStaleWitnessEra indicates that a reply was self-consistent but
	// carried a era older than one already accepted by this client.
	errStaleWitnessEra = nokverrors.New(nokverrors.KindStaleEpoch, "coordinator client: stale witness era")
	// errInvalidWitness indicates that a reply carried malformed monotone-duty
	// witness fields and cannot be admitted as a legal continuation reply.
	errInvalidWitness = nokverrors.New(nokverrors.KindProtocolViolation, "coordinator client: invalid witness")
)

const (
	coordinatorReasonMetadata = "coordinator_reason"
	leaderIDMetadata          = "leader_id"
	reasonNotLeader           = "not_leader"
	reasonGrantNotHeld        = "grant_not_held"
)

// IsEmptyAddress reports whether err represents an empty coordinator address set.
func IsEmptyAddress(err error) bool {
	return errors.Is(err, errEmptyAddress)
}

// IsNoReachableAddress reports whether err represents that no configured coordinator endpoint was reachable.
func IsNoReachableAddress(err error) bool {
	return errors.Is(err, errNoReachableAddress)
}

// IsConnectionShutdown reports whether err represents a gRPC connection entering shutdown during client dial.
func IsConnectionShutdown(err error) bool {
	return errors.Is(err, errConnectionShutdown)
}

// IsStaleWitnessEra reports whether err represents a stale reply whose
// witness era regressed behind one already accepted by this client.
func IsStaleWitnessEra(err error) bool {
	return errors.Is(err, errStaleWitnessEra)
}

// IsInvalidWitness reports whether err represents malformed reply witness
// metadata that failed local client verification.
func IsInvalidWitness(err error) bool {
	return errors.Is(err, errInvalidWitness)
}

// IsNotLeader reports whether err is a coordinator not-leader write rejection.
func IsNotLeader(err error) bool {
	return nokverrors.KindOf(err) == nokverrors.KindNotLeader && coordinatorReason(err) == reasonNotLeader
}

// IsGrantNotHeld reports whether err is a coordinator rejecting a
// grant-gated write because it is not the current grant holder.
// Treated as retryable: another endpoint in the client's pool may hold the
// grant.
func IsGrantNotHeld(err error) bool {
	return nokverrors.KindOf(err) == nokverrors.KindNotLeader && coordinatorReason(err) == reasonGrantNotHeld
}

// LeaderHint extracts the stable leader_id metadata from not-leader coordinator
// errors when present. The diagnostic error string is intentionally ignored.
func LeaderHint(err error) (uint64, bool) {
	if !IsNotLeader(err) {
		return 0, false
	}
	_, metadata, ok := nokverrors.RPCErrorInfo(err)
	if !ok {
		return 0, false
	}
	raw := metadata[leaderIDMetadata]
	if raw == "" {
		return 0, false
	}
	id, parseErr := strconv.ParseUint(raw, 10, 64)
	if parseErr != nil || id == 0 {
		return 0, false
	}
	return id, true
}

func coordinatorReason(err error) string {
	_, metadata, ok := nokverrors.RPCErrorInfo(err)
	if !ok {
		return ""
	}
	return metadata[coordinatorReasonMetadata]
}
