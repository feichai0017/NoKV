package client

import (
	"errors"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// errEmptyAddress indicates that no coordinator endpoint was provided.
	errEmptyAddress = errors.New("coordinator client: empty address")
	// errNoReachableAddress indicates that the client could not reach any configured endpoint.
	errNoReachableAddress = errors.New("coordinator client: no reachable address")
	// errConnectionShutdown indicates that the underlying gRPC connection shut down before becoming ready.
	errConnectionShutdown = errors.New("coordinator client: grpc connection shutdown")
	// errStaleWitnessGeneration indicates that a reply was self-consistent but
	// carried a generation older than one already accepted by this client.
	errStaleWitnessGeneration = errors.New("coordinator client: stale witness generation")
	// errInvalidWitness indicates that a reply carried malformed monotone-duty
	// witness fields and cannot be admitted as a legal continuation reply.
	errInvalidWitness = errors.New("coordinator client: invalid witness")
)

const errNotLeaderPrefix = "coordinator not leader"
const errLeaseNotHeldPrefix = "coordinator lease not held"

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

// IsStaleWitnessGeneration reports whether err represents a stale reply whose
// witness generation regressed behind one already accepted by this client.
func IsStaleWitnessGeneration(err error) bool {
	return errors.Is(err, errStaleWitnessGeneration)
}

// IsInvalidWitness reports whether err represents malformed reply witness
// metadata that failed local client verification.
func IsInvalidWitness(err error) bool {
	return errors.Is(err, errInvalidWitness)
}

// IsNotLeader reports whether err is a coordinator not-leader write rejection.
func IsNotLeader(err error) bool {
	return status.Code(err) == codes.FailedPrecondition && strings.Contains(err.Error(), errNotLeaderPrefix)
}

// IsLeaseNotHeld reports whether err is a coordinator rejecting a
// lease-gated write because it is not the current Tenure holder.
// Treated as retryable: another endpoint in the client's pool may hold the
// lease.
func IsLeaseNotHeld(err error) bool {
	return status.Code(err) == codes.FailedPrecondition && strings.Contains(err.Error(), errLeaseNotHeldPrefix)
}

// LeaderHint extracts leader_id=N from not-leader coordinator errors when present.
func LeaderHint(err error) (uint64, bool) {
	if !IsNotLeader(err) {
		return 0, false
	}
	msg := err.Error()
	_, after, ok := strings.Cut(msg, "leader_id=")
	if !ok {
		return 0, false
	}
	raw := after
	end := len(raw)
	for i, r := range raw {
		if r < '0' || r > '9' {
			end = i
			break
		}
	}
	if end == 0 {
		return 0, false
	}
	id, parseErr := strconv.ParseUint(raw[:end], 10, 64)
	if parseErr != nil || id == 0 {
		return 0, false
	}
	return id, true
}
