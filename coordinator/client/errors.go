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
)

const errNotLeaderPrefix = "coordinator not leader"

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

// IsNotLeader reports whether err is a coordinator not-leader write rejection.
func IsNotLeader(err error) bool {
	return status.Code(err) == codes.FailedPrecondition && strings.Contains(err.Error(), errNotLeaderPrefix)
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
