package state

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrCoordinatorLeaseHeld    = errors.New("meta/root/state: coordinator lease held")
	ErrInvalidCoordinatorLease = errors.New("meta/root/state: invalid coordinator lease")
	ErrCoordinatorLeaseOwner   = errors.New("meta/root/state: coordinator lease owner mismatch")
)

// ValidateCoordinatorLeaseCampaign verifies whether holder can install a new
// coordinator lease over current at nowUnixNano.
func ValidateCoordinatorLeaseCampaign(current CoordinatorLease, holderID string, expiresUnixNano, nowUnixNano int64) error {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return fmt.Errorf("%w: holder id is required", ErrInvalidCoordinatorLease)
	}
	if expiresUnixNano <= nowUnixNano {
		return fmt.Errorf("%w: expiry must be in the future", ErrInvalidCoordinatorLease)
	}
	if current.ActiveAt(nowUnixNano) && current.HolderID != holderID {
		return fmt.Errorf("%w: holder=%s expires_unix_nano=%d", ErrCoordinatorLeaseHeld, current.HolderID, current.ExpiresUnixNano)
	}
	return nil
}

// ValidateCoordinatorLeaseRelease verifies whether holder can explicitly release
// the current coordinator lease at nowUnixNano.
func ValidateCoordinatorLeaseRelease(current CoordinatorLease, holderID string, nowUnixNano int64) error {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return fmt.Errorf("%w: holder id is required", ErrInvalidCoordinatorLease)
	}
	if strings.TrimSpace(current.HolderID) == "" {
		return fmt.Errorf("%w: no current holder", ErrCoordinatorLeaseOwner)
	}
	if current.HolderID != holderID {
		return fmt.Errorf("%w: current=%s requested=%s", ErrCoordinatorLeaseOwner, current.HolderID, holderID)
	}
	if current.ExpiresUnixNano <= nowUnixNano {
		return nil
	}
	return nil
}
