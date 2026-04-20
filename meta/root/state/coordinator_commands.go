package state

import rootproto "github.com/feichai0017/NoKV/meta/root/protocol"

type CoordinatorLeaseCommandKind = rootproto.CoordinatorLeaseCommandKind

const (
	CoordinatorLeaseCommandUnknown = rootproto.CoordinatorLeaseCommandUnknown
	CoordinatorLeaseCommandIssue   = rootproto.CoordinatorLeaseCommandIssue
	CoordinatorLeaseCommandRelease = rootproto.CoordinatorLeaseCommandRelease
)

type CoordinatorLeaseCommand = rootproto.CoordinatorLeaseCommand

type CoordinatorClosureCommandKind = rootproto.CoordinatorClosureCommandKind

const (
	CoordinatorClosureCommandUnknown  = rootproto.CoordinatorClosureCommandUnknown
	CoordinatorClosureCommandSeal     = rootproto.CoordinatorClosureCommandSeal
	CoordinatorClosureCommandConfirm  = rootproto.CoordinatorClosureCommandConfirm
	CoordinatorClosureCommandClose    = rootproto.CoordinatorClosureCommandClose
	CoordinatorClosureCommandReattach = rootproto.CoordinatorClosureCommandReattach
)

type CoordinatorClosureCommand = rootproto.CoordinatorClosureCommand

type CoordinatorProtocolState struct {
	Lease   CoordinatorLease
	Seal    CoordinatorSeal
	Closure CoordinatorClosure
}

func (s State) CoordinatorProtocol() CoordinatorProtocolState {
	return CoordinatorProtocolState{
		Lease:   s.CoordinatorLease,
		Seal:    s.CoordinatorSeal,
		Closure: s.CoordinatorClosure,
	}
}
