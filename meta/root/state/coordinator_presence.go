package state

import "strings"

func (l CoordinatorLease) Empty() bool {
	return strings.TrimSpace(l.HolderID) == "" &&
		l.ExpiresUnixNano == 0 &&
		l.CertGeneration == 0 &&
		l.IssuedCursor == (Cursor{}) &&
		l.DutyMask == 0 &&
		strings.TrimSpace(l.PredecessorDigest) == ""
}

func (s CoordinatorSeal) Present() bool {
	return s.CertGeneration != 0 && strings.TrimSpace(s.HolderID) != ""
}

func (c CoordinatorClosure) Empty() bool {
	return strings.TrimSpace(c.HolderID) == "" &&
		c.SealGeneration == 0 &&
		c.SuccessorGeneration == 0 &&
		strings.TrimSpace(c.SealDigest) == "" &&
		c.Stage == CoordinatorClosureStagePendingConfirm &&
		c.ConfirmedAtCursor == (Cursor{}) &&
		c.ClosedAtCursor == (Cursor{}) &&
		c.ReattachedAtCursor == (Cursor{})
}
