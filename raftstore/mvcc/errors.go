package mvcc

import (
	"errors"
	"fmt"
)

var (
	errNilMVCCStore            = errors.New("raftstore/mvcc: nil MVCC store")
	errNilMaintenanceProposer  = errors.New("raftstore/mvcc: nil maintenance proposer")
	errNilLockResolver         = errors.New("raftstore/mvcc: nil lock resolver")
	errNilCheckTxnStatusResult = errors.New("raftstore/mvcc: nil check txn status response")
	errStop                    = errors.New("raftstore/mvcc: stop batch")
)

func errDecodeCFLock(key []byte, err error) error {
	return fmt.Errorf("raftstore/mvcc: decode CFLock %x: %w", key, err)
}

func errCheckTxnStatusKeyError(primary []byte, err any) error {
	return fmt.Errorf("raftstore/mvcc: check txn status key error for primary %x: %v", primary, err)
}
