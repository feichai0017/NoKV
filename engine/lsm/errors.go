package lsm

import "errors"

var (
	ErrFillTables             = errors.New("lsm: fill tables")
	ErrMemtableNotInitialized = errors.New("lsm: memtable not initialized")
	ErrFlushNilMemtable       = errors.New("lsm: flush nil memtable")
	ErrLSMNilOptions          = errors.New("lsm: nil options")
	ErrLSMNilWALManager       = errors.New("lsm: nil wal manager")
	ErrLSMNilClonedOptions    = errors.New("lsm: nil cloned options")
	ErrLSMNil                 = errors.New("lsm: nil lsm")
	ErrLSMClosed              = errors.New("lsm: closed")
)
